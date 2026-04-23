from mcp.server.fastmcp import FastMCP
import paho.mqtt.client as mqtt
import influxdb_client
import json
import time
import sys
import uuid

mcp = FastMCP("TrafficOptimizerMCP")
 
# --- CONFIGURAZIONE INFLUXDB ---
INFLUX_URL = "http://semafori-tsdb:8086"
INFLUX_TOKEN = "supersecrettoken123"
INFLUX_ORG = "srs_org"
INFLUX_BUCKET = "traffic_data"
 
# --- CONFIGURAZIONE MQTT (Fire and Forget) ---
# 1. ID univoco per evitare conflitti tra container o worker
MIO_CLIENT_ID = f"MCP_OPT_{uuid.uuid4().hex[:6]}"

def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        print(f"[{MIO_CLIENT_ID}] Connesso al broker MQTT!", file=sys.stderr, flush=True)
 
def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    print(f"[{MIO_CLIENT_ID}] Disconnesso dal broker MQTT. Riconnessione automatica in corso...", file=sys.stderr, flush=True)

# 2. Creazione client globale
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MIO_CLIENT_ID)
mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect
 
def avvia_connessione_mqtt():
    brokers = ["srs-haproxy-1", "srs-haproxy-2"]
    for broker in brokers:
        try:
            print(f"[{MIO_CLIENT_ID}] Tento connessione a {broker}...", file=sys.stderr)
            # connect_async non blocca il codice. Il loop_start() gestirà tutto.
            mqtt_client.connect_async(broker, 1883, keepalive=60)
            mqtt_client.loop_start() 
            # Usciamo subito, se fallisce ci pensa la libreria a riprovare in background
            return 
        except Exception as e:
            print(f"[{MIO_CLIENT_ID}] Errore avvio verso {broker}: {e}", file=sys.stderr)

# 3. Avviamo il motore MQTT una volta sola all'avvio dello script
avvia_connessione_mqtt()
 
# --- TOOLS MCP ---

@mcp.tool()
def get_traffic_status() -> str:
    """
    Legge da InfluxDB lo stato attuale di TUTTI gli incroci della città.
    Restituisce un report generale e segnala gli assi che superano la 'soglia_allarme' di auto in coda.
    Da usare come primo step diagnostico per decidere dove ottimizzare il traffico.
    """
    soglia_allarme = 15
    try:
        client = influxdb_client.InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        query_api = client.query_api()
 
        query = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -45s)
          |> filter(fn: (r) => r["_measurement"] == "stato_traffico")
          |> filter(fn: (r) => r["_field"] == "auto_in_coda" or r["_field"] == "durata_verde")
          |> last()
          |> pivot(rowKey:["incrocio", "direzione"], columnKey: ["_field"], valueColumn: "_value")
        """
        result = query_api.query(org=INFLUX_ORG, query=query)
 
        if not result:
            return "Nessun dato recente trovato nel database. Assicurati che la simulazione sia in START e che i dati arrivino."
 
        mappa_incroci = {}
        for table in result:
            for record in table.records:
                incrocio = record.values.get("incrocio")
                dir_id = record.values.get("direzione")
                coda = int(record.values.get("auto_in_coda", 0))
                verde = int(record.values.get("durata_verde", 0))
                
                if incrocio not in mappa_incroci:
                    mappa_incroci[incrocio] = []
                    
                mappa_incroci[incrocio].append({
                    "direzione": dir_id,
                    "coda": coda,
                    "verde": verde
                })
 
        report = f"--- REPORT TRAFFICO GLOBALE (Soglia allarme: {soglia_allarme} auto) ---\n\n"
        allarmi_trovati = 0
        
        for incrocio, direzioni in mappa_incroci.items():
            report += f"INCROCIO: {incrocio}\n"
            for d in direzioni:
                alert_tag = ""
                if d['coda'] >= soglia_allarme:
                    alert_tag = "[ALLARME CONGESTIONE]"
                    allarmi_trovati += 1
                    
                report += f"   - Asse {d['direzione']}: {d['coda']} auto in coda (Verde attuale: {d['verde']}s){alert_tag}\n"
            report += "\n"
 
        if allarmi_trovati > 0:
            report += "SUGGERIMENTO PER L'IA: Hai trovato delle congestioni critiche! Usa subito il tool 'optimize_green_phase' per aumentare il verde (es. 30 secondi) sugli assi contrassegnati con [ALLARME CONGESTIONE]."
        else:
            report += "SUGGERIMENTO PER L'IA: La situazione è sotto controllo. Nessun asse supera la soglia di allarme. Non è necessario intervenire al momento."
            
        return report
 
    except Exception as e:
        return f"ERRORE TELEMETRIA InfluxDB: {str(e)}"
 
@mcp.tool()
def optimize_green_phase(incrocio_id: str, asse: str, new_duration: int, override_cycles: int ) -> str:
    """
    Modifica la durata del semaforo verde per alleviare la congestione.
    Imposta un verde prolungato sull'asse congestionato senza penalizzare l'asse opposto.
 
    Parametri:
    - incrocio_id: (es. 'INC_0_0')
    - asse: 'NORD_SUD' oppure 'EST_OVEST'
    - new_duration: Secondi di verde per l'asse congestionato (Max: 60)
    - override_cycles: Numero di cicli semaforici per mantenere la modifica. 
      REGOLE DI DECISIONE: 
      - Coda lieve (15-20 auto): imposta 1 o 2 cicli.
      - Coda moderata (20-30 auto): imposta 3 cicli.
      - Coda critica (>30 auto): imposta massimo 4 cicli.
    """
    # VIA I CONTROLLI MANUALI MQTT! Se è disconnesso, Paho MQTT metterà in coda il messaggio.
 
    if new_duration > 60:
        return "OPERAZIONE RIFIUTATA: La durata massima consentita per motivi di sicurezza è 60 secondi."
 
    if asse not in ["NORD_SUD", "EST_OVEST"]:
        return "OPERAZIONE RIFIUTATA: Il parametro 'asse' deve essere esattamente 'NORD_SUD' o 'EST_OVEST'."
 
    try:
        assi_da_modificare = ["NORD", "SUD"] if asse == "NORD_SUD" else ["EST", "OVEST"]
 
        payload = json.dumps({
            "green_duration": new_duration,
            "override_cycles": override_cycles
        })
 
        for d in assi_da_modificare:
            topic = f"srs/edge/{incrocio_id}_{d}/config"
            # Invia il comando. Se la rete cade un attimo, il client lo trattiene in RAM e lo spara appena torna online.
            mqtt_client.publish(topic, payload, qos=1)
 
        print(f"[MCP_OPTIMIZER] Intervento su {incrocio_id}: Asse {asse} portato a {new_duration}s per {override_cycles} cicli.", file=sys.stderr)
 
        return (f"SUCCESSO: All'incrocio {incrocio_id}, l'asse {asse} avrà un verde allungato a {new_duration}s "
                f"per i prossimi {override_cycles} cicli per smaltire la coda. L'asse opposto manterrà la sua configurazione standard.")
    
    except Exception as e:
        return f"ERRORE MQTT: {str(e)}"

@mcp.tool()
def verify_green_duration(incrocio_id: str) -> str:
    """
    Verifica la durata attuale del verde leggendo i dati reali da InfluxDB.
    Da usare per confermare che un comando di ottimizzazione sia stato applicato.
    """
    try:
        client = influxdb_client.InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        query_api = client.query_api()
 
        query = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -1m)
          |> filter(fn: (r) => r["_measurement"] == "stato_traffico")
          |> filter(fn: (r) => r["incrocio"] == "{incrocio_id}")
          |> filter(fn: (r) => r["_field"] == "durata_verde")
          |> last()
          |> pivot(rowKey:["direzione"], columnKey: ["_field"], valueColumn: "_value")
        """
        result = query_api.query(org=INFLUX_ORG, query=query)
 
        if not result:
            return f"Nessun dato trovato per l'incrocio {incrocio_id} negli ultimi 2 minuti."
 
        report = f"VERIFICA TELEMETRIA: {incrocio_id}\n\n"
        for table in result:
            for record in table.records:
                dir_id = record.values.get("direzione")
                verde = record.values.get("durata_verde", "N/A")
                report += f"- Asse {dir_id}: Verde = {verde}s\n"
 
        return report
 
    except Exception as e:
        return f"ERRORE ACCESSO DATABASE: {str(e)}"
    
@mcp.tool()
def check_hardware_lock(incrocio_id: str) -> str:
    """
    CONTROLLO DI CONCORRENZA (Distributed Lock):
    Verifica se l'incrocio sta subendo un riavvio hardware o se ha già un'ottimizzazione del traffico in corso.
    Da usare OBBLIGATORIAMENTE prima di ogni ottimizzazione del traffico.
    """
    try:
        client = influxdb_client.InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        query_api = client.query_api()
 
        # 1. CONTROLLO HARDWARE: Cerca eventi di RESTART negli ultimi 5 minuti
        query_hw = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -90s)
          |> filter(fn: (r) => r["_measurement"] == "agent_audit")
          |> filter(fn: (r) => r["node_id"] =~ /{incrocio_id}/)
          |> filter(fn: (r) => r["action"] == "RESTART")
          |> last()
        """
        result_hw = query_api.query(org=INFLUX_ORG, query=query_hw)

        # 2. CONTROLLO TRAFFICO (Anti Ping-Pong): Verifica se c'è già un verde prolungato
        query_traffic = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -2m)
          |> filter(fn: (r) => r["_measurement"] == "stato_traffico")
          |> filter(fn: (r) => r["incrocio"] == "{incrocio_id}")
          |> filter(fn: (r) => r["_field"] == "durata_verde")
          |> last()
        """
        result_traffic = query_api.query(org=INFLUX_ORG, query=query_traffic)
        client.close()
 
        # Valutazione 1: Lucchetto Hardware
        if result_hw and len(result_hw) > 0:
            return f"LUCCHETTO ATTIVO 🔒 (HARDWARE): L'incrocio {incrocio_id} ha subito un riavvio critico negli ultimi 5 minuti. NON intervenire per evitare conflitti."
            
        # Valutazione 2: Lucchetto Traffico (Cooldown)
        if result_traffic:
            for table in result_traffic:
                for record in table.records:
                    # Se la durata del verde è 30s o superiore, la policy è già attiva
                    if int(record.get_value()) >= 30: 
                        return f"LUCCHETTO ATTIVO 🔒 (TRAFFICO): L'incrocio {incrocio_id} ha già una policy di ottimizzazione in corso (Verde = {record.get_value()}s). Attendere la fine dei cicli di override. NON intervenire."

        # Se passa entrambi i controlli, via libera
        return f"LUCCHETTO SBLOCCATO 🔓: Nessun intervento hardware o policy traffico in corso su {incrocio_id}. Ottimizzazione consentita."
            
    except Exception as e:
        # Fail-Safe: in caso di errore di lettura, neghiamo l'azione per sicurezza
        return f"ERRORE LETTURA LOCK: {str(e)}. Per policy di sicurezza, NON procedere."
 
if __name__ == "__main__":
    mcp.run()
