import sys
import logging
import uuid
 
# === FIX BUG #5 ===
# force=True rimuove tutti gli handler già configurati dalla libreria mcp
# durante il suo import, e li sostituisce con uno su stderr.
# DEVE stare PRIMA di qualsiasi import di mcp.
logging.basicConfig(
    level=logging.WARNING,
    stream=sys.stderr,
    force=True,
    format="%(name)s - %(levelname)s - %(message)s"
)
 
from mcp.server.fastmcp import FastMCP
import paho.mqtt.client as mqtt
import influxdb_client
import json
import time
 
mcp = FastMCP("TrafficOptimizerMCP")
 
# --- CONFIGURAZIONE INFLUXDB ---
INFLUX_URL = "http://semafori-tsdb:8086"
INFLUX_TOKEN = "supersecrettoken123"
INFLUX_ORG = "srs_org"
INFLUX_BUCKET = "traffic_data"
 
is_mqtt_connected = False
 
def on_connect(client, userdata, flags, rc, properties):
    global is_mqtt_connected
    if rc == 0:
        is_mqtt_connected = True
        print("[MCP_OPTIMIZER] Connesso al broker MQTT!", file=sys.stderr, flush=True)
 
def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    global is_mqtt_connected
    # === FIX BUG #3 ===
    # Il flag DEVE essere resettato qui, altrimenti optimize_green_phase
    # pensa di essere connessa e non tenta la riconnessione.
    is_mqtt_connected = False
    if reason_code == 0:
        print("[MCP_OPTIMIZER] Disconnesso volontariamente.", file=sys.stderr, flush=True)
    else:
        print(f"[MCP_OPTIMIZER] Disconnesso inaspettatamente! reason_code={reason_code}", file=sys.stderr, flush=True)
 
# === FIX BUG #4 ===
# Il client_id univoco (UUID) evita che EMQX espella una sessione precedente
# ancora attiva quando viene avviato un nuovo container.
_CLIENT_ID = f"MCP_OPTIMIZER_{uuid.uuid4().hex[:8]}"
print(f"[MCP_OPTIMIZER] Avvio con client_id: {_CLIENT_ID}", file=sys.stderr, flush=True)
 
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=_CLIENT_ID)
mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect
 
def connetti_mqtt_agente():
    brokers = ["srs-haproxy-1", "srs-haproxy-2"]
 
    # === FIX BUG #2 ===
    # loop_start() avvia UN SOLO thread di rete per il client.
    # Deve essere chiamato UNA SOLA VOLTA, PRIMA del ciclo sui broker.
    # Chiamarlo dentro il for avviava un nuovo thread ad ogni iterazione
    # e la seconda chiamata a connect() disconnetteva implicitamente la prima.
    mqtt_client.loop_start()
 
    for broker in brokers:
        try:
            print(f"[MCP_OPTIMIZER] Tentativo di connessione a {broker}...", file=sys.stderr)
            mqtt_client.connect(broker, 1883, keepalive=60)
            # Diamo un po' più di tempo alla callback on_connect per scattare
            time.sleep(1.5)
            if is_mqtt_connected:
                print(f"[MCP_OPTIMIZER] Connesso con successo a {broker}.", file=sys.stderr, flush=True)
                return True
        except Exception as e:
            print(f"[MCP_OPTIMIZER] Fallita connessione a {broker}: {e}", file=sys.stderr)
 
    print("[MCP_OPTIMIZER] ATTENZIONE: Nessun broker raggiungibile all'avvio.", file=sys.stderr, flush=True)
    return False
 
#connetti_mqtt_agente()
 
 
@mcp.tool()
def get_traffic_status(incrocio_id: str) -> str:
    """
    Legge da InfluxDB lo stato attuale delle code e la durata del verde per un incrocio.
    Da usare per capire se c'è congestione e su quale asse intervenire.
    """
    try:
        client = influxdb_client.InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        query_api = client.query_api()
 
        query = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -2m)
          |> filter(fn: (r) => r["_measurement"] == "stato_traffico")
          |> filter(fn: (r) => r["incrocio"] == "{incrocio_id}")
          |> filter(fn: (r) => r["_field"] == "auto_in_coda" or r["_field"] == "durata_verde")
          |> last()
          |> pivot(rowKey:["direzione"], columnKey: ["_field"], valueColumn: "_value")
        """
        result = query_api.query(org=INFLUX_ORG, query=query)
 
        if not result:
            return f"Nessun dato recente trovato per l'incrocio {incrocio_id}. Assicurati che la simulazione sia START."
 
        report = f"--- STATO TRAFFICO: {incrocio_id} ---\n"
        for table in result:
            for record in table.records:
                dir_id = record.values.get("direzione")
                coda = record.values.get("auto_in_coda", 0)
                verde = record.values.get("durata_verde", 0)
                report += f"- Direzione {dir_id}: {coda} auto in coda (Verde attuale: {verde}s)\n"
 
        report += "\nSUGGERIMENTO PER L'IA: Confronta l'asse NORD/SUD con l'asse EST/OVEST. Se un asse ha una coda molto superiore all'altro, usa 'optimize_green_phase' per aumentare il verde."
        return report
 
    except Exception as e:
        return f"ERRORE TELEMETRIA InfluxDB: {str(e)}"
 
 
@mcp.tool()
def optimize_green_phase(incrocio_id: str, asse: str, new_duration: int, override_cycles: int = 1) -> str:
    """
    Modifica la durata del semaforo verde per alleviare la congestione.
 
    Parametri:
    - incrocio_id: (es. 'INC_0_0')
    - asse: 'NORD_SUD' oppure 'EST_OVEST'
    - new_duration: Secondi di verde (Max raccomandato: 30)
    - override_cycles: Per quanti cicli semaforici mantenere questa modifica (es. 2).
    """
    if not is_mqtt_connected:
        if not connetti_mqtt_agente():
            return "ERRORE: Impossibile raggiungere il broker MQTT."
 
    if new_duration > 60:
        return "OPERAZIONE RIFIUTATA: La durata massima consentita per motivi di sicurezza è 60 secondi."
 
    if asse not in ["NORD_SUD", "EST_OVEST"]:
        return "OPERAZIONE RIFIUTATA: Il parametro 'asse' deve essere esattamente 'NORD_SUD' o 'EST_OVEST'."
 
    try:
        dirs = ["NORD", "SUD"] if asse == "NORD_SUD" else ["EST", "OVEST"]
 
        payload = {
            "green_duration": new_duration,
            "override_cycles": override_cycles
        }
        payload_str = json.dumps(payload)
 
        for d in dirs:
            topic = f"srs/edge/{incrocio_id}_{d}/config"
            mqtt_client.publish(topic, payload_str, qos=1)
 
        print(f"[MCP_OPTIMIZER] Intervento IA su {incrocio_id} Asse {asse}: Verde a {new_duration}s per {override_cycles} cicli.", file=sys.stderr)
 
        return (f"SUCCESSO: L'asse {asse} dell'incrocio {incrocio_id} "
                f"avrà il verde allungato a {new_duration} secondi per i prossimi {override_cycles} cicli. "
                f"Il traffico dovrebbe iniziare a defluire.")
    except Exception as e:
        return f"ERRORE MQTT: {str(e)}"
 
 
if __name__ == "__main__":
    mcp.run()