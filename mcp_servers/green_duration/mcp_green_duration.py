import os
import sys
import json
import time
import uuid
import re
import uvicorn
from starlette.middleware.trustedhost import TrustedHostMiddleware

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from fastmcp import FastMCP
import paho.mqtt.client as mqtt
import influxdb_client
from simulator.utility.influx_utils import query_with_failover, INFLUX_BUCKET, INFLUX_ORG

mcp = FastMCP("TrafficOptimizerMCP")
 
# --- CONFIGURAZIONE MQTT (Resiliente tramite HAProxy) ---
MIO_CLIENT_ID = f"MCP_OPT_{uuid.uuid4().hex[:6]}"

def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        print(f"[{MIO_CLIENT_ID}] Connesso al broker MQTT!", file=sys.stderr, flush=True)
 
def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    print(f"[{MIO_CLIENT_ID}] Disconnesso dal broker MQTT. Riconnessione...", file=sys.stderr, flush=True)

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MIO_CLIENT_ID)
mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect
 
def avvia_connessione_mqtt():
    brokers = ["srs-haproxy-1", "srs-haproxy-2"]
    for broker in brokers:
        try:
            print(f"[{MIO_CLIENT_ID}] Tento connessione a {broker}...", file=sys.stderr)
            mqtt_client.connect_async(broker, 1883, keepalive=60)
            mqtt_client.loop_start() 
            return 
        except Exception as e:
            print(f"[{MIO_CLIENT_ID}] Errore verso {broker}: {e}", file=sys.stderr)

def log_mcp(emoji, message):
    print(f"[{MIO_CLIENT_ID}] {emoji} {message}", file=sys.stderr, flush=True)

avvia_connessione_mqtt()
 
# --- TOOLS MCP ---

@mcp.tool()
def get_traffic_status() -> str:
    """Restituisce solo gli incroci con anomalie o un riepilogo rapido."""
    soglia_allarme = 15
    try:
        query = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -45s)
          |> filter(fn: (r) => r["_measurement"] == "stato_traffico")
          |> filter(fn: (r) => r["_field"] == "auto_in_coda" or r["_field"] == "durata_verde")
          |> last()
          |> pivot(rowKey:["incrocio", "direzione"], columnKey: ["_field"], valueColumn: "_value")
        """
        result = query_with_failover(query)
        if not result: return "STATO: Nessun dato recente (Sensori OFFLINE?)"
 
        allarmi = []
        for table in result:
            for record in table.records:
                coda = int(record.values.get("auto_in_coda", 0))
                if coda >= soglia_allarme:
                    allarmi.append(f"{record.values.get('direzione')}: {coda} auto")

        if not allarmi: return "STATO: Traffico regolare su tutti i nodi."
        
        return "ALLARME CONGESTIONE: " + " | ".join(allarmi)
 
    except Exception as e:
        log_mcp("🔥", f"Errore telemetria: {e}")
        return "ERRORE: Datasource non raggiungibile."
 
@mcp.tool()
def optimize_green_phase(incrocio_id: str, asse: str, new_duration: int, override_cycles: int ) -> str:
    """Invia comando MQTT con validazione incrociata ID/ASSE e LIMITI DINAMICI."""
    
    # 1. AUTO-PULIZIA E VALIDAZIONE (Smart Parsing)
    match = re.match(r"^(INC_\d+_\d+)(?:_([A-Z]+))?$", incrocio_id.upper())
    if not match:
        return "RIFIUTATO: Formato ID non valido. Usa INC_X_Y o INC_X_Y_DIR."
    
    id_radice = match.group(1)
    dir_specifica = match.group(2) 
    asse_up = asse.upper()

    if asse_up not in ["NORD_SUD", "EST_OVEST"]:
        return "RIFIUTATO: Asse illegale. Valori ammessi SOLO 'NORD_SUD' o 'EST_OVEST'."

    # 2. CROSS-CHECK DI SICUREZZA
    if dir_specifica:
        mappa_assi = {
            "NORD": "NORD_SUD", "SUD": "NORD_SUD",
            "EST": "EST_OVEST", "OVEST": "EST_OVEST"
        }
        if mappa_assi.get(dir_specifica) != asse_up:
            return f"RIFIUTATO: Conflitto tra ID ({dir_specifica}) e ASSE ({asse_up})."
        
    # 3. CONTROLLO CICLI MASSIMI
    if override_cycles < 1 or override_cycles > 3:
        return "BLOCCATO: 'override_cycles' deve essere compreso tra 1 e 3."

    # 4. VERIFICA DINAMICA DEL VERDE ATTUALE
    try:
        query = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -2m)
          |> filter(fn: (r) => r["_measurement"] == "stato_traffico")
          |> filter(fn: (r) => r["incrocio"] == "{id_radice}")
          |> filter(fn: (r) => r["_field"] == "durata_verde")
          |> last()
        """
        result = query_with_failover(query)
        
        verde_attuale = 10 
        if result and len(result) > 0 and len(result[0].records) > 0:
            verde_attuale = int(result[0].records[0].get_value())
            
        # 5. APPLICAZIONE DEI LIMITI MATEMATICI
        limite_massimo = min(verde_attuale + 20, 60)
        
        if new_duration > limite_massimo:
            log_mcp("🚫", f"Allucinazione sventata. Verde attuale: {verde_attuale}s. IA ha chiesto: {new_duration}s.")
            return f"BLOCCATO DAL SISTEMA: Richiesta eccessiva. Puoi aumentare il verde massimo fino a {limite_massimo}s."
            
    except Exception as e:
        log_mcp("⚠️", f"Errore lettura DB per validazione. {e}")
        if new_duration > 30: 
            return "BLOCCATO: DB non raggiungibile, impossibile validare aumenti oltre 30s."

    # 6. ESECUZIONE
    try:
        direzioni = ["NORD", "SUD"] if asse_up == "NORD_SUD" else ["EST", "OVEST"]
        payload = json.dumps({"green_duration": new_duration, "override_cycles": override_cycles})
 
        for d in direzioni:
            topic = f"srs/edge/{id_radice}_{d}/config"
            mqtt_client.publish(topic, payload, qos=1)
 
        log_mcp("🟢", f"Ottimizzazione {id_radice} [{asse_up}] -> Da {verde_attuale}s a {new_duration}s per {override_cycles} cicli")
        return f"SUCCESSO: {id_radice} asse {asse_up} aggiornato a {new_duration}s."
    except Exception as e:
        log_mcp("💥", f"Errore MQTT: {e}")
        return "ERRORE: Comando non inviato."

@mcp.tool()
def verify_green_duration(incrocio_id: str) -> str:
    """Verifica l'applicazione della policy."""
    # Auto-pulizia dell'ID
    match = re.match(r"^(INC_\d+_\d+)", incrocio_id.upper())
    if match:
        id_radice = match.group(1)
    else:
        return "ERRORE: Formato ID non valido."

    try:
        query = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -1m)
          |> filter(fn: (r) => r["_measurement"] == "stato_traffico")
          |> filter(fn: (r) => r["incrocio"] == "{id_radice}")
          |> filter(fn: (r) => r["_field"] == "durata_verde")
          |> last()
          |> pivot(rowKey:["direzione"], columnKey: ["_field"], valueColumn: "_value")
        """
        result = query_with_failover(query)
 
        if not result: return f"Nessun dato per {id_radice}."
 
        report = f"VERIFICA TELEMETRIA: {id_radice}\n"
        for table in result:
            for record in table.records:
                report += f"- Asse {record.values.get('direzione')}: Verde = {record.values.get('durata_verde')}s\n"
        return report
    except Exception as e:
        return f"ERRORE ACCESSO DB: {str(e)}"
    
@mcp.tool()
def check_hardware_lock(incrocio_id: str) -> str:
    """Verifica lock di sicurezza. Risponde con OK o LOCKED."""    
    # Auto-pulizia dell'ID
    match = re.match(r"^(INC_\d+_\d+)", incrocio_id.upper())
    if match:
        id_radice = match.group(1)
    else:
        return "LOCKED 🔒: Errore sicurezza formato ID."

    try:
        # Controllo Hardware
        res_hw = query_with_failover(f'from(bucket: "{INFLUX_BUCKET}") |> range(start: -90s) |> filter(fn: (r) => r["_measurement"] == "agent_audit" and r["node_id"] =~ /{id_radice}/) |> last()')
        if res_hw: return f"LOCKED 🔒: Riavvio hardware recente su {id_radice}."

        # Controllo Policy
        res_tr = query_with_failover(f'from(bucket: "{INFLUX_BUCKET}") |> range(start: -1m) |> filter(fn: (r) => r["_measurement"] == "stato_traffico" and r["incrocio"] == "{id_radice}" and r["_field"] == "durata_verde") |> last()')
        if res_tr:
            for table in res_tr:
                for record in table.records:
                    if int(record.get_value()) >= 30: return f"LOCKED 🔒: Policy già attiva ({record.get_value()}s)."

        return f"UNLOCKED 🔓: {id_radice} pronto."
            
    except Exception as e:
        log_mcp("⚠️", f"Errore Lock Check: {e}")
        return "LOCKED 🔒: Errore sistema. Intervento negato in via precauzionale."
 
if __name__ == "__main__":
    mcp.run(transport="sse", host="0.0.0.0", port=8001)
