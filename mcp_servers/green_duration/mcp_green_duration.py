import os
import sys
import json
import time
import uuid

# 1. SETUP PERCORSI E IMPORT
# Saliamo di due livelli: mcp_servers/green_duration -> mcp_servers -> radice_progetto
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from mcp.server.fastmcp import FastMCP
import paho.mqtt.client as mqtt
import influxdb_client

# Importiamo le utility e le costanti centralizzate
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
        
        # Restituiamo un report denso per l'IA
        return "ALLARME CONGESTIONE: " + " | ".join(allarmi)
 
    except Exception as e:
        log_mcp("🔥", f"Errore telemetria: {e}")
        return "ERRORE: Datasource non raggiungibile."
 
@mcp.tool()
@mcp.tool()
def optimize_green_phase(incrocio_id: str, asse: str, new_duration: int, override_cycles: int ) -> str:
    """Invia comando MQTT. Accetta assi (NORD_SUD) o nodi singoli (INC_0_0_NORD)."""
    if new_duration > 60: return "RIFIUTATO: Massimo 60s."
    
    asse_up = asse.upper()
    if "NORD" in asse_up or "SUD" in asse_up: asse_target = "NORD_SUD"
    elif "EST" in asse_up or "OVEST" in asse_up: asse_target = "EST_OVEST"
    else: return f"RIFIUTATO: Asse '{asse}' non riconosciuto."
 
    try:
        direzioni = ["NORD", "SUD"] if asse_target == "NORD_SUD" else ["EST", "OVEST"]
        payload = json.dumps({"green_duration": new_duration, "override_cycles": override_cycles})
 
        for d in direzioni:
            topic = f"srs/edge/{incrocio_id}_{d}/config"
            mqtt_client.publish(topic, payload, qos=1)
 
        log_mcp("🟢", f"Ottimizzazione {incrocio_id} [{asse_target}] -> {new_duration}s")
        return f"SUCCESSO: {incrocio_id} asse {asse_target} aggiornato."
    except Exception as e:
        log_mcp("💥", f"Errore MQTT: {e}")
        return "ERRORE: Comando non inviato."

@mcp.tool()
def verify_green_duration(incrocio_id: str) -> str:
    """Verifica l'applicazione della policy leggendo dal database disponibile."""
    try:
        query = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -1m)
          |> filter(fn: (r) => r["_measurement"] == "stato_traffico")
          |> filter(fn: (r) => r["incrocio"] == "{incrocio_id}")
          |> filter(fn: (r) => r["_field"] == "durata_verde")
          |> last()
          |> pivot(rowKey:["direzione"], columnKey: ["_field"], valueColumn: "_value")
        """
        result = query_with_failover(query)
 
        if not result: return f"Nessun dato per {incrocio_id}."
 
        report = f"VERIFICA TELEMETRIA (Failover DB): {incrocio_id}\n"
        for table in result:
            for record in table.records:
                report += f"- Asse {record.values.get('direzione')}: Verde = {record.values.get('durata_verde')}s\n"
        return report
    except Exception as e:
        return f"ERRORE ACCESSO DB: {str(e)}"
    
@mcp.tool()
def check_hardware_lock(incrocio_id: str) -> str:
    """Verifica lock di sicurezza. Risponde con OK o LOCKED."""
    try:
        # Controllo Hardware (Riavvii recenti)
        res_hw = query_with_failover(f'from(bucket: "{INFLUX_BUCKET}") |> range(start: -90s) |> filter(fn: (r) => r["_measurement"] == "agent_audit" and r["node_id"] =~ /{incrocio_id}/) |> last()')
        if res_hw: return f"LOCKED 🔒: Riavvio hardware recente su {incrocio_id}."

        # Controllo Policy (Verde già alto)
        res_tr = query_with_failover(f'from(bucket: "{INFLUX_BUCKET}") |> range(start: -1m) |> filter(fn: (r) => r["_measurement"] == "stato_traffico" and r["incrocio"] == "{incrocio_id}" and r["_field"] == "durata_verde") |> last()')
        if res_tr:
            for table in res_tr:
                for record in table.records:
                    if int(record.get_value()) >= 30: return f"LOCKED 🔒: Policy già attiva ({record.get_value()}s)."

        return f"UNLOCKED 🔓: {incrocio_id} pronto."
            
    except Exception as e:
        log_mcp("⚠️", f"Errore Lock Check: {e}")
        return "LOCKED 🔒: Errore sicurezza, intervento negato."
 
if __name__ == "__main__":
    mcp.run()
