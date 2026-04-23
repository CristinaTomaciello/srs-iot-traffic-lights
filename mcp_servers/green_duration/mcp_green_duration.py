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

avvia_connessione_mqtt()
 
# --- TOOLS MCP ---

@mcp.tool()
def get_traffic_status() -> str:
    """Legge lo stato di TUTTI gli incroci usando la logica di failover database."""
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
        
        # ESEGUE LA QUERY CON FAILOVER (Alpha -> Beta)
        result = query_with_failover(query)
 
        if not result:
            return "Nessun dato recente trovato. Verifica che i sensori siano attivi."
 
        mappa_incroci = {}
        for table in result:
            for record in table.records:
                incrocio = record.values.get("incrocio")
                if incrocio not in mappa_incroci: mappa_incroci[incrocio] = []
                    
                mappa_incroci[incrocio].append({
                    "direzione": record.values.get("direzione"),
                    "coda": int(record.values.get("auto_in_coda", 0)),
                    "verde": int(record.values.get("durata_verde", 0))
                })
 
        report = f"--- REPORT TRAFFICO GLOBALE (Failover DB Attivo) ---\n\n"
        allarmi = 0
        for incrocio, direzioni in mappa_incroci.items():
            report += f"INCROCIO: {incrocio}\n"
            for d in direzioni:
                tag = " [ALLARME CONGESTIONE]" if d['coda'] >= soglia_allarme else ""
                if tag: allarmi += 1
                report += f"   - Asse {d['direzione']}: {d['coda']} auto (Verde: {d['verde']}s){tag}\n"
 
        report += "\nSUGGERIMENTO: Intervenire sugli assi congestionati." if allarmi > 0 else "\nSituazione regolare."
        return report
 
    except Exception as e:
        return f"ERRORE TELEMETRIA (Database non raggiungibili): {str(e)}"
 
@mcp.tool()
def optimize_green_phase(incrocio_id: str, asse: str, new_duration: int, override_cycles: int ) -> str:
    """Invia comando di ottimizzazione tramite MQTT (Resiliente)."""
    if new_duration > 60: return "RIFIUTATO: Massimo 60s per sicurezza."
    if asse not in ["NORD_SUD", "EST_OVEST"]: return "RIFIUTATO: Asse non valido."
 
    try:
        assi = ["NORD", "SUD"] if asse == "NORD_SUD" else ["EST", "OVEST"]
        payload = json.dumps({"green_duration": new_duration, "override_cycles": override_cycles})
 
        for d in assi:
            topic = f"srs/edge/{incrocio_id}_{d}/config"
            mqtt_client.publish(topic, payload, qos=1)
 
        return f"SUCCESSO: {incrocio_id} asse {asse} portato a {new_duration}s per {override_cycles} cicli."
    except Exception as e:
        return f"ERRORE COMANDO MQTT: {str(e)}"

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
    """Controlla i lock di sicurezza (Hardware e Traffico) su entrambi i DB se necessario."""
    try:
        # 1. CONTROLLO HARDWARE
        query_hw = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -90s)
          |> filter(fn: (r) => r["_measurement"] == "agent_audit")
          |> filter(fn: (r) => r["node_id"] =~ /{incrocio_id}/)
          |> filter(fn: (r) => r["action"] == "RESTART")
          |> last()
        """
        res_hw = query_with_failover(query_hw)

        # 2. CONTROLLO TRAFFICO
        query_tr = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -2m)
          |> filter(fn: (r) => r["_measurement"] == "stato_traffico")
          |> filter(fn: (r) => r["incrocio"] == "{incrocio_id}")
          |> filter(fn: (r) => r["_field"] == "durata_verde")
          |> last()
        """
        res_tr = query_with_failover(query_tr)
 
        if res_hw:
            return f"LUCCHETTO ATTIVO 🔒 (HARDWARE): Riavvio recente su {incrocio_id}. Intervento negato."
            
        if res_tr:
            for table in res_tr:
                for record in table.records:
                    if int(record.get_value()) >= 30: 
                        return f"LUCCHETTO ATTIVO 🔒 (TRAFFICO): Policy già attiva su {incrocio_id} ({record.get_value()}s)."

        return f"LUCCHETTO SBLOCCATO 🔓 su {incrocio_id}."
            
    except Exception as e:
        return f"ERRORE SICUREZZA: {str(e)}. Azione BLOCCATA per precauzione."
 
if __name__ == "__main__":
    mcp.run()
