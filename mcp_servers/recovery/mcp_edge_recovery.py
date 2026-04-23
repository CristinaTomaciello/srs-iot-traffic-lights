import os
import sys
import json
import time
import uuid

# 1. SETUP PERCORSI E IMPORT
# Saliamo di due livelli: mcp_servers/recovery -> mcp_servers -> radice_progetto
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from mcp.server.fastmcp import FastMCP
import paho.mqtt.client as mqtt
import influxdb_client

# Importiamo le utility e le costanti centralizzate
from simulator.utility.influx_utils import query_with_failover, write_dual, INFLUX_BUCKET, INFLUX_ORG

mcp = FastMCP("EdgeRecoveryMCP")

# --- CONFIGURAZIONE MQTT ---
is_mqtt_connected = False

def on_connect(client, userdata, flags, rc, properties):
    global is_mqtt_connected
    if rc == 0:
        is_mqtt_connected = True
        print("[MCP_AGENT] Connesso al broker MQTT!", file=sys.stderr, flush=True)

def on_disconnect(client, userdata, flags, rc, properties):
    global is_mqtt_connected
    is_mqtt_connected = False
    print("[MCP_AGENT] Disconnesso dal broker MQTT!", file=sys.stderr, flush=True)

id = f"MCP_RECOVERY_{uuid.uuid4().hex[:6]}"
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=id)
mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect

def connetti_mqtt_agente():
    brokers = ["srs-haproxy-1", "srs-haproxy-2"]
    for broker in brokers:
        try:
            print(f"[MCP_AGENT] Tentativo di connessione a {broker}...", file=sys.stderr)
            mqtt_client.connect(broker, 1883, 60)
            mqtt_client.loop_start()
            time.sleep(1) 
            if is_mqtt_connected:
                return True
        except Exception as e:
            print(f"[MCP_AGENT] Fallita connessione a {broker}: {e}", file=sys.stderr)
    return False

connetti_mqtt_agente()

# =======================================================
# TOOL 1: TELEMETRIA (Usato dall'Orchestratore)
# =======================================================
@mcp.tool()
def get_node_telemetry(node_id: str) -> str:
    """Interroga InfluxDB tramite failover per lo stato del nodo."""
    try:
        query = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -1m)
          |> filter(fn: (r) => r["_measurement"] == "stato_traffico")
          |> filter(fn: (r) => r["direzione"] == "{node_id}") 
          |> last()
        """
        
        # USA IL FAILOVER: Prova Alpha, se fallisce prova Beta
        result = query_with_failover(query)
        
        if not result or len(result) == 0:
            return f"DIAGNOSI: Il nodo {node_id} non ha inviato dati recenti. Probabile OFFLINE critico."

        record = result[0].records[0]
        ts_ultimo = record.get_time().timestamp()
        secondi_fa = int(time.time() - ts_ultimo)
        
        status = "ONLINE" if secondi_fa < 30 else "OFFLINE_CRITICO"
        
        return (f"--- STATO NODO {node_id} ---\n"
                f"Ultimo segnale: {secondi_fa} secondi fa\n"
                f"Diagnosi: {status}\n"
                f"Ultimo Colore: {record.get_value()}\n"
                f"Auto in coda: {record.values.get('auto_in_coda', 0)}")
        
    except Exception as e:
        return f"ERRORE TELEMETRIA (Failover attivo): {str(e)}"

# =======================================================
# TOOL 2: CONTROLLO AUDIT LOG (Usato dal Validatore)
# =======================================================
@mcp.tool()
def get_last_restart_time(node_id: str) -> str:
    """Verifica l'ultimo riavvio con logica di failover database."""
    try:
        query = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -1h)
          |> filter(fn: (r) => r["_measurement"] == "agent_audit")
          |> filter(fn: (r) => r["node_id"] == "{node_id}")
          |> filter(fn: (r) => r["_field"] == "action")
          |> filter(fn: (r) => r["_value"] == "RESTART")
          |> last()
        """
        
        # USA IL FAILOVER
        result = query_with_failover(query)
        
        if not result or len(result) == 0:
            return '{"node": "' + node_id + '", "seconds_since_last_restart": 999999, "can_restart": true}'

        record = result[0].records[0]
        secondi_fa = int(time.time() - record.get_time().timestamp())
        can_restart = "true" if secondi_fa > 60 else "false"

        return f'{{"node": "{node_id}", "seconds_since_last_restart": {secondi_fa}, "can_restart": {can_restart}}}'
        
    except Exception as e:
        return f'{{"error": "{str(e)}"}}'

# =======================================================
# TOOL 3: ESECUZIONE (Usato dal Recovery Agent)
# =======================================================
@mcp.tool()
def restart_edge_node(node_id: str) -> str:
    """Invia comando MQTT e logga l'audit su entrambi i database (Dual-Write)."""
    if not is_mqtt_connected:
        if not connetti_mqtt_agente():
            return "ERRORE: Impossibile raggiungere i bilanciatori MQTT."

    try:
        # 1. Comando fisico MQTT
        topic = f"srs/admin/node/{node_id}/fault_injection"
        mqtt_client.publish(topic, json.dumps({"type": "REPAIR"}), qos=1)
        
        # 2. Creazione Point per Audit
        point = influxdb_client.Point("agent_audit") \
            .tag("node_id", node_id) \
            .field("action", "RESTART") \
            .field("status", "success")
            
        # 3. USA IL DUAL-WRITE: Scrive su Alpha e Beta in parallelo
        # Usiamo synchronous=True perché in un'operazione di recovery vogliamo la conferma
        successo_scrittura = write_dual(point, synchronous=True)
        
        time.sleep(1)
        
        audit_msg = "e registrato correttamente." if successo_scrittura else "ma registrazione audit FALLITA."
        return f"SUCCESSO: Riavvio inviato a {node_id} {audit_msg}"

    except Exception as e:
        return f"ERRORE CRITICO: {str(e)}"

# =======================================================
# TOOL 4: ESCALATION
# =======================================================
@mcp.tool()
def escalate_to_human(node_id: str, diagnostic_summary: str) -> str:
    """Scala il problema a un operatore umano."""
    print("\n" + "="*60, file=sys.stderr)
    print("ESCALATION UMANA RICHIESTA DALL'AGENTE 🚨", file=sys.stderr)
    print(f"NODO COINVOLTO: {node_id}", file=sys.stderr)
    print(f"ANALISI: {diagnostic_summary}", file=sys.stderr)
    print("="*60 + "\n", file=sys.stderr, flush=True)
    return "ESCALATION COMPLETATA. Il controllo è passato all'umano."

if __name__ == "__main__":
    mcp.run()
