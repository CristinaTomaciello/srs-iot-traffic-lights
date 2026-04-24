import os
import sys
import json
import time
import uuid
import re  # AGGIUNTO PER LA VALIDAZIONE DI SICUREZZA

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from mcp.server.fastmcp import FastMCP
import paho.mqtt.client as mqtt
import influxdb_client
from simulator.utility.influx_utils import query_with_failover, write_dual, INFLUX_BUCKET, INFLUX_ORG, log_audit

mcp = FastMCP("EdgeRecoveryMCP")

VALID_NODE_PATTERN = re.compile(r"^INC_\d+_\d+_(NORD|SUD|EST|OVEST)$")

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

def log_mcp(emoji, message):
    print(f"[{id}] {emoji} {message}", file=sys.stderr, flush=True)

connetti_mqtt_agente()

# =======================================================
# TOOL 1: TELEMETRIA
# =======================================================
@mcp.tool()
def get_node_telemetry(node_id: str) -> str:
    """Diagnosi rapida dello stato del nodo."""
    if not VALID_NODE_PATTERN.match(node_id):
        return "ERRORE: ID nodo non valido. Formato richiesto: INC_X_Y o INC_X_Y_DIR."

    try:
        query = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -1m)
          |> filter(fn: (r) => r["_measurement"] == "stato_traffico")
          |> filter(fn: (r) => r["direzione"] == "{node_id}") 
          |> last()
        """
        result = query_with_failover(query)
        
        if not result:
            return f"STATO: {node_id} è OFFLINE (nessun dato)."

        record = result[0].records[0]
        secondi_fa = int(time.time() - record.get_time().timestamp())
        status = "OK" if secondi_fa < 30 else "TIMEOUT"
        
        return f"NODO: {node_id} | STATO: {status} ({secondi_fa}s fa) | COLORE: {record.get_value()}"
        
    except Exception as e:
        log_mcp("🔥", f"Errore telemetria: {e}")
        return f"ERRORE: Database non raggiungibile."

# =======================================================
# TOOL 2: CONTROLLO AUDIT LOG
# =======================================================
@mcp.tool()
def get_last_restart_time(node_id: str) -> str:
    """Verifica l'ultimo riavvio con logica di failover database."""
    if not VALID_NODE_PATTERN.match(node_id):
        return '{"error": "ID nodo non valido."}'

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
# TOOL 3: ESECUZIONE
# =======================================================
@mcp.tool()
def restart_edge_node(node_id: str) -> str:
    """Invia REPAIR e logga l'azione su Alpha e Beta."""
    if not VALID_NODE_PATTERN.match(node_id):
        return "BLOCCATO: Riavvio negato. ID nodo non valido o non autorizzato."

    if not is_mqtt_connected:
        if not connetti_mqtt_agente():
            return "ERRORE: MQTT Down."

    try:
        topic = f"srs/admin/node/{node_id}/fault_injection"
        mqtt_client.publish(topic, json.dumps({"type": "REPAIR"}), qos=1)
        
        point = influxdb_client.Point("agent_audit") \
            .tag("node_id", node_id) \
            .field("action", "RESTART")
            
        successo = write_dual(point, synchronous=True)
        
        log_mcp("🛠️", f"Riavvio inviato a {node_id}. Audit: {'✅' if successo else '❌'}")
        log_audit("RECOVERY_MCP", "EXECUTE_REPAIR", f"Restart inviato a {node_id}", level="INFO")
        return f"SUCCESSO: Nodo {node_id} riavviato."

    except Exception as e:
        log_mcp("💥", f"Fallimento restart: {e}")
        return "ERRORE CRITICO nell'esecuzione."

# =======================================================
# TOOL 4: ESCALATION
# =======================================================
@mcp.tool()
def escalate_to_human(node_id: str, diagnostic_summary: str) -> str:
    """Passa il controllo all'operatore."""
    log_mcp("🚨", f"ESCALATION RICHIESTA per {node_id}: {diagnostic_summary}")
    return "ESCALATION COMPLETATA."

if __name__ == "__main__":
    mcp.run()
