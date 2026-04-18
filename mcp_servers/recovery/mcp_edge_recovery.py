from mcp.server.fastmcp import FastMCP
import paho.mqtt.client as mqtt
import influxdb_client
import json
import time
import sys

mcp = FastMCP("EdgeRecoveryMCP")

INFLUX_URL = "http://semafori-tsdb:8086" 
INFLUX_TOKEN = "supersecrettoken123"
INFLUX_ORG = "srs_org"
INFLUX_BUCKET = "traffic_data"

is_mqtt_connected = False

def on_connect(client, userdata, flags, rc, properties):
    global is_mqtt_connected
    if rc == 0:
        is_mqtt_connected = True
        # Usiamo sys.stderr per non corrompere il canale JSON di MCP
        print("[MCP_AGENT] Connesso al broker MQTT!", file=sys.stderr, flush=True)

def on_disconnect(client, userdata, flags, rc, properties):
    global is_mqtt_connected
    is_mqtt_connected = False
    print("[MCP_AGENT] Disconnesso dal broker MQTT!", file=sys.stderr, flush=True)

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="MCP_RECOVERY_AGENT")
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

restart_counters = {}
MAX_RESTARTS = 2

@mcp.tool()
def get_node_telemetry(node_id: str) -> str:
    """Interroga InfluxDB e calcola da quanto tempo il nodo non invia dati."""
    try:
        client = influxdb_client.InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        query_api = client.query_api()
        
        # Estrarre l'ID incrocio e direzione (es. INC_0_0 e NORD)
        parts = node_id.split('_')
        incrocio_id = f"{parts[0]}_{parts[1]}_{parts[2]}"
        direzione = parts[-1]

        query = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -1h)
          |> filter(fn: (r) => r["_measurement"] == "stato_traffico")
          |> filter(fn: (r) => r["direzione"] == "{node_id}") 
          |> last()
        """
        result = query_api.query(org=INFLUX_ORG, query=query)
        
        if not result:
            return f"DIAGNOSI: Il nodo {node_id} non ha mai inviato dati nell'ultima ora. Probabile OFFLINE critico."

        record = result[0].records[0]
        ts_ultimo = record.get_time().timestamp()
        ora_mcp = time.time()
        secondi_fa = int(ora_mcp - ts_ultimo)
        
        print(f"[DEBUG MCP] Ora MCP: {ora_mcp} | Timestamp DB: {ts_ultimo} | Differenza: {secondi_fa}s", file=sys.stderr, flush=True)

        status = "OPERATIVO" if secondi_fa < 30 else "SOSPETTO CRASH (Nessun dato da >30s)"
        
        return (f"--- STATO NODO {node_id} ---\n"
                f"Ultimo segnale: {secondi_fa} secondi fa\n"
                f"Diagnosi: {status}\n"
                f"Ultimo Colore: {record.get_value()}\n"
                f"Auto in coda: {record.values.get('auto_in_coda', 0)}")
        
    except Exception as e:
        return f"ERRORE TELEMETRIA: {str(e)}"

@mcp.tool()
def restart_edge_node(node_id: str) -> str:
    """Invia un comando di RIAVVIO a un singolo nodo tramite MQTT."""
    current_restarts = restart_counters.get(node_id, 0)
    
    if current_restarts >= MAX_RESTARTS:
        return (f"BLOCCO DI SICUREZZA: Il nodo '{node_id}' è già stato riavviato "
                f"{MAX_RESTARTS} volte. L'automazione è sospesa. "
                f"Usa il tool 'escalate_to_human' per richiedere approvazione manuale.")

    if not is_mqtt_connected:
        if not connetti_mqtt_agente():
            return "ERRORE: Impossibile raggiungere i bilanciatori MQTT. Rete compromessa."

    try:
        topic = f"srs/admin/node/{node_id}/fault_injection"
        payload = {"type": "REPAIR"} # Questo attiverà is_faulty = False
        
        mqtt_client.publish(topic, json.dumps(payload), qos=1)
        time.sleep(1)
        
        return f"SUCCESSO: Inviato comando REPAIR a {node_id}. Il simulatore dovrebbe ora riprendere l'attività."
    except Exception as e:
        return f"ERRORE MQTT: {str(e)}"

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