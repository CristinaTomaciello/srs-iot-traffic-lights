import paho.mqtt.client as mqtt
import json
import influxdb_client
import time
import sys
import os
import uuid
import threading
import redis

# 1. CONFIGURAZIONE PERCORSI E UTILITY
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import simulator.utility.mqtt_utils as utils
from simulator.utility.influx_utils import write_dual, INFLUX_BUCKET, INFLUX_ORG ,log_audit

# --- IDENTIFICATIVO E REDLOCK ---
MIO_NODE_ID = os.environ.get("NODE_NAME", f"NODO-{uuid.uuid4().hex[:4].upper()}")
LOCK_KEY = "srs_controller_leader"
LOCK_TTL = 10
HEARTBEAT_TOPIC = "srs/system/survival_heartbeat"

ero_gia_leader = False
ultimo_heartbeat_leader_ricevuto = time.time()

redis_nodes = [
    redis.Redis(host='redis-lock-1', port=6379, db=0, decode_responses=True, socket_timeout=1),
    redis.Redis(host='redis-lock-2', port=6379, db=0, decode_responses=True, socket_timeout=1),
    redis.Redis(host='redis-lock-3', port=6379, db=0, decode_responses=True, socket_timeout=1)
]

is_mqtt_connected = False
is_simulation_active = False
global_state = {}

start_time = time.time()
last_heartbeat_time = 0
HEARTBEAT_INTERVAL = 10
last_traffic_check_time = 0
TRAFFIC_CHECK_INTERVAL = 60
HARDWARE_CHECK_INTERVAL = 30

# -------------------------------------------------------------------
# LOGGING SYSTEM PER IL CONTROLLER
# -------------------------------------------------------------------
def log_system(emoji: str, event_type: str, message: str, level="INFO"):
    print(f"[{MIO_NODE_ID}] {emoji} {message}", flush=True)
    try:
        point = influxdb_client.Point("system_logs") \
            .tag("service", "MainController") \
            .tag("worker", MIO_NODE_ID) \
            .tag("level", level) \
            .field("event_type", event_type) \
            .field("message", message)
        write_dual(point, synchronous=False)
    except: pass

def on_connect(client, userdata, flags, rc, properties):
    global is_mqtt_connected
    if rc == 0:
        is_mqtt_connected = True
        log_system("🌐", "NETWORK", "Central Controller online!")
        client.subscribe("srs/edge/+/+/stato")
        client.subscribe("srs/admin/control")
        client.subscribe(HEARTBEAT_TOPIC)

def on_message(client, userdata, msg):
    global is_simulation_active, last_traffic_check_time, ultimo_heartbeat_leader_ricevuto, ero_gia_leader
    try:
        if msg.topic == HEARTBEAT_TOPIC:
            sender_id = msg.payload.decode('utf-8')
            ultimo_heartbeat_leader_ricevuto = time.time()
            if ero_gia_leader and sender_id != MIO_NODE_ID:
                log_system("⚠️", "LEADERSHIP", f"SPLIT-BRAIN EVITATO! Nodo {sender_id} è il vero Leader. Mi dimetto.", level="WARN")
                ero_gia_leader = False
            return

        if msg.topic == "srs/admin/control":
            payload = json.loads(msg.payload.decode('utf-8'))
            comando = payload.get("command")
            if comando == "START":
                is_simulation_active = True
                log_system("🚀", "SIMULATION", "Simulazione AVVIATA.")
                ora = time.time()
                last_traffic_check_time = ora 
                for inc_id in global_state:
                    for sem_id in global_state[inc_id]:
                        global_state[inc_id][sem_id]["last_seen"] = ora
            elif comando == "PAUSE":
                is_simulation_active = False
                log_system("⏸️", "SIMULATION", "Simulazione IN PAUSA.")
            return
        
        parti_topic = msg.topic.split("/")
        incrocio_id = parti_topic[2]
        semaforo_id = parti_topic[3]
        dati_semaforo = json.loads(msg.payload.decode('utf-8'))
        
        if incrocio_id not in global_state:
            global_state[incrocio_id] = {}
            
        old_state = global_state[incrocio_id].get(semaforo_id, {})
        traffic_alert_sent = old_state.get("traffic_alert_sent", False)
        
        auto_in_coda = int(dati_semaforo.get('auto_in_coda', 0))
        if auto_in_coda <= 15:
            traffic_alert_sent = False
        
        if ero_gia_leader:
            try:
                punto_storico = influxdb_client.Point("stato_traffico") \
                    .tag("incrocio", incrocio_id) \
                    .tag("direzione", semaforo_id) \
                    .field("auto_in_coda", int(dati_semaforo.get('auto_in_coda', 0))) \
                    .field("colore", dati_semaforo.get('colore', 'UNKNOWN')) \
                    .field("durata_verde", int(dati_semaforo.get('green_duration', 0)))
                write_dual(punto_storico, synchronous=False)
            except:
                pass

        global_state[incrocio_id][semaforo_id] = {
            "dati": dati_semaforo,
            "last_seen": time.time(),
            "alert_sent": old_state.get("alert_sent", False),
            "last_traffic_alert_time": old_state.get("last_traffic_alert_time", 0)
        }
        
    except json.JSONDecodeError: pass
    except Exception as e:
        if ero_gia_leader: log_system("🔥", "ERROR", f"Errore salvataggio DB: {e}", level="ERROR")

def on_disconnect(client, userdata, flags, rc, properties):
    global is_mqtt_connected, ero_gia_leader
    is_mqtt_connected = False
    ero_gia_leader = False
    log_system("⚠️", "NETWORK", "Connessione persa! Avvio failover...", level="WARN")

def check_controller_connection():
    return is_mqtt_connected

def valuta_leadership(client_mqtt):
    voti_favorevoli = 0
    for r in redis_nodes:
        try:
            leader_attuale = r.get(LOCK_KEY)
            if leader_attuale == MIO_NODE_ID:
                r.expire(LOCK_KEY, LOCK_TTL)
                voti_favorevoli += 1
            else:
                if r.set(LOCK_KEY, MIO_NODE_ID, nx=True, ex=LOCK_TTL):
                    voti_favorevoli += 1
        except Exception: pass

    if voti_favorevoli >= 2:
        client_mqtt.publish(HEARTBEAT_TOPIC, MIO_NODE_ID, qos=0)
        return True
    if voti_favorevoli < 2 and ero_gia_leader:
        client_mqtt.publish(HEARTBEAT_TOPIC, MIO_NODE_ID, qos=0)
        return True
    tempo_silenzio = time.time() - ultimo_heartbeat_leader_ricevuto
    if voti_favorevoli < 2 and not ero_gia_leader and tempo_silenzio > 12: 
        return True
    return False

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"CTRL_{MIO_NODE_ID}")
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect 

print(f"Inizializzazione del Controller {MIO_NODE_ID} in HA...", flush=True)
utils.connetti_con_failover(client, f"CTRL_{MIO_NODE_ID}", check_controller_connection)

try:
    while True:
        ora_attuale = time.time()
        if not check_controller_connection():
            utils.connetti_con_failover(client, f"CTRL_{MIO_NODE_ID}", check_controller_connection)

        is_leader_now = valuta_leadership(client)

        if is_leader_now:
            if not ero_gia_leader:
                log_system("👑", "LEADERSHIP", "Leadership ACQUISITA. Monitoraggio attivo.")
                ero_gia_leader = True

            if ora_attuale - last_heartbeat_time >= HEARTBEAT_INTERVAL:
                heartbeat_payload = {"component": "main_controller", "status": "ONLINE", "leader_id": MIO_NODE_ID, "uptime": int(ora_attuale - start_time)}
                client.publish("srs/controller/heartbeat", json.dumps(heartbeat_payload), qos=0)
                last_heartbeat_time = ora_attuale

            if is_simulation_active:
                for inc_id, semafori in global_state.items():
                    for sem_id, info in semafori.items():
                        secondi_silenzio = ora_attuale - info["last_seen"]
                        is_node_crashed = secondi_silenzio > HARDWARE_CHECK_INTERVAL

                        if is_node_crashed:
                            if not info.get("alert_sent", False):
                                log_system("⚠️", "WATCHDOG_HW", f"{sem_id} OFFLINE. Recovery inviata.", level="WARN")
                                alert_payload = {"event": "NODE_SILENCE_TIMEOUT", "node_id": sem_id, "seconds_offline": int(secondi_silenzio)}
                                client.publish("srs/alerts/recovery_needed", json.dumps(alert_payload), qos=1)
                                info["alert_sent"] = True
                            continue
                        else:
                            if info.get("alert_sent", False):
                                info["alert_sent"] = False

                        auto_attuali = int(info["dati"].get("auto_in_coda", 0))
                        if auto_attuali > 15:
                            ultimo_alert = info.get("last_traffic_alert_time", 0)
                            if (ora_attuale - ultimo_alert > 120):
                                alert_payload = {"event": "PROACTIVE_CHECK", "target_node": sem_id, "cars": auto_attuali}
                                client.publish("srs/alerts/traffic_check", json.dumps(alert_payload), qos=1)
                                info["last_traffic_alert_time"] = ora_attuale
                                print(f"[{MIO_NODE_ID}] 🚗 [WATCHDOG TR] Allarme inviato per {sem_id}.", flush=True)
        else:
            if ero_gia_leader:
                log_system("💤", "LEADERSHIP", "Leadership CEDUTA. In ascolto...")
                ero_gia_leader = False

        time.sleep(1)

except KeyboardInterrupt:
    print("\nSpegnimento del Controller in corso...")
    client.loop_stop()
    client.disconnect()
