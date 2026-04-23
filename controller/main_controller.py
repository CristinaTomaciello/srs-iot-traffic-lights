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
# Importiamo la logica di scrittura ridondata e le costanti
from simulator.utility.influx_utils import write_dual, INFLUX_BUCKET, INFLUX_ORG

# --- IDENTIFICATIVO E REDLOCK ---
MIO_NODE_ID = os.environ.get("NODE_NAME", f"NODO-{uuid.uuid4().hex[:4].upper()}")
LOCK_KEY = "srs_controller_leader"
LOCK_TTL = 10
HEARTBEAT_TOPIC = "srs/system/survival_heartbeat"

# Stato della Leadership
ero_gia_leader = False
ultimo_heartbeat_leader_ricevuto = time.time()

# --- CONNESSIONI REDIS ---
redis_nodes = [
    redis.Redis(host='redis-lock-1', port=6379, db=0, decode_responses=True, socket_timeout=1),
    redis.Redis(host='redis-lock-2', port=6379, db=0, decode_responses=True, socket_timeout=1),
    redis.Redis(host='redis-lock-3', port=6379, db=0, decode_responses=True, socket_timeout=1)
]

# --- STATO GLOBALE DEL CONTROLLER ---
is_mqtt_connected = False
is_simulation_active = False
global_state = {}

start_time = time.time()
last_heartbeat_time = 0
HEARTBEAT_INTERVAL = 10
last_traffic_check_time = 0
TRAFFIC_CHECK_INTERVAL = 60
HARDWARE_CHECK_INTERVAL = 30


def on_connect(client, userdata, flags, rc, properties):
    global is_mqtt_connected
    if rc == 0:
        is_mqtt_connected = True
        print(f"[{MIO_NODE_ID}] Central Controller online!", flush=True)
        client.subscribe("srs/edge/+/+/stato")
        client.subscribe("srs/admin/control")
        client.subscribe(HEARTBEAT_TOPIC)

def on_message(client, userdata, msg):
    global is_simulation_active, last_traffic_check_time, ultimo_heartbeat_leader_ricevuto, ero_gia_leader
    try:
        # 1. GESTIONE SURVIVAL HEARTBEAT (P2P MQTT)
        if msg.topic == HEARTBEAT_TOPIC:
            sender_id = msg.payload.decode('utf-8')
            ultimo_heartbeat_leader_ricevuto = time.time()
            if ero_gia_leader and sender_id != MIO_NODE_ID:
                print(f"[{MIO_NODE_ID}] ⚠️ SPLIT-BRAIN EVITATO! Nodo {sender_id} è il vero Leader. Mi dimetto.")
                ero_gia_leader = False
            return

        # 2. GESTIONE COMANDI SIMULAZIONE
        if msg.topic == "srs/admin/control":
            payload = json.loads(msg.payload.decode('utf-8'))
            comando = payload.get("command")
            if comando == "START":
                is_simulation_active = True
                print(f"[{MIO_NODE_ID}] Simulazione AVVIATA.")
                ora = time.time()
                last_traffic_check_time = ora 
                for inc_id in global_state:
                    for sem_id in global_state[inc_id]:
                        global_state[inc_id][sem_id]["last_seen"] = ora
            elif comando == "PAUSE":
                is_simulation_active = False
                print(f"[{MIO_NODE_ID}] Simulazione IN PAUSA.")
            return
        
        # 3. ELABORAZIONE DATI TRAFFICO
        parti_topic = msg.topic.split("/")
        incrocio_id = parti_topic[2]
        semaforo_id = parti_topic[3]
        dati_semaforo = json.loads(msg.payload.decode('utf-8'))
        
        if incrocio_id not in global_state:
            global_state[incrocio_id] = {}
        
        global_state[incrocio_id][semaforo_id] = {
            "dati": dati_semaforo,
            "last_seen": time.time(),
            "alert_sent": False
        }

        # <<< NOVITÀ: SCRITTURA RIDONDATA TRAMITE UTILITY >>>
        if ero_gia_leader:
            punto_storico = influxdb_client.Point("stato_traffico") \
                .tag("incrocio", incrocio_id) \
                .tag("direzione", semaforo_id) \
                .field("auto_in_coda", int(dati_semaforo.get('auto_in_coda', 0))) \
                .field("colore", dati_semaforo.get('colore', 'UNKNOWN')) \
                .field("durata_verde", int(dati_semaforo.get('green_duration', 0)))
            
            # Scrittura asincrona per massimizzare le performance del controller
            write_dual(punto_storico, synchronous=False)
        
    except json.JSONDecodeError:
        pass
    except Exception as e:
        if ero_gia_leader:
            print(f"Errore salvataggio DB: {e}")

def on_disconnect(client, userdata, flags, rc, properties):
    global is_mqtt_connected, ero_gia_leader
    is_mqtt_connected = False
    ero_gia_leader = False
    print(f"[{MIO_NODE_ID}] Connessione persa! Avvio failover...")

def check_controller_connection():
    return is_mqtt_connected

# --- LOGICA DI ELEZIONE E SURVIVAL MODE ---
def valuta_leadership(client_mqtt):
    global ero_gia_leader
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
        except Exception:
            pass

    if voti_favorevoli >= 2:
        ero_gia_leader = True
        client_mqtt.publish(HEARTBEAT_TOPIC, MIO_NODE_ID, qos=0)
        return True

    if voti_favorevoli < 2 and ero_gia_leader:
        print(f"[{MIO_NODE_ID}] 🔴 ALLARME QUORUM: Persa connessione a Redis! Continuo in SURVIVAL MODE.")
        client_mqtt.publish(HEARTBEAT_TOPIC, MIO_NODE_ID, qos=0)
        return True

    tempo_silenzio = time.time() - ultimo_heartbeat_leader_ricevuto
    if voti_favorevoli < 2 and not ero_gia_leader:
        if tempo_silenzio > 12: 
            print(f"[{MIO_NODE_ID}] ⚡ USURPAZIONE! Leader sparito. Prendo il comando!")
            ero_gia_leader = True
            client_mqtt.publish(HEARTBEAT_TOPIC, MIO_NODE_ID, qos=0)
            return True
        else:
            return False

    ero_gia_leader = False
    return False

# --- SETUP E AVVIO ---
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

        if valuta_leadership(client):
            if ora_attuale - last_heartbeat_time >= HEARTBEAT_INTERVAL:
                heartbeat_payload = {"component": "main_controller", "status": "ONLINE", "leader_id": MIO_NODE_ID, "uptime": int(ora_attuale - start_time)}
                client.publish("srs/controller/heartbeat", json.dumps(heartbeat_payload), qos=0)
                last_heartbeat_time = ora_attuale

            if is_simulation_active:
                for inc_id, semafori in global_state.items():
                    for sem_id, info in semafori.items():
                        secondi_silenzio = ora_attuale - info["last_seen"]
                        if secondi_silenzio > HARDWARE_CHECK_INTERVAL and not info.get("alert_sent", False):
                            alert_payload = {
                                "event": "NODE_SILENCE_TIMEOUT", "node_id": sem_id, "intersection_id": inc_id,
                                "last_known_state": info["dati"], "seconds_offline": int(secondi_silenzio)
                            }
                            client.publish("srs/alerts/recovery_needed", json.dumps(alert_payload), qos=1)
                            info["alert_sent"] = True

                if ora_attuale - last_traffic_check_time >= TRAFFIC_CHECK_INTERVAL:
                    traffic_payload = {"event": "PROACTIVE_TRAFFIC_CHECK", "timestamp": ora_attuale}
                    client.publish("srs/alerts/traffic_check", json.dumps(traffic_payload), qos=1)
                    last_traffic_check_time = ora_attuale
            
            if int(ora_attuale) % 10 == 0:
                print(f"[{MIO_NODE_ID}] 👑 Sono il Leader. Monitorando {len(global_state)} incroci.")
        else:
            if int(ora_attuale) % 10 == 0:
                print(f"[{MIO_NODE_ID}] 💤 In Standby. Ascolto il Leader")

        time.sleep(1)

except KeyboardInterrupt:
    print("\nSpegnimento del Controller in corso...")
    client.loop_stop()
    client.disconnect()
    # Non è più necessario chiudere manualmente i client Influx qui, 
    # poiché l'utility usa il context manager 'with' per ogni scrittura.
