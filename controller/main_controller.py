import paho.mqtt.client as mqtt
import json
import influxdb_client
import time
from influxdb_client.client.write_api import ASYNCHRONOUS, WriteOptions
import sys
import os

#path per le utility
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import simulator.utility.mqtt_utils as utils

INFLUX_URL = "http://influxdb:8086"
INFLUX_TOKEN = "supersecrettoken123"
INFLUX_ORG = "srs_org"
INFLUX_BUCKET = "traffic_data"

# Connessione al database
db_client = influxdb_client.InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
opzioni_asincrone = WriteOptions(
    batch_size=50,
    flush_interval=1000,
    jitter_interval=0,
    retry_interval=5000
)
write_api = db_client.write_api(write_options=opzioni_asincrone)

#Stato globale del Controller
is_mqtt_connected = False
is_simulation_active = False
global_state = {}

# Variabili per il monitoraggio del Controller
start_time = time.time()
last_heartbeat_time = 0
HEARTBEAT_INTERVAL = 10  # Invia un battito ogni 10 secondi

def on_connect(client, userdata, flags, rc, properties):
    global is_mqtt_connected
    if rc == 0:
        is_mqtt_connected = True
        print("[CONTROLLER_CLIENT] Central Controller online e connesso!", flush=True)
        client.subscribe("srs/edge/+/+/stato")
        client.subscribe("srs/admin/control")

def on_message(client, userdata, msg):
    global is_simulation_active
    try:
        # Gestione comandi simulazione
        if msg.topic == "srs/admin/control":
            payload = json.loads(msg.payload.decode('utf-8'))
            comando = payload.get("command")
            if comando == "START":
                is_simulation_active = True
                print("[CONTROLLER] Simulazione AVVIATA. Watchdog attivato.")
                ora = time.time()
                for inc_id in global_state:
                    for sem_id in global_state[inc_id]:
                        global_state[inc_id][sem_id]["last_seen"] = ora
            elif comando == "PAUSE":
                is_simulation_active = False
                print("[CONTROLLER] Simulazione IN PAUSA. Watchdog sospeso.")
            return
        
        # Elaborazione dati traffico
        parti_topic = msg.topic.split("/")
        incrocio_id = parti_topic[2]
        semaforo_id = parti_topic[3]
        
        payload = msg.payload.decode('utf-8')
        dati_semaforo = json.loads(payload)
        
        if incrocio_id not in global_state:
            global_state[incrocio_id] = {}
        
        # Aggiornamento stato per Watchdog
        global_state[incrocio_id][semaforo_id] = {
            "dati": dati_semaforo,
            "last_seen": time.time(),
            "alert_sent": False
        }

        # Scrittura su InfluxDB
        punto_storico = influxdb_client.Point("stato_traffico") \
            .tag("incrocio", incrocio_id) \
            .tag("direzione", semaforo_id) \
            .field("auto_in_coda", int(dati_semaforo.get('auto_in_coda', 0))) \
            .field("colore", dati_semaforo.get('colore', 'UNKNOWN')) \
            .field("durata_verde", int(dati_semaforo.get('green_duration', 0)))
        
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=punto_storico)
        
    except json.JSONDecodeError:
        print(f"Errore di decodifica JSON dal topic {msg.topic}")
    except Exception as e:
        print(f"Errore salvataggio DB: {e}")

def on_disconnect(client, userdata, flags, rc, properties):
    global is_mqtt_connected
    is_mqtt_connected = False
    print("[CONTROLLER_CLIENT] Disconnesso dal broker!", flush=True)

def check_controller_connection():
    global is_mqtt_connected
    return is_mqtt_connected

# Setup Client MQTT
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="CONTROLLER_CLIENT")
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect 

print("Inizializzazione del Controller e connessione in HA...", flush=True)
utils.connetti_con_failover(client, "CONTROLLER_CLIENT", check_controller_connection)

# Loop principale del Controller
try:
    while True:
        ora_attuale = time.time()

        # Gestione del heartbeat del Controller
        if ora_attuale - last_heartbeat_time >= HEARTBEAT_INTERVAL:
            heartbeat_payload = {
                "component": "main_controller",
                "status": "ONLINE",
                "uptime": int(ora_attuale - start_time),
                "timestamp": ora_attuale
            }
            client.publish("srs/controller/heartbeat", json.dumps(heartbeat_payload), qos=0)
            last_heartbeat_time = ora_attuale
            # print(f"[DEBUG] Heartbeat inviato a {ora_attuale}", flush=True)

        # Controllo della connessione del Controller e failover se necessario
        if not check_controller_connection():
            print("[CONTROLLER_CLIENT] Connessione persa! Rotazione verso il bilanciatore secondario...", flush=True)
            utils.connetti_con_failover(client, "CONTROLLER_CLIENT", check_controller_connection)
            
        # Watchdog per nodi silenti
        if is_simulation_active:
            for inc_id, semafori in global_state.items():
                for sem_id, info in semafori.items():
                    secondi_silenzio = ora_attuale - info["last_seen"]
                    
                    if secondi_silenzio > 30 and not info.get("alert_sent", False):
                        # Payload per l'Agente AI di OpenCode
                        alert_payload = {
                            "event": "NODE_SILENCE_TIMEOUT",
                            "node_id": sem_id,
                            "intersection_id": inc_id,
                            "last_known_state": info["dati"],
                            "seconds_offline": int(secondi_silenzio),
                            "timestamp": ora_attuale
                        }
                        
                        client.publish("srs/alerts/recovery_needed", json.dumps(alert_payload), qos=1)
                        info["alert_sent"] = True
                        print(f"[WATCHDOG] Nodo {sem_id} silente. Segnale di recovery inviato.")

        time.sleep(1)
        
except KeyboardInterrupt:
    print("\nSpegnimento del Controller in corso...")
    client.loop_stop()
    client.disconnect()
    db_client.close()