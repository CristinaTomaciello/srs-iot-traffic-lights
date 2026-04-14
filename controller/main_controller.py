import paho.mqtt.client as mqtt
import json
import influxdb_client
import time
from influxdb_client.client.write_api import ASYNCHRONOUS,WriteOptions
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import simulator.utility.mqtt_utils as utils

# CONFIGURAZIONE INFLUXDB
INFLUX_URL = "http://influxdb:8086"
INFLUX_TOKEN = "supersecrettoken123"
INFLUX_ORG = "srs_org"
INFLUX_BUCKET = "traffic_data"

# Connettiamo al database InfluxDB
db_client = influxdb_client.InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
opzioni_asincrone = WriteOptions(
    batch_size=50,         # Invia pacchetti piccoli
    flush_interval=1000,   # Svuota il buffer ogni 1 secondo
    jitter_interval=0,
    retry_interval=5000
)
write_api = db_client.write_api(write_options=opzioni_asincrone)



is_mqtt_connected = False
is_simulation_active = False
global_state = {}

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
        if msg.topic == "srs/admin/control":
            payload = json.loads(msg.payload.decode('utf-8'))
            comando = payload.get("command")
            if comando == "START":
                is_simulation_active = True
                print("[CONTROLLER] Simulazione AVVIATA. Watchdog attivato.")
                # Resettiamo tutti i timer per evitare allarmi istantanei
                ora = time.time()
                for inc_id in global_state:
                    for sem_id in global_state[inc_id]:
                        global_state[inc_id][sem_id]["last_seen"] = ora
            elif comando == "PAUSE":
                is_simulation_active = False
                print("[CONTROLLER] Simulazione IN PAUSA. Watchdog sospeso.")
            return # Esce dalla funzione senza cercare di salvare dati su InfluxDB
        
        parti_topic = msg.topic.split("/")
        incrocio_id = parti_topic[2]
        semaforo_id = parti_topic[3]
        
        payload = msg.payload.decode('utf-8')
        dati_semaforo = json.loads(payload)
        
        if incrocio_id not in global_state:
            global_state[incrocio_id] = {}
        
        # --- SALVATAGGIO IN RAM PER IL WATCHDOG ---
        global_state[incrocio_id][semaforo_id] = {
            "dati": dati_semaforo,
            "last_seen": time.time(),
            "alert_sent": False # Flag per non spammare l'allarme
        }
        # Salvataggio su InfluxDB
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

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="CONTROLLER_CLIENT")
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect 

print("Inizializzazione del Controller e connessione in HA...", flush=True)
utils.connetti_con_failover(client, "CONTROLLER_CLIENT", check_controller_connection)

try:
    while True:
        # Controllo Failover
        if not check_controller_connection():
            print("[CONTROLLER_CLIENT] Connessione persa! Rotazione verso il bilanciatore secondario in corso...", flush=True)
            utils.connetti_con_failover(client, "CONTROLLER_CLIENT", check_controller_connection)
            
        # --- IL WATCHDOG (CANE DA GUARDIA) ---
        if is_simulation_active: # <--- ORA SCATTA SOLO SE LA SIMULAZIONE È IN CORSO
            ora_attuale = time.time()
            for inc_id, semafori in global_state.items():
                for sem_id, info in semafori.items():
                    secondi_silenzio = ora_attuale - info["last_seen"]
                    
                    # Se il nodo tace da > 30s e l'allarme non è già scattato
                    if secondi_silenzio > 30 and not info.get("alert_sent", False):
                        nodo_guasto = sem_id
                        
                        client.publish("srs/alerts/node_down", nodo_guasto, qos=1)
                        info["alert_sent"] = True # Evita di inviare loop di allarmi per lo stesso nodo

        time.sleep(1) # Previene l'abuso della CPU e mantiene il ciclo stabile
        
except KeyboardInterrupt:
    print("\nSpegnimento del Controller")
    client.loop_stop()
    client.disconnect()