import paho.mqtt.client as mqtt
import json
import influxdb_client
import time
from influxdb_client.client.write_api import ASYNCHRONOUS
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import simulator.utility.mqtt_utils as utils

# CONFIGURAZIONE INFLUXDB
INFLUX_URL = "http://influxdb:8086"
INFLUX_TOKEN = "supersecrettoken123"
INFLUX_ORG = "srs_org"
INFLUX_BUCKET = "traffic_data"

# Connettiamo al database InfluxDB e prepariamo l'API di scrittura
db_client = influxdb_client.InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = db_client.write_api(write_options=ASYNCHRONOUS)

is_mqtt_connected = False
global_state = {}

def on_connect(client, userdata, flags, rc, properties):
    global is_mqtt_connected
    if rc == 0:
        is_mqtt_connected = True
        print("[CONTROLLER_CLIENT] Central Controller online e connesso!", flush=True)
        client.subscribe("srs/edge/+/+/stato")

def on_message(client, userdata, msg):
    try:
        parti_topic = msg.topic.split("/")
        incrocio_id = parti_topic[2]
        semaforo_id = parti_topic[3]
        
        payload = msg.payload.decode('utf-8')
        dati_semaforo = json.loads(payload)
        
        if incrocio_id not in global_state:
            global_state[incrocio_id] = {}
        global_state[incrocio_id][semaforo_id] = dati_semaforo
        
        # Creazione del punto storico da salvare in InfluxDB
        punto_storico = influxdb_client.Point("stato_traffico") \
        .tag("incrocio", incrocio_id) \
        .tag("direzione", semaforo_id) \
        .field("auto_in_coda", int(dati_semaforo.get('auto_in_coda', 0))) \
        .field("colore", dati_semaforo.get('colore', 'UNKNOWN')) \
        .field("durata_verde", int(dati_semaforo.get('green_duration', 0)))
        
        # Scrive fisicamente nel database
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

# Avvio del Sistema
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="CONTROLLER_CLIENT")
client.on_connect = on_connect
client.on_message = on_message
# FIX FONDAMENTALE: Assegnazione dell'evento di disconnessione per attivare il failover
client.on_disconnect = on_disconnect 

print("Inizializzazione del Controller e connessione in HA...", flush=True)

# --- CONNESSIONE INIZIALE MODULARE ---
# Deleghiamo la gestione dei socket e della rotazione al nostro modulo condiviso
utils.connetti_con_failover(client, "CONTROLLER_CLIENT", check_controller_connection)

# --- CICLO PRINCIPALE CON MONITORAGGIO FAILOVER ---
try:
    while True:
        # Il cuore dell'Alta Affidabilità per il Controller:
        # Se cade HAProxy-1, il ciclo se ne accorge e cerca HAProxy-2 in automatico.
        if not check_controller_connection():
            print("[CONTROLLER_CLIENT] Connessione persa! Rotazione verso il bilanciatore secondario in corso...", flush=True)
            utils.connetti_con_failover(client, "CONTROLLER_CLIENT", check_controller_connection)
            
        # Lascia respirare la CPU (essenziale per non soffocare InfluxDB o Docker)
        time.sleep(1)
        
except KeyboardInterrupt:
    print("\nSpegnimento del Controller")
    client.loop_stop()
    client.disconnect()