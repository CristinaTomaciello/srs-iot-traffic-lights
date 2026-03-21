import paho.mqtt.client as mqtt
import json
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

# CONFIGURAZIONE INFLUXDB
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "supersecrettoken123"
INFLUX_ORG = "srs_org"
INFLUX_BUCKET = "traffic_data"

# Connettiamo al database InfluxDB e prepariamo l'API di scrittura
db_client = influxdb_client.InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = db_client.write_api(write_options=SYNCHRONOUS)


global_state = {}

def on_connect(client, userdata, flags, reason_code, properties):
    print("Central Controller online")
    client.subscribe("srs/edge/+/+/stato")
    print("In ascolto degli stati dei semafori e salvataggio storico in corso")

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
        .field("durata_verde", int(dati_semaforo.get('green_duration', 0))) # Aggiungi questo
        
        # Scrive fisicamente nel database
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=punto_storico)
        
        print(f"STORICO SALVATO: {incrocio_id}/{semaforo_id} -> {dati_semaforo.get('auto_in_coda')} auto")
        
    except json.JSONDecodeError:
        print(f"Errore di decodifica JSON dal topic {msg.topic}")
    except Exception as e:
        print(f"Errore salvataggio DB: {e}")

# Avvio del Sistema
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message

print("Connessione al broker MQTT in corso")
client.connect("localhost", 1883, 60)

# Avviamo il loop in un thread separato per gestire le comunicazioni MQTT senza bloccare il programma principale
client.loop_start()

try:
    while True:
        pass
except KeyboardInterrupt:
    print("\nSpegnimento del Controller")
    client.loop_stop()
    client.disconnect()