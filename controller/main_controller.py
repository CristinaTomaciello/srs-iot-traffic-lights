import paho.mqtt.client as mqtt
import json

# Vista globale che tiene traccia dello stato di tutti gli incroci e semafori
global_state = {
    "incrocio_1": {}
}

def on_connect(client, userdata, flags, reason_code, properties):
    print("Central Controller online!")
    # Ci iscriviamo SOLO ai topic di stato. IGNORIAMO i topic "inbox_auto" del P2P.
    client.subscribe("srs/edge/+/+/stato")
    print("In ascolto degli stati dei semafori")

def on_message(client, userdata, msg):
    try:
        # Estraiamo i dati dell'incrocio e del semaforo dal topic ricevuto
        parti_topic = msg.topic.split("/")
        incrocio_id = parti_topic[2]
        semaforo_id = parti_topic[3]
        
        # Leggiamo il JSON inviato dal nodo Edge
        payload = msg.payload.decode('utf-8')
        dati_semaforo = json.loads(payload)
        
        # Aggiorniamo la nostra "Vista Globale"
        if incrocio_id not in global_state:
            global_state[incrocio_id] = {}
            
        global_state[incrocio_id][semaforo_id] = dati_semaforo
        
        print(f"MAPPA AGGIORNATA: {incrocio_id} -> {semaforo_id} è {dati_semaforo.get('colore')} con {dati_semaforo.get('auto_in_coda')} auto.")
        
        
    except json.JSONDecodeError:
        print(f"Errore di decodifica JSON dal topic {msg.topic}")
    except Exception as e:
        print(f"Errore imprevisto: {e}")

# Metodo per inviare comandi di emergenza
def send_global_command(client, comando):
    payload = json.dumps(comando)
    client.publish("srs/central/comandi/emergenza", payload, qos=1)
    print(f"COMANDO GLOBALE INVIATO: {payload}")

# --- Avvio del Sistema ---
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message

print("Connessione al broker MQTT in corso...")
client.connect("localhost", 1883, 60)

# Avviamo il loop in un thread separato per gestire le comunicazioni MQTT senza bloccare il programma principale
client.loop_start()

try:
    # Manteniamo in vita il programma principale
    while True:
        pass
except KeyboardInterrupt:
    print("\nSpegnimento del Controller")
    client.loop_stop()
    client.disconnect()