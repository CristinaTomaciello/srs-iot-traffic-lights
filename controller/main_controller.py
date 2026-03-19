import paho.mqtt.client as mqtt
import json


"""Controller Centrale per la gestione dei semafori"""
def on_connect(client, userdata, flags, reason_code, properties):
    print("Controller Centrale connesso al Broker MQTT!")
    # Iscrizione a TUTTI i messaggi che iniziano con "semafori/"
    client.subscribe("semafori/+") 
    print("In ascolto dei dati dai nodi semaforici")


'''Il controller centrale riceve i dati dai nodi semaforici
e li elabora per visualizzare lo stato attuale dei semafori e il numero di auto in coda.'''
def on_message(client, userdata, msg):
    try:
        # Trasforma il messaggio ricevuto in un dizionario Python
        payload = msg.payload.decode('utf-8')
        dati = json.loads(payload)
        
        
        print(f"Aggiornamento da {msg.topic} -> Stato: {dati.get('stato')}, Code: {dati.get('coda_auto')} auto")
    except json.JSONDecodeError:
        print(f"Errore: Ricevuto un messaggio non valido sul topic {msg.topic}")

# Configurazione del client
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message

# Connessione al broker Docker
print("Tentativo di connessione al broker (localhost:1883)...")
client.connect("localhost", 1883, 60)


client.loop_forever()