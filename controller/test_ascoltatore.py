import paho.mqtt.client as mqtt

# Cosa fare quando arriva un messaggio
def on_message(client, userdata, message):
    testo = message.payload.decode("utf-8")
    print(f"DATO RICEVUTO dal topic '{message.topic}': {testo}")

# Creto il client e lo si collega al Docker (localhost, porta 1883)
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_message = on_message

print("Connessione al broker MQTT in corso...")
client.connect("localhost", 1883)

# Ci sintonizziamo sul "canale" di test
client.subscribe("semafori/test")
print("Connesso! In ascolto dei semafori. Premi Ctrl+C per fermare.")

# Rimane in ascolto all'infinito
client.loop_forever()