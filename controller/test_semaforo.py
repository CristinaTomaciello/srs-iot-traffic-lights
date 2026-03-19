import paho.mqtt.client as mqtt

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect("localhost", 1883)

messaggio = '{"id": "semaforo_nord", "stato": "verde", "coda_auto": 12}'

print(f"Invio dati al broker: {messaggio}")
client.publish("semafori/test", messaggio)
print("Messaggio inviato con successo!")