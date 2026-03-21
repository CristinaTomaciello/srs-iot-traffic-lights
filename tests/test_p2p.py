import paho.mqtt.client as mqtt
import json
import time

# Configurazione del client MQTT per il test
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect("localhost", 1883)

print("🚦 INIZIO SIMULAZIONE P2P...")
time.sleep(1)

# --- ATTO 1: Stato Iniziale ---
print("\n[NORD] Aggiorno il mio stato: ho 1 auto e sono VERDE.")
stato_nord = {"colore": "VERDE", "auto_in_coda": 1}
client.publish("srs/edge/incrocio_1/nord/stato", json.dumps(stato_nord))
time.sleep(2)

# --- ATTO 2: L'Handoff (Il passaggio P2P dell'Agente Auto) ---
print("\n[NORD -> EST] Invio l'auto 'auto_123' al semaforo Est (Handoff P2P)...")
auto_payload = {"id": "auto_123", "path": ["NORD", "EST"], "timestamp": time.time()}
# Nota il topic: inbox_auto. Il Controller Centrale NON deve leggere questo!
client.publish("srs/edge/incrocio_1/est/inbox_auto", json.dumps(auto_payload))
time.sleep(2)

# --- ATTO 3: Aggiornamento Finale ---
print("\n[NORD] L'auto è passata. Aggiorno il mio stato: 0 auto.")
stato_nord_nuovo = {"colore": "VERDE", "auto_in_coda": 0}
client.publish("srs/edge/incrocio_1/nord/stato", json.dumps(stato_nord_nuovo))

print("[EST] Ho ricevuto l'auto! Aggiorno il mio stato: 1 auto, sono ROSSO.")
stato_est = {"colore": "ROSSO", "auto_in_coda": 1}
client.publish("srs/edge/incrocio_1/est/stato", json.dumps(stato_est))

print("\n✅ Simulazione completata!")
client.disconnect()