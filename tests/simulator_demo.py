import paho.mqtt.client as mqtt
import json
import time
import random

# Configurazione MQTT
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect("localhost", 1883)

# Lista degli incroci simulati
incroci = ["INC_CENTRO", "INC_STAZIONE", "INC_PERIFERIA", "INC_STADIO"]
direzioni = ["NORD", "SUD", "EST", "OVEST"]

print("🚦 Avvio Simulatore Speciale per Demo (InfluxDB)...")
print("Tieni acceso il main_controller.py!")

try:
    while True:
        # 1. Simula Traffico Normale (Dati di Stato)
        for incrocio in incroci:
            for direzione in direzioni:
                # Generiamo code casuali tra 2 e 15 auto
                queue = random.randint(2, 15)
                # Durata verde base (es. 30 secondi)
                green_time = 30
                
                # --- SIMULAZIONE IA (Reazione) ---
                # Se la coda è > 12, simuliamo l'intervento dell'MCP
                # Il colore diventa VERDE e aumentiamo la durata a 60s
                color = random.choice(["ROSSO", "VERDE", "GIALLO"])
                if queue > 12:
                    color = "VERDE" # IA forza il verde per smaltire
                    green_time = 60 # IA aumenta la durata

                payload = {
                    "stato": color, 
                    "coda_auto": queue,
                    "durata_verde": green_time # <-- Nuovo campo fondamentale!
                }
                
                topic = f"srs/edge/{incrocio}/{direzione}/stato"
                client.publish(topic, json.dumps(payload))
        
        # 2. Simula Auto Evacuate (Il Throughput - Cella 3)
        # Ogni ciclo, facciamo finta che 3-7 auto abbiano lasciato la città
        cars_out = random.randint(3, 7)
        out_payload = {"count": cars_out, "timestamp": time.time()}
        # Nota il topic speciale "OUT"
        client.publish("srs/edge/CITTA/OUT/stato", json.dumps(out_payload))

        print(".", end="", flush=True) # Feedback visivo
        time.sleep(5) # Aggiornamento ogni 5 secondi (coerente con l'auto-refresh)

except KeyboardInterrupt:
    print("\nSimulatore fermato.")
    client.disconnect()