import paho.mqtt.client as mqtt
import time
import threading
import sys

# --- CONFIGURAZIONE ---
# Usa localhost se esegui lo script dal tuo PC (con la porta 1883 esposta da Docker)
# Usa "srs-haproxy-1" se lo esegui dentro un container nella rete srs-net
BROKER = "127.0.0.1" 
PORT = 1883
TOPIC = "srs/test/stress"
NUM_WORKERS = 20            # Numero di connessioni parallele
MSG_PER_SEC_PER_WORKER = 500 # Messaggi al secondo per ogni worker
QOS = 1                    # 0 per massimo throughput di rete, 1 per stressare disco/ACK

# Generiamo un payload dummy fisso (es. circa 200 byte per simulare la telemetria)
PAYLOAD = '{"incrocio": "INC_TEST", "auto": 42, "status": "VERDE", "padding": "' + 'X'*100 + '"}'

def worker_task(worker_id):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"FLOODER_{worker_id}")
    try:
        client.connect(BROKER, PORT, 60)
        client.loop_start()
        print(f"[Worker {worker_id}] Connesso. Inizio flooding a {MSG_PER_SEC_PER_WORKER} msg/s...")
        
        while True:
            start_time = time.time()
            for _ in range(MSG_PER_SEC_PER_WORKER):
                client.publish(TOPIC, PAYLOAD, qos=QOS)
            
            # Calcola quanto tempo ci ha messo per inviare il batch e dorme per il resto del secondo
            elapsed = time.time() - start_time
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            else:
                print(f"[Worker {worker_id}] WARNING: Sistema troppo lento, impossibile mantenere il rate!")
                
    except Exception as e:
        print(f"[Worker {worker_id}] ERRORE: {e}")

if __name__ == "__main__":
    print(f"--- AVVIO STRESS TEST MQTT ---")
    print(f"Target: {BROKER}:{PORT} | Workers: {NUM_WORKERS} | Rate Totale: {NUM_WORKERS * MSG_PER_SEC_PER_WORKER} msg/s")
    
    threads = []
    for i in range(NUM_WORKERS):
        t = threading.Thread(target=worker_task, args=(i,))
        t.daemon = True
        t.start()
        threads.append(t)
        time.sleep(0.1) # Evita il Thundering Herd in fase di connessione

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nTest interrotto dall'utente.")
        sys.exit(0)
