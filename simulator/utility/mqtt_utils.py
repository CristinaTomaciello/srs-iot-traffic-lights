import time
import random

def connetti_con_failover(client, client_id, check_connessione, brokers=["haproxy-1", "haproxy-2"]):
    """
    Gestisce la connessione MQTT e il failover per qualsiasi client.
    - client: L'istanza di paho.mqtt.client
    - client_id: Il nome del nodo (per stampare log leggibili)
    - check_connessione: Una funzione che ritorna True se il nodo è connesso
    - brokers: La lista dei bilanciatori
    """
    client.loop_stop()
    
    while not check_connessione():
        for broker in brokers:
            try:
                client.connect(broker, 1883, 10)
                client.loop_start()
                
                for _ in range(30):
                    if check_connessione():
                        print(f"[{client_id}] Connessione stabilita su {broker}", flush=True)
                        return
                    time.sleep(0.1)
                    
                # Se fallisce, spegniamo di nuovo il motore
                client.loop_stop()
            except Exception:
                client.loop_stop()
                
        if not check_connessione():
            time.sleep(1 + random.random() * 2)