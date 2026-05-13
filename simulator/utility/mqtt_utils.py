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
    # Solo ferma il loop se NON è già fermo
    try:
        client.loop_stop()
    except:
        pass
    
    # Disconnetti completamente prima di riconnettersi
    try:
        client.disconnect()
    except:
        pass
    
    time.sleep(0.5)  # Piccola pausa per lasciare il cleanup
    
    while not check_connessione():
        for broker in brokers:
            try:
                print(f"[{client_id}] 🔄 Tentativo connessione a {broker}...", flush=True)
                client.connect(broker, 1883, 10)
                client.loop_start()
                
                for _ in range(30):
                    if check_connessione():
                        print(f"[{client_id}] ✅ Connessione stabilita su {broker}", flush=True)
                        return
                    time.sleep(0.1)
                    
                # Se fallisce, spegniamo di nuovo il motore
                client.loop_stop()
            except Exception as e:
                print(f"[{client_id}] ❌ Errore su {broker}: {e}", flush=True)
                try:
                    client.loop_stop()
                except:
                    pass
                
        if not check_connessione():
            print(f"[{client_id}] ⏳ Retry tra 2s...", flush=True)
            time.sleep(1 + random.random() * 2)
