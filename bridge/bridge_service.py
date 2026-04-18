import paho.mqtt.client as mqtt
import json
import requests
import time
import os
import queue
import threading

# Configurazione
MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
OPENCODE_URL = os.getenv("OPENCODE_URL", "http://brain_server:4096")
BRIDGE_CLIENT_ID = "SRS_BRIDGE_DISPATCHER"

OPENCODE_USER = os.getenv("OPENCODE_USER", "opencode")
OPENCODE_PASS = os.getenv("OPENCODE_PASS", "srs")

# Variabili di stato
opencode_session_id = None

# Coda per gestire gli allarmi in arrivo e processarli uno alla volta
alarm_queue = queue.Queue()

def get_opencode_session():
    """Crea una sessione persistente su OpenCode per il Bridge."""
    global opencode_session_id
    try:
        url = f"{OPENCODE_URL}/session"
        payload = {"title": "SRS-IOT Recovery Session"}
        response = requests.post(url, json=payload,auth=(OPENCODE_USER, OPENCODE_PASS), timeout=5)
        if response.status_code == 200:
            opencode_session_id = response.json().get("id")
            print(f"[BRIDGE] Sessione OpenCode creata: {opencode_session_id}")
            return True
    except Exception as e:
        print(f"[BRIDGE] Errore connessione a OpenCode: {e}")
    return False

def send_to_agent(agent_name, message_text):
    """Invia il comando all'Agente designato tramite OpenCode."""
    if not opencode_session_id:
        if not get_opencode_session(): return

    url = f"{OPENCODE_URL}/session/{opencode_session_id}/message"
    payload = {
        "agent": agent_name,
        "parts": [{"type": "text", "text": message_text}]
    }
    
    try:
        print(f"[BRIDGE] Inoltro allerta a Agente: {agent_name}...")
        res = requests.post(url, json=payload,auth=(OPENCODE_USER, OPENCODE_PASS), timeout=60) # Timeout lungo per l'IA
        if res.status_code == 200:
            print(f"[BRIDGE] Risposta Agente ricevuta con successo.")
            
            risposta_json = res.json()
            for part in risposta_json.get("parts", []):
                if part.get("type") == "reasoning":
                    print(f"\nPENSIERO DELL'IA:\n{part.get('text')}")
                elif part.get("type") == "text":
                    print(f"\nREPORT FINALE:\n{part.get('text')}\n" + "-"*40)
        else:
            print(f"[BRIDGE] L'Agente ha risposto con errore: {res.status_code}")
    except Exception as e:
        print(f"[BRIDGE] Fallimento durante la chiamata all'Agente: {e}")

def ai_worker():
    """Thread in background che processa un allarme alla volta dalla coda."""
    print("[BRIDGE-WORKER] Thread coda AI avviato e in attesa di allarmi")
    while True:
        # Resta in attesa finché non c'è un task
        task = alarm_queue.get() 
        node = task['node']
        prompt = task['prompt']
        
        print(f"\n[BRIDGE-WORKER] Prendo in carico l'allarme per {node} ({alarm_queue.qsize()} allarmi rimanenti in attesa)")
        
        # Invia ad OpenCode e aspetta pazientemente la fine del processo
        send_to_agent("recovery-agent", prompt)
        
        # Task completato
        alarm_queue.task_done()
        time.sleep(2) # Pausa di sicurezza per non stressare le API dell'LLM

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("[BRIDGE] Connesso al Broker MQTT.")
        client.subscribe("srs/alerts/recovery_needed")
        client.subscribe("srs/controller/heartbeat")
    else:
        print(f"[BRIDGE] Errore connessione MQTT: {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        
        # Watchdog del Nodo
        if msg.topic == "srs/alerts/recovery_needed":
            node = payload.get("node_id")
            last_state = payload.get("last_known_state")
            prompt = (
                f"EVENTO DI SISTEMA RICEVUTO: Allarme timeout sul nodo {node}. "
                f"Ultimo stato noto: {last_state}."
            )
            
            # Aggiunto alla coda per essere processato dal worker thread
            alarm_queue.put({"node": node, "prompt": prompt})
            print(f"[BRIDGE] Allarme {node} inserito in Coda. (Totale in coda: {alarm_queue.qsize()})")

        # ToDo
        elif msg.topic == "srs/controller/heartbeat":
            pass

    except Exception as e:
        print(f"[BRIDGE] Errore processamento messaggio: {e}")

#Avvia il worker thread AI in background prima di connettere MQTT
worker_thread = threading.Thread(target=ai_worker, daemon=True)
worker_thread.start()

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=BRIDGE_CLIENT_ID)
client.on_connect = on_connect
client.on_message = on_message

print("[BRIDGE] Avvio del servizio Bridge Dispatcher...")

# Tenta di connettersi a OpenCode prima di partire
while not get_opencode_session():
    print("[BRIDGE] In attesa che il Brain (OpenCode) sia online...")
    time.sleep(5)

# Avvia
client.connect(MQTT_BROKER, 1883, 60)
client.loop_forever()