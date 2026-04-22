import paho.mqtt.client as mqtt
import json
import requests
import time
import os
import queue
import threading

# Configurazione
MQTT_BROKER = os.getenv("MQTT_BROKER", "srs-haproxy-1") # Assicurati che il nome corrisponda al docker-compose
OPENCODE_URL = os.getenv("OPENCODE_URL", "http://brain_server:4096")
BRIDGE_CLIENT_ID = "SRS_BRIDGE_DISPATCHER"

OPENCODE_AUTH = (os.getenv("OPENCODE_USER", "opencode"), os.getenv("OPENCODE_PASS", "srs"))

# 1. DUE CODE SEPARATE PER IL PARALLELISMO
hardware_queue = queue.Queue()
traffic_queue = queue.Queue()
traffic_agent_busy = False

def send_to_agent_isolated(agent_name, message_text, prefix="[BRIDGE]"):
    """
    Crea una SESSIONE ISOLATA per garantire 
    che chiamate parallele non inquinino il contesto dell'LLM.
    """
    try:
        # Crea sessione effimera
        url_session = f"{OPENCODE_URL}/session"
        payload_session = {"title": f"Task: {agent_name}"}
        s_res = requests.post(url_session, json=payload_session, auth=OPENCODE_AUTH, timeout=5)
        
        if s_res.status_code == 200:
            sid = s_res.json().get("id")
            
            # Invia messaggio a questa specifica sessione
            url_message = f"{OPENCODE_URL}/session/{sid}/message"
            payload_msg = {"agent": agent_name, "parts": [{"type": "text", "text": message_text}]}
            
            res = requests.post(url_message, json=payload_msg, auth=OPENCODE_AUTH, timeout=300)
            
            if res.status_code == 200:
                for part in res.json().get("parts", []):
                    if part.get("type") == "text": 
                        print(f"\n{prefix} REPORT: {part.get('text')}\n" + "-"*40)
            else:
                print(f"{prefix} L'Agente ha risposto con errore: {res.status_code}")
        else:
            print(f"{prefix} Errore creazione sessione: {s_res.status_code}")
            
    except Exception as e: 
        print(f"{prefix} Timeout o fallimento chiamata: {e}")

# ==========================================
# WORKER THREADS (Lavorano in Parallelo)
# ==========================================

def hardware_worker():
    print("[HW-WORKER] Avviato e in attesa di guasti critici...")
    while True:
        task = hardware_queue.get()
        node = task['node']
        print(f"\n[HW-WORKER] Preso in carico allarme {node} (In attesa: {hardware_queue.qsize()})")
        send_to_agent_isolated("hardware-orchestrator", task['prompt'], "[HW-AGENT]")
        hardware_queue.task_done()

def traffic_worker():
    global traffic_agent_busy
    print("[TR-WORKER] Avviato e in attesa di ronde del traffico...")
    while True:
        task = traffic_queue.get()
        traffic_agent_busy = True
        print(f"\n[TR-WORKER] Avvio ronda di controllo (In attesa: {traffic_queue.qsize()})")
        send_to_agent_isolated("traffic-orchestrator", task['prompt'], "[TR-AGENT]")
        traffic_agent_busy = False
        traffic_queue.task_done()

# ==========================================
# MQTT HANDLERS
# ==========================================

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("[BRIDGE] Connesso al Broker MQTT.")
        client.subscribe("srs/alerts/recovery_needed")
        client.subscribe("srs/alerts/traffic_check")
    else:
        print(f"[BRIDGE] Errore connessione MQTT: {rc}")

def on_message(client, userdata, msg):
    try:
        # 1. WATCHDOG HARDWARE
        if msg.topic == "srs/alerts/recovery_needed":
            payload = json.loads(msg.payload.decode())
            node = payload.get("node_id", "SCONOSCIUTO")
            last_state = payload.get("last_known_state", "SCONOSCIUTO")
            prompt = f"EVENTO DI SISTEMA RICEVUTO: Allarme timeout sul nodo {node}. Ultimo stato: {last_state}."
            
            # Smistamento diretto nella coda Hardware
            hardware_queue.put({"node": node, "prompt": prompt})
            print(f"[BRIDGE] -> Allarme {node} inserito in Coda Hardware.")

        # 2. WATCHDOG TRAFFICO (Innescato dal Controller)
        elif msg.topic == "srs/alerts/traffic_check":
            global traffic_agent_busy
            if traffic_agent_busy or not traffic_queue.empty():
                print("[BRIDGE] -> TRIGGER IGNORATO: L'Agente Traffico è ancora impegnato in un ragionamento.")
            else:
                prompt = "[PROATTIVO] Esegui ronda di controllo globale: verifica code e ottimizza la rete."
                traffic_queue.put({"prompt": prompt})
                print("[BRIDGE] -> Trigger Controller ricevuto. Ronda inserita in Coda Traffico.")
    except Exception as e:
        print(f"[BRIDGE] Errore parsing messaggio: {e}")

# ==========================================
# AVVIO DEL SISTEMA
# ==========================================

# 1. Avvia i Thread in parallelo
threading.Thread(target=hardware_worker, daemon=True).start()
threading.Thread(target=traffic_worker, daemon=True).start()

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=BRIDGE_CLIENT_ID)
client.on_connect = on_connect
client.on_message = on_message

print("[BRIDGE] Avvio del servizio Bridge Dispatcher (Modalità Parallela)...")

# 2. Attesa che il server OpenCode sia pronto
while True:
    try:
        # Un semplice GET all'URL base per assicurarsi che il container sia su
        requests.get(OPENCODE_URL, timeout=3) 
        print("[BRIDGE] Server OpenCode rilevato. Avvio connettività MQTT.")
        break
    except requests.ConnectionError:
        print("[BRIDGE] In attesa del server OpenCode...")
        time.sleep(5)

# 3. Connessione MQTT e Loop
client.connect(MQTT_BROKER, 1883, 60)
client.loop_forever()