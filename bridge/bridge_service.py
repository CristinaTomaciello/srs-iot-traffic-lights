import paho.mqtt.client as mqtt
import json
import requests
import time
import os
import queue
import threading
import sys
import uuid

# 1. CONFIGURAZIONE PERCORSI E UTILITY
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import simulator.utility.mqtt_utils as utils

# 2. IDENTITÀ DEL WORKER (Presa dal Docker Compose)
# Usiamo l'ID numerico per rendere i log leggibili (BRIDGE_1, BRIDGE_2, etc.)
WORKER_ID = os.getenv("WORKER_ID", "UNKNOWN")
MY_NAME = f"BRIDGE_{WORKER_ID}"

# Configurazione endpoint
MQTT_BROKER = os.getenv("MQTT_BROKER", "haproxy-1")
OPENCODE_URLS = [
    os.getenv("OPENCODE_URL_1", "http://haproxy-ai-1:4096"),
    os.getenv("OPENCODE_URL_2", "http://haproxy-ai-2:4096")
]
current_ai_host_index = 0
OPENCODE_AUTH = (os.getenv("OPENCODE_USER", "opencode"), os.getenv("OPENCODE_PASS", "srs"))

# Stato della connessione per l'utility di failover
is_mqtt_connected = False

# 3. CODE E LOGICA PARALLELA
hardware_queue = queue.Queue()
traffic_queue = queue.Queue()
traffic_agent_busy = False

def post_with_failover(endpoint_path, json_data, auth, timeout):
    """Esegue una POST HTTP provando i bilanciatori IA a rotazione in caso di crash."""
    global current_ai_host_index
    tentativi = 0
    
    while tentativi < len(OPENCODE_URLS):
        base_url = OPENCODE_URLS[current_ai_host_index]
        full_url = f"{base_url}{endpoint_path}"
        try:
            response = requests.post(full_url, json=json_data, auth=auth, timeout=timeout)
            return response
        except requests.exceptions.RequestException as e:
            print(f"[{MY_NAME}] ⚠️ Errore su {base_url}: {e}. Tento failover IA...", flush=True)
            current_ai_host_index = (current_ai_host_index + 1) % len(OPENCODE_URLS)
            tentativi += 1
            
    raise Exception("Tutti i bilanciatori IA sono offline o irraggiungibili!")


def send_to_agent_isolated(agent_name, message_text, prefix="[BRIDGE]"):
    """Crea una sessione isolata nell'LLM usando il failover HTTP."""
    try:
        payload_session = {"title": f"Task: {agent_name} su {MY_NAME}"}
        # Sostituito requests.post con post_with_failover
        s_res = post_with_failover("/session", payload_session, OPENCODE_AUTH, 10)
        
        if s_res.status_code == 200:
            sid = s_res.json().get("id")
            payload_msg = {"agent": agent_name, "parts": [{"type": "text", "text": message_text}]}
            
            # Sostituito requests.post con post_with_failover
            res = post_with_failover(f"/session/{sid}/message", payload_msg, OPENCODE_AUTH, 300)
            
            if res.status_code == 200:
                for part in res.json().get("parts", []):
                    if part.get("type") == "text": 
                        print(f"\n{prefix} [{MY_NAME}] REPORT: {part.get('text')}\n" + "-"*40, flush=True)
            else:
                print(f"{prefix} [{MY_NAME}] Errore Agente: {res.status_code}", flush=True)
        else:
            print(f"{prefix} [{MY_NAME}] Errore Sessione: {s_res.status_code}", flush=True)
            
    except Exception as e: 
        print(f"{prefix} [{MY_NAME}] Timeout o fallimento chiamata: {e}", flush=True)
# --- WORKERS ---

def hardware_worker():
    print(f"[{MY_NAME}] [HW-WORKER] Pronto per gestire guasti critici.", flush=True)
    while True:
        task = hardware_queue.get()
        node = task['node']
        print(f"[{MY_NAME}] [HW-WORKER] Processo allarme {node}. Code residue: {hardware_queue.qsize()}", flush=True)
        send_to_agent_isolated("hardware-orchestrator", task['prompt'], "[HW-AGENT]")
        hardware_queue.task_done()

def traffic_worker():
    global traffic_agent_busy
    print(f"[{MY_NAME}] [TR-WORKER] Pronto per ronde del traffico.", flush=True)
    while True:
        task = traffic_queue.get()
        traffic_agent_busy = True
        print(f"[{MY_NAME}] [TR-WORKER] Avvio analisi traffico proattiva.", flush=True)
        send_to_agent_isolated("traffic-orchestrator", task['prompt'], "[TR-AGENT]")
        traffic_agent_busy = False
        traffic_queue.task_done()

# --- MQTT HANDLERS ---

def check_connection():
    return is_mqtt_connected

def on_connect(client, userdata, flags, rc, properties=None):
    global is_mqtt_connected
    if rc == 0:
        is_mqtt_connected = True
        print(f"[{MY_NAME}] Online! Iscrizione al gruppo condiviso...", flush=True)
        # Shared Subscription: EMQX distribuisce i messaggi tra i vari bridge
        client.subscribe("$share/bridge_workers/srs/alerts/recovery_needed", qos=1)
        client.subscribe("$share/bridge_workers/srs/alerts/traffic_check", qos=1)
    else:
        print(f"[{MY_NAME}] Connessione fallita (codice {rc})", flush=True)

def on_disconnect(client, userdata, flags, rc, properties=None):
    global is_mqtt_connected
    is_mqtt_connected = False
    print(f"[{MY_NAME}] Connessione persa! Il main loop attiverà il failover.", flush=True)

def on_message(client, userdata, msg):
    try:
        # 1. WATCHDOG HARDWARE
        if msg.topic == "srs/alerts/recovery_needed":
            payload = json.loads(msg.payload.decode())
            node = payload.get("node_id", "SCONOSCIUTO")
            prompt = f"EVENTO DI SISTEMA: Allarme timeout sul nodo {node}."
            hardware_queue.put({"node": node, "prompt": prompt})
            print(f"[{MY_NAME}] -> Ricevuto guasto {node}. Messo in coda HW.", flush=True)

        # 2. WATCHDOG TRAFFICO
        elif msg.topic == "srs/alerts/traffic_check":
            global traffic_agent_busy
            if traffic_agent_busy or not traffic_queue.empty():
                print(f"[{MY_NAME}] -> Ignorato: Agente ancora occupato.", flush=True)
            else:
                prompt = "[PROATTIVO] Verifica code e ottimizza la rete."
                traffic_queue.put({"prompt": prompt})
                print(f"[{MY_NAME}] -> Ricevuta ronda. Messa in coda Traffico.", flush=True)
    except Exception as e:
        print(f"[{MY_NAME}] Errore parsing: {e}", flush=True)

# --- AVVIO SISTEMA ---

# Avvio thread paralleli
threading.Thread(target=hardware_worker, daemon=True).start()
threading.Thread(target=traffic_worker, daemon=True).start()

# Configurazione Client MQTT (ID unico fondamentale!)
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"BRIDGE_CLIENT_{MY_NAME}")
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

# Attesa OpenCode
print(f"[{MY_NAME}] Controllo disponibilità bilanciatori IA...", flush=True)
while True:
    try:
        # Basta che uno dei due HAProxy risponda per dichiarare l'IA pronta
        requests.get(OPENCODE_URLS[current_ai_host_index], timeout=3) 
        break
    except Exception:
        print(f"[{MY_NAME}] IA non ancora pronta. Riprovo...", flush=True)
        current_ai_host_index = (current_ai_host_index + 1) % len(OPENCODE_URLS)
        time.sleep(5)

# Loop principale con failover HAProxy
try:
    while True:
        if not is_mqtt_connected:
            # Sfruttiamo la tua funzione in mqtt_utils
            utils.connetti_con_failover(client, MY_NAME, check_connection)
        
        time.sleep(5) # Controllo periodico dello stato
except KeyboardInterrupt:
    print(f"[{MY_NAME}] Spegnimento in corso...")
    client.disconnect()