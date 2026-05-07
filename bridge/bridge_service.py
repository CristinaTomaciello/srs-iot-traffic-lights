import paho.mqtt.client as mqtt
import json
import time
import os
import sys
import uuid
import asyncio
import aiohttp
import re
import redis
from typing import Optional
import redis.exceptions

# 1. CONFIGURAZIONE PERCORSI E UTILITY
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import simulator.utility.mqtt_utils as utils
from simulator.utility.influx_utils import write_dual, INFLUX_BUCKET, INFLUX_ORG, log_audit

main_loop = None

# 2. IDENTITÀ E CONFIGURAZIONI
WORKER_ID = os.getenv("WORKER_ID", "UNKNOWN")
MY_NAME = f"BRIDGE_{WORKER_ID}"

MQTT_BROKER = os.getenv("MQTT_BROKER", "haproxy-1")
OPENCODE_URLS = [
    os.getenv("OPENCODE_URL_1", "http://haproxy-ai-1:4096"),
    os.getenv("OPENCODE_URL_2", "http://haproxy-ai-2:4096")
]
OPENCODE_AUTH = aiohttp.BasicAuth(
    os.getenv("OPENCODE_USER", "opencode"),
    os.getenv("OPENCODE_PASS", "srs")
)

# 3. CODE ASINCRONE E STATI
hardware_queue = asyncio.Queue()
traffic_queue = asyncio.Queue()
mqtt_client = None
is_mqtt_connected = False

# ---------------------------------------------------------
# 4. REDIS LOCK GLOBALE (CON RUNTIME FAILOVER)
# ---------------------------------------------------------

REDIS_URLS = [url.strip() for url in os.getenv("REDIS_URLS", "redis://redis-lock-1:6379,redis://redis-lock-2:6379,redis://redis-lock-3:6379").split(",")]
current_redis_index = 0
redis_client = None

def get_redis_client():
    """Restituisce un client Redis vivo, facendo failover se necessario."""
    global redis_client, current_redis_index
    
    # Se abbiamo un client, verifichiamo che sia ancora vivo
    if redis_client:
        try:
            redis_client.ping()
            return redis_client
        except redis.exceptions.ConnectionError:
            log_event("REDIS", "⚠️", "Nodo Redis caduto. Inizio failover...")
            redis_client = None

    # Se non c'è client o è caduto, cicliamo la lista per trovarne uno vivo
    for _ in range(len(REDIS_URLS)):
        url = REDIS_URLS[current_redis_index]
        try:
            r = redis.Redis.from_url(url, socket_connect_timeout=2, socket_timeout=2)
            r.ping()
            redis_client = r
            log_event("REDIS", "✅", f"Agganciato al nodo: {url}")
            return redis_client
        except redis.exceptions.ConnectionError:
            # Passa al prossimo URL della lista
            current_redis_index = (current_redis_index + 1) % len(REDIS_URLS)
            
    return None

def acquire_global_traffic_lock() -> bool:
    client = get_redis_client()
    
    if not client:
        # FAIL-CLOSED: Se TUTTI e 3 i Redis sono morti, meglio non fare nulla 
        # piuttosto che far partire tutti i Bridge in parallelo distruggendo InfluxDB.
        log_event("REDIS", "🚨", "CRITICO: Tutti i nodi Redis offline! Ronda annullata per sicurezza.")
        return False 
        
    try:
        return bool(client.set("global_lock:traffic_round", MY_NAME, nx=True, px=45000))
    except redis.exceptions.ConnectionError:
        return False

# ---------------------------------------------------------
# 5. UTILITY & METRICHE
# ---------------------------------------------------------
def log_event(worker: str, emoji: str, message: str):
    print(f"[{MY_NAME}] {emoji} [{worker}] {message}", flush=True)

def escalate_to_human_mqtt(node_id: str):
    if mqtt_client and is_mqtt_connected:
        payload = {"node_id": node_id, "status": "ESCALATED", "reason": "AI_EXHAUSTED", "timestamp": time.time()}
        mqtt_client.publish("srs/alerts/human_escalation", json.dumps(payload), qos=2)
        log_event("FALLBACK", "📣", f"Escalation MQTT per {node_id}")

latency_stats = {"total": 0, "time": 0.0, "min": float("inf"), "max": 0.0, "last10": []}

def update_latency(duration: float):
    latency_stats["total"] += 1
    latency_stats["time"] += duration
    latency_stats["min"] = min(latency_stats["min"], duration)
    latency_stats["max"] = max(latency_stats["max"], duration)
    latency_stats["last10"] = (latency_stats["last10"] + [duration])[-10:]

# ---------------------------------------------------------
# 6. ESTRAZIONE JSON INTELLIGENTE (REGEX)
# ---------------------------------------------------------
def extract_valid_json(text: str) -> Optional[dict]:
    """Cerca qualsiasi blocco JSON valido all'interno della risposta testuale dell'IA"""
    try:
        # Cerca il primo { e l'ultimo } nella stringa
        match = re.search(r'\{.*\}', text.replace('\n', ''), re.DOTALL)
        if match:
            parsed = json.loads(match.group(0))
            # Verifica che abbia almeno una delle nostre firme di sicurezza
            if any(key in parsed for key in ["final_report", "action", "outcome"]):
                return parsed
    except json.JSONDecodeError:
        pass
    return None

# ---------------------------------------------------------
# 7. AGENTI
# ---------------------------------------------------------
async def post_with_failover(endpoint_path: str, json_data: dict, timeout: int) -> Optional[aiohttp.ClientResponse]:
    last_err = None
    for url in OPENCODE_URLS:
        try:
            async with aiohttp.ClientSession(auth=OPENCODE_AUTH, timeout=aiohttp.ClientTimeout(total=timeout)) as session:
                start = time.monotonic()
                resp = await session.post(f"{url}{endpoint_path}", json=json_data)
                update_latency(time.monotonic() - start)
                await resp.read()
                return resp
        except Exception as e:
            last_err = e
    raise Exception(f"Tutti i bilanciatori offline. Errore: {last_err}")

async def send_to_agent_isolated(agent_name: str, message_text: str, node_id: str = None):
    try:
        log_event(agent_name.upper(), "⏳", "Creazione sessione...")
        s_res = await post_with_failover("/session", {"title": f"Task su {MY_NAME}"}, 10)
        
        if s_res.status != 200: return
        sid = (await s_res.json()).get("id")
        
        current_prompt = message_text

        # IL POLLING DI SICUREZZA (Necessario per aspettare i tool)
        for attempt in range(1, 16):
            res = await post_with_failover(f"/session/{sid}/message", {"agent": agent_name, "parts": [{"type": "text", "text": current_prompt}]}, 600)
            if res.status != 200:
                await asyncio.sleep(2)
                continue

            data = await res.json()
            raw_text = ""
            has_tool_calls = False
            
            # Analisi risposta strutturale
            for part in data.get("parts", []):
                if part.get("type") == "text":
                    raw_text += part['text'] + " "
                elif part.get("type") in ["call", "tool_call", "function_call"]:
                    has_tool_calls = True

            # Estrattore JSON Indipendente (Senza curarsi di <thinking> o format)
            parsed_json = extract_valid_json(raw_text)

            # 1. SUCCESSO: JSON Trovato
            if parsed_json:
                final_str = json.dumps(parsed_json)
                log_event(agent_name.upper(), "✅", f"REPORT FINALE: {final_str}")
                log_audit(MY_NAME, "AI_DECISION", final_str, level="SUCCESS")
                return 

            # 2. STEP INTERMEDIO: L'IA sta usando i tool
            elif has_tool_calls:
                log_event(agent_name.upper(), "🔄", f"Tool in uso (Ciclo {attempt}/15). Attendo...")
                current_prompt = "Tool execution recorded. Proceed and output ONLY the final JSON."
                await asyncio.sleep(1)

            # 3. ERRORE DI FORMATO: L'IA ha parlato invece di restituire JSON
            else:
                log_event(agent_name.upper(), "⚠️", f"Testo non valido (Ciclo {attempt}/15). Correggo...")
                current_prompt = 'SYSTEM WARNING: Invalid output. You MUST return ONLY a valid JSON object.'
                await asyncio.sleep(1)

        # 4. FALLBACK: Cicli esauriti
        log_event(agent_name.upper(), "🔥", "Timeout: L'agente si è bloccato.")
        if "hardware" in agent_name:
            escalate_to_human_mqtt(node_id)
        else:
            log_event("FALLBACK", "ℹ️", "Ottimizzazione scartata. Mantenimento parametri default.")

    except Exception as e:
        log_event("SYSTEM", "🔥", f"Fallimento critico: {e}")

# ---------------------------------------------------------
# 8. WORKER & MQTT
# ---------------------------------------------------------
async def hardware_worker():
    while True:
        task = await hardware_queue.get()
        log_event("HW-WORKER", "🛠️", f"In carico: {task['node']}")
        await send_to_agent_isolated("hardware-orchestrator", task['prompt'], node_id=task['node'])
        hardware_queue.task_done()

async def traffic_worker():
    while True:
        task = await traffic_queue.get()
        log_event("TR-WORKER", "🔍", "Analisi traffico avviata.")
        await send_to_agent_isolated("traffic-orchestrator", task['prompt'])
        traffic_queue.task_done()

def on_connect(client, userdata, flags, rc, properties=None):
    global is_mqtt_connected
    if rc == 0:
        is_mqtt_connected = True
        print(f"[{MY_NAME}] 🌐 Online. Shared Subscription attiva.", flush=True)
        client.subscribe("$share/bridge_workers/srs/alerts/recovery_needed", qos=1)
        client.subscribe("$share/bridge_workers/srs/alerts/traffic_check", qos=1)

def on_disconnect(client, userdata, flags, rc, properties=None):
    global is_mqtt_connected
    is_mqtt_connected = False

def on_message(client, userdata, msg):
    try:
        if msg.topic == "srs/alerts/recovery_needed":
            node = json.loads(msg.payload.decode()).get("node_id", "???")
            asyncio.run_coroutine_threadsafe(hardware_queue.put({"node": node, "prompt": f"Guasto su {node}"}), main_loop)
            
        elif msg.topic == "srs/alerts/traffic_check":
            if not acquire_global_traffic_lock(): return
            
            # Estraiamo i dati esatti dal controller
            payload_data = json.loads(msg.payload.decode('utf-8'))
            target_node = payload_data.get("target_node", "SCONOSCIUTO")
            cars = payload_data.get("cars", 0)
            
            # Creiamo un prompt mirato invece del generico [PROATTIVO]
            prompt = f"[REATTIVO] Emergenza su {target_node} ({cars} auto). Esegui validazione e ottimizzazione."
            
            asyncio.run_coroutine_threadsafe(traffic_queue.put({"prompt": prompt}), main_loop)
            log_event("BRIDGE", "📊", f"Allarme traffico in coda per {target_node}.")
    except Exception as e:
        pass

# ---------------------------------------------------------
# 9. AVVIO
# ---------------------------------------------------------
async def main():
    global main_loop, mqtt_client, is_mqtt_connected
    main_loop = asyncio.get_running_loop()
    
    asyncio.create_task(hardware_worker())
    asyncio.create_task(traffic_worker())

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"BRIDGE_CLIENT_{MY_NAME}")
    mqtt_client = client
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    while not is_mqtt_connected:
        utils.connetti_con_failover(client, MY_NAME, lambda: is_mqtt_connected)
        await asyncio.sleep(5)

    while True:
        if not is_mqtt_connected:
            utils.connetti_con_failover(client, MY_NAME, lambda: is_mqtt_connected)
        await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main())
