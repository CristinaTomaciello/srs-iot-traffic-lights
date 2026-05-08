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
import random

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

# --- MIGLIORIA 2.C: CODA A PRIORITÀ UNIFICATA ---
# Priority 1: Hardware (Emergenza) | Priority 2: Traffico (Ottimizzazione)
combined_queue = asyncio.PriorityQueue()

mqtt_client = None
is_mqtt_connected = False

# ---------------------------------------------------------
# 3. REDIS LOCK DINAMICO (MIGLIORIA 1.C)
# ---------------------------------------------------------
REDIS_URLS = [url.strip() for url in os.getenv("REDIS_URLS", "redis://redis-lock-1:6379,redis://redis-lock-2:6379,redis://redis-lock-3:6379").split(",")]
current_redis_index = 0
redis_client = None

def get_redis_client():
    global redis_client, current_redis_index
    if redis_client:
        try:
            redis_client.ping()
            return redis_client
        except redis.exceptions.ConnectionError:
            redis_client = None

    for _ in range(len(REDIS_URLS)):
        url = REDIS_URLS[current_redis_index]
        try:
            r = redis.Redis.from_url(url, socket_connect_timeout=2, socket_timeout=2)
            r.ping()
            redis_client = r
            return redis_client
        except redis.exceptions.ConnectionError:
            current_redis_index = (current_redis_index + 1) % len(REDIS_URLS)
    return None

def acquire_node_lock(node_id: str, task_type: str, ttl: int = 20) -> bool:
    """Acquisisce un lock breve (default 20s) per permettere il refresh dinamico."""
    client = get_redis_client()
    if not client: return False 
    lock_key = f"lock:{task_type}:{node_id}"
    try:
        return bool(client.set(lock_key, MY_NAME, nx=True, ex=ttl))
    except: return False

def release_node_lock(node_id: str, task_type: str):
    client = get_redis_client()
    if client:
        try: client.delete(f"lock:{task_type}:{node_id}")
        except: pass

async def lock_refresher(node_id: str, task_type: str, stop_event: asyncio.Event):
    """Miglioria 1.C: Mantiene il lock vivo ogni 10s finché il worker è attivo."""
    client = get_redis_client()
    lock_key = f"lock:{task_type}:{node_id}"
    while not stop_event.is_set():
        try:
            await asyncio.sleep(10)
            if client:
                client.expire(lock_key, 20) # Rinnova per altri 20s
        except: break

# ---------------------------------------------------------
# 4. AGENTI E AI (ACTIVE-ACTIVE)
# ---------------------------------------------------------
async def send_to_agent_isolated(agent_name: str, message_text: str, node_id: str = None):
    urls = OPENCODE_URLS.copy()
    random.shuffle(urls)
    
    for url in urls:
        try:
            log_event(agent_name.upper(), "⏳", f"Richiesta su Cella: {url}")
            async with aiohttp.ClientSession(auth=OPENCODE_AUTH) as session:
                # 1. Creazione Sessione
                async with session.post(f"{url}/session", json={"title": f"Task {node_id}"}, timeout=10) as s_res:
                    if s_res.status != 200: continue
                    sid = (await s_res.json()).get("id")
                
                # 2. Loop Messaggi (Polling)
                current_prompt = message_text
                for attempt in range(1, 11):
                    async with session.post(f"{url}/session/{sid}/message", 
                                           json={"agent": agent_name, "parts": [{"type": "text", "text": current_prompt}]}, 
                                           timeout=35) as res:
                        if res.status != 200: break
                        data = await res.json()
                        raw_text = "".join([p['text'] for p in data.get("parts", []) if p.get("type") == "text"])
                        
                        # Controllo Guasti MCP
                        if "mcp_unavailable" in raw_text.lower() or "error" in raw_text.lower():
                            log_event(agent_name.upper(), "🚫", "Guasto MCP rilevato. Failover cella.")
                            break

                        parsed = extract_valid_json(raw_text)
                        if parsed:
                            log_event(agent_name.upper(), "✅", f"REPORT: {json.dumps(parsed)}")
                            return 
                        
                        current_prompt = "Proceed and return ONLY the final JSON report."
                        await asyncio.sleep(1)
        except: continue

# ---------------------------------------------------------
# 5. UNIFIED PRIORITY WORKER (MIGLIORIA 2.C)
# ---------------------------------------------------------
async def unified_priority_worker():
    """Gestisce i task in ordine di priorità (1: HW, 2: Traffic)."""
    while True:
        priority, _, task = await combined_queue.get()
        node_id = task.get("node_id")
        task_type = task.get("type") # 'hardware' o 'traffic'
        
        stop_refresh = asyncio.Event()
        try:
            # SPIN-LOCK: Attesa che l'incrocio/nodo si liberi
            while not acquire_node_lock(node_id, task_type, ttl=20):
                await asyncio.sleep(1)
            
            # Avvio Refresh del Lock in background
            refresh_task = asyncio.create_task(lock_refresher(node_id, task_type, stop_refresh))
            
            log_event("WORKER", "⚡", f"Priorità {priority} | Gestione {task_type} su {node_id}")
            await send_to_agent_isolated(task['agent'], task['prompt'], node_id=node_id)
            
        except Exception as e:
            log_event("WORKER", "🔥", f"Errore: {e}")
        finally:
            stop_refresh.set() # Ferma il rinnovo automatico del lock
            release_node_lock(node_id, task_type)
            combined_queue.task_done()

# ---------------------------------------------------------
# 6. MQTT CALLBACKS
# ---------------------------------------------------------
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
        
        if msg.topic == "srs/alerts/recovery_needed":
            node = payload.get("node_id", "???")
            task = {
                "type": "hardware",
                "node_id": node,
                "agent": "hardware-orchestrator",
                "prompt": f"Esegui recovery hardware per {node}."
            }
            # AGGIUNTO time.time() COME SPAREGGIO
            asyncio.run_coroutine_threadsafe(combined_queue.put((1, time.time(), task)), main_loop)
            log_event("BRIDGE", "📥", f"Emergenza HW in coda: {node}")
            
        elif msg.topic == "srs/alerts/traffic_check":
            target_node = payload.get("target_node", "SCONOSCIUTO")
            parts = target_node.split('_')
            base_id = "_".join(parts[:3]) if len(parts) >= 3 else target_node
            
            task = {
                "type": "traffic",
                "node_id": base_id,
                "agent": "traffic-orchestrator",
                "prompt": f"Ottimizza traffico per {target_node} ({payload.get('cars')} auto)."
            }
            # AGGIUNTO time.time() COME SPAREGGIO
            asyncio.run_coroutine_threadsafe(combined_queue.put((2, time.time(), task)), main_loop)
            log_event("BRIDGE", "📥", f"Task Traffico in coda: {base_id}")

    except Exception as e:
        log_event("SYSTEM", "⚠️", f"Errore MQTT: {e}")

# ---------------------------------------------------------
# 7. UTILS & MAIN
# ---------------------------------------------------------
def log_event(worker: str, emoji: str, message: str):
    print(f"[{MY_NAME}] {emoji} [{worker}] {message}", flush=True)

def extract_valid_json(text: str) -> Optional[dict]:
    try:
        match = re.search(r'\{.*\}', text.replace('\n', ''), re.DOTALL)
        if match: return json.loads(match.group(0))
    except: return None

async def main():
    global main_loop, mqtt_client, is_mqtt_connected
    main_loop = asyncio.get_running_loop()
    
    # Avviamo 2 worker paralleli per bridge per gestire più incrocio contemporaneamente
    asyncio.create_task(unified_priority_worker())
    asyncio.create_task(unified_priority_worker())

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"CLIENT_{MY_NAME}")
    mqtt_client = client
    client.on_message = on_message

    client.on_connect = lambda c,u,f,rc,p: client.subscribe([
        ("$share/bridge_workers/srs/alerts/recovery_needed", 1),
        ("$share/bridge_workers/srs/alerts/traffic_check", 1)
    ]) or print(f"[{MY_NAME}] 🌐 Connesso via Shared Subscription.")
    
    while True:
        try:
            client.connect(MQTT_BROKER, 1883, 60)
            client.loop_start()
            break
        except: await asyncio.sleep(5)

    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
