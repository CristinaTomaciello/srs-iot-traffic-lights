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
import threading
import influxdb_client # <-- AGGIUNTO

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

combined_queue = asyncio.PriorityQueue()
mqtt_client = None
is_mqtt_connected = False
tasks_in_queue = set()

# ---------------------------------------------------------
# UTILITY LOGGING AVANZATO (System Logs & Agent Audit)
# ---------------------------------------------------------
def log_event(worker: str, emoji: str, message: str, level="INFO"):
    """Stampa a video E invia a InfluxDB come system_logs"""
    print(f"[{MY_NAME}] {emoji} [{worker}] {message}", flush=True)
    try:
        point = influxdb_client.Point("system_logs") \
            .tag("service", MY_NAME) \
            .tag("worker", worker) \
            .tag("level", level) \
            .field("message", message)
        write_dual(point, synchronous=False)
    except: pass

def log_agent_decision(agent_name: str, node_id: str, action: str, reason: str):
    """Traccia le decisioni dell'IA in agent_audit"""
    try:
        point = influxdb_client.Point("agent_audit") \
            .tag("agent", agent_name) \
            .tag("node_id", node_id) \
            .field("action", action) \
            .field("reason", reason)
        write_dual(point, synchronous=False)
    except: pass

async def delayed_requeue(priority, task, delay=40):
    await asyncio.sleep(delay)
    await combined_queue.put((priority, time.time(), task))
    log_event("SYSTEM", "♻️", f"RETRY: Task {task.get('type')} per {task.get('node_id')} rientrato in coda dopo {delay}s.", level="WARN")

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
    client = get_redis_client()
    if not client: return False 
    try: return bool(client.set(f"lock:{task_type}:{node_id}", MY_NAME, nx=True, ex=ttl))
    except: return False

def release_node_lock(node_id: str, task_type: str):
    client = get_redis_client()
    if client:
        try: client.delete(f"lock:{task_type}:{node_id}")
        except: pass

async def lock_refresher(node_id: str, task_type: str, stop_event: asyncio.Event):
    client = get_redis_client()
    lock_key = f"lock:{task_type}:{node_id}"
    while not stop_event.is_set():
        try:
            await asyncio.sleep(10)
            if client: client.expire(lock_key, 20)
        except: break

# ---------------------------------------------------------
# AGENTI E AI (ACTIVE-ACTIVE)
# ---------------------------------------------------------
async def send_to_agent_isolated(agent_name: str, message_text: str, node_id: str = None) -> Optional[dict]:
    urls = OPENCODE_URLS.copy()
    random.shuffle(urls)
    
    for url in urls:
        try:
            log_event(agent_name.upper(), "⏳", f"Richiesta su Cella: {url}")
            async with aiohttp.ClientSession(auth=OPENCODE_AUTH) as session:
                async with session.post(f"{url}/session", json={"title": f"Task {node_id}"}, timeout=10) as s_res:
                    if s_res.status != 200: continue
                    sid = (await s_res.json()).get("id")
                
                current_prompt = message_text
                for attempt in range(1, 11):
                    async with session.post(f"{url}/session/{sid}/message", 
                                           json={"agent": agent_name, "parts": [{"type": "text", "text": current_prompt}]}, 
                                           timeout=35) as res:
                        if res.status != 200: break
                        data = await res.json()
                        raw_text = "".join([p['text'] for p in data.get("parts", []) if p.get("type") == "text"])
                        
                        if "mcp_unavailable" in raw_text.lower() or "critical error:" in raw_text.lower() or "connectionrefusederror" in raw_text.lower():
                            log_event(agent_name.upper(), "🚫", "Guasto MCP rilevato. Failover cella.", level="ERROR")
                            break

                        parsed = extract_valid_json(raw_text)
                        if parsed:
                            log_event(agent_name.upper(), "✅", f"REPORT: {json.dumps(parsed)}")
                            # Scrittura Audit IA
                            log_agent_decision(
                                agent_name.upper(), 
                                node_id, 
                                str(parsed.get("action", "UNKNOWN")), 
                                str(parsed.get("reason", parsed.get("outcome", "No reason provided")))
                            )
                            return parsed
                        
                        current_prompt = "Proceed and return ONLY the final JSON report."
                        await asyncio.sleep(1)
        except: continue
    return None

# ---------------------------------------------------------
# UNIFIED PRIORITY WORKER 
# ---------------------------------------------------------
async def unified_priority_worker():
    while True:
        # Recupero del task dalla coda a priorità
        priority, _, task = await combined_queue.get()
        node_id = task.get("node_id")
        task_type = task.get("type")
        
        stop_refresh = asyncio.Event()
        try:
            # Acquisizione del lock distribuito su Redis
            while not acquire_node_lock(node_id, task_type, ttl=20):
                await asyncio.sleep(1)
            
            # Avvio del refresher per mantenere il lock durante l'elaborazione
            refresh_task = asyncio.create_task(lock_refresher(node_id, task_type, stop_refresh))
            log_event("WORKER", "⚡", f"Priorità {priority} | Gestione {task_type} su {node_id}")
            
            # Chiamata all'agente IA isolato
            parsed_report = await send_to_agent_isolated(task['agent'], task['prompt'], node_id=node_id)
            
            if parsed_report:
                action = str(parsed_report.get("action", "")).upper()
                
                # 1. GESTIONE RETRY (Lock tecnico o Cooldown attivo)
                if action == "RETRY":
                    # Aumentato il delay a 40s per evitare il flooding del sistema
                    log_event("WORKER", "⏳", f"Task {task_type} su {node_id} in RETRY. Parcheggio per 40s...")
                    asyncio.create_task(delayed_requeue(priority, task, delay=40))
                
                # 2. GESTIONE SKIPPED (Traffico già smaltito o situazione risolta)
                elif action == "SKIPPED":
                    reason = parsed_report.get("reason", "Situazione già risolta")
                    log_event("WORKER", "✅", f"Task {node_id} CONCLUSO (SKIPPED): {reason}")
                
                # 3. GESTIONE SUCCESS / OPTIMIZED
                elif action in ["OPTIMIZED", "REPAIR", "SUCCESS"]:
                    log_event("WORKER", "🎉", f"Task {node_id} completato con successo: {action}")

        except Exception as e:
            log_event("WORKER", "🔥", f"Errore critico durante l'esecuzione: {e}", level="ERROR")
        finally:
            # Rimuovi dal set dei task in coda
            tasks_in_queue.discard(f"{task_type}_{node_id}")
            stop_refresh.set()
            release_node_lock(node_id, task_type)
            combined_queue.task_done()

def on_connect_bridge(client, userdata, flags, rc, properties):
    if rc == 0:
        log_event("SYSTEM", "🌐", "MQTT connesso!")
        client.subscribe([
            ("$share/bridge_workers/srs/alerts/recovery_needed", 1),
            ("$share/bridge_workers/srs/alerts/traffic_check", 1)
        ])

def on_disconnect_bridge(client, userdata, flags, rc, properties):
    log_event("SYSTEM", "⚠️", "MQTT disconnesso! Avvio failover in background...", level="WARN")
    threading.Thread(target=utils.connetti_con_failover, args=(mqtt_client, MY_NAME, lambda: mqtt_client.is_connected(), ["haproxy-1", "haproxy-2"])).start()

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
        
        # --- 1. GESTIONE RECOVERY HARDWARE (Mancava questa!) ---
        if msg.topic == "srs/alerts/recovery_needed":
            node = payload.get("node_id", "???")
            
            # Filtro per evitare duplicati in coda
            if f"hardware_{node}" in tasks_in_queue:
                return
            
            tasks_in_queue.add(f"hardware_{node}")
            task = {
                "type": "hardware",
                "node_id": node,
                "agent": "hardware-orchestrator", # Assicurati che il nome agente sia corretto
                "prompt": f"Esegui recovery hardware per {node}."
            }
            # Priorità 1 (più alta del traffico che è 2)
            asyncio.run_coroutine_threadsafe(combined_queue.put((1, time.time(), task)), main_loop)
            log_event("BRIDGE", "🚨", f"Emergenza HW in coda: {node}")

        # --- 2. GESTIONE TRAFFICO (Già presente) ---
        elif msg.topic == "srs/alerts/traffic_check":
            target_node = payload.get("target_node", "SCONOSCIUTO")
            parts = target_node.split('_')
            base_id = "_".join(parts[:3]) if len(parts) >= 3 else target_node
            
            if f"traffic_{base_id}" in tasks_in_queue:
                return
            
            tasks_in_queue.add(f"traffic_{base_id}")
            task = {
                "type": "traffic", 
                "node_id": base_id, 
                "agent": "traffic-orchestrator", 
                "prompt": f"Ottimizza traffico per {target_node}..."
            }
            # Priorità 2
            asyncio.run_coroutine_threadsafe(combined_queue.put((2, time.time(), task)), main_loop)
            log_event("BRIDGE", "📥", f"Task Traffico in coda: {base_id}")
            
    except Exception as e:
        log_event("SYSTEM", "⚠️", f"Errore MQTT: {e}", level="ERROR")

def extract_valid_json(text: str) -> Optional[dict]:
    try:
        match = re.search(r'\{.*\}', text.replace('\n', ''), re.DOTALL)
        if match: return json.loads(match.group(0))
    except: return None

async def main():
    global main_loop, mqtt_client, is_mqtt_connected
    main_loop = asyncio.get_running_loop()
    asyncio.create_task(unified_priority_worker())
    asyncio.create_task(unified_priority_worker())
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"CLIENT_{MY_NAME}")
    mqtt_client = client
    client.on_message = on_message
    client.on_disconnect = on_disconnect_bridge
    client.on_connect = on_connect_bridge
    utils.connetti_con_failover(client=client, client_id=MY_NAME, check_connessione=lambda: client.is_connected(), brokers=["haproxy-1", "haproxy-2"])
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
