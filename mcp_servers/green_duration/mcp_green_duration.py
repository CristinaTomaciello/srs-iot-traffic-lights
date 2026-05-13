import os
import sys
import json
import threading
import time
import uuid
import re
import influxdb_client

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import redis
import redis.exceptions
from fastmcp import FastMCP
import paho.mqtt.client as mqtt
from simulator.utility.influx_utils import query_with_failover, write_dual, INFLUX_BUCKET, INFLUX_ORG
from simulator.utility.mqtt_utils import connetti_con_failover

mcp = FastMCP("TrafficOptimizerMCP")

# ... (Mantieni intatta tutta la Sezione 1: Redis Lock & Cache) ...
REDIS_URLS = os.getenv("REDIS_URLS", "redis://redis-lock-1:6379,redis://redis-lock-2:6379,redis://redis-lock-3:6379").split(",")
current_redis_index = 0
redis_client = None

def get_redis_client():
    global redis_client, current_redis_index
    if redis_client:
        try:
            redis_client.ping()
            return redis_client
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            redis_client = None
    for _ in range(len(REDIS_URLS)):
        url = REDIS_URLS[current_redis_index].strip()
        try:
            r = redis.Redis.from_url(url, socket_connect_timeout=2, socket_timeout=2)
            r.ping()
            redis_client = r
            return redis_client
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            current_redis_index = (current_redis_index + 1) % len(REDIS_URLS)
    return None

LOCK_TTL = 10
LOCK_PREFIX = "lock:green:"
CACHE_PREFIX = "cache:green:"
CACHE_TTL = 5
MIO_CLIENT_ID = f"MCP_OPT_{uuid.uuid4().hex[:4]}_{os.getpid()}"

def acquire_lock(resource: str) -> bool:
    client = get_redis_client()
    if not client: return True
    try: return bool(client.set(f"{LOCK_PREFIX}{resource}", MIO_CLIENT_ID, nx=True, px=LOCK_TTL * 1000))
    except: return True

def cache_get(key: str) -> str | None:
    client = get_redis_client()
    if not client: return None
    try:
        val = client.get(f"{CACHE_PREFIX}{key}")
        return val.decode() if val else None
    except: return None

def cache_set(key: str, value: str, ttl=CACHE_TTL):
    client = get_redis_client()
    if client:
        try: client.setex(f"{CACHE_PREFIX}{key}", ttl, value)
        except: pass

# -------------------------------------------------------------------
# LOGGING SYSTEM
# -------------------------------------------------------------------
def log_mcp(emoji: str, message: str, level="INFO"):
    print(f"[{MIO_CLIENT_ID}] {emoji} {message}", file=sys.stderr, flush=True)
    try:
        point = influxdb_client.Point("system_logs") \
            .tag("service", "TrafficOptimizerMCP") \
            .tag("worker", MIO_CLIENT_ID) \
            .tag("level", level) \
            .field("message", message)
        write_dual(point, synchronous=False)
    except: pass

# -------------------------------------------------------------------
# 2. MQTT
# -------------------------------------------------------------------
def on_connect(client, userdata, flags, rc, properties):
    if rc == 0: 
        log_mcp("🌐", "Connesso al broker MQTT!")

def on_disconnect(client, userdata, flags, rc, properties):
    log_mcp("⚠️", "MQTT disconnesso! Avvio failover...", level="WARN")
    threading.Thread(target=connetti_con_failover, args=(mqtt_client, MIO_CLIENT_ID, lambda: mqtt_client.is_connected(), ["srs-haproxy-1", "srs-haproxy-2"])).start()

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MIO_CLIENT_ID)
mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect

connetti_con_failover(
    client=mqtt_client,
    client_id=MIO_CLIENT_ID,
    check_connessione=lambda: mqtt_client.is_connected(),
    brokers=["srs-haproxy-1", "srs-haproxy-2"]
)

# ===================================================================
# 3. I NUOVI TOOL A TARGET SINGOLO PER L'IA
# ===================================================================

@mcp.tool()
def green_duration_check_lock(base_id: str) -> str:
    """
    Verifica se un incrocio è pronto per l'ottimizzazione.
    Restituisce: LOCKED/UNLOCKED + numero auto reale + timestamp ultimo dato.
    """
    log_mcp("📡", f"Lock check per: {base_id}")
    
    match = re.match(r"^(INC_\d+_\d+)", base_id.upper())
    if not match: 
        return "LOCKED|reason=invalid_id|cars=0|ts=0"
    
    id_radice = match.group(1)
    cache_key = f"active_val:{id_radice}"
    cached = cache_get(cache_key)
    if cached: 
        return cached

    try:
        # 1. CHECK COOLDOWN (15s invece di 45s per maggiore reattività)
        res_hw = query_with_failover(f"""
            from(bucket: "{INFLUX_BUCKET}") 
            |> range(start: -15s) 
            |> filter(fn: (r) => r["_measurement"] == "agent_audit")
            |> filter(fn: (r) => r["node_id"] =~ /{id_radice}/)
            |> filter(fn: (r) => r["_field"] == "action")
            |> filter(fn: (r) => r["_value"] == "RESTART" or r["_value"] == "OPTIMIZE_TRAFFIC")
            |> last()
        """)
        
        if res_hw:
            val = f"LOCKED|reason=cooldown|cars=0|ts={int(time.time())}"
            cache_set(cache_key, val, ttl=5)
            return val

        # 2. QUERY TRAFFICO MIGLIORATA
        # - Range ridotto a 90s (dati freschi)
        # - Filtro su ENTRAMBI i campi (incrocio E node_id)
        # - Somma delle auto su TUTTE le direzioni
        query_traffico = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -90s) 
          |> filter(fn: (r) => r["_measurement"] == "stato_traffico")
          |> filter(fn: (r) => r["incrocio"] == "{id_radice}" or r["node_id"] == "{id_radice}")
          |> filter(fn: (r) => r["_field"] == "auto_in_coda")
          |> last()
        """
        res_traffico = query_with_failover(query_traffico)
        
        if not res_traffico or len(res_traffico) == 0:
            # NESSUN DATO RECENTE → Permetti ottimizzazione (approccio prudenziale)
            val = f"UNLOCKED|reason=no_data|cars=0|ts={int(time.time())}"
            cache_set(cache_key, val, ttl=3)
            return val

        # 3. CALCOLO TOTALE AUTO (somma su tutte le direzioni)
        total_cars = 0
        latest_ts = 0
        for table in res_traffico:
            for record in table.records:
                auto = int(record.values.get("_value", 0))
                total_cars += auto
                rec_ts = int(record.get_time().timestamp())
                if rec_ts > latest_ts:
                    latest_ts = rec_ts
        
        # 4. DECISIONE BASATA SU SOGLIA REALISTICA
        if total_cars < 5:
            val = f"LOCKED|reason=low_traffic|cars={total_cars}|ts={latest_ts}"
            cache_set(cache_key, val, ttl=8)
            return val
        
        # UNLOCKED: C'è traffico sufficiente per ottimizzare
        val = f"UNLOCKED|reason=traffic_detected|cars={total_cars}|ts={latest_ts}"
        cache_set(cache_key, val, ttl=3)
        log_mcp("🟢", f"Check OK: {val}")
        return val

    except Exception as e:
        log_mcp("⚠️", f"Errore lock check: {e}", level="ERROR")
        return f"LOCKED|reason=error|cars=0|ts={int(time.time())}"


@mcp.tool()
def green_duration_optimize_node(target_id: str, duration: int, cycles: int) -> str:
    """
    Applica l'ottimizzazione al semaforo.
    Restituisce JSON strutturato per parsing automatico.
    """
    log_mcp("📡", f"Optimize node: {target_id} → {duration}s x {cycles} cicli")

    match = re.match(r"^(INC_\d+_\d+)_([A-Z]+)$", target_id.upper())
    if not match: 
        return json.dumps({"status": "ERROR", "reason": "invalid_id", "target": target_id})
        
    id_radice = match.group(1)
    dir_specifica = match.group(2)
    asse = "NORD_SUD" if dir_specifica in ["NORD", "SUD"] else "EST_OVEST"

    # Validazioni
    if not acquire_lock(f"optimize:{id_radice}"):
        return json.dumps({"status": "ERROR", "reason": "concurrent_operation", "target": target_id})
    if not (1 <= cycles <= 4):
        return json.dumps({"status": "ERROR", "reason": "invalid_cycles", "target": target_id})
    if duration > 40:
        return json.dumps({"status": "ERROR", "reason": "duration_too_high", "target": target_id})

    try:
        if not mqtt_client.is_connected():
            connetti_con_failover(mqtt_client, MIO_CLIENT_ID, 
                                  lambda: mqtt_client.is_connected(), 
                                  ["srs-haproxy-1", "srs-haproxy-2"])
            if not mqtt_client.is_connected():
                return json.dumps({"status": "ERROR", "reason": "mqtt_unavailable", "target": target_id})
        
        # Pubblica comando
        direzioni = ["NORD", "SUD"] if asse == "NORD_SUD" else ["EST", "OVEST"]
        payload = json.dumps({"green_duration": duration, "override_cycles": cycles})
        for d in direzioni:
            topic = f"srs/edge/{id_radice}_{d}/config"
            result = mqtt_client.publish(topic, payload, qos=1)
            if result.rc != 0:
                return json.dumps({"status": "ERROR", "reason": "mqtt_publish_failed", "target": target_id})

        # Audit Log
        point = influxdb_client.Point("agent_audit") \
            .tag("node_id", id_radice) \
            .tag("agent", "TrafficOptimizerMCP") \
            .tag("mcp_instance", MIO_CLIENT_ID) \
            .field("action", "OPTIMIZE_TRAFFIC") \
            .field("duration", duration) \
            .field("cycles", cycles) \
            .field("axis", asse)
        write_dual(point, synchronous=False)

        response = {
            "status": "SUCCESS",
            "intersection": target_id,
            "axis": asse,
            "duration": duration,
            "cycles": cycles
        }
        log_mcp("✅", f"Ottimizzazione applicata: {response}")
        return json.dumps(response)
        
    except Exception as e:
        log_mcp("💥", f"Errore optimize: {e}", level="ERROR")
        return json.dumps({"status": "ERROR", "reason": str(e), "target": target_id})

if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8001)
