import os
import sys
import json
import time
import uuid
import re
import influxdb_client

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import redis
from fastmcp import FastMCP
import paho.mqtt.client as mqtt
from simulator.utility.influx_utils import query_with_failover, write_dual, INFLUX_BUCKET, INFLUX_ORG

mcp = FastMCP("TrafficOptimizerMCP")

# -------------------------------------------------------------------
# 1. Redis Lock & Cache (Invariato e Funzionante)
# -------------------------------------------------------------------
REDIS_URLS = os.getenv(
    "REDIS_URLS",
    "redis://redis-lock-1:6379,redis://redis-lock-2:6379,redis://redis-lock-3:6379",
).split(",")

redis_client = None

def connect_redis():
    global redis_client
    for url in REDIS_URLS:
        url = url.strip()
        try:
            r = redis.Redis.from_url(url, socket_connect_timeout=2, socket_timeout=2)
            r.ping()
            redis_client = r
            print(f"[MCP_AGENT] Connesso a Redis: {url}", file=sys.stderr, flush=True)
            return True
        except Exception as e:
            pass
    print("[MCP_AGENT] Nessun nodo Redis raggiungibile. Lock disabilitati.", file=sys.stderr)
    return False

connect_redis()

LOCK_TTL = 10
LOCK_PREFIX = "lock:green:"
CACHE_PREFIX = "cache:green:"
CACHE_TTL = 30
MIO_CLIENT_ID = f"MCP_OPT_{uuid.uuid4().hex[:6]}"

def acquire_lock(resource: str) -> bool:
    if not redis_client: return True
    return bool(redis_client.set(f"{LOCK_PREFIX}{resource}", MIO_CLIENT_ID, nx=True, px=LOCK_TTL * 1000))

def cache_get(key: str) -> str | None:
    if not redis_client: return None
    val = redis_client.get(f"{CACHE_PREFIX}{key}")
    return val.decode() if val else None

def cache_set(key: str, value: str, ttl=CACHE_TTL):
    if redis_client: redis_client.setex(f"{CACHE_PREFIX}{key}", ttl, value)

# -------------------------------------------------------------------
# 2. MQTT (Invariato)
# -------------------------------------------------------------------
def on_connect(client, userdata, flags, rc, properties):
    if rc == 0: print(f"[{MIO_CLIENT_ID}] Connesso al broker MQTT!", file=sys.stderr, flush=True)

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MIO_CLIENT_ID)
mqtt_client.on_connect = on_connect

def avvia_connessione_mqtt():
    brokers = ["srs-haproxy-1", "srs-haproxy-2"]
    for broker in brokers:
        try:
            mqtt_client.connect_async(broker, 1883, keepalive=60)
            mqtt_client.loop_start()
            return
        except Exception:
            pass

def log_mcp(emoji, message):
    print(f"[{MIO_CLIENT_ID}] {emoji} {message}", file=sys.stderr, flush=True)

avvia_connessione_mqtt()

# ===================================================================
# 3. I NUOVI TOOL A TARGET SINGOLO PER L'IA
# ===================================================================

@mcp.tool()
def green_duration_check_lock(base_id: str) -> str:
    """
    Verifica se un singolo incrocio (es. INC_0_0) è bloccato da cooldown o interventi in corso.
    """
    log_mcp("📡", f"Richiesta ricevuta per: green_duration_check_lock -> {base_id}")
    
    match = re.match(r"^(INC_\d+_\d+)", base_id.upper())
    if not match:
        return "LOCKED 🔒: Formato ID non valido. Fornire ID base (es. INC_0_0)."
    id_radice = match.group(1)

    cache_key = f"hardware_lock:{id_radice}"
    cached = cache_get(cache_key)
    if cached: return cached

    try:
        # 1. Riavvio HW Recente
        res_hw = query_with_failover(f'from(bucket: "{INFLUX_BUCKET}") |> range(start: -90s) |> filter(fn: (r) => r["_measurement"] == "agent_audit" and r["node_id"] =~ /{id_radice}/ and r["_field"] == "action" and r["_value"] == "RESTART") |> last()')
        if res_hw:
            val = f"LOCKED 🔒: Riavvio hardware recente su {id_radice}."
            cache_set(cache_key, val)
            return val

        # 2. Cooldown Ottimizzazione AI
        res_audit = query_with_failover(f'from(bucket: "{INFLUX_BUCKET}") |> range(start: -2m) |> filter(fn: (r) => r["_measurement"] == "agent_audit" and r["node_id"] == "{id_radice}" and r["_field"] == "action" and r["_value"] == "OPTIMIZE_TRAFFIC") |> last()')
        if res_audit:
            val = f"LOCKED 🔒: Cooldown attivo. {id_radice} è stato ottimizzato di recente."
            cache_set(cache_key, val)
            return val

        val = f"UNLOCKED 🔓: {id_radice} è libero per l'ottimizzazione."
        cache_set(cache_key, val)
        log_mcp("📤", f"Esito check_lock: {val}")
        return val

    except Exception as e:
        log_mcp("⚠️", f"Errore Lock Check: {e}")
        return "LOCKED 🔒: Errore telemetria. Intervento negato in via precauzionale."


@mcp.tool()
def green_duration_optimize_node(target_id: str, duration: int, cycles: int) -> str:
    """
    Applica una specifica durata (max 35s) e cicli a un nodo bersaglio.
    """
    log_mcp("📡", f"Richiesta ricevuta per: green_duration_optimize_node -> {target_id} | {duration}s | {cycles} cicli")

    # Parsing intelligente dell'ID per ricavare asse e direzione
    match = re.match(r"^(INC_\d+_\d+)_([A-Z]+)$", target_id.upper())
    if not match:
        return f"ERRORE: Formato ID non valido per '{target_id}'. Manca la direzione (es. _NORD)."
        
    id_radice = match.group(1)
    dir_specifica = match.group(2)
    asse = "NORD_SUD" if dir_specifica in ["NORD", "SUD"] else "EST_OVEST"

    # Validazione di sicurezza
    if not acquire_lock(f"optimize:{id_radice}"):
        return f"BLOCCATO: Operazione simultanea in corso su {id_radice}."
    
    if cycles < 1 or cycles > 3:
        return "BLOCCATO: I cicli devono essere compresi tra 1 e 3."
        
    if duration > 35:
        return f"BLOCCATO: Il limite massimo di sicurezza consentito per il verde è 35s. Ricevuto: {duration}s."

    try:
        # Invia MQTT a entrambe le direzioni dell'asse coinvolto
        direzioni = ["NORD", "SUD"] if asse == "NORD_SUD" else ["EST", "OVEST"]
        payload = json.dumps({"green_duration": duration, "override_cycles": cycles})
        
        for d in direzioni:
            topic = f"srs/edge/{id_radice}_{d}/config"
            mqtt_client.publish(topic, payload, qos=1)

        # Audit Log per innescare il cooldown
        point = influxdb_client.Point("agent_audit") \
            .tag("node_id", id_radice) \
            .tag("mcp_instance", MIO_CLIENT_ID) \
            .field("action", "OPTIMIZE_TRAFFIC")
        write_dual(point, synchronous=False)

        msg = f"[{target_id}] SUCCESSO: Asse {asse} aggiornato a {duration}s per {cycles} cicli."
        log_mcp("🟢", msg)
        return msg
        
    except Exception as e:
        log_mcp("💥", f"Errore MQTT: {e}")
        return "ERRORE CRITICO: Impossibile inviare il comando ai semafori."

if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8001)
