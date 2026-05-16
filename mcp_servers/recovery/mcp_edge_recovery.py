import os
import sys
import json
import threading
import time
import uuid
import re

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import redis
import redis.exceptions
from fastmcp import FastMCP
import paho.mqtt.client as mqtt
import influxdb_client
from simulator.utility.influx_utils import (
    query_with_failover,
    write_dual,
    INFLUX_BUCKET,
    INFLUX_ORG,
    log_audit,
)
from simulator.utility.mqtt_utils import connetti_con_failover

mcp = FastMCP("EdgeRecoveryMCP")

VALID_NODE_PATTERN = re.compile(r"^INC_\d+_\d+_(NORD|SUD|EST|OVEST)$")

REDIS_URLS = os.getenv(
    "REDIS_URLS",
    "redis://redis-lock-1:6379,redis://redis-lock-2:6379,redis://redis-lock-3:6379",
).split(",")

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
            print(f"[MCP_RECOVERY] ✅ Redis: {url}", file=sys.stderr, flush=True)
            return redis_client
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            current_redis_index = (current_redis_index + 1) % len(REDIS_URLS)
            
    print("[MCP_RECOVERY] ⚠️ No Redis. Lock/cache disabled.", file=sys.stderr)
    return None

LOCK_TTL = 10
CACHE_TTL = 30

def acquire_lock(node_id: str) -> bool:
    client = get_redis_client()
    if not client:
        return True
    lock_key = f"lock:edge:{node_id}"
    owner = f"recovery_{uuid.uuid4().hex[:8]}"
    try:
        return bool(client.set(lock_key, owner, nx=True, ex=LOCK_TTL))
    except:
        return True

def cache_get(key: str):
    client = get_redis_client()
    if not client:
        return None
    try:
        val = client.get(f"cache:edge:{key}")
        return val.decode() if val else None
    except:
        return None

def cache_set(key: str, value: str, ttl: int = CACHE_TTL):
    client = get_redis_client()
    if client:
        try:
            client.setex(f"cache:edge:{key}", ttl, value)
        except:
            pass

# MQTT
is_mqtt_connected = False
server_id = f"MCP_RECOVERY_{uuid.uuid4().hex[:4]}_{os.getpid()}"

def on_connect(client, userdata, flags, rc, properties):
    global is_mqtt_connected
    if rc == 0:
        is_mqtt_connected = True
        print("[MCP_RECOVERY] ✅ MQTT connected", file=sys.stderr, flush=True)

def on_disconnect(client, userdata, flags, rc, properties):
    global is_mqtt_connected
    is_mqtt_connected = False
    print("[MCP_RECOVERY] ⚠️ MQTT disconnesso! Avvio failover in background...", file=sys.stderr, flush=True)
    
    threading.Thread(target=connetti_con_failover, args=(
        mqtt_client,
        server_id,
        lambda: is_mqtt_connected,
        ["srs-haproxy-1", "srs-haproxy-2"]
    )).start()



def log_mcp(emoji: str, message: str):
    print(f"[{server_id}] {emoji} {message}", file=sys.stderr, flush=True)

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=server_id)
mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect

connetti_con_failover(
    client=mqtt_client,
    client_id=server_id,
    check_connessione=lambda: is_mqtt_connected,
    brokers=["srs-haproxy-1", "srs-haproxy-2"]
)

# ============================================================
# TOOL 1: CONSOLIDATED VALIDATION (2 queries in 1 tool)
# ============================================================
@mcp.tool()
def validate_node_for_restart(node_id: str) -> str:
    """
    Consolidated validation: checks both telemetry freshness and restart history.
    Returns structured decision to avoid agent reasoning overhead.
    
    Returns JSON with:
    - verdict: APPROVED | BLOCKED
    - reason: short explanation
    - telemetry: last status
    - last_restart_seconds: seconds since last restart
    """
    if not VALID_NODE_PATTERN.match(node_id):
        return json.dumps({
            "verdict": "BLOCKED",
            "reason": "Invalid node ID format",
            "telemetry": "N/A",
            "last_restart_seconds": 0
        })

    # Check cache first
    cached = cache_get(f"validation:{node_id}")
    if cached:
        return cached

    try:
        # Query 1: Telemetry (check if node is really offline)
        query_telemetry = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -1m)
          |> filter(fn: (r) => r["_measurement"] == "stato_traffico")
          |> filter(fn: (r) => r["direzione"] == "{node_id}")
          |> last()
        """
        result_telem = query_with_failover(query_telemetry)

        if not result_telem:
            # Node is offline - APPROVED for restart
            response = json.dumps({
                "verdict": "APPROVED",
                "reason": "Node offline (no data)",
                "telemetry": "OFFLINE",
                "last_restart_seconds": 999999
            })
            cache_set(f"validation:{node_id}", response, ttl=10)
            return response

        # Node has recent data - check if it's actually problematic
        record = result_telem[0].records[0]
        seconds_ago = int(time.time() - record.get_time().timestamp())

        if seconds_ago < 30:
            # Fresh data = node is working fine
            response = json.dumps({
                "verdict": "BLOCKED",
                "reason": "Node operational (fresh data)",
                "telemetry": f"OK ({seconds_ago}s ago)",
                "last_restart_seconds": 0
            })
            cache_set(f"validation:{node_id}", response, ttl=10)
            return response

        # Query 2: Check restart history (prevent loops)
        query_restart = f"""
        from(bucket: "{INFLUX_BUCKET}")
        |> range(start: -1h)
        |> filter(fn: (r) => r["_measurement"] == "agent_audit")
        |> filter(fn: (r) => r["node_id"] == "{node_id}")
        |> filter(fn: (r) => r["_field"] == "action")
        |> filter(fn: (r) => r["_value"] == "RESTART")
        |> last()
        """
        result_restart = query_with_failover(query_restart)

        seconds_since_restart = None  # <-- INIZIALIZZA QUI

        if result_restart:
            try:
                restart_record = result_restart[0].records[0]
                seconds_since_restart = int(time.time() - restart_record.get_time().timestamp())
                
                if seconds_since_restart < 300:
                    response = json.dumps({
                        "verdict": "BLOCKED",
                        "reason": "Recent restart detected (loop prevention)",
                        "telemetry": f"STALE ({seconds_ago}s ago)",
                        "last_restart_seconds": seconds_since_restart
                    })
                    cache_set(f"validation:{node_id}", response, ttl=10)
                    return response
            except (IndexError, AttributeError):
                pass

        # All checks passed - approve restart
        response = json.dumps({
            "verdict": "APPROVED",
            "reason": "Stale data + no recent restart",
            "telemetry": f"STALE ({seconds_ago}s ago)",
            "last_restart_seconds": seconds_since_restart if seconds_since_restart is not None else 999999
        })
        cache_set(f"validation:{node_id}", response, ttl=10)
        return response

    except Exception as e:
        log_mcp("🔥", f"Validation error: {e}")
        return json.dumps({
            "verdict": "BLOCKED",
            "reason": f"Database error: {str(e)}",
            "telemetry": "ERROR",
            "last_restart_seconds": 0
        })


# ============================================================
# TOOL 2: EXECUTE RESTART (with lock)
# ============================================================
@mcp.tool()
def execute_node_restart(node_id: str) -> str:
    """
    Execute restart command with distributed lock and audit logging.
    Returns simple status message.
    """
    if not VALID_NODE_PATTERN.match(node_id):
        return "BLOCKED: Invalid node ID"

    if not acquire_lock(node_id):
        log_mcp("🔒", f"Lock held for {node_id}")
        return "SKIPPED: Another replica processing"

    if not is_mqtt_connected:
        connetti_con_failover(mqtt_client, server_id, lambda: is_mqtt_connected, ["srs-haproxy-1", "srs-haproxy-2"])
        if not is_mqtt_connected:
            return "ERROR: MQTT unavailable"

    try:
        topic = f"srs/admin/node/{node_id}/fault_injection"
        mqtt_client.publish(topic, json.dumps({"type": "REPAIR"}), qos=1)

        point = influxdb_client.Point("agent_audit") \
        .tag("node_id", node_id) \
        .tag("mcp_instance", server_id) \
        .field("action", "RESTART")

        success = write_dual(point, synchronous=True)

        log_mcp("🛠️", f"Restart sent: {node_id} (audit: {'✅' if success else '❌'})")
        log_audit("RECOVERY_MCP", "EXECUTE_REPAIR", f"Restart {node_id}", level="INFO")

        # Clear validation cache
        cache_set(f"validation:{node_id}", "", ttl=1)

        return "SUCCESS: Restart command sent"

    except Exception as e:
        log_mcp("💥", f"Restart failed: {e}")
        return f"ERROR: {str(e)}"


# ============================================================
# TOOL 3: VERIFY POST-RESTART
# ============================================================
@mcp.tool()
def verify_node_recovery(node_id: str) -> str:
    """
    Quick post-restart verification.
    Returns simple status: RESTORED | PENDING | FAILED
    """
    if not VALID_NODE_PATTERN.match(node_id):
        return "FAILED: Invalid node ID"

    try:
        query = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -30s)
          |> filter(fn: (r) => r["_measurement"] == "stato_traffico")
          |> filter(fn: (r) => r["direzione"] == "{node_id}")
          |> last()
        """
        result = query_with_failover(query)

        if not result:
            return "PENDING: Waiting for node to come back online"

        record = result[0].records[0]
        seconds_ago = int(time.time() - record.get_time().timestamp())

        if seconds_ago < 15:
            return f"RESTORED: Node online ({seconds_ago}s ago)"
        else:
            return f"PENDING: Last data {seconds_ago}s ago"

    except Exception as e:
        return f"FAILED: Verification error - {str(e)}"


# ============================================================
# TOOL 4: ESCALATE
# ============================================================
@mcp.tool()
def escalate_to_human(node_id: str, diagnostic_summary: str) -> str:
    """Escalate to human operator."""
    log_mcp("🚨", f"ESCALATION: {node_id} - {diagnostic_summary}")
    log_audit("RECOVERY_MCP", "ESCALATION", f"{node_id}: {diagnostic_summary}", level="WARN")
    return "ESCALATION COMPLETED"

if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8000)
