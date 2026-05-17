import influxdb_client
import paho.mqtt.client as mqtt
import os
import uuid

# ==========================================
# 1. CONFIGURAZIONE LETTURE (HAProxy)
# ==========================================
# Puntiamo ai due bilanciatori HAProxy per le letture
HAPROXY_URLS = ["http://haproxy-influx-1:8086", "http://haproxy-influx-2:8086"]
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "supersecrettoken123")
INFLUX_ORG = os.getenv("INFLUX_ORG", "srs_org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "traffic_data")

def query_with_failover(query_string):
    """
    Tenta la lettura passando dai proxy. 
    Il proxy sano instradera' la richiesta al DB (Alpha o Beta) funzionante.
    """
    for url in HAPROXY_URLS:
        try:
            # Timeout basso: se un proxy è giù, passiamo subito all'altro
            with influxdb_client.InfluxDBClient(url=url, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=1000) as client:
                return client.query_api().query(org=INFLUX_ORG, query=query_string)
        except Exception:
            continue 
            
    raise Exception("CRITICO: Entrambi gli HAProxy di InfluxDB sono offline o i Database non rispondono.")


# ==========================================
# 2. CONFIGURAZIONE SCRITTURE (MQTT Failover)
# ==========================================
# All'interno della rete Docker, entrambi gli haproxy MQTT rispondono sulla porta 1883
MQTT_BROKERS = [("haproxy-1", 1883), ("haproxy-2", 1883)]
MQTT_TOPIC = "telemetry/influx_write"
current_broker_index = 0

def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    """
    Gestisce la caduta del broker MQTT e passa alla replica.
    """
    global current_broker_index
    print(f"[MQTT] Connessione persa. Failover sul proxy di scorta...")
    
    current_broker_index = (current_broker_index + 1) % len(MQTT_BROKERS)
    host, port = MQTT_BROKERS[current_broker_index]
    
    try:
        client.connect_async(host, port)
    except Exception as e:
        print(f"[MQTT] Errore di riconnessione: {e}")


# Genera un ID univoco per ogni container (es: influx_utils_pub_a1b2c3d4)
unique_client_id = f"influx_utils_pub_{uuid.uuid4().hex[:8]}"

# Inizializziamo il client con l'ID univoco
mqtt_client = mqtt.Client(client_id=unique_client_id, protocol=mqtt.MQTTv5)
mqtt_client.on_disconnect = on_disconnect

# Connessione iniziale
initial_host, initial_port = MQTT_BROKERS[current_broker_index]
mqtt_client.connect_async(initial_host, initial_port)
mqtt_client.loop_start() # Avvia il thread in background


def write_dual(point, synchronous=True):
    """
    Pubblica il dato su MQTT. Telegraf si occuperà della doppia scrittura.
    """
    try:
        # InfluxDB Line Protocol
        if isinstance(point, influxdb_client.Point):
            line_protocol = point.to_line_protocol()
        else:
            line_protocol = str(point)
            
        # qos=1 garantisce che il dato arrivi anche in caso di instabilità di rete
        mqtt_client.publish(topic=MQTT_TOPIC, payload=line_protocol, qos=1)
        return True
    except Exception as e:
        print(f"Errore pubblicazione MQTT: {e}")
        return False

def log_audit(component, event_type, message, level="INFO"):
    """
    Traccia gli eventi dell'IA. Rimane invariata ma ora sfrutta MQTT.
    """
    try:
        point = influxdb_client.Point("system_logs") \
            .tag("component", component) \
            .tag("level", level) \
            .tag("event", event_type) \
            .field("message", str(message))
        
        write_dual(point, synchronous=False)
    except Exception as e:
        print(f"Errore scrittura log audit: {e}")
