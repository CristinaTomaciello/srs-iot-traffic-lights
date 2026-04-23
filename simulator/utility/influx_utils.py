import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
import os

# Configurazione centralizzata (Alpha e Beta)
INFLUX_URLS = ["http://semafori-tsdb-alpha:8086", "http://semafori-tsdb-beta:8086"]
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "supersecrettoken123")
INFLUX_ORG = "srs_org"
INFLUX_BUCKET = "traffic_data"

def query_with_failover(query_string):
    for url in INFLUX_URLS:
        try:
            with influxdb_client.InfluxDBClient(url=url, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=1000) as client:
                return client.query_api().query(org=INFLUX_ORG, query=query_string)
        except Exception:
            continue 
    raise Exception("Tutti i database InfluxDB sono offline.")

def write_dual(point, synchronous=True):
    success = False
    for url in INFLUX_URLS:
        try:
            with influxdb_client.InfluxDBClient(url=url, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=2000) as client:
                write_api = client.write_api(write_options=SYNCHRONOUS if synchronous else ASYNCHRONOUS)
                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
                success = True
        except Exception:
            continue
    return success
