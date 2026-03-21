import influxdb_client

# Le stesse credenziali del controller
URL = "http://localhost:8086"
TOKEN = "supersecrettoken123"
ORG = "srs_org"

# Connessione
client = influxdb_client.InfluxDBClient(url=URL, token=TOKEN, org=ORG)
query_api = client.query_api()

print("Interrogo il database per gli ultimi 5 minuti")

# Query scritta in linguaggio Flux (il linguaggio di InfluxDB)
query = """
from(bucket: "traffic_data")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "stato_traffico")
  |> filter(fn: (r) => r._field == "auto_in_coda")
"""

# Eseguiamo la query
tabelle = query_api.query(query, org=ORG)

if not tabelle:
    print("Nessun dato trovato. I semafori stanno trasmettendo?")
else:
    for tabella in tabelle:
        for record in tabella.records:
            incrocio = record.values.get("incrocio")
            direzione = record.values.get("direzione")
            coda = record.get_value()
            orario = record.get_time().strftime("%H:%M:%S")
            
            print(f"[{orario}] {incrocio}/{direzione}: {coda} auto in coda")

client.close()