from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import uvicorn
import asyncio
import paho.mqtt.client as mqtt
import json
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = FastAPI(title="Semafori UI - MQTT Bridge")

# Costruiamo i percorsi assoluti
static_path = os.path.join(BASE_DIR, "static")
templates_path = os.path.join(BASE_DIR, "templates")

# Montiamo le cartelle usando i percorsi sicuri
app.mount("/static", StaticFiles(directory=static_path), name="static")
templates = Jinja2Templates(directory=templates_path)

connessioni_attive = []

# MQTT Callbacks
def on_connect(client, userdata, flags, rc, properties):
    client.subscribe("srs/edge/+/+/stato")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        parts = msg.topic.split("/")
        
        incrocio_id = parts[2]
        semaforo_id = parts[3]
        direzione = semaforo_id.split("_")[-1]
        
        data_for_ui = {
            "incrocio": incrocio_id,
            "direzione": direzione,
            "stato": payload.get("colore", "ROSSO"),
            "coda": payload.get("auto_in_coda", 0),
            "id": f"{incrocio_id}_{direzione}"
        }
        
        for connection in connessioni_attive:
            asyncio.run_coroutine_threadsafe(connection.send_json(data_for_ui), loop)
    except Exception as e:
        pass

# Inizializziamo il client MQTT
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# --- MODELLI DATI PER IL PANNELLO ADMIN ---
class ControlCommand(BaseModel):
    command: str

class InjectCommand(BaseModel):
    incrocio: str
    direzione: str
    count: int
# ------------------------------------------

# Endpoint base
@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/topology")
async def get_topology():
    """Il frontend chiama questa API per conoscere com'è fatta la città."""
    topo_path = os.path.join(BASE_DIR, "..", "topology.json")
    try:
        with open(topo_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print("topology.json non trovato dalla UI")
        return {}

# --- NUOVI ENDPOINT ADMIN ---
@app.post("/api/admin/control")
async def admin_control(cmd: ControlCommand):
    # Pubblica il comando globale su MQTT (START / PAUSE)
    mqtt_client.publish("srs/admin/control", json.dumps({"command": cmd.command}))
    return {"status": "ok"}

@app.post("/api/admin/inject")
async def admin_inject(inj: InjectCommand):
    # Pubblica il comando sul topic specifico dell'incrocio per l'iniezione
    topic = f"srs/admin/inject/{inj.incrocio}"
    payload = {"direzione": inj.direzione, "count": inj.count}
    mqtt_client.publish(topic, json.dumps(payload))
    return {"status": "ok"}
# ----------------------------

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connessioni_attive.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connessioni_attive.remove(websocket)

@app.on_event("startup")
async def startup_event():
    global loop
    loop = asyncio.get_event_loop()
    mqtt_client.connect("localhost", 1883, 60)
    mqtt_client.loop_start()

@app.on_event("shutdown")
async def shutdown_event():
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

if __name__ == "__main__":
    uvicorn.run("server_ui:app", host="0.0.0.0", port=8080, reload=True)