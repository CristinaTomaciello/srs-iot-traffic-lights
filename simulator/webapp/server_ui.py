from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware


import uvicorn
import asyncio
import paho.mqtt.client as mqtt
import json
import os
import threading
import time

import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import simulator.utility.mqtt_utils as utils

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Percorsi assoluti
static_path = os.path.join(BASE_DIR, "static")
templates_path = os.path.join(BASE_DIR, "templates")
topo_path = "/app/topology.json" 

app.mount("/static", StaticFiles(directory=static_path), name="static")
templates = Jinja2Templates(directory=templates_path)

is_mqtt_connected = False

# Caricamento topologia
try:
    with open(topo_path, 'r') as f:
        topology = json.load(f)
    print(f"Topologia caricata correttamente da {topo_path}")
except Exception as e:
    print(f"ERRORE CRITICO: Impossibile caricare {topo_path}: {e}")
    topology = {}
    
connessioni_attive = []

# --- MQTT Callbacks ---
def on_connect(client, userdata, flags, rc, properties):
    global is_mqtt_connected
    if rc == 0:
        is_mqtt_connected = True
        print("[UI_CLIENT] UI connessa al broker MQTT in HA!", flush=True)
        client.subscribe("srs/edge/+/+/stato")

def on_disconnect(client, userdata, flags, rc, properties):
    global is_mqtt_connected
    is_mqtt_connected = False
    print("[UI_CLIENT] Disconnesso dal broker!", flush=True)

def check_ui_connection():
    global is_mqtt_connected
    return is_mqtt_connected

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        parts = msg.topic.split("/")
        
        if len(parts) >= 4 and parts[1] == "edge":
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
    except Exception:
        pass

# --- Inizializzazione Client MQTT ---
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="UI_CLIENT")
mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_message = on_message

# --- Thread di Failover in Background ---
def mqtt_monitor():
    """
    Gira in background per tenere in vita la connessione MQTT 
    senza bloccare il server web asincrono di FastAPI.
    """
    print("Inizializzazione della UI e connessione ai bilanciatori...", flush=True)
    utils.connetti_con_failover(mqtt_client, "UI_CLIENT", check_ui_connection)
    
    while True:
        if not check_ui_connection():
            print("[UI_CLIENT] Connessione persa! Rotazione verso il bilanciatore secondario...", flush=True)
            utils.connetti_con_failover(mqtt_client, "UI_CLIENT", check_ui_connection)
        time.sleep(1)

# --- Modelli Pydantic ---
class ControlCommand(BaseModel):
    command: str

class InjectCommand(BaseModel):
    incrocio: str
    direzione: str
    count: int

class CrashCommand(BaseModel):
    incrocio: str
    direzione: str

# --- API Endpoints ---
@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/topology")
async def get_topology():
    path = "/app/topology.json" 
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"Errore UI caricamento topologia: {e}")
        return {}

@app.post("/api/admin/control")
async def admin_control(cmd: ControlCommand):
    mqtt_client.publish("srs/admin/control", json.dumps({"command": cmd.command}), retain=True)
    return {"status": "ok"}

@app.post("/api/admin/inject")
async def admin_inject(inj: InjectCommand):
    topic = f"srs/admin/inject/{inj.incrocio}"
    payload = {"direzione": inj.direzione, "count": inj.count}
    mqtt_client.publish(topic, json.dumps(payload))
    return {"status": "ok"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connessioni_attive.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connessioni_attive.remove(websocket)

# --- Eventi del Ciclo di Vita FastAPI ---
@app.on_event("startup")
async def startup_event():
    global loop
    loop = asyncio.get_event_loop()
    
    # Avviamo il monitor MQTT come demone in background
    threading.Thread(target=mqtt_monitor, daemon=True).start()

@app.on_event("shutdown")
async def shutdown_event():
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

@app.post("/api/admin/crash")
async def admin_crash(cmd: CrashCommand):
    # Formatta esattamente come fa scenario_manager.py
    incrocio = cmd.incrocio.strip().upper()
    if not incrocio.startswith("INC_"):
        incrocio = "INC_" + incrocio
        
    direzione = cmd.direzione.strip().upper()
    node_id = f"{incrocio}_{direzione}"
    
    # Topic e payload identici a scenario_manager.py
    topic = f"srs/admin/node/{node_id}/fault_injection"
    payload = {"type": "HARDWARE_FAILURE"}
    
    mqtt_client.publish(topic, json.dumps(payload))
    print(f"[UI ADMIN] 💥 HARDWARE_FAILURE inviato a {node_id}")
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)