from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import uvicorn
import asyncio

app = FastAPI(title="Semafori UI - Dashboard")
app.mount("/static", StaticFiles(directory="webapp/static"), name="static")
templates = Jinja2Templates(directory="webapp/templates")

# Gestore dei client WebSocket connessi
connessioni_attive = []

# Modello dati per lo stato del semaforo
class StatoSemaforo(BaseModel):
    id: str
    stato: str
    coda: int

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Endpoint per la pagina web per ricevere aggiornamenti in tempo reale."""
    await websocket.accept()
    connessioni_attive.append(websocket)
    try:
        while True:
            await websocket.receive_text() # Mantiene la connessione attiva
    except WebSocketDisconnect:
        connessioni_attive.remove(websocket)

@app.post("/update_node")
async def update_node(stato: StatoSemaforo):
    """Il nodo semaforo chiama questa API per dire all'interfaccia come sta."""
    for connection in connessioni_attive:
        await connection.send_json(stato.dict())
    return {"status": "ok"}

if __name__ == "__main__":
    print("Avvio del server UI su http://localhost:8080")
    uvicorn.run("server_ui:app", host="0.0.0.0", port=8080, reload=True)