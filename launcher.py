import subprocess
import sys
import time
import json
import os
import atexit
from xmlrpc import client
import paho.mqtt.client as mqtt

# Dizionario per tenere traccia dei processi attivi dei nodi
active_nodes = {}

def cleanup():
    """Chiude tutti i nodi semaforo all'uscita del container"""
    print("\n[Node Manager] Chiusura di tutti i nodi in corso...")
    for node_id, p in active_nodes.items():
        if p.poll() is None:
            p.kill()
    print("[Node Manager] Simulazione terminata.")

atexit.register(cleanup)

# Mqtt Callbacks
def on_connect(client, userdata, flags, rc, properties):
    print("[Node Manager] Connesso al broker.")
    client.subscribe("srs/launcher/control")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        command = payload.get("command")
        node_id = payload.get("node_id")

        if command == "RESTART_NODE" and node_id:
            print(f"[Node Manager] RESTART richiesto per: {node_id}")
            if node_id in active_nodes:
                old_process = active_nodes[node_id]
                if old_process.poll() is None:
                    old_process.kill()
            
            # Lancio del nuovo processo
            new_process = subprocess.Popen([sys.executable, "simulator/nodo_semaforo.py", node_id])
            active_nodes[node_id] = new_process
            print(f"[Node Manager] Nodo {node_id} riavviato (PID: {new_process.pid})")
    except Exception as e:
        print(f"[Node Manager] Errore nel comando MQTT: {e}")

# Main
def main():
    # Caricamento Topologia
    topo_path = os.path.join("simulator", "topology.json")
    if not os.path.exists(topo_path):
        print(f"[Node Manager] ERRORE: {topo_path} non trovato!")
        sys.exit(1)
        
    with open(topo_path, "r") as f:
        topology = json.load(f)
        
    print(f"[Node Manager] Avvio di {len(topology)} processi NodoSemaforo")
    
    # Avvio di tutti i nodi
    for node_id in topology.keys():
        p_node = subprocess.Popen([sys.executable, "simulator/nodo_semaforo.py", node_id])
        active_nodes[node_id] = p_node
        
    print("[Node Manager] Tutti i nodi sono operativi.")
    
    # Connessione MQTT per la gestione remota
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        connected = False
        while not connected:
            try:
                # Tenta la connessione al container del broker
                client.connect("mqtt-broker", 1883, 60) 
                connected = True
                print("[Node Manager] Connesso al Broker per i comandi di riavvio")
            except Exception as e:
                print(f"[Node Manager] In attesa del Broker MQTT (Errore: {e})")
                time.sleep(2)
        
        # Una volta connesso, avvia il loop
        client.loop_forever()

    except KeyboardInterrupt:
        print("\n[Node Manager] Chiusura manuale della simulazione.")
        sys.exit(0)
    except Exception as e:
        print(f"[Node Manager] Errore critico MQTT: {e}")

if __name__ == "__main__":
    main()