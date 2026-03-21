import subprocess
import time
import json
import os
import signal
import sys
import atexit

processes = []
log_file = open("simulator/simulation.log", "w")

def spegni_tutto():
    print("\nPulizia in corso: chiusura di tutti i processi e liberazione delle porte...")
    for p in processes:
        try:
            p.kill()
        except:
            pass
    try:
        log_file.close()
    except:
        pass
    print("Simulatore spento correttamente.")

atexit.register(spegni_tutto)

def signal_handler(sig, frame):
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def start_simulation():
    topo_path = os.path.join("simulator", "topology.json")
    if not os.path.exists(topo_path):
        print(f"Errore: {topo_path} non trovato!")
        return

    with open(topo_path, "r") as f:
        topology = json.load(f)

    print(f"Avvio simulazione per {len(topology)} nodi")

    print("Avvio Central Controller")
    p_controller = subprocess.Popen(
        ["python3", "controller/main_controller.py"], 
        stdout=log_file, stderr=log_file
    )
    processes.append(p_controller)
    time.sleep(2)

    print("Avvio Web Dashboard")
    p_ui = subprocess.Popen(
        ["python3", "simulator/webapp/server_ui.py"]
    )
    processes.append(p_ui)
    time.sleep(2)

    print("Avvio dei nodi in corso")
    for node_id in topology.keys():
        p = subprocess.Popen(
            ["python3", "-u", "simulator/nodo_semaforo.py", node_id], 
            stdout=log_file, stderr=log_file
        )
        processes.append(p)
        time.sleep(0.1)

    print("\nCitta online! Premi CTRL+C per terminare.")
    
    while True:
        time.sleep(1)

if __name__ == "__main__":
    start_simulation()