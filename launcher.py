import subprocess
import sys
import time
import json
import os
import atexit
import paho.mqtt.client as mqtt

active_intersections = {}

def cleanup():
    print("\n[Node Manager] Chiusura di tutti gli incroci in corso...")
    for inc_id, p in active_intersections.items():
        if p.poll() is None:
            p.kill()
    print("[Node Manager] Simulazione terminata.")

atexit.register(cleanup)

def on_connect(client, userdata, flags, rc, properties):
    print("[Node Manager] Connesso al broker.")
    client.subscribe("srs/launcher/control")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        command = payload.get("command")
        inc_id = payload.get("incrocio_id") # Ora basato su incrocio

        if command == "RESTART_INCROCIO" and inc_id:
            print(f"[Node Manager] RESTART richiesto per incrocio: {inc_id}")
            if inc_id in active_intersections:
                old_process = active_intersections[inc_id]
                if old_process.poll() is None:
                    old_process.kill()
            
            new_process = subprocess.Popen([sys.executable, "simulator/nodo_incrocio.py", inc_id])
            active_intersections[inc_id] = new_process
    except Exception as e:
        print(f"[Node Manager] Errore comando MQTT: {e}")

def main():
    topo_path = os.path.join("simulator", "topology.json")
    if not os.path.exists(topo_path):
        print(f"[Node Manager] ERRORE: {topo_path} non trovato!")
        sys.exit(1)
        
    with open(topo_path, "r") as f:
        topology = json.load(f)
        
    # Raggruppiamo i semafori per incrocio (es. INC_0_0)
    unique_intersections = sorted(list(set("_".join(nid.split("_")[:3]) for nid in topology.keys())))
    
    print(f"[Node Manager] Avvio di {len(unique_intersections)} processi NodoIncrocio")
    
    for inc_id in unique_intersections:
        p_inc = subprocess.Popen([sys.executable,"-u", "simulator/nodo_incrocio.py", inc_id], stdout=sys.stdout, stderr=sys.stderr)
        active_intersections[inc_id] = p_inc
        time.sleep(0.5)
        
    print("[Node Manager] Tutti gli incroci sono operativi.")
    
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        brokers = ["haproxy-1", "haproxy-2"]
        connected = False
        while not connected:
            for broker in brokers:
                try:
                    client.connect(broker, 1883, 60) 
                    connected = True
                    break
                except:
                    pass
            if not connected:
                time.sleep(2)
        
        client.loop_forever()
    except KeyboardInterrupt:
        sys.exit(0)

if __name__ == "__main__":
    main()