import subprocess
import sys
import time
import json
import os
import atexit
import paho.mqtt.client as mqtt

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
active_intersections = {}

def log_manager(emoji, message):
    print(f"[NODE-MANAGER] {emoji} {message}", flush=True)

def cleanup():
    log_manager("🛑", "Chiusura di tutti i processi incrocio...")
    for inc_id, p in active_intersections.items():
        if p.poll() is None:
            p.kill()
    log_manager("🏁", "Simulazione terminata.")

atexit.register(cleanup)

def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        log_manager("🌐", "Connesso al broker. In attesa di comandi.")
        client.subscribe("srs/launcher/control")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        command = payload.get("command")
        inc_id = payload.get("incrocio_id")
        if command == "RESTART_INCROCIO" and inc_id:
            log_manager("🔄", f"RESTART richiesto per: {inc_id}")
            if inc_id in active_intersections:
                old_p = active_intersections[inc_id]
                if old_p.poll() is None: old_p.kill()
            nodo_path = os.path.join(BASE_DIR, "nodo_incrocio.py")
            new_p = subprocess.Popen([sys.executable, "-u", nodo_path, inc_id])
            active_intersections[inc_id] = new_p
            log_manager("✅", f"Incrocio {inc_id} riavviato.")
    except Exception as e:
        log_manager("💥", f"Errore launcher: {e}")

def main():
    topo_path = os.path.join(BASE_DIR, "topology.json")
    with open(topo_path, "r") as f:
        topology = json.load(f)
    unique_inc = sorted(list(set("_".join(nid.split("_")[:3]) for nid in topology.keys())))
    log_manager("🏗️", f"Avvio simulazione: {len(unique_inc)} incroci.")
    for inc_id in unique_inc:
        nodo_path = os.path.join(BASE_DIR, "nodo_incrocio.py")
        p = subprocess.Popen([sys.executable, "-u", nodo_path, inc_id], stdout=sys.stdout, stderr=sys.stderr)
        active_intersections[inc_id] = p
        time.sleep(0.2)
    log_manager("🚀", "Tutti gli incroci operativi.")
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    try:
        connected = False
        while not connected:
            for b in ["haproxy-1", "haproxy-2"]:
                try:
                    client.connect(b, 1883, 60); connected = True; break
                except: pass
            if not connected: time.sleep(2)
        client.loop_forever()
    except KeyboardInterrupt: sys.exit(0)

if __name__ == "__main__":
    main()
