import paho.mqtt.client as mqtt
import json
import time
import sys

BROKER = "localhost"
PORT = 1883

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
try:
    client.connect(BROKER, PORT)
    client.loop_start()
except Exception as e:
    print(f"Impossibile connettersi: {e}")
    sys.exit(1)

def send_fault(node_id, fault_type):
    topic = f"srs/admin/node/{node_id}/fault_injection"
    payload = {"type": fault_type}
    client.publish(topic, json.dumps(payload))
    print(f"\n[INVIATO] Comando '{fault_type}' al nodo: {node_id}")

def send_restart(node_id):
    topic = "srs/launcher/control"
    payload = {"command": "RESTART_NODE", "node_id": node_id}
    client.publish(topic, json.dumps(payload))
    print(f"\n[INVIATO] Ordine di RESTART al Launcher per il nodo: {node_id}")

def format_node_id(raw_id):
    """Corregge automaticamente gli errori di digitazione più comuni"""
    clean_id = raw_id.strip().upper()
    
    if not clean_id:
        return "INC_0_0_NORD"
        
    if not clean_id.startswith("INC_"):
        clean_id = "INC_" + clean_id
        
    if not any(clean_id.endswith(d) for d in ["_NORD", "_SUD", "_EST", "_OVEST"]):
        print(f"\nATTENZIONE: Manca la direzione! Aggiungo '_NORD' in automatico per il test.")
        clean_id += "_NORD"
        
    return clean_id

def print_menu():
    print("\n" + "="*45)
    print(" SRS SCENARIO MANAGER - CONTROL ROOM")
    print("="*45)
    print(" 1. Genera SOFTWARE CRASH (Il nodo muore)")
    print(" 2. Genera HARDWARE FAILURE (Il nodo va OFFLINE)")
    print(" 3. Ripara HARDWARE FAILURE (Il nodo riparte)")
    print(" 4. Ordina RESTART al Launcher (Resuscita nodo)")
    print(" 0. Esci")
    print("="*45)

def main():
    try:
        while True:
            print_menu()
            scelta = input("Seleziona un'opzione (0-4): ").strip()
            
            if scelta == "0":
                print("Chiusura Scenario Manager.")
                client.loop_stop()
                sys.exit(0)
                
            if scelta in ["1", "2", "3", "4"]:
                raw_id = input("Inserisci ID (es. INC_1_1_SUD): ")
                node_id = format_node_id(raw_id)
                    
                if scelta == "1":
                    send_fault(node_id, "SOFTWARE_CRASH")
                elif scelta == "2":
                    send_fault(node_id, "HARDWARE_FAILURE")
                elif scelta == "3":
                    send_fault(node_id, "REPAIR")
                elif scelta == "4":
                    send_restart(node_id)
            else:
                print("Scelta non valida.")
                
            time.sleep(1)
            
    except KeyboardInterrupt:
        client.loop_stop()
        print("\nUscita forzata. A presto!")

if __name__ == "__main__":
    main()