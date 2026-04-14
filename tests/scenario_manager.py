import paho.mqtt.client as mqtt
import json
import time
import sys

# Usa localhost perché lo script viene lanciato dal PC host
# La porta 1883 è esposta dal container srs-haproxy-1
BROKER = "localhost"
PORT = 1883

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
try:
    client.connect(BROKER, PORT)
    client.loop_start()
except Exception as e:
    print(f"Impossibile connettersi al broker locale: {e}")
    sys.exit(1)

def send_fault(node_id, fault_type):
    # Il topic chirurgico per colpire il singolo semaforo
    topic = f"srs/admin/node/{node_id}/fault_injection"
    payload = {"type": fault_type}
    client.publish(topic, json.dumps(payload))
    print(f"\n💥 [INVIATO] Guasto '{fault_type}' iniettato nel semaforo: {node_id}")

def format_node_id(raw_id):
    """Formatta l'ID per puntare a un singolo semaforo di un incrocio."""
    clean_id = raw_id.strip().upper()
    
    if not clean_id:
        return "INC_1_1_NORD"
        
    if not clean_id.startswith("INC_"):
        clean_id = "INC_" + clean_id
        
    # Se l'utente ha inserito solo l'incrocio (es. INC_1_1), chiediamo la direzione
    if not any(clean_id.endswith(d) for d in ["_NORD", "_SUD", "_EST", "_OVEST"]):
        print(f"\n⚠️ Hai selezionato l'intero incrocio ({clean_id}).")
        dir_scelta = input("Quale semaforo vuoi far crashare? (NORD, SUD, EST, OVEST): ").strip().upper()
        if dir_scelta in ["NORD", "SUD", "EST", "OVEST"]:
            clean_id = f"{clean_id}_{dir_scelta}"
        else:
            print("Direzione non valida, uso NORD come default.")
            clean_id = f"{clean_id}_NORD"
            
    return clean_id

def print_menu():
    print("\n" + "="*50)
    print(" 🚦 SRS CHAOS MANAGER - INIEZIONE GUASTI")
    print("="*50)
    print(" 1. SOFTWARE CRASH (Il semaforo si blocca/smette di aggiornare)")
    print(" 2. HARDWARE FAILURE (Il semaforo va OFFLINE)")
    print(" 0. Esci")
    print("="*50)

def main():
    try:
        while True:
            print_menu()
            scelta = input("Seleziona un'opzione (0-2): ").strip()
            
            if scelta == "0":
                print("Chiusura Chaos Manager.")
                client.loop_stop()
                sys.exit(0)
                
            if scelta in ["1", "2"]:
                raw_id = input("Inserisci ID Incrocio (es. INC_1_1 oppure INC_1_1_SUD): ")
                node_id = format_node_id(raw_id)
                    
                if scelta == "1":
                    send_fault(node_id, "SOFTWARE_CRASH")
                elif scelta == "2":
                    send_fault(node_id, "HARDWARE_FAILURE")
            else:
                print("Scelta non valida.")
                
            time.sleep(1)
            
    except KeyboardInterrupt:
        client.loop_stop()
        print("\nUscita forzata.")

if __name__ == "__main__":
    main()