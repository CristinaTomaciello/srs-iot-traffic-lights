import time
import random
import json
import os
import threading
import uuid
import paho.mqtt.client as mqtt

class NodoSemaforo:
    def __init__(self, semaforo_id):
        self.id = semaforo_id
        self.incrocio_id = "_".join(self.id.split("_")[:3]) 
        self.direzione_assoluta = self.id.split("_")[-1]
        
        if self.direzione_assoluta in ["NORD", "SUD"]:
            self.asse = "NORD_SUD"
            self.is_master = (self.direzione_assoluta == "NORD")
        else:
            self.asse = "EST_OVEST"
            self.is_master = (self.direzione_assoluta == "EST")

        self.connesso = False 
        self.tick_count = 0
        self.stato = "ROSSO"
        self.ha_il_token = False
        
        self.sim_active = False 
        self.token_iniziato = False
        self.is_hardware_fault = False
        
        self.coda_auto = []
        self.auto_in_arrivo = []

        base_dir = os.path.dirname(os.path.abspath(__file__))
        topo_path = os.path.join(base_dir, 'topology.json')
        
        if os.path.exists(topo_path):
            with open(topo_path, 'r') as f:
                mappa_completa = json.load(f)
            self.config_nodo = mappa_completa.get(self.id, {})
            self.strade_uscita = self.config_nodo.get("strade_uscita", {})
            self.is_border = self.config_nodo.get("is_border", False)
            self.base_green_duration = self.config_nodo.get("green_duration", 5)
        else:
            self.config_nodo = {}
            self.strade_uscita = {}
            self.base_green_duration = 5

        self.current_green_duration = self.base_green_duration
        self.override_cycles_left = 0

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
        self.topic_stato = f"srs/edge/{self.incrocio_id}/{self.id}/stato"
        self.topic_inbox = f"srs/edge/{self.id}/inbox_auto"
        self.topic_fase = f"srs/edge/{self.incrocio_id}/fase"
        
        self.topic_admin_ctrl = "srs/admin/control"
        self.topic_admin_inject = f"srs/admin/inject/{self.incrocio_id}"
        self.topic_config = f"srs/edge/{self.id}/config"
        
        # Topic dedicato per la ricezione delle iniezioni di guasto
        self.topic_fault = f"srs/admin/node/{self.id}/fault_injection" 

    def on_connect(self, client, userdata, flags, rc, properties):
        self.connesso = True
        self.client.subscribe(self.topic_inbox)
        self.client.subscribe(self.topic_fase)
        self.client.subscribe(self.topic_admin_ctrl)
        self.client.subscribe(self.topic_admin_inject)
        self.client.subscribe(self.topic_config)
        self.client.subscribe(self.topic_fault)

    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            
            if msg.topic == self.topic_inbox:
                self.auto_in_arrivo.append(payload)
                
            elif msg.topic == self.topic_fase:
                if self.is_hardware_fault:
                    return

                nuova_fase = payload.get("asse")
                
                if nuova_fase == self.asse:
                    self.ha_il_token = True
                    self.stato = "VERDE_PRINCIPALE"
                    self.tick_count = 0
                elif nuova_fase == f"GIALLO_{self.asse}":
                    self.stato = "GIALLO"
                else:
                    self.ha_il_token = False
                    self.stato = "ROSSO"
                    
            elif msg.topic == self.topic_admin_ctrl:
                cmd = payload.get("command")
                if cmd == "START":
                    self.sim_active = True
                    if self.direzione_assoluta == "NORD" and not self.token_iniziato:
                        self.client.publish(self.topic_fase, json.dumps({"asse": "NORD_SUD"}))
                        self.token_iniziato = True
                elif cmd == "PAUSE":
                    self.sim_active = False
                    
            elif msg.topic == self.topic_admin_inject:
                if payload.get("direzione") == self.direzione_assoluta and not self.is_hardware_fault:
                    num_auto = payload.get("count", 1)
                    for _ in range(num_auto):
                        target_r, target_c = random.randint(0, 3), random.randint(0, 7)
                        nuova_auto = {
                            "id": f"inj_{uuid.uuid4().hex[:4]}",
                            "timestamp_arrivo": time.time(),
                            "destinazione": {"r": target_r, "c": target_c}
                        }
                        self.coda_auto.append(nuova_auto)
                        print(f"[{self.id}]: Iniezione Admin: Auto {nuova_auto['id']} aggiunta.")
            
            elif msg.topic == self.topic_config:
                if "green_duration" in payload:
                    nuova_durata = int(payload["green_duration"])
                    cicli = int(payload.get("override_cycles", 1))
                    
                    self.current_green_duration = nuova_durata
                    self.override_cycles_left = cicli
                    print(f"[{self.id}]: MCP OVERRIDE Verde a {nuova_durata}s per {cicli} cicli.")

            elif msg.topic == self.topic_fault:
                fault_type = payload.get("type")
                if fault_type == "SOFTWARE_CRASH":
                    print(f"[{self.id}] Simulazione CRASH SOFTWARE.")
                    os._exit(1) # Terminazione brutale del processo
                elif fault_type == "HARDWARE_FAILURE":
                    print(f"[{self.id}] Simulazione GUASTO HARDWARE.")
                    self.is_hardware_fault = True
                    self.stato = "OFFLINE"
                    self.ha_il_token = False
                elif fault_type == "REPAIR":
                    print(f"[{self.id}] Riparazione HARDWARE completata.")
                    self.is_hardware_fault = False
                    self.stato = "ROSSO"
                    
        except Exception as e:
            pass

    def passa_token(self):
        if self.is_hardware_fault:
            return
        self.ha_il_token = False 
        self.client.publish(self.topic_fase, json.dumps({"asse": f"GIALLO_{self.asse}"}))
        
        def delayed_sequence():
            time.sleep(2) 
            self.client.publish(self.topic_fase, json.dumps({"asse": "ALL_RED_CLEARANCE"}))
            time.sleep(2)
            next_asse = "EST_OVEST" if self.asse == "NORD_SUD" else "NORD_SUD"
            self.client.publish(self.topic_fase, json.dumps({"asse": next_asse}))

        threading.Thread(target=delayed_sequence, daemon=True).start()

    def logica_autonoma(self):
        if not self.sim_active or not self.ha_il_token or self.stato == "GIALLO" or self.is_hardware_fault:
            return

        self.tick_count += 1
        
        if self.is_master and self.tick_count >= self.current_green_duration:
            if self.override_cycles_left > 0:
                self.override_cycles_left -= 1
                if self.override_cycles_left == 0:
                    print(f"[{self.id}]: MCP scaduto. Ritorno al tempo base di {self.base_green_duration}s.")
                    self.current_green_duration = self.base_green_duration
            
            self.passa_token()

    def aggiorna_traffico(self):
        if not self.sim_active or self.is_hardware_fault:
            return

        intensita = 0.4 if (int(time.time()) % 60) < 20 else 0.1
        if self.is_border and random.random() < intensita:
    
            curr_parts = self.id.split("_")
            curr_r, curr_c = int(curr_parts[1]), int(curr_parts[2])

            if curr_c <= 1:
                target_c = random.randint(6, 7)
            elif curr_c >= 6:
                target_c = random.randint(0, 1)
            else:
                target_c = random.randint(0, 7)

            if curr_r == 0:       
                target_r = 3      
            elif curr_r == 3:     
                target_r = 0      
            else:
                target_r = random.randint(0, 3)
            
            nuova_auto = {
                "id": f"car_{uuid.uuid4().hex[:4]}",
                "timestamp_arrivo": time.time(),
                "destinazione": {"r": target_r, "c": target_c}
            }
            self.coda_auto.append(nuova_auto)

        tempo_attuale = time.time()
        ancora_in_viaggio = []
        for auto in self.auto_in_arrivo:
            if tempo_attuale >= auto.get("timestamp_arrivo", 0):
                self.coda_auto.append(auto)
            else:
                ancora_in_viaggio.append(auto)
        self.auto_in_arrivo = ancora_in_viaggio

        if self.stato == "VERDE_PRINCIPALE" and self.coda_auto:
            auto = self.coda_auto.pop(0)
            if self.strade_uscita:
                dest = auto.get("destinazione", {"r": 0, "c": 0})
                
                best_dir = None
                min_distance = 9999
                
                for dir_name, target_info in self.strade_uscita.items():
                    target_str = target_info["target"]
                    
                    if target_str == "OUT":
                        curr_parts = self.id.split("_")
                        curr_r, curr_c = int(curr_parts[1]), int(curr_parts[2])
                        distanza = abs(curr_r - dest["r"]) + abs(curr_c - dest["c"])
                        
                        if distanza <= 1: 
                            best_dir = dir_name
                            min_distance = 0
                            break
                        continue 
                    
                    t_parts = target_str.split("_")
                    t_r, t_c = int(t_parts[1]), int(t_parts[2])
                    
                    distanza = abs(t_r - dest["r"]) + abs(t_c - dest["c"])
                    
                    if distanza < min_distance:
                        min_distance = distanza
                        best_dir = dir_name
                
                if not best_dir:
                    best_dir = random.choice(list(self.strade_uscita.keys()))

                target_info = self.strade_uscita[best_dir]
                destinazione_nodo = target_info["target"]
                
                if destinazione_nodo != "OUT":
                    auto["timestamp_arrivo"] = time.time() + target_info["tempo_transito"]
                    topic_target = f"srs/edge/{destinazione_nodo}/inbox_auto"
                    self.client.publish(topic_target, json.dumps(auto))

    def run(self):
        payload_morte = json.dumps({
            "colore": "OFFLINE", 
            "auto_in_coda": len(self.coda_auto),
            "green_duration": 0
        })
        
        # Impostiamo il testamento sul topic di stato del nodo
        self.client.will_set(self.topic_stato, payload_morte, qos=1, retain=False)
        
        self.client.connect("mqtt-broker", 1883, 60)
        self.client.loop_start()

        while True:
            self.logica_autonoma()
            self.aggiorna_traffico()
            
            payload = {
                "colore": self.stato, 
                "auto_in_coda": len(self.coda_auto),
                "green_duration": self.current_green_duration
            }
            self.client.publish(self.topic_stato, json.dumps(payload))
            time.sleep(1)

if __name__ == "__main__":
    import sys
    id_nodo = sys.argv[1] if len(sys.argv) > 1 else "INC_0_0_NORD"
    NodoSemaforo(id_nodo).run()