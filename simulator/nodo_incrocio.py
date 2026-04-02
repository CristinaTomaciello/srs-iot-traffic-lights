import time
import random
import json
import os
import uuid
import paho.mqtt.client as mqtt
import sys
import traceback
from utility.mqtt_utils import connetti_con_failover

class NodoIncrocio:
    def __init__(self, incrocio_id):
        self.id = incrocio_id
        self.sim_active = False
        self.fase_attuale = "NORD_SUD"
        self.tick_count = 0
        self.in_giallo = False
        self.clearance_active = False
        
        self.mqtt_connected = False
        
        topo_path = "/app/topology.json"
        if not os.path.exists(topo_path):
            topo_path = os.path.join(os.path.dirname(__file__), 'topology.json')
        
        try:
            with open(topo_path, 'r') as f:
                full_topo = json.load(f)
        except Exception as e:
            print(f"[{self.id}] ERRORE CRITICO: Impossibile caricare la mappa: {e}")
            sys.exit(1)
        
        self.semafori = {}
        for s_id, cfg in full_topo.items():
            if s_id.startswith(self.id):
                dir_name = s_id.split("_")[-1]
                self.semafori[dir_name] = {
                    "id": s_id, "stato": "ROSSO", "coda": [], "auto_in_arrivo": [],
                    "config": cfg, "green_duration": cfg.get("green_duration", 5),
                    "base_green": cfg.get("green_duration", 5), "override_cycles": 0,
                    "is_faulty": False
                }

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"CLIENT_{self.id}")
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        
        self.client.will_set(f"srs/status/incrocio/{self.id}", "OFFLINE", qos=1, retain=True)

    def on_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            self.mqtt_connected = True
            self.client.subscribe("srs/admin/control")
            for s in self.semafori.values():
                self.client.subscribe(f"srs/edge/{s['id']}/inbox_auto")
                self.client.subscribe(f"srs/edge/{s['id']}/config")
                self.client.subscribe(f"srs/admin/node/{s['id']}/fault_injection")
            self.client.subscribe(f"srs/admin/inject/{self.id}")
        else:
            print(f"[{self.id}] EMQX ha rifiutato la connessione, codice: {rc}")

    def is_connected(self):
        return self.mqtt_connected
    
    def on_disconnect(self, client, userdata, flags, rc, properties):
        self.mqtt_connected = False

    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            if msg.topic == "srs/admin/control":
                nuovo_stato = (payload.get("command") == "START")
                if nuovo_stato and not self.sim_active:
                    self.timer_fase = 0
                    self.in_giallo = False
                self.sim_active = nuovo_stato
            elif "fault_injection" in msg.topic:
                dir_name = msg.topic.split("/")[3].split("_")[-1]
                f_type = payload.get("type")
                if f_type == "SOFTWARE_CRASH": os._exit(1)
                elif f_type == "HARDWARE_FAILURE": self.semafori[dir_name]["is_faulty"] = True
                elif f_type == "REPAIR": self.semafori[dir_name]["is_faulty"] = False
            elif "config" in msg.topic:
                dir_name = msg.topic.split("/")[2].split("_")[-1]
                self.semafori[dir_name]["green_duration"] = int(payload["green_duration"])
                self.semafori[dir_name]["override_cycles"] = int(payload.get("override_cycles", 1))
            elif "inbox_auto" in msg.topic:
                dir_name = msg.topic.split("/")[2].split("_")[-1]
                self.semafori[dir_name]["auto_in_arrivo"].append(payload)
            elif f"srs/admin/inject/{self.id}" == msg.topic:
                d = payload.get("direzione")
                if d in self.semafori: self.genera_auto(d, payload.get("count", 1))
        except: pass

    def genera_auto(self, dir_name, count=1):
        s = self.semafori[dir_name]
        for _ in range(count):
            s["coda"].append({
                "id": f"car_{uuid.uuid4().hex[:4]}",
                "timestamp_arrivo": time.time(),
                "destinazione": {"r": random.randint(0, 3), "c": random.randint(0, 7)}
            })

    def aggiorna_logica_luci(self):
        if not self.sim_active: return
        self.tick_count += 1
        master = "NORD" if self.fase_attuale == "NORD_SUD" else "EST"
        limit = self.semafori[master]["green_duration"]

        if not self.in_giallo and not self.clearance_active:
            if self.tick_count >= limit:
                self.in_giallo = True
                self.tick_count = 0
        elif self.in_giallo:
            if self.tick_count >= 2:
                self.in_giallo = False
                self.clearance_active = True
                self.tick_count = 0
        elif self.clearance_active:
            if self.tick_count >= 1:
                self.clearance_active = False
                self.tick_count = 0
                old_fase = self.fase_attuale
                self.fase_attuale = "EST_OVEST" if old_fase == "NORD_SUD" else "NORD_SUD"
                for d in (["NORD", "SUD"] if old_fase == "NORD_SUD" else ["EST", "OVEST"]):
                    if self.semafori[d]["override_cycles"] > 0:
                        self.semafori[d]["override_cycles"] -= 1
                        if self.semafori[d]["override_cycles"] == 0:
                            self.semafori[d]["green_duration"] = self.semafori[d]["base_green"]

        asse_attivo = ["NORD", "SUD"] if self.fase_attuale == "NORD_SUD" else ["EST", "OVEST"]
        for d, s in self.semafori.items():
            if s["is_faulty"]: s["stato"] = "OFFLINE"
            elif self.clearance_active: s["stato"] = "ROSSO"
            elif d in asse_attivo: s["stato"] = "GIALLO" if self.in_giallo else "VERDE_PRINCIPALE"
            else: s["stato"] = "ROSSO"

    def processa_traffico(self):
        if not self.sim_active: return
        ora = time.time()
        for d, s in self.semafori.items():
            if s["is_faulty"]: continue
            intensita = 0.4 if (int(ora) % 60) < 20 else 0.1
            if s["config"].get("is_border") and random.random() < (intensita/4):
                self.genera_auto(d)
            for a in list(s["auto_in_arrivo"]):
                if ora >= a["timestamp_arrivo"]:
                    s["coda"].append(a)
                    s["auto_in_arrivo"].remove(a)
            if s["stato"] == "VERDE_PRINCIPALE" and s["coda"]:
                auto = s["coda"].pop(0)
                dest = auto["destinazione"]
                best_dir = None
                min_dist = 9999
                strade = s["config"].get("strade_uscita", {})
                for dir_uscita, info in strade.items():
                    target = info["target"]
                    if target == "OUT":
                        r, c = int(self.id.split("_")[1]), int(self.id.split("_")[2])
                        dist = abs(r - dest["r"]) + abs(c - dest["c"])
                        if dist <= 1: 
                            best_dir = dir_uscita
                            break
                        continue
                    tr, tc = int(target.split("_")[1]), int(target.split("_")[2])
                    dist = abs(tr - dest["r"]) + abs(tc - dest["c"])
                    if dist < min_dist:
                        min_dist = dist
                        best_dir = dir_uscita
                if not best_dir and strade: best_dir = random.choice(list(strade.keys()))
                if best_dir:
                    target_info = strade[best_dir]
                    if target_info["target"] != "OUT":
                        auto["timestamp_arrivo"] = ora + target_info["tempo_transito"]
                        self.client.publish(f"srs/edge/{target_info['target']}/inbox_auto", json.dumps(auto))

    def run(self):
        """
        Ciclo principale dell'incrocio: gestisce la connessione e la simulazione.
        """
        # --- CONNESSIONE INIZIALE ---
        # Deleghiamo tutto al modulo esterno 'mqtt_utils'
        connetti_con_failover(self.client, self.id, self.is_connected)
        
        # Una volta connessi, dichiariamo la presenza al sistema
        self.client.publish(f"srs/status/incrocio/{self.id}", "ONLINE", qos=1, retain=True)

        # Memoria locale per evitare di inviare dati identici (Anti-Spam)
        last_states = {}

        try:
            while True:
                # --- MONITORAGGIO FAILOVER ---
                # Se la connessione cade, invochiamo di nuovo la logica rotante sui broker
                if not self.mqtt_connected:
                    print(f"[{self.id}] Rilevata disconnessione! Avvio recupero su broker di scorta...", flush=True)
                    connetti_con_failover(self.client, self.id, self.is_connected)
                    # Ri-pubblichiamo lo stato online sul nuovo broker
                    self.client.publish(f"srs/status/incrocio/{self.id}", "ONLINE", qos=1, retain=True)

                # --- LOGICA DI SIMULAZIONE ---
                self.aggiorna_logica_luci() #
                self.processa_traffico()    #
                
                # --- PUBBLICAZIONE STATO ---
                for d, s in self.semafori.items():
                    payload = {
                        "colore": s["stato"], 
                        "auto_in_coda": len(s["coda"]), 
                        "green_duration": s["green_duration"]
                    }
                    
                    # Pubblichiamo SOLO se qualcosa è cambiato rispetto all'ultimo secondo
                    if payload != last_states.get(s['id']):
                        self.client.publish(f"srs/edge/{self.id}/{s['id']}/stato", json.dumps(payload))
                        last_states[s['id']] = payload # Aggiorna la memoria locale
                
                # Respiro per la CPU per mantenere la sincronia temporale
                time.sleep(1)

        except Exception as e:
            print(f"[{self.id}] ERRORE FATALE NEL CICLO RUN: {e}", flush=True)
            traceback.print_exc()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        NodoIncrocio(sys.argv[1]).run()