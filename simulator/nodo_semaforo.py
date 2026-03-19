import time
import random
import requests
import uuid

class NodoSemaforo:
    def __init__(self, semaforo_id):
        self.id = semaforo_id
        self.connesso_al_centro = False 
        self.tick_count = 0
        
        # Stato iniziale
        self.stato = "ROSSO"
        
        # Invece di una lista unica, abbiamo 3 code separate
        self.code_auto = {
            "dritto": [],
            "destra": [],
            "sinistra": []
        }
        
        #Mappa per tradurre la direzione di destinazione in corsia
        self.mappa_direzioni = {
            "NORD": {"SUD": "dritto", "OVEST": "destra", "EST": "sinistra"},
            "SUD": {"NORD": "dritto", "EST": "destra", "OVEST": "sinistra"},
            "EST": {"OVEST": "dritto", "NORD": "destra", "SUD": "sinistra"},
            "OVEST": {"EST": "dritto", "SUD": "destra", "NORD": "sinistra"}
        }
        self.direzioni_nodo = self.mappa_direzioni.get(self.id, {})
        self.nodi_adiacenti = list(self.direzioni_nodo.keys())

        # Token Ring per la gestione del turno di verde
        self.ha_il_token = False
        if self.id == "NORD":
            self.ha_il_token = True # Per il test locale, NORD parte col verde

    def determina_corsia(self, prossima_destinazione):
        """Traduce il nome del nodo di destinazione nella corsia corretta."""
        return self.direzioni_nodo.get(prossima_destinazione, "dritto")

    def genera_auto(self):
        """Genera un'auto con ID univoco, path e timestamp"""
        lunghezza_path = random.randint(1, 2)
        path = []
        nodo_corrente = self.id
        
        for _ in range(lunghezza_path):
            next_node = random.choice([n for n in ["NORD", "SUD", "EST", "OVEST"] if n != nodo_corrente and n != self.id])
            path.append(next_node)
            nodo_corrente = next_node

        return {
            "id": f"car_{uuid.uuid4().hex[:6]}",
            "path": path,
            "step_corrente": 0,
            "timestamp_ingresso": time.time() # ⏱️ NUOVO: Cruciale per l'Agente LLM!
        }

    def aggiorna_traffico(self):
        # Genera auto casualmente (30% di probabilità ogni secondo)
        if random.random() < 0.3:
            nuova_auto = self.genera_auto()
            prossima_destinazione = nuova_auto["path"][0]
            
            # Smistiamo l'auto nella corsia corretta
            corsia = self.determina_corsia(prossima_destinazione)
            self.code_auto[corsia].append(nuova_auto)
            print(f"[{self.id}] : Auto {nuova_auto['id']} in corsia '{corsia.upper()}' (verso {prossima_destinazione})")

        # Gestione uscita auto se il semaforo è verde per la loro corsia
        corsie_verdi = []
        if self.stato == "VERDE_PRINCIPALE":
            corsie_verdi = ["dritto", "destra"]
        elif self.stato == "VERDE_SINISTRA":
            corsie_verdi = ["sinistra"]

        for corsia in corsie_verdi:
            if self.code_auto[corsia]:
                auto_in_uscita = self.code_auto[corsia].pop(0)
                
                # Calcoliamo quanto ha aspettato
                tempo_attesa = round(time.time() - auto_in_uscita["timestamp_ingresso"], 1)
                prossima_destinazione = auto_in_uscita["path"][auto_in_uscita["step_corrente"]]
                
                print(f"[{self.id}] :Auto {auto_in_uscita['id']} esce verso {prossima_destinazione} (Attesa: {tempo_attesa}s)")

    def logica_autonoma(self):
        """Ciclo a Fasi per evitare incidenti sulle svolte."""
        self.tick_count += 1

        if not self.connesso_al_centro:
            self.stato = "GIALLO_LAMPEGGIANTE"
            return

        if not self.ha_il_token:
            self.stato = "ROSSO"
            self.tick_count = 0
            return

        # Macchina a stati per le Fasi del Semaforo
        if self.stato == "ROSSO":
            self.stato = "VERDE_PRINCIPALE" #
            self.tick_count = 0
        elif self.stato == "VERDE_PRINCIPALE" and self.tick_count >= 8:
            self.stato = "GIALLO_1"
            self.tick_count = 0
        elif self.stato == "GIALLO_1" and self.tick_count >= 2:
            self.stato = "VERDE_SINISTRA"
            self.tick_count = 0
        elif self.stato == "VERDE_SINISTRA" and self.tick_count >= 4:
            self.stato = "GIALLO_2"
            self.tick_count = 0
        elif self.stato == "GIALLO_2" and self.tick_count >= 2:
            self.stato = "ROSSO"
            self.tick_count = 0
            
            # Fine del turno. Cediamo il token.
            self.ha_il_token = False
            print(f"[{self.id}] Ciclo Fasi terminato. In attesa di MQTT per cedere il token")

    def run(self):
        print(f"Avvio Nodo Semaforo [{self.id}]")
        while True:
            self.logica_autonoma()
            self.aggiorna_traffico()
            
            # Calcoliamo il totale delle auto per la UI
            totale_auto = sum(len(coda) for coda in self.code_auto.values())
            
            # Adattiamo gli stati interni complessi per la UI
            stato_ui = "ROSSO"
            if "VERDE" in self.stato: stato_ui = "VERDE"
            elif "GIALLO" in self.stato: stato_ui = "GIALLO"
            
            print(f"[{self.id}] Fase: {self.stato: <18} | Auto totali: {totale_auto: <3} | Tick: {self.tick_count}")
            
            payload = {
                "id": self.id,
                "stato": stato_ui, 
                "coda": totale_auto 
            }
            try:
                requests.post("http://localhost:8080/update_node", json=payload)
            except requests.exceptions.ConnectionError:
                pass 

            time.sleep(1)

if __name__ == "__main__":
    nodo = NodoSemaforo("NORD")
    nodo.connesso_al_centro = True
    try:
        nodo.run()
    except KeyboardInterrupt:
        print("\nArresto manuale del nodo.")