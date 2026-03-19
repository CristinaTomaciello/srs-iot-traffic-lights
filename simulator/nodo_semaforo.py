import time
import random
import requests

class NodoSemaforo:
    def __init__(self, semaforo_id):
        self.id = semaforo_id
        self.stato = "ROSSO"
        self.coda = 0
        self.tick_count = 0
        
        # Se scende a False, il semaforo sa di essere isolato dal sistema centrale.
        self.connesso_al_centro = False 

    def aggiorna_traffico(self):
        """Simula l'arrivo e il deflusso delle auto."""
        if self.stato == "ROSSO":
            self.coda += random.randint(0, 3)  # Arrivano auto
        elif self.stato == "VERDE":
            self.coda = max(0, self.coda - 5)  # Le auto passano

    def logica_autonoma(self):
        """Il ciclo vitale del semaforo che non dipende da nessuno."""
        self.tick_count += 1

        # Semaforo rotto
        if not self.connesso_al_centro:
            self.stato = "GIALLO_LAMPEGGIANTE"
            return

        # Ciclo normale
        if self.stato == "ROSSO" and self.tick_count >= 10:
            self.stato = "VERDE"
            self.tick_count = 0
        elif self.stato == "VERDE" and self.tick_count >= 10:
            self.stato = "GIALLO"
            self.tick_count = 0
        elif self.stato == "GIALLO" and self.tick_count >= 3:
            self.stato = "ROSSO"
            self.tick_count = 0

    def run(self):
        print(f"Avvio Nodo Semaforo [{self.id}]...")
        while True:
            self.logica_autonoma()
            self.aggiorna_traffico()
            
            print(f"[{self.id}] Stato: {self.stato: <18} | Coda: {self.coda: <3} | Tick: {self.tick_count}") #Log
            
            time.sleep(1) #1 tick = 1 secondo
    def run(self):
        print(f"Avvio Nodo Semaforo [{self.id}]...")
        while True:
            self.logica_autonoma()
            self.aggiorna_traffico()
            
            print(f"[{self.id}] Stato: {self.stato: <18} | Coda: {self.coda: <3} | Tick: {self.tick_count}")
            
            # Invio dati al server UI
            payload = {
                "id": self.id,
                "stato": self.stato,
                "coda": self.coda
            }
            try:
                # Invia una POST al server UI
                requests.post("http://localhost:8080/update_node", json=payload)
            except requests.exceptions.ConnectionError:
                pass #Server non disponibile quindi ignora e continua

            time.sleep(1)

if __name__ == "__main__":
    # Test locale senza rete
    nodo = NodoSemaforo("NORD")
    
    nodo.connesso_al_centro = True
    
    try:
        nodo.run()
    except KeyboardInterrupt:
        print("\nArresto manuale del nodo.")