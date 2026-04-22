import json
import os
import random

# Cambia questi valori per ingrandire o rimpicciolire la città
ROWS = 3
COLS = 5

topology = {}

for r in range(ROWS):
    for c in range(COLS):
        inc_id = f"INC_{r}_{c}"

        for dir_in, token_next in [("NORD", "EST"), ("EST", "SUD"), ("SUD", "OVEST"), ("OVEST", "NORD")]:
            node_id = f"{inc_id}_{dir_in}"
            strade_uscita = {}
            is_border = False

            # Identifica se il semaforo è un punto di ingresso in città
            if dir_in == "NORD" and r == 0: is_border = True
            if dir_in == "SUD" and r == ROWS - 1: is_border = True
            if dir_in == "OVEST" and c == 0: is_border = True
            if dir_in == "EST" and c == COLS - 1: is_border = True

            green_duration = 10
            
            # Calcolo delle rotte con vie di fuga se si tocca il bordo
            if dir_in == "NORD":
                strade_uscita["dritto"] = {"target": f"INC_{r+1}_{c}_NORD", "tempo_transito": 6} if r + 1 < ROWS else {"target": "OUT", "tempo_transito": 2}
                strade_uscita["destra"] = {"target": f"INC_{r}_{c-1}_EST", "tempo_transito": 4} if c - 1 >= 0 else {"target": "OUT", "tempo_transito": 2}
                strade_uscita["sinistra"] = {"target": f"INC_{r}_{c+1}_OVEST", "tempo_transito": 5} if c + 1 < COLS else {"target": "OUT", "tempo_transito": 2}

            elif dir_in == "SUD":
                strade_uscita["dritto"] = {"target": f"INC_{r-1}_{c}_SUD", "tempo_transito": 6} if r - 1 >= 0 else {"target": "OUT", "tempo_transito": 2}
                strade_uscita["destra"] = {"target": f"INC_{r}_{c+1}_OVEST", "tempo_transito": 4} if c + 1 < COLS else {"target": "OUT", "tempo_transito": 2}
                strade_uscita["sinistra"] = {"target": f"INC_{r}_{c-1}_EST", "tempo_transito": 5} if c - 1 >= 0 else {"target": "OUT", "tempo_transito": 2}

            elif dir_in == "OVEST":
                strade_uscita["dritto"] = {"target": f"INC_{r}_{c+1}_OVEST", "tempo_transito": 6} if c + 1 < COLS else {"target": "OUT", "tempo_transito": 2}
                strade_uscita["destra"] = {"target": f"INC_{r+1}_{c}_NORD", "tempo_transito": 4} if r + 1 < ROWS else {"target": "OUT", "tempo_transito": 2}
                strade_uscita["sinistra"] = {"target": f"INC_{r-1}_{c}_SUD", "tempo_transito": 5} if r - 1 >= 0 else {"target": "OUT", "tempo_transito": 2}

            elif dir_in == "EST":
                strade_uscita["dritto"] = {"target": f"INC_{r}_{c-1}_EST", "tempo_transito": 6} if c - 1 >= 0 else {"target": "OUT", "tempo_transito": 2}
                strade_uscita["destra"] = {"target": f"INC_{r-1}_{c}_SUD", "tempo_transito": 4} if r - 1 >= 0 else {"target": "OUT", "tempo_transito": 2}
                strade_uscita["sinistra"] = {"target": f"INC_{r+1}_{c}_NORD", "tempo_transito": 5} if r + 1 < ROWS else {"target": "OUT", "tempo_transito": 2}

            # Assemblaggio finale del nodo con il nuovo parametro
            topology[node_id] = {
                "token_next": f"{inc_id}_{token_next}",
                "is_border": is_border,
                "green_duration": green_duration,
                "strade_uscita": strade_uscita
            }

# Salvataggio nel file topology.json
base_dir = os.path.dirname(os.path.abspath(__file__))
topo_path = os.path.join(base_dir, '../topology.json')

with open(topo_path, "w") as f:
    json.dump(topology, f, indent=2)

print(f"Topologia generata con successo e salvata")