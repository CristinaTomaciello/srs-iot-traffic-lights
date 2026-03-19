const ws = new WebSocket("ws://" + window.location.host + "/ws");

ws.onopen = function() {
    console.log("Connesso al server UI");
};

ws.onmessage = function(event) {
    const data = JSON.parse(event.data);
    console.log("Nuovo dato dal semaforo:", data);
    
    // Trova l'elemento HTML del semaforo corrispondente e aggiorna il suo stato
    const nodeId = "tl-" + data.id.toLowerCase();
    const semaforoDiv = document.getElementById(nodeId);
    
    if (semaforoDiv) {
        // Aggiorna la coda
        const codaBadge = semaforoDiv.querySelector(".coda-badge");
        codaBadge.innerText = "Code: " + data.coda;

        // Aggiorna il colore della luce
        const light = semaforoDiv.querySelector(".light");
        
        // Rimuove tutti i colori attuali
        light.classList.remove("red", "green", "yellow");
        
        // Applica il colore in base allo stato del semaforo
        if (data.stato === "ROSSO") {
            light.classList.add("red");
        } else if (data.stato === "VERDE") {
            light.classList.add("green");
        } else if (data.stato === "GIALLO" || data.stato === "GIALLO_LAMPEGGIANTE") {
            light.classList.add("yellow");
        }
    }
};