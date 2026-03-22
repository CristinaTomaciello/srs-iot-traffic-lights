document.addEventListener("DOMContentLoaded", async () => {
    const response = await fetch('/api/topology');
    const topology = await response.json();

    const cityMap = document.getElementById("city-map");
    let maxRow = 0;
    let maxCol = 0;
    const incroci = new Map(); 
    const roadConnections = []; 

    for (const nodeId in topology) {
        const parts = nodeId.split("_");
        if (parts.length >= 4) {
            const r = parseInt(parts[1]);
            const c = parseInt(parts[2]);
            const incrocioId = `${parts[0]}_${parts[1]}_${parts[2]}`;
            
            if (!incroci.has(incrocioId)) {
                incroci.set(incrocioId, { row: r, col: c });
            }
            
            if (r > maxRow) maxRow = r;
            if (c > maxCol) maxCol = c;

            const strade = topology[nodeId].strade_uscita;
            if (strade) {
                for (const dir in strade) {
                    const targetId = strade[dir].target;
                    if (targetId !== "OUT") {
                        const targetParts = targetId.split("_");
                        const targetIncrocio = `${targetParts[0]}_${targetParts[1]}_${targetParts[2]}`;
                        roadConnections.push({ from: incrocioId, to: targetIncrocio });
                    }
                }
            }
        }
    }

    cityMap.style.gridTemplateColumns = `repeat(${maxCol + 1}, auto)`;

    for (let r = 0; r <= maxRow; r++) {
        for (let c = 0; c <= maxCol; c++) {
            const incrocioId = `INC_${r}_${c}`;
            const div = document.createElement("div");
            div.className = "intersection";
            div.id = `inc-${incrocioId}`;

            // Etichetta centrale
            const label = document.createElement("div");
            label.className = "intersection-label";
            label.innerText = `${r},${c}`;
            div.appendChild(label);

            if (incroci.has(incrocioId)) {
                const direzioni = ["NORD", "SUD", "EST", "OVEST"];
                direzioni.forEach(dir => {
                    const fullId = `${incrocioId}_${dir}`;
                    if (topology[fullId]) {
                        const tlDiv = document.createElement("div");
                        tlDiv.className = `traffic-light tl-${dir.toLowerCase()}`;
                        tlDiv.id = fullId;
                        tlDiv.innerHTML = `
                            <span class="coda-badge">0</span>
                            <div class="light-bulb red"></div>
                        `;
                        div.appendChild(tlDiv);
                    }
                });
            } else {
                div.style.visibility = "hidden";
            }
            cityMap.appendChild(div);
        }
    }

    setTimeout(() => {
        drawRoads(roadConnections);
    }, 100);
});

function drawRoads(connections) {
    const wrapper = document.getElementById("map-wrapper");
    const cityMap = document.getElementById("city-map");
    const canvas = document.getElementById("road-canvas");
    
    canvas.width = wrapper.clientWidth;
    canvas.height = wrapper.clientHeight;
    const ctx = canvas.getContext("2d");

    ctx.lineWidth = 140;          
    ctx.strokeStyle = "#2f3640"; 
    ctx.lineCap = "square";

    const drawn = new Set();

    const offsetX = cityMap.offsetLeft;
    const offsetY = cityMap.offsetTop;

    connections.forEach(conn => {
        const pathId1 = `${conn.from}-${conn.to}`;
        const pathId2 = `${conn.to}-${conn.from}`;
        
        if (drawn.has(pathId1) || drawn.has(pathId2)) return;
        drawn.add(pathId1);

        const divFrom = document.getElementById("inc-" + conn.from);
        const divTo = document.getElementById("inc-" + conn.to);
        
        if (divFrom && divTo) {
            const startX = divFrom.offsetLeft + offsetX + (divFrom.offsetWidth / 2);
            const startY = divFrom.offsetTop + offsetY + (divFrom.offsetHeight / 2);
            const endX = divTo.offsetLeft + offsetX + (divTo.offsetWidth / 2);
            const endY = divTo.offsetTop + offsetY + (divTo.offsetHeight / 2);

            ctx.beginPath();
            ctx.moveTo(startX, startY);
            ctx.lineTo(endX, endY);
            ctx.stroke();
            
            ctx.save();
            ctx.lineWidth = 4;
            ctx.strokeStyle = "#ffffff";
            ctx.setLineDash([25, 20]);
            ctx.beginPath();
            ctx.moveTo(startX, startY);
            ctx.lineTo(endX, endY);
            ctx.stroke();
            ctx.restore();
        }
    });
}

const ws = new WebSocket("ws://" + window.location.host + "/ws");

ws.onmessage = function(event) {
    const data = JSON.parse(event.data);
    const elementId = `${data.incrocio}_${data.direzione}`;
    const semaforoDiv = document.getElementById(elementId);

    if (semaforoDiv) {
        const light = semaforoDiv.querySelector(".light-bulb");
        
        if (data.stato.includes("ROSSO")) {
            light.className = "light-bulb red";
        } else if (data.stato.includes("VERDE")) {
            light.className = "light-bulb green";
        } else if (data.stato.includes("GIALLO")) {
            light.className = "light-bulb yellow";
        } else if (data.stato === "OFFLINE") {
            light.className = "light-bulb offline"; // <--- NUOVA RIGA
        }

        semaforoDiv.querySelector(".coda-badge").innerText = data.coda;
    }
};

"Funzioni per i comandi di controllo e iniezione auto"

function sendCommand(cmd) {
    fetch('/api/admin/control', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ command: cmd })
    })
    .then(response => {
        if (!response.ok) {
            console.error("Errore nell'invio del comando:", cmd);
        } else {
            console.log("Comando inviato con successo:", cmd);
        }
    })
    .catch(error => console.error("Errore di rete:", error));
}

function injectCars() {
    const incrocio = document.getElementById('inj-incrocio').value;
    const direzione = document.getElementById('inj-direzione').value;
    const count = parseInt(document.getElementById('inj-count').value);

    fetch('/api/admin/inject', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ incrocio: incrocio, direzione: direzione, count: count })
    })
    .then(response => {
        if (!response.ok) {
            console.error("Errore nell'iniezione delle auto");
        } else {
            console.log(`Iniettate ${count} auto in ${incrocio}_${direzione}`);
            // Opzionale: piccolo feedback visivo sul pulsante
            const btn = document.querySelector('.btn-inject');
            const oldText = btn.innerText;
            btn.innerText = "FATTO";
            btn.style.background = "#2ed573";
            setTimeout(() => {
                btn.innerText = oldText;
                btn.style.background = "#3742fa";
            }, 1000);
        }
    })
    .catch(error => console.error("Errore di rete:", error));
}