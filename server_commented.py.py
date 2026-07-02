from fastapi import FastAPI #importiert FastAPI für die erstellung des servers
import os                   #ermöglicht zugriff auf umgebungsvariablen
import socket               #wird für netzwerkkomunikatin und UDP broadcast genutzt
import threading            # erlaubt parallele Prozesse/Threads
import time                 #wartezeit

app = FastAPI()             #erstellt fastapi anwendung

SERVER_ID = int(os.getenv("SERVER_ID", 1)) 
#liest die eindeutige server-ID aus umgebung
#falls ID nicht gesetzt ist -> standardwert 1

PORT = int(os.getenv("PORT", 8001))
#liest port aus umgebung
# falls nicht gesetzt -> standardport 8001

leader_id = 3      #speichert aktuelle leader-id
tasks = []         #liste aller gespeicherten tasks

#dictionary mit bekannten servern
# am anfang kennt jeder server nur sich selbst

known_servers = {
    SERVER_ID: f"http://127.0.0.1:{PORT}"
}

BROADCAST_PORT = 10000 #port für udp broadcast kommunikation

#gibt aktuelle leader_id zurück
def get_current_leader():  
    return leader_id

#überprüft, ob dieser server aktuell der leader ist
def is_current_leader():
    return SERVER_ID == leader_id

#sendet regelmäßig infos über diesen server in andere server
def broadcast_presence():
    message = f"{SERVER_ID}|http://127.0.0.1:{PORT}" #nachricht enthält server-id und url

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #erstellt einen udp-socket
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1) #aktiviert broadcast-kommunikation
#endlosschleife: sendet alle 3 sek die serverdaten
    while True:
        sock.sendto(message.encode(), ("127.0.0.1", BROADCAST_PORT))
        time.sleep(3)

#hört auf nachrichten anderer server
def listen_for_servers():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #erstellt udp-socket zum empfangen
#erlaubt wiederverwendung des ports
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
#verbindet socket mit dem broadcast-port
    sock.bind(("", BROADCAST_PORT))
#wartet dauerhaft auf neue nachrichten
    while True:
        data, _ = sock.recvfrom(1024)   #empfängt daten
        message = data.decode()          #wandelt bytes in lesbaren text um

        try:  
            server_id, server_url = message.split("|") #trennt die nachricht in server-id und url
            server_id = int(server_id)                 #wandelt server-id in integer um
#speichert neue server, außer sich selbst
            if server_id != SERVER_ID: 
                known_servers[server_id] = server_url
#falls nachricht fehlerhaft ist -> ignorieren
        except ValueError:
            pass

#repliziert einen task auf alle bekannten server
def replicate_task(task: str):
    for server_id, server_url in known_servers.items():  #geht alle bekannten server durch
        if server_id != SERVER_ID:                       #replziere nicht auf sich selbst
            try: #sendet task an anderen server
                requests.post(
                    f"{server_url}/replicate",
                    params={"task": task},
                    timeout=2
                ) #falls server nicht erreichbar ist
            except requests.exceptions.RequestException:
                print(f"Server {server_id} nicht erreichbar")
                
#überprüft regelmäßig ob andere server noch aktiv sind
def monitor_servers():
    while True:
#prüft alle bekannten server
        for server_id, server_url in known_servers.items():
#überspringt eigenen server
            if server_id != SERVER_ID:

                try: #sendet heartbeat anfrage
                    requests.get(
                        f"{server_url}/heartbeat",
                        timeout=2
                    )
#falls keine antwort kommt
                except requests.exceptions.RequestException:
                    print(f"Server {server_id} ausgefallen")
#wartet 5 sek bis zur nächsten prüfung
        time.sleep(5)

#wird automatisch beim start des servers ausgeführt
@app.on_event("startup")
def startup_event():
    threading.Thread(target=broadcast_presence, daemon=True).start()
    threading.Thread(target=listen_for_servers, daemon=True).start()
    threading.Thread(target=monitor_servers, daemon=True).start()

#startseite des servers
@app.get("/")
def home():
    return { #gibt infos über den server zurück
        "message": "TaskSync Server laeuft",
        "server_id": SERVER_ID,
        "leader_id": get_current_leader(),
        "is_leader": is_current_leader(),
        "known_servers": known_servers
    }

# gibt bekannte server zurück
@app.get("/servers")
def get_servers():
    return {
        "server_id": SERVER_ID,
        "known_servers": known_servers
    }

#gibt alle gespeicherten tasks zurück
@app.get("/tasks")
def get_tasks():
    return {
        "server_id": SERVER_ID,
        "tasks": tasks
    }

#fügt einen neuen task hinzu
@app.post("/tasks")
def add_task(task: str):
    if task not in tasks:  #verhindert doppelte tasks
        tasks.append(task) #speichert task lokal
#repliziert task auf andere server
        replicate_task(task)

    return {
        "message": "Task hinzugefuegt",
        "server_id": SERVER_ID,
        "tasks": tasks
    }

#endpoint für replizierte tasks
@app.post("/replicate")
def replicate(task: str):
    if task not in tasks:   #speichert replizierten task nur wenn nicht vorhanden
        tasks.append(task)

    return {
        "message": "Task repliziert",
        "server_id": SERVER_ID,
        "tasks": tasks
    }

#heartbeat endpunkt
#andere server prüfen damit ob dieser server aktiv ist
@app.get("/heartbeat")
def heartbeat():
    return {
        "status": "alive",
        "server_id": SERVER_ID,
        "is_leader": is_current_leader()
    }

#startet eine leader election
@app.post("/elect_leader")
def elect_leader():
    global leader_id
#liste aktiver server
    alive_servers = []
#pürft alle bekannten server
    for server_id, server_url in known_servers.items():
        try: #sendet heartbeat anfrage
            response = requests.get(f"{server_url}/heartbeat", timeout=2)
#falls server erreichbar ist
            if response.status_code == 200:
                alive_servers.append(server_id)
#falls server nicht erreichbar ist
        except requests.exceptions.RequestException:
            print(f"Server {server_id} nicht erreichbar")
#wählt den server mit der höchsten ID als leader
    if alive_servers:
        leader_id = max(alive_servers)
#informiert alle anderen server über neuen leader
    for server_id, server_url in known_servers.items():
        if server_id != SERVER_ID:
            try:
                requests.post(
                    f"{server_url}/update_leader",
                    params={"new_leader_id": leader_id},
                    timeout=2
                )
            except requests.exceptions.RequestException:
                print(f"Server {server_id} konnte nicht aktualisiert werden")

    return {
        "message": "Leader Election abgeschlossen",
        "new_leader_id": leader_id,
        "server_id": SERVER_ID,
        "is_leader": is_current_leader()
    }

#aktualisiert den leader auf diesem server
@app.post("/update_leader")
def update_leader(new_leader_id: int):
    global leader_id
#speichert neue leader-id
    leader_id = new_leader_id

    return {
        "message": "Leader aktualisiert",
        "server_id": SERVER_ID,
        "leader_id": leader_id,
        "is_leader": is_current_leader()
    }