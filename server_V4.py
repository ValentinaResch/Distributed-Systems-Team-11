from fastapi import FastAPI
from pydantic import BaseModel
import os
import socket
import threading
import time
import requests


app = FastAPI(title="TaskSync Server")

SERVER_ID = int(os.getenv("SERVER_ID", 3))
PORT = int(os.getenv("PORT", 8003))


class TaskSyncRequest(BaseModel):
    tasks: list[str]


def get_local_ip():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("8.8.8.8", 80))
        local_ip = sock.getsockname()[0]
    except:
        local_ip = "127.0.0.1"
    finally:
        sock.close()
    return local_ip


LOCAL_IP = get_local_ip()
SERVER_URL = f"http://{LOCAL_IP}:{PORT}"

tasks = []

known_servers = {
    SERVER_ID: SERVER_URL
}

leader_id = None

DISCOVERY_PORTS = [10001, 10002, 10003]
DISCOVERY_PORT = 10000 + SERVER_ID
BROADCAST_IP = "255.255.255.255"


def is_current_leader():
    return SERVER_ID == leader_id


def elect_leader_internal():
    global leader_id

    print(f"Server {SERVER_ID} startet Bully Election")

    higher_servers = {
        sid: url for sid, url in known_servers.items()
        if sid > SERVER_ID
    }

    if higher_servers:
        print(f"Server {SERVER_ID} sendet ELECTION an {list(higher_servers.keys())}")

        for server_id, server_url in higher_servers.items():
            try:
                requests.post(
                    f"{server_url}/election",
                    params={"candidate_id": SERVER_ID},
                    timeout=2
                )
                print(f"Server {server_id} antwortet OK")
            except:
                print(f"Server {server_id} antwortet nicht")

    if known_servers:
        leader_id = max(known_servers.keys())

    print(f"COORDINATOR: Server {leader_id} ist neuer Leader")

    for server_id, server_url in list(known_servers.items()):
        if server_id == SERVER_ID:
            continue

        try: #sagt den anderen Servern bescheid wer der neue Leader ist
            requests.post(
                f"{server_url}/coordinator",
                params={"new_leader_id": leader_id},
                timeout=2
            )
        except:
            print(f"Coordinator-Nachricht konnte nicht an Server {server_id} gesendet werden")


def sync_tasks_with_server(server_id: int, server_url: str):
    try:
        requests.post(
            f"{server_url}/sync_tasks",
            json={"tasks": tasks},
            timeout=2
        )
        print(f"Tasks wurden mit Server {server_id} synchronisiert")
    except:
        print(f"Tasks konnten nicht mit Server {server_id} synchronisiert werden")


def synchronize_from_current_leader():
    global tasks, leader_id

    for server_id, server_url in list(known_servers.items()):
        if server_id == SERVER_ID:
            continue

        try:
            response = requests.get(f"{server_url}/", timeout=2)
            data = response.json()

            if data["is_leader"]:
                leader_id = data["leader_id"]

                task_response = requests.get(f"{server_url}/tasks", timeout=2)
                task_data = task_response.json()

                tasks = task_data["tasks"]

                print(
                    f"Server {SERVER_ID} hat die komplette Taskliste "
                    f"vom aktuellen Leader Server {leader_id} synchronisiert"
                )
                return

        except:
            pass


def delayed_startup_recovery():
    time.sleep(6)

    synchronize_from_current_leader()

    if leader_id is None or SERVER_ID > leader_id:
        print("Server mit hoeherer ID ist online. Bully Election wird automatisch gestartet.")
        elect_leader_internal()


def broadcast_presence():
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        message = f"{SERVER_ID}|{SERVER_URL}"

        for port in DISCOVERY_PORTS:
            try:
                sock.sendto(message.encode(), (BROADCAST_IP, port))
            except:
                pass

        sock.close()
        time.sleep(3)


def listen_for_servers():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", DISCOVERY_PORT))

    while True:
        data, _ = sock.recvfrom(1024)
        message = data.decode()

        try:
            discovered_id, discovered_url = message.split("|")
            discovered_id = int(discovered_id)

            if discovered_id != SERVER_ID:
                is_new_or_returning = discovered_id not in known_servers

                if is_new_or_returning:
                    print(f"Server {discovered_id} entdeckt oder wieder online: {discovered_url}")

                known_servers[discovered_id] = discovered_url

                if is_new_or_returning:
                    sync_tasks_with_server(discovered_id, discovered_url)

                    if leader_id is None or discovered_id > leader_id:
                        print("Hoehere Server-ID entdeckt. Bully Election wird automatisch gestartet.")
                        elect_leader_internal()

        except ValueError:
            pass


def send_heartbeats():
    while True:
        for server_id, server_url in list(known_servers.items()):
            if server_id == SERVER_ID:
                continue

            try:
                requests.get(
                    f"{server_url}/heartbeat",
                    timeout=2
                )

            except:
                print(f"Server {server_id} nicht erreichbar")

                if server_id in known_servers:
                    del known_servers[server_id]

                if server_id == leader_id:
                    print("Leader ist ausgefallen. Neue Election wird gestartet.")
                    elect_leader_internal()

        time.sleep(3)


def replicate_task(task: str):
    for server_id, server_url in list(known_servers.items()):
        if server_id == SERVER_ID:
            continue

        try:
            requests.post(
                f"{server_url}/replicate",
                params={"task": task},
                timeout=2
            )
            print(f"Task wurde auf Server {server_id} repliziert")

        except:
            print(f"Task konnte nicht auf Server {server_id} repliziert werden")


@app.on_event("startup")
def startup_event():
    print(f"Server {SERVER_ID} startet auf {SERVER_URL}")
    print(f"Discovery Port: {DISCOVERY_PORT}")

    threading.Thread(target=listen_for_servers, daemon=True).start()
    threading.Thread(target=broadcast_presence, daemon=True).start()
    threading.Thread(target=send_heartbeats, daemon=True).start()
    threading.Thread(target=delayed_startup_recovery, daemon=True).start()


@app.get("/")
def home():
    return {
        "message": "TaskSync Server laeuft",
        "server_id": SERVER_ID,
        "local_ip": LOCAL_IP,
        "server_url": SERVER_URL,
        "leader_id": leader_id,
        "is_leader": is_current_leader(),
        "known_servers": known_servers,
        "description": "Distributed collaborative To-Do management system"
    }


@app.get("/servers")
def get_servers():
    return {
        "server_id": SERVER_ID,
        "local_ip": LOCAL_IP,
        "discovery_port": DISCOVERY_PORT,
        "known_servers": known_servers
    }


@app.get("/heartbeat")
def heartbeat():
    return {
        "server_id": SERVER_ID,
        "leader_id": leader_id,
        "status": "alive"
    }


@app.get("/tasks")
def get_tasks():
    return {
        "server_id": SERVER_ID,
        "leader_id": leader_id,
        "is_leader": is_current_leader(),
        "tasks": tasks
    }


@app.post("/tasks")
def add_task(task: str):
    if task not in tasks:
        tasks.append(task)
        replicate_task(task)

    return {
        "message": "Task wurde auf diesem Server hinzugefuegt und an die anderen Server repliziert",
        "server_id": SERVER_ID,
        "leader_id": leader_id,
        "is_leader": is_current_leader(),
        "tasks": tasks
    }


@app.post("/replicate")
def replicate(task: str):
    if task not in tasks:
        tasks.append(task)

    return {
        "message": "Task repliziert",
        "server_id": SERVER_ID,
        "leader_id": leader_id,
        "tasks": tasks
    }


@app.post("/sync_tasks")
def sync_tasks(request: TaskSyncRequest):
    global tasks

    for task in request.tasks:
        if task not in tasks:
            tasks.append(task)

    print("Tasks wurden nach Server-Recovery synchronisiert")

    return {
        "message": "Tasks erfolgreich synchronisiert",
        "server_id": SERVER_ID,
        "leader_id": leader_id,
        "tasks": tasks
    }


@app.post("/elect_leader")
def elect_leader():
    elect_leader_internal()

    return {
        "message": "Leader Election abgeschlossen",
        "server_id": SERVER_ID,
        "leader_id": leader_id,
        "is_leader": is_current_leader()
    }


@app.post("/update_leader")
def update_leader(new_leader_id: int):
    global leader_id

    leader_id = new_leader_id

    return {
        "message": "Leader aktualisiert",
        "server_id": SERVER_ID,
        "leader_id": leader_id,
        "is_leader": is_current_leader()
    }


@app.post("/election")
def receive_election(candidate_id: int):
    global leader_id

    print(f"ELECTION von Server {candidate_id} empfangen")

    if SERVER_ID > candidate_id:
        print(f"Server {SERVER_ID} antwortet OK an Server {candidate_id}")

        highest_known_id = max(known_servers.keys())

        if SERVER_ID == highest_known_id:
            leader_id = SERVER_ID
            print(f"Server {SERVER_ID} hat die hoechste ID und wird Leader")

            for server_id, server_url in list(known_servers.items()):
                if server_id == SERVER_ID:
                    continue

                try:
                    requests.post(
                        f"{server_url}/coordinator",
                        params={"new_leader_id": leader_id},
                        timeout=2
                    )
                except:
                    print(f"COORDINATOR konnte nicht an Server {server_id} gesendet werden")

        else:
            elect_leader_internal()

        return {
            "message": "OK",
            "server_id": SERVER_ID
        }

    return {
        "message": "Keine hoehere ID",
        "server_id": SERVER_ID
    }


@app.post("/coordinator")
def receive_coordinator(new_leader_id: int):
    global leader_id

    leader_id = new_leader_id

    print(f"COORDINATOR empfangen: Server {leader_id} ist Leader")

    return {
        "message": "Coordinator empfangen",
        "server_id": SERVER_ID,
        "leader_id": leader_id,
        "is_leader": is_current_leader()
    }