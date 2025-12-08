from typing import List, Dict, DefaultDict
from fastapi import WebSocket
from collections import defaultdict

class ConnectionManager:
    def __init__(self):
        # Map job_id to list of websockets
        self.active_connections: DefaultDict[str, List[WebSocket]] = defaultdict(list)

    async def connect(self, websocket: WebSocket, job_id: str):
        await websocket.accept()
        self.active_connections[job_id].append(websocket)

    def disconnect(self, job_id: str, websocket: WebSocket):
        if job_id in self.active_connections:
            if websocket in self.active_connections[job_id]:
                self.active_connections[job_id].remove(websocket)
            if not self.active_connections[job_id]:
                del self.active_connections[job_id]

    async def broadcast(self, job_id: str, message: dict):
        if job_id in self.active_connections:
            for connection in self.active_connections[job_id]:
                try:
                    await connection.send_json(message)
                except Exception as e:
                    print(f"Error sending message: {e}")

manager = ConnectionManager()
