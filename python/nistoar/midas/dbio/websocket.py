# websocket_server.py
import asyncio
import websockets

class WebSocketService:
    def __init__(self, host="localhost", port=8765):
        self.host = host
        self.port = port
        self.server = None

    async def start(self):
        self.server = await websockets.serve(self.websocket_handler, self.host, self.port)
        print(f"WebSocket server started on ws://{self.host}:{self.port}")
        await self.server.wait_closed()

    async def websocket_handler(self, websocket, path):
        # Add the new client to the set
        self.clients.add(websocket)
        try:
            async for message in websocket:
                await self.send_message_to_clients(message)
        finally:
            # Remove the client from the set when they disconnect
            self.clients.remove(websocket)

    async def send_message_to_clients(self, message):
        if self.clients:
            await asyncio.wait([client.send(message) for client in self.clients])

    def is_running(self):
        return self.server is not None and self.server.is_serving()

# Singleton instance of WebSocketServer
WebsocketServer = WebSocketService()