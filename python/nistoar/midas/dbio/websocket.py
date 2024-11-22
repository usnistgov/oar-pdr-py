# websocket_server.py
import asyncio
import websockets
from concurrent.futures import ThreadPoolExecutor
import copy

class WebSocketServer:
    def __init__(self, host="localhost", port=8765):
        self.host = host
        self.port = port
        self.server = None
        self.clients = set()  # Initialize the clients set

    async def start(self):
        self.server = await websockets.serve(self.websocket_handler, self.host, self.port)
        #print(f"WebSocket server started on ws://{self.host}:{self.port}")
        

    async def websocket_handler(self, websocket):
        # Add the new client to the set
        self.clients.add(websocket)
        try:
            async for message in websocket:
                await self.send_message_to_clients(message)
        finally:
            # Remove the client from the set when they disconnect
            self.clients.remove(websocket)

    async def send_message_to_clients(self, message):
        for client in self.clients:
            print(client)
        if self.clients:
            for client in self.clients:
                asyncio.create_task(client.send(message))

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

    async def wait_closed(self):
        if self.server:
            await self.server.wait_closed()

    def __deepcopy__(self, memo):
        # Create a shallow copy of the object
        new_copy = copy.copy(self)
        # Deep copy the attributes that are not problematic
        new_copy.host = copy.deepcopy(self.host, memo)
        new_copy.port = copy.deepcopy(self.port, memo)
        new_copy.clients = copy.deepcopy(self.clients, memo)
        # Do not copy the problematic attribute
        new_copy.server = self.server
        return new_copy
