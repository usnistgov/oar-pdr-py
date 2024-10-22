# websocket_server.py
import asyncio
import websockets
from concurrent.futures import ThreadPoolExecutor

class WebSocketServer:
    def __init__(self, host="localhost", port=8765):
        self.host = host
        self.port = port
        self.server = None
        self.clients = set()  # Initialize the clients set

    async def start(self):
        self.server = await websockets.serve(self.websocket_handler, self.host, self.port)
        print(f"WebSocket server started on ws://{self.host}:{self.port}")
        

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

    def start_in_thread(self):
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(self._start_event_loop)

    def _start_event_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.start())
    
    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

    async def wait_closed(self):
        if self.server:
            await self.server.wait_closed()

