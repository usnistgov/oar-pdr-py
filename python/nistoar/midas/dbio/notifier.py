import asyncio
import websockets
from concurrent.futures import ThreadPoolExecutor
import copy
import logging

logger = logging.getLogger(__name__)

class Notifier:
    def __init__(self, host="0.0.0.0", port=8765):
        self.host = host
        self.port = port
        self.server = None
        self.clients = set()

    async def start(self):
        try:
            self.server = await websockets.serve(self.websocket_handler, self.host, self.port)
            logger.info(f"WebSocket server started on ws://{self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to start WebSocket server: {e}")

    async def websocket_handler(self, websocket, path):
        self.clients.add(websocket)
        try:
            async for message in websocket:
                await self.send_message_to_clients(message)
        except websockets.ConnectionClosed as e:
            logger.info(f"Client disconnected: {e}")
        except Exception as e:
            logger.error(f"Error in websocket_handler: {e}")
        finally:
            self.clients.remove(websocket)
            logger.info("Client removed")

    async def send_message_to_clients(self, message):
        if self.clients:
            for client in self.clients:
                try:
                    await client.send(message)
                except websockets.ConnectionClosed as e:
                    logger.info(f"Failed to send message to client (disconnected): {e}")
                except Exception as e:
                    logger.error(f"Failed to send message to client: {e}")

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
            logger.info("WebSocket server stopped")

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