import asyncio
import websockets
import logging

logging.basicConfig(level=logging.INFO)

class WebSocketServer:
    def __init__(self, host="0.0.0.0", port=8765):
        self.host = host
        self.port = port
        self.clients = set()

    async def handler(self, websocket):
        self.clients.add(websocket)
        try:
            async for message in websocket:
                logging.info(f"Received message: {message}")
                # Broadcast the received message to all connected clients
                await asyncio.gather(*(client.send(message) for client in self.clients if client != websocket))
        except websockets.ConnectionClosed:
            logging.info("Client disconnected")
        finally:
            self.clients.remove(websocket)

    async def main(self):
        async with websockets.serve(self.handler, self.host, self.port):
            logging.info(f"WebSocket server started on ws://{self.host}:{self.port}")
            await asyncio.Future()  # Run forever

if __name__ == "__main__":
    server = WebSocketServer()
    asyncio.run(server.main())