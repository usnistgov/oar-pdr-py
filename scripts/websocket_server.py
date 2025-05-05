import asyncio
import websockets
import logging
import argparse

logging.basicConfig(level=logging.INFO)

class WebSocketServer:
    def __init__(self, host="0.0.0.0", port=8765, api_key="123456_secret_key"):
        self.host = host
        self.port = port
        self.api_key = api_key
        self.clients = set()

    async def handler(self, websocket):
        self.clients.add(websocket)
        try:
            async for message in websocket:
                logging.info(f"Received message: {message}")
                logging.info(f"Authenticating message: {self.api_key}")

                if not self._authenticate_message(message):
                    logging.warning("Unauthorized message received. Ignoring.")
                    continue

                _, message_content = message.split(",", 1)

                await asyncio.gather(*(client.send(message_content) for client in self.clients if client != websocket))
                logging.info(f"Message distributed to {len(self.clients) - 1} clients: {message_content}")
        except websockets.ConnectionClosed:
            logging.info("Client disconnected")
        finally:
            self.clients.remove(websocket)

    def _authenticate_message(self, message):
        """
        Authenticate the received message by checking the API key.
        :param str message: The message to authenticate.
        :return: True if the message is authenticated, False otherwise.
        """
        try:
            # Expect the message to be in the format: "api_key,message_content"
            api_key, _ = message.split(",", 1)
            return api_key == self.api_key
        except ValueError:
            # Message format is invalid
            logging.error("Invalid message format. Expected 'api_key,message_content'.")
            return False

    async def main(self):
        async with websockets.serve(self.handler, self.host, self.port):
            logging.info(f"WebSocket server started on ws://{self.host}:{self.port}")
            await asyncio.Future()  # Run forever

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start the WebSocket server.")
    parser.add_argument("--port", type=int, default=8765, help="The port to run the WebSocket server on (default: 8765).")
    parser.add_argument("--api_key", type=str, default="123456_secret_key", help="The API key for authenticating messages from DBIO.")
    args = parser.parse_args()

    server = WebSocketServer(host="0.0.0.0", port=args.port, api_key=args.api_key)
    asyncio.run(server.main())