import asyncio
import websockets
import logging

# Configure logging
logger = logging.getLogger(__name__)

class Notifier:
    def __init__(self, uri, api_key=None):
        """
        Initialize the Notifier with the WebSocket server URI.
        :param str uri: The WebSocket server address.
        """
        self.uri = uri
        self.api_key = api_key

    def notify(self, message):
        """
        Asynchronously sends a notification message via WebSocket.
        :param str message: The message to send.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running event loop; create a new one
            logger.debug("No running event loop found. Creating a new event loop.")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        if not loop.is_running():
            logger.debug("Event loop is not running. Starting the event loop.")
            loop.run_until_complete(self._send_notification(message))
        else:
            future = asyncio.run_coroutine_threadsafe(self._send_notification(message), loop)
            logger.info(f"Notification coroutine submitted to the event loop: {future}")

    async def _send_notification(self, message):
        """
        Coroutine to send a notification message via WebSocket.
        :param str message: The message to send.
        """
        logger.debug(f"Connecting to WebSocket server at {self.uri}...")
        try:
            async with websockets.connect(self.uri) as websocket:
                if self.api_key:
                    
                    logger.info(f"Sending WebSocket message: {message}")
                    authentified_message = f"{self.api_key},{message}"
                    await websocket.send(authentified_message)
                    logger.debug("WebSocket message sent successfully.")
                    
                await websocket.close()
                logger.debug("WebSocket connection closed cleanly.")
        except Exception as e:
            logger.error(f"Error sending WebSocket message: {e}")
