"""
a module that allows the DBIO to alert listening clients about changes and updates made to 
DBIO's data contents.  The DBIO's interface into this capability is the 
:py:class:`DBIOClientNotifier`.  
"""
import asyncio
import websockets
import logging
from logging import Logger

deflogger = logging.getLogger(__name__)

class DBIOClientNotifier:
    """
    A class that provides an interface for sending messages DBIO listeners in which the DBIO plays
    the role of a message "broadcaster".

    This implemenation uses a websocket server.  The server recognizes this client as a broadcaster
    of messages via its use of a broadcast key.  
    """
    def __init__(self, uri: str, broadcast_key: str=None, logger: Logger=None):
        """
        Create the notifier
        :param str uri:  the websocket server address
        :param str broadcast_key: a key that identifies this client to the serve as a broadcaster.
        """
        self.uri = uri
        self.api_key = broadcast_key
        if not logger:
            logger = deflogger
        self.log = logger

    def notify(self, message):
        """
        Asynchronously sends a notification message via WebSocket.
        :param str message: The message to send.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running event loop; create a new one
            self.log.debug("No running event loop found. Creating a new event loop.")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        if not loop.is_running():
            self.log.debug("Event loop is not running. Starting the event loop.")
            loop.run_until_complete(self._send_notification(message))
        else:
            future = asyncio.run_coroutine_threadsafe(self._send_notification(message), loop)
            self.log.info(f"Notification coroutine submitted to the event loop: {future}")

    async def _send_notification(self, message):
        """
        Coroutine to send a notification message via WebSocket.
        :param str message: The message to send.
        """
        self.log.debug(f"Connecting to WebSocket server at {self.uri}...")
        try:
            async with websockets.connect(self.uri) as websocket:
                if self.api_key:
                    
                    self.log.info(f"Sending WebSocket message: {message}")
                    authentified_message = f"{self.api_key},{message}"
                    await websocket.send(authentified_message)
                    self.log.debug("WebSocket message sent successfully.")
                    
                await websocket.close()
                self.log.debug("WebSocket connection closed cleanly.")
        except Exception as e:
            self.log.error(f"Error sending WebSocket message: {e}")
