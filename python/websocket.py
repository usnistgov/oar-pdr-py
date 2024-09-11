import asyncio
import websockets
from datetime import datetime

# Set to store connected WebSocket clients
clients = set()

#Defines an asynchronous function that will handle incoming WebSocket connections
#var websocket being the WebSocket connection and var path being the URL path requested by the client
async def websocket_handler(websocket, path):
    # Add new client to the set
    clients.add(websocket)
    try:
        async for message in websocket:
            pass  # Handle incoming messages if needed asynchronously to make sure nothing gets blocked.
    finally:
        # Remove client from the set when they disconnect
        clients.remove(websocket)
#defines an asynchronous function that will send a message to all connected WebSocket clients
#var message being the message to send to the clients
async def send_message_to_clients(message):
    if clients:
        await asyncio.wait([client.send(message) for client in clients])

#Custom function I wrote to interact with the websocket and create events whenever I hit enter on the command line
#This function will be run concurrently with the WebSocket server and send the timestamp to all connected clients whenever the user hits enter
async def read_from_command_line():
    while True:
        # Wait for the user to hit enter
        await asyncio.get_event_loop().run_in_executor(None, input, "Press Enter to send a message: ")
        # Get the current timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"Message from server at {timestamp}"
        await send_message_to_clients(message)
        print("Message sent to clients")

async def main():
    # Start the WebSocket server with our handler
    server = await websockets.serve(websocket_handler, "localhost", 8765)
    print('WebSocket server running on port 8765')

    # Run the command line reader and WebSocket server at the same time
    await asyncio.gather(server.wait_closed(), read_from_command_line())

if __name__ == '__main__':
    asyncio.run(main())