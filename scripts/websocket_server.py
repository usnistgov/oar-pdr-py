"""
A stand-alone server application allowing a broadcaster to send messages to connected clients

To run this server with a default configuration, type:

    python3 client_notifier_server.py

To display the available command-line options for this server, type:

    python3 client_notifier_server.py --help

This server provides a websocket endpoint for a broadcaster and any number of clients to connect
(the default is ws://localhost:8735).  Messages sent by a client are ignored; messages sent by 
the broadcaster are sent out to all connected clients.

Messages are (mostly) arbitrary, comma-delimited strings. Each substring between commas are fields
of the message.  Messages whose first field is a token that matches configured authorization key 
identifies the sender as a broadcaster.  The message will be sent to all connected clients with 
the authorization key removed.  
"""
import asyncio
import websockets
import logging
import argparse
import os
import sys
import traceback

try:
    import nistoar
except ImportError:
    if os.environ.get('OAR_PYTHONPATH'):
        sys.path.append(os.environ['OAR_PYTHONPATH'])
    elif os.environ.get('OAR_HOME'):
        sys.path.append(os.path.join(os.environ['OAR_HOME'], "lib", "python"))

def_broadcast_key="123456_secret_key"
def_port=8765
def_host="0.0.0.0"

class WebClientNotifierServer:
    """
    the server class that implements the broadcasting service using Web Sockets.

    Broadcasters and clients connect to the same web socket endpoint ("ws://host:port/").
    Any message sent by a client is ignored; a message sent by a broadcaster are sent out
    to all the connected clients (and any other broadcasters).  

    All messages is an arbitrary, comma-delimited string.  Each substring between commas is 
    message field.  A broadcast message is recognized as such if its first field is a token
    that matches a broadcast authorization key set at construction time.  This token is 
    removed from the field before it is distributed to all other clients.
    """
    def __init__(self, host=None, port=None, broadcast_key=None, config=None):
        """
        create the server.
        :param str host:   the name or ip address representing the local network interface
                           that this server will be listening on (default: "0.0.0.0")
        :param str port:   the port the server will listen on (default: 8765)
        :param str broadcast_key:  a token that identifies a broadcaster.  A broadcaster
                           must include this key as the first field of all messages.  
        :param dict config:  a dictionary of parameters that serve as default configuration
                           values.  This constructor will look for keys matching the other 
                           parameters specified in this function signature which will be used 
                           if the argument is not provided to this constructor explicitly.
        """
        self.host = host or config.get('host') or def_host
        self.port = port or config.get('port') or def_port
        self.bk_key = broadcast_key or config.get('broadcast_key') or def_broadcast_key
        self.clients = set()
        self.log = logging.getLogger("client_notifier")

    async def handler(self, websocket):
        """
        handle a connection to a client or broadcaster
        """
        self.clients.add(websocket)
        try:
            async for message in websocket:
                if not self._authenticate_message(message):
                    self.log.warning("Ignoring unauthorized message: %s", message)
                    continue

                _, message_content = message.split(",", 1)
                await asyncio.gather(*(client.send(message_content)
                                       for client in self.clients if client != websocket))

                self.log.info("Message distributed to %d client%s: %s", len(self.clients) - 1,
                              "s" if len(self.clients) != 2 else "", message_content)

        except websockets.ConnectionClosed:
            self.log.info("Client disconnected")
        except Exception as ex:
            self.log.error("Unexpected communication error: %s; closing client", str(ex))
            websocket.close(1002)
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
            return api_key == self.bk_key
        except ValueError:
            # Message format is invalid
            self.log.error("Invalid message format. Expected 'broadcast_key,message_content'.")
            return False

    async def main(self):
        async with websockets.serve(self.handler, self.host, self.port):
            self.log.info(f"WebSocket server started on ws://{self.host}:{self.port}")
            await asyncio.Future()  # Run forever

def define_options(progname):
    global def_broadcast_key
    parser = argparse.ArgumentParser(progname, description="Start the Client Notifier server.")
                        
    parser.add_argument("-p", "--port", type=int, default=None, metavar="PORT", dest='port',
                        help="The port to run the WebSocket server on (default: 8765).")
    parser.add_argument("-k", "--broadcast-key", type=str, dest='bkey', metavar="KEY", default=None,
                        help="The authorization key for identifing messages from DBIO.")
    parser.add_argument("-c", "--config-file", type=str, metavar="FILE", dest='config',
                        help="load server configuration data from FILE.  Other command line " +
                             "options will override the configuration.  It is preferred to " +
                             "provide the broadcast key in a configuration file rather than " +
                             "on the command-line when running in production.")
    parser.add_argument("-v", "--verbose", action="store_true", dest='verbose', default=False,
                        help="include debug messages in logging output")
    parser.add_argument("-l", "--logfile", type=str, metavar="FILE", dest='logfile',
                        help="send logging messages to FILE instead of standard error")

    return parser


class CLIError(Exception):
    pass

def main(progname, args):
    parser = define_options(progname)
    opts = parser.parse_args(args)

    cfg = { }
    if opts.config:
        from nistoar.base import config
        cfg = config.resolve_configuration(opts.config)

        # set up logging from config
        extra = { }
        if opts.verbose:
            extra['level'] = logging.DEBUG
        config.configure_log(logfile=opts.logfile, config=cfg, addstderr=True, **extra)

    else:
        # default logging
        lev = logging.DEBUG if opts.verbose else logging.INFO
        fmt = "%(asctime)s %(name)s: %(levelname)s: %(message)s"
        logging.basicConfig(level=lev, format=fmt)
        
    # launch the server (Note: None args will defer to config/defaults)
    server = WebClientNotifierServer(None, opts.port, opts.bkey, config=cfg)
    asyncio.run(server.main())


if __name__ == "__main__":
    progname = sys.argv[0]
    try:
        main(progname, sys.argv[1:])
    except CLIError as ex:
        print(f"{progname}: {str(ex)}", file=sys.stderr)
        sys.exit(1)
    except Exception as ex:
        print(f"Unexpected error: {str(ex)}", file=sys.stderr)
        traceback.print_tb(sys.exc_info()[2])
        sys.exit(4)

