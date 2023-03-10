"""
Some common code for implementing the WSGI front end to dbio
"""
import logging, json
from collections import OrderedDict
from collections.abc import Callable

from nistoar.pdr.publish.service.wsgi import SubApp, Handler   # same infrastructure as the publishing service
from nistoar.pdr.publish.prov import PubAgent
from .. import DBClient

class DBIOHandler(Handler):
    """
    a base class for handling requests for DBIO data.  It provides some common utililty functions 
    for sending responses and dealing with errors.
    """
    def __init__(self, subapp: SubApp, dbclient: DBClient, wsgienv: dict, start_resp: Callable, 
                 who: PubAgent, path: str="", config: dict=None, log: logging.Logger=None):
        """
        Initialize this handler with the request particulars.  

        :param SubApp subapp:  the web service SubApp receiving the request and calling this constructor
        :param DBClient dbclient: the DBIO client to use
        :param dict  wsgienv:  the WSGI request context dictionary
        :param Callable start_resp:  the WSGI start-response function used to send the response
        :param PubAgent  who:  the authenticated user making the request.  
        :param str      path:  the relative path to be handled by this handler; typically, some starting 
                               portion of the original request path has been stripped away to handle 
                               produce this value.
        :param dict   config:  the handler's configuration; if not provided, the inherited constructor
                               will extract the configuration from `subapp`.  Normally, the constructor
                               is called without this parameter.
        :param Logger    log:  the logger to use within this handler; if not provided (typical), the 
                               logger attached to the SubApp will be used.  
        """
        if config is None and hasattr(subapp, 'cfg'):
            config = subapp.cfg
        if not log and hasattr(subapp, 'log'):
            log = subapp.log
        Handler.__init__(self, path, wsgienv, start_resp, who, config, log, subapp)
        self._dbcli = dbclient
        self._reqrec = None
        if hasattr(self._app, "_recorder") and self._app._recorder:
            self._reqrec = self._app._recorder.from_wsgi(self._env)

    class FatalError(Exception):
        def __init__(self, code, reason, explain=None, id=None):
            if not explain:
                explain = reason or ''
            super(DBIOHandler.FatalError, self).__init__(explain)
            self.code = code
            self.reason = reason
            self.explain = explain
            self.id = id

    def send_fatal_error(self, fatalex: FatalError, ashead=False):
        self.send_error_resp(fatalex.code, fatalex.reason, fatalex.explain, fatalex.id, ashead)

    def send_error_resp(self, code, reason, explain, id=None, ashead=False):
        """
        respond to client with a JSON-formated error response.
        :param int code:    the HTTP code to respond with 
        :param str reason:  the reason to return as the HTTP status message
        :param str explain: the more extensive explanation as to the reason for the error; 
                            this is returned only in the body of the message
        :param str id:      the record ID for the requested record; if None, it is not applicable or known
        :param bool ashead: if true, do not send the body as this is a HEAD request
        """
        resp = {
            'http:code': code,
            'http:reason': reason,
            'midas:message': explain,
        }
        if id:
            resp['midas:id'] = id

        return self.send_json(resp, reason, code, ashead)

    def get_json_body(self):
        """
        read in the request body assuming that it is in JSON format
        """
        try:
            bodyin = self._env.get('wsgi.input')
            if bodyin is None:
                if self._reqrec:
                    self._reqrec.record()
                raise self.FatalError(400, "Missing input", "Missing expected input JSON data")

            if self.log.isEnabledFor(logging.DEBUG) or self._reqrec:
                body = bodyin.read()
                out = json.loads(body, object_pairs_hook=OrderedDict)
            else:
                out = json.load(bodyin, object_pairs_hook=OrderedDict)
            if self._reqrec:
                self._reqrec.add_body_text(json.dumps(name, indent=2)).record()
            return out

        except (ValueError, TypeError) as ex:
            if self.log.isEnabledFor(logging.DEBUG):
                self.log.error("Failed to parse input: %s", str(ex))
                self.log.debug("\n%s", body)
            if self._reqrec:
                self._reqrec.add_body_text(body).record()
            raise self.FatalError(400, "Input not parseable as JSON",
                                  "Input document is not parse-able as JSON: "+str(ex))

        except Exception as ex:
            if self._reqrec:
                self._reqrec.add_body_text(body).record()
            raise

