"""
Some common code for implementing the WSGI front end to dbio
"""
import logging, json, re
from collections import OrderedDict
from collections.abc import Callable

from nistoar.web.rest import ServiceApp, Handler, Agent
from .. import DBClient

class DBIOHandler(Handler):
    """
    a base class for handling requests for DBIO data.  It provides some common utililty functions 
    for sending responses and dealing with errors.
    """
    def __init__(self, svcapp: ServiceApp, dbclient: DBClient, wsgienv: dict, start_resp: Callable, 
                 who: Agent, path: str="", config: dict=None, log: logging.Logger=None):
        """
        Initialize this handler with the request particulars.  

        :param ServiceApp svcapp: the web service ServiceApp receiving the request and calling this 
                                  constructor
        :param DBClient dbclient: the DBIO client to use
        :param dict  wsgienv:  the WSGI request context dictionary
        :param Callable start_resp:  the WSGI start-response function used to send the response
        :param Agent     who:  the authenticated user making the request.  
        :param str      path:  the relative path to be handled by this handler; typically, some starting 
                               portion of the original request path has been stripped away to handle 
                               produce this value.
        :param dict   config:  the handler's configuration; if not provided, the inherited constructor
                               will extract the configuration from ``svcapp``.  Normally, the constructor
                               is called without this parameter.
        :param Logger    log:  the logger to use within this handler; if not provided (typical), the 
                               logger attached to the ServiceApp will be used.  
        """
        if config is None and hasattr(svcapp, 'cfg'):
            config = svcapp.cfg
        if not log and hasattr(svcapp, 'log'):
            log = svcapp.log
        Handler.__init__(self, path, wsgienv, start_resp, who, config, log, svcapp)
        self._dbcli = dbclient
        self._reqrec = None
        if hasattr(self._app, "_recorder") and self._app._recorder:
            self._reqrec = self._app._recorder.from_wsgi(self._env)

    def preauthorize(self):
        """
        determine if the client agent is authorized to access this endpoint.  This implementation 
        ensures that a valid client agent has been established and is valid.
        """
        return bool(self.who) and self.who.agent_class != Agent.INVALID

    def acceptable(self):
        """
        return True if the client's Accept request is compatible with this handler.

        This implementation will return True if "*/*" or "application/json" is included in the 
        Accept request or if the Accept header is not specified.
        """
        accepts = self.get_accepts()
        if not accepts:
            return True;
        jsonre = re.compile(r"/json$")
        return "*/*" in accepts or any(jsonre.search(a) for a in accepts);

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
        return self.send_error_resp(fatalex.code, fatalex.reason, fatalex.explain, fatalex.id, ashead)

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

    def get_text_body(self, limit=100000000):
        """
        read in the request body assuming that it is in JSON format

        :param int limit:  a limit on the number of bytes to read.  If input exceeds this limit, 
                           it will be truncated.  The default limit is 100 MB.
        """
        try:
            bodyin = self._env.get('wsgi.input')
            if bodyin is None:
                if self._reqrec:
                    self._reqrec.record()
                raise self.FatalError(400, "Missing input", "Missing expected input text data")

            out = bodyin.read(limit)
            if isinstance(out, bytes):
                out = out.decode('utf-8')
            if self._reqrec:
                self._reqrec.add_body_text(out).record()
            return out

        except UnicodeError as ex:
            if self._reqrec:
                self._reqrec.add_body_text("<<not convertable to text>>")
            raise self.FatalError(400, "Unconvertable Text",
                                  "Input text is not UTF-8 convertable: "+str(ex)) from ex

        except Exception as ex:
            if self._reqrec:
                if out:
                    self._reqrec.add_body_text(out).record()
                else:
                    self._reqrec.add_body_text("<<empty body>>")
            raise

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
                self._reqrec.add_body_text(json.dumps(out, indent=2)).record()
            return out

        except (ValueError, TypeError) as ex:
            if self.log.isEnabledFor(logging.DEBUG):
                self.log.error("Failed to parse input: %s", str(ex))
                self.log.debug("\n%s", body)
            if self._reqrec:
                try:
                    if isinstance(body, bytes):
                        body = body.decode('utf-8')
                    self._reqrec.add_body_text(body).record()
                except Exception:
                    self._reqrec.record()
            raise self.FatalError(400, "Input not parseable as JSON",
                                  "Input document is not parse-able as JSON: "+str(ex))

        except Exception as ex:
            if self._reqrec:
                try:
                    if body:
                        if isinstance(body, bytes):
                            body = body.decode('utf-8')
                        self._reqrec.add_body_text(body).record()
                    else:
                        self._reqrec.add_body_text("<<empty body>>")
                except Exception:
                    self._reqrec.add_body_text("<<not convertable to text>>")
            raise

