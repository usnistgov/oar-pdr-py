"""
The base Handler class for handler implementations used by the PDP WSGI app
"""
import sys, re, json
from abc import ABCMeta, abstractmethod, abstractproperty
from typing import Callable
from functools import reduce
from logging import Logger
from urllib.parse import parse_qs
from collections import OrderedDict
from collections.abc import Mapping

from wsgiref.headers import Headers
# from urllib.parse import parse_qs

from nistoar.pdr.publish.prov import PubAgent
from nistoar.pdr import config as cfgmod
from nistoar.pdr.utils.web import order_accepts

__all__ = ['Unacceptable', 'SubApp', 'Handler']

class Unacceptable(Exception):
    """
    An expection indicating that the requested (or otherwise selected) format corresponds to a 
    content-type that is not acceptable to the client.  This exception is expected to result in a 
    406 (Not Acceptable) response to the client.  
    """
    pass
            
class Handler(object):
    """
    a default web request handler that also serves as a base class for the 
    handlers specialized for the supported resource paths.  Key features built into this 
    class include:
      * the ``who`` property that holds the identity of the remote user making the request
      * support for an ``action`` query parameter to request an action to be applied to 
        the resource being requested (perhaps in addition to that implied by the HTTP 
        request method); see :py:meth:`get_action`.
    """
    default_agent = PubAgent("public", PubAgent.UNKN, "anonymous")
    ACTION_UPDATE   = ''
    ACTION_FINALIZE = "finalize"
    ACTION_PUBLISH  = "publish"

    def __init__(self, path: str, wsgienv: dict, start_resp: Callable, who=None, 
                 config: dict={}, log: Logger=None, app=None):
        self._path = path
        self._env = wsgienv
        self._start = start_resp
        self._hdr = Headers([])
        self._code = 0
        self._msg = "unknown status"
        self.cfg = config
        if not who:
            who = self.default_agent
        self.who = who
        self.log = log

        self._app = app
        if self._app and hasattr(app, 'include_headers'):
            self._hdr = Headers(list(app.include_headers.items()))

        self._meth = self._env.get('REQUEST_METHOD', 'GET')

    def get_action(self):
        """
        return the value of the action query parameter of None if it was not provided
        """
        qstr = self._env.get('QUERY_STRING')
        if not qstr:
            return self.ACTION_UPDATE

        params = parse_qs(qstr)
        action = params.get('action')
        if len(action) > 0 and action[0] in [self.ACTION_FINALIZE, self.ACTION_PUBLISH]:
            return action[0]
        return self.ACTION_UPDATE

    def send_error(self, code, message, content=None, contenttype=None, ashead=None, encoding='utf-8'):
        """
        respond to the client with an error of a given code and reason

        This method is meant to be called by a method handler (or an override of :py:meth:`handle`) 
        and is provided as a simple way to send an error response (instead of calling 
        :py:meth:`set_response` and :py:meth:`end_headers` directly).  

        :param int code:        the HTTP response code to assign
        :param str message:     the briefly-stated reason to give for the error; this text
                                is sent as the message that accompanies the code in the HTTP 
                                response header
        :param content:         Content to return as the body.  
                                :type content: str or byte or a list of either
        :param str contenttype: the MIME type to associate with the returned content.
        :param bool ashead:     True if this is being sent as if in response to a HEAD request; if so,
                                the size and type of the content will be included in the headers, but 
                                the actual content will be withheld.  If not provided, it will be set 
                                to True if the originally requested method is "HEAD"; otherwise it is 
                                False
        :param str encoding:    The encoding required to turn the content--when given as str--into bytes.
                                The default is 'utf-8'.  
        """
        return self._send(code, message, content, contenttype, ashead, encoding)

    def send_unauthorized(self, message="Unauthorized", content=None, contenttype=None, ashead=None,
                          encoding='utf-8'):
        return self.send_error(401, message, content, contenttype, ashead, encoding)

    def send_unacceptable(self, message="Not acceptable", content=None, contenttype=None, ashead=None,
                          encoding='utf-8'):
        return self.send_error(406, message, content, contenttype, ashead, encoding)

    def send_ok(self, content=None, contenttype=None, message="OK", code=200, ashead=None, encoding='utf-8'):
        """
        respond to the client a response of success.  

        This method is meant to be called by a method handler (or an override of :py:meth:`handle`) 
        and is provided as a short-cut for small, simple successful responses instead of calling 
        :py:meth:`set_response` and :py:meth:`end_headers` directly.  

        :param str message:     the briefly-stated reason to give for the error; this text
                                is sent as the message that accompanies the code in the HTTP 
                                response header.  The default if not specified is "OK". 
        :param content:         Content to return as the body.  If not provided, the body will be
                                empty.
                                :type content: str or byte
        :param int code:        the HTTP response code to assign.  This should be between greater
                                than or equal to 200 and less than 300; the default is 200.
        :param str contenttype: the MIME type to associate with the returned content.
        :param bool ashead:     True if this is being sent as if in response to a HEAD request; if so,
                                the size and type of the content will be included in the headers, but 
                                the actual content will be withheld.  If not provided, it will be set 
                                to True if the originally requested method is "HEAD"; otherwise it is 
                                False
        :param str encoding:    The encoding required to turn the content--when given as str--into bytes.
                                The default is 'utf-8'.  
        """
        return self._send(code, message, content, contenttype, ashead, encoding)

    def send_json(self, data, message="OK", code=200, ashead=False, encoding='utf-8'):
        """
        Send some data formatted as JSON.  
        :param data:     the data to encode in JSON
                         :type data: dict, list, or string
        """
        return self._send(code, message, json.dumps(data, indent=2), "application/json", ashead, encoding)

    def _send(self, code, message, content, contenttype, ashead, encoding):
        if ashead is None:
            ashead = self._meth.upper() == "HEAD"
        # status = "{0} {1}".format(str(code), message)
        self.set_response(code, message)

        if content:
            if not isinstance(content, list):
                content = [ content ]
            badtype = [type(c) for c in content if not isinstance(c, (str, bytes))]
            if badtype:
                raise TypeError("send_*: non-str/bytes found in content")
            if not contenttype:
                contenttype = (isinstance(content[0], str) and "text/plain") or "application/octet-stream"
        elif content is None:
            content = []
        # convert to bytes
        content = [(isinstance(c, str) and c.encode(encoding)) or c for c in content]

        if contenttype:
            self.add_header("Content-Type", contenttype)
        if len(content) > 0:
            self.add_header("Content-Length", str(reduce(lambda x, t: x+len(t), content, 0)))

        self.end_headers()
        return (not ashead and content) or []

    def add_header(self, name, value):
        """
        record a name-value pair to be sent as part of the response header.

        :param str name:  the name of the header field to cache
        :param str value: the value to give to the header field
        :raises UnicodeEncodeError:  if name or value includes Unicode characters (see PEP 333)
        """
        # Caution: HTTP does not support Unicode characters (see
        # https://www.python.org/dev/peps/pep-0333/#unicode-issues);
        # thus, this will raise a UnicodeEncodeError if the input strings
        # include Unicode (char code > 255).
        #
        # make sure values are encodable
        e = "ISO-8859-1"
        (name.encode(e), value.encode(e))

        self._hdr.add_header(name, value)

    def set_response(self, code, message):
        """
        record the response code and message to be sent when the response is triggered to push out.
        """
        self._code = code
        self._msg = message

    def end_headers(self):
        """
        trigger the delivery of response's header to the web client.  

        This method is meant to be called by a method handler (or an override of :py:meth:`handle`).
        It should be preceded with a call to :py:meth:`set_response`; afterward, the handler should 
        return the body content (as an iterable).  
        """
        status = "{0} {1}".format(str(self._code), self._msg)
        self._start(status, self._hdr.items(), None)

    def handle(self):
        """
        handle the request encapsulated in this Handler (at construction time).  

        The default implementation looks for a Handler method of the form, `do_`METH(), where METH is 
        is the HTTP method requested (e.g. GET, HEAD, etc.) and calls it with the requested URL path
        (as set at construction).  If the requested method is HEAD and there is no HEAD, `do_GET()` 
        is called with a second argument set to True which should prevent the content from the 
        path to be excluded.  
        """
        meth = self._meth
        if self._env.get('HTTP_X_HTTP_METHOD_OVERRIDE'):
            meth = self._env.get('HTTP_X_HTTP_METHOD_OVERRIDE')
        
        meth_handler = 'do_'+meth

        if not self.authorize():
            return self.send_unauthorized()

        if not self.acceptable():
            return self.send_unacceptable()

        try:
            if hasattr(self, meth_handler):
                return getattr(self, meth_handler)(self._path)
            elif self._meth == "HEAD":
                return self.do_GET(self._path, ashead=True)
            else:
                return self.send_error(405, self._meth + " not supported on this resource")
        except Exception as ex:
            if self.log:
                self.log.exception("Unexpected failure: "+str(ex))
            return self.send_error(500, "Server failure")

    def authorize(self):
        """
        determine if the client agent is authorized to access this endpoint.  The implementation 
        will generally examine the client agent identity (`self.who`) and the requested resource 
        path (`self.path`) to make a decision.  The handler may do further authorization checks 
        downstream (e.g. within a method handler or other delegate), but this check serves as an
        an initial gate.

        This default implementation requires only that client agent has been set for this handler;
        this can be over-ridden for tighter or looser authorization.  
        """
        return bool(self.who)

    def acceptable(self):
        """
        return True if the client's Accept request is compatible with this handler.

        This default implementation will return True if "*/*" is included in the Accept request
        or if the Accept header is not specified.
        """
        accepts = self._env.get('HTTP_ACCEPT')
        if not accepts:
            return True;
        return "*/*" in order_accepts(accepts)

    
class SubApp(metaclass=ABCMeta):
    """
    a base class WSGI implementation intended to run as a delegate handling a particular path 
    within another WSGI application.  A SubApp is usually plugged into a larger WSGI app to 
    handle requests for a particular path and its descendent paths (as in <path> and <path>/*).  
    """

    def __init__(self, appname, log, config=None):
        self.log = log
        if config is None:
            config = {}
        self.cfg = config
        self._name = appname
                
        self._recorder = None
        wrlogf = config.get('record_to')
        if wrlogf:
            if not os.path.isabs(wrlogf) and cfgmod.global_logdir:
                wrlogf = os.path.join(cfgmod.global_logdir, wrlogf)
            self._recorder = WebRecorder(wrlogf, self._name)

        self.include_headers = Headers()
        if config.get("include_headers"):
            try:
                if isinstance(config.get("include_headers"), Mapping):
                    self.include_headers = Headers(list(config.get("include_headers").items()))
                elif isinstance(config.get("include_headers"), list):
                    self._default_headers = Headers(list(config.get("include_headers")))
                else:
                    raise TypeError("Not a list of 2-tuples")
            except TypeError as ex:
                raise ConfigurationException("include_headers: must be either a dict or a list of "+
                                             "name-value pairs")

    @abstractmethod
    def create_handler(self, env: dict, start_resp: Callable, path: str, who: PubAgent) -> Handler:
        """
        return a handler instance to handle a particular request to a path
        :param Mapping env:  the WSGI environment containing the request
        :param Callable start_resp:  the start_resp function to use initiate the response
        :param str path:     the path to the resource being requested.  This is usually 
                             relative to a parent path that this SubApp is configured to 
                             handle.  
        """
        raise NotImplementedError()
        
    def handle_path_request(self, env: dict, start_resp: Callable, path: str=None, who: PubAgent=None):
        """
        respond to a request on a particular (relative) URL path.
        :param Mapping env:  the WSGI environment containing the request
        :param Callable start_resp:  the start_resp function to use initiate the response
        :param str path:     the path to the resource being requested.  This is usually 
                             relative to a parent path that this SubApp is configured to 
                             handle.  If None, the value of env['PATH_INFO'] should be 
                             assumed.
        """
        if path is None:
            path = env.get('PATH_INFO', '')
        return self.create_handler(env, start_resp, path, who).handle()

    def __call__(self, env, start_resp):
        return self.handle_path_request(env, start_resp)

class Ready(SubApp):
    """
    a WSGI sub-app that handles unsupported path or proof-of-life requests
    """

    def __init(self, log, config=None):
        super(SubApp, self).__inti__(log, config)

    class _Handler(Handler):

        def __init__(self, path: str, wsgienv: dict, start_resp: Callable, who=None,
                     config: dict={}, log: Logger=None):
            Handler.__init__(self, path, wsgienv, start_resp, who, config, log)

        def do_GET(self, path, ashead=False):
            path = path.lstrip('/')
            if path:
                # only the root path is supported
                return self.send_error(404, "Not found")

            return self.send_ok("Publishing service is up.\n", ashead=ashead)

    def create_handler(self, env: dict, start_resp: Callable, path: str, who: PubAgent) -> Handler:
        """
        return a handler instance to handle a particular request to a path
        :param Mapping env:  the WSGI environment containing the request
        :param Callable start_resp:  the start_resp function to use initiate the response
        :param str path:     the path to the resource being requested.  This is usually 
                             relative to a parent path that this SubApp is configured to 
                             handle.  
        """
        return self._Handler(path, env, start_resp, who, log=self.log)

