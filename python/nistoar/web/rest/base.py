"""
The base REST framework classes
"""
import sys, os, re, json
from abc import ABCMeta, abstractmethod, abstractproperty
from typing import Callable, List
from functools import reduce
from logging import Logger
from urllib.parse import parse_qs
from collections import OrderedDict
from typing import Mapping, Iterable, Tuple, Callable, Union

import jwt

from wsgiref.headers import Headers
# from urllib.parse import parse_qs

from ..utils import order_accepts
from ..webrecord import WebRecorder
from ..formats import Unacceptable, UnsupportedFormat, FormatSupport
from nistoar.base.config import ConfigurationException
import nistoar.base.config as cfgmod
from nistoar.pdr.utils.prov import Agent

__all__ = ["Handler", "NotFoundHandler", "ServiceApp", "Unauthenticated", "WSGIServiceApp", 
           "AuthenticatedWSGIApp", "WSGIAppSuite", "Agent",
           "authenticate_via_authkey", "authenticate_via_proxy_x509", "authenticate_via_jwt" ]

class Handler(object):
    """
    a default web request handler that also serves as a base class for the 
    handlers specialized for the supported resource paths.  Key features built into this 
    class include:
      * the ``who`` property that holds the identity of the remote user making the request
      * [content negotiation support]
    """

    def __init__(self, path: str, wsgienv: dict, start_resp: Callable, who=None, 
                 config: dict={}, log: Logger=None, app=None):
        self._path = path
        self._env = wsgienv
        self._start = start_resp
        self._hdr = Headers([])
        self._code = 0
        self._msg = "unknown status"
        self.cfg = config
        self.log = log

        self._app = app
        if self._app and hasattr(app, 'include_headers'):
            self._hdr = Headers(list(app.include_headers.items()))
        if not who:
            who = self._default_agent()
        self.who = who

        # the output formats supported by this Handler; if None, the client has no choice
        # over the output format.  This should be set at construction time via
        # _set_default_format_support()
        self._fmtsup = None
        
        # set to the name of the query parameter for requesting a named format (e.g. "format")
        self._format_qp = None   

        self._meth = self._env.get('REQUEST_METHOD', 'GET')

    @property
    def app(self):
        """
        the ServiceApp instance that created this handler
        """
        return self._app

    @property
    def format_qp(self):
        """
        the name of the query parameter that can by used by clients to request a named output 
        format.  If None, such a parameter is not supported.  Note that the parameter need not 
        be supported on all paths or methods; where it is, this is the name that can be used.
        By default, this is set to None; subclasses can set this value at construction time 
        via :py:meth:`_set_format_qp`. 
        """
        return self._format_qp

    def _set_format_qp(self, qpname):
        """
        set the name of the query parameter with which clients may request a named query format.
        This is usually called in a subclass constructor if such a query parameter should be 
        supported.
        """
        self._format_qp = qpname

    def _default_agent(self):
        name = "nistoar" if not self.app else self.app.name
        return Agent(name, Agent.UNKN, Agent.ANONYMOUS, groups=["public"])

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

    def send_unacceptable(self, message="Not Acceptable", content=None, contenttype=None, ashead=None,
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

    def send_options(self, allowed_methods: List[str]=None, origin: str=None, extra=None,
                     forcors: bool=True):
        """
        send a response to a OPTIONS request.  This implememtation is primarily for CORS preflight requests
        :param List[str] allowed_methods:   a list of the HTTP methods that are allowed for request
        :param str                origin: 
        :param dict|Headers        extra:   extra headers to include in the output.  This is either a 
                                            dictionary-like object or a list of 2-tuples (like 
                                            wsgiref.header.Headers).  
        """
        meths = list(allowed_methods)
        if 'OPTIONS' not in meths:
            meths.append('OPTIONS')
        if forcors:
            self.add_header('Access-Control-Allow-Methods', ", ".join(meths))
            if origin:
                self.add_header('Access-Control-Allow-Origin', origin)
            self.add_header('Access-Control-Allow-Headers', "Content-Type")
        if isinstance(extra, Mapping):
            for k,v in extra.items():
                self.add_header(k, v)
        elif isinstance(extra, (list, tuple)):
            for k,v in extra:
                self.add_header(k, v)

        return self.send_ok(message="No Content")

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

        if not self.preauthorize():
            return self.send_unauthorized()

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

    def preauthorize(self):
        """
        do an initial test to see if the client identity is authorized to access this service.  
        This method will get called prior to calling the specific method handling function (e.g. 
        ``do_GET()``).  In many cases, the full authorization test is best done either within 
        the method function or within the underlying service it accesses; however, this function
        allows an implementation to filter out certain requests early, typically based just on 
        the identity of the client (``self.who``) and the resource requested (``self.path``).
        This implementation always returns True; however, subclasses may override this to provide
        tighter restrictions.
        """
        return True

    def get_accepts(self):
        """
        return the requested content types as a list ordered by their q-values.  An empty list
        is returned if no types were specified.
        """
        accepts = self._env.get('HTTP_ACCEPT')
        if not accepts:
            return [];
        return order_accepts(accepts)

    def get_requested_formats(self):
        """
        return the formats requested via format query parameters on the request URL.  (The actual 
        query parameter name is given by ``self.format_qp``.)  An empty list is returned if 
        parameter was not set or is not supported by this implementation.  The order will usually 
        be taken as the order of preference by the client.
        """
        format = []
        if self.format_qp and 'QUERY_STRING' in self._env:
            params = parse_qs(self._env['QUERY_STRING'])
            if self.format_qp in params:
                format = params[self.format_qp]
        return format

    def select_format(self, format: str=None, path: str=None, meth: str="GET"):
        """
        determine the best output format the given context.  For this to return a non-None 
        format name, either the ``format`` parameter must be specified or 
        `get_format_support(path, method)` must return a non-None result.  For the latter,
        a subclass must override this method to return a :py:class:`FormatSupport` instance.
        This instance will be used to choose a format based on the client's preferences.

        This function is not called automatically.  Subclass implementations _may_ use this
        function (e.g. say, within a method handler function like ``do_GET()``) to determine
        the output format, but this function must be called explicitly.  It must also establish
        which formats are supported either by setting up one or more FormatSupport objects 
        in the constructor or by overriding :py:meth:get_format_support:. 

        :param str format:   the name of a format that programmatically asked for, which 
                             will override any preferences specified by the client.  This 
                             is typically non-None if the best format has already be 
                             determined.  If `get_format_support(path, method)` returns 
                             a :py:class:`FormatSupport` instance, it will be checked to 
                             make sure the format name is recognized.  
        :param str   path:   the client-requested path that should be considered (and 
                             passed to :py:meth:`get_format_support`).  
        :param str   meth:   the client-requested HTTP method to be considered (and 
                             passed to :py:meth:`get_format_support`).  
        """
        if isinstance(format, str):
            fmt = self._fmtsup.match(format)
            if not fmt:
                raise UnsupportedFormat(f"{format} not a supported format")
            else:
                return fmt
        else:
            format = None

        fmtsup = self.get_format_support(path, meth)
        if fmtsup:
            # may raise UnsupportedFormat or Unacceptable
            format = fmtsup.select_format(self.get_requested_formats(), self.get_accepts())

            if not format:
                format = fmtsup.default_format()

        return format

    def get_format_support(self, path: str, method: str="GET") -> FormatSupport:
        """
        return a FormatSupport instance to use that is appropriate for a requested resource
        path and HTTP method.  This function is called automatically by :py:meth:`select_format`.
        This implementation ignores the ``path`` and ``method`` and simply returns the instance
        that was last set with :py:meth:`_set_default_format_support` or None, if none was not set. 
        Subclasses may override this method if the returned instance depends on the input 
        parameters.
        """
        return self._fmtsup  # may be None

    def _set_default_format_support(self, fmtsup: FormatSupport):
        self._fmtsup = fmtsup

class NotFoundHandler(Handler):
    """
    a request Handler that always returns 404 Not Found.  This can be used in :py:class:`ServiceApp`
    implementations that create a handler (via :py:meth:`~ServiceApp.create_handler`) based on the 
    requested path.  If the path is not recognized, an instance of this class can be returned.
    """
    def do_GET(self, path, ashead=False, format=None):
        return self.send_error(404, "Not Found")

    def do_OPTIONS(self, path):
        return self.send_options(["GET"])

    
class ServiceApp(metaclass=ABCMeta):
    """
    a base class WSGI implementation intended to run as a delegate handling a particular path 
    within another WSGI application.  A ServiceApp is usually plugged into a larger WSGI app to 
    handle requests for a particular path and its descendent paths (as in <path> and <path>/*).  
    """

    def __init__(self, appname: str, log: Logger, config: Mapping=None):
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
    @property
    def name(self):
        """
        a name for the service provided by this ServiceApp instance (set at construction time).
        This can be used in messages targeted to clients.
        """
        return self._name

    @abstractmethod
    def create_handler(self, env: dict, start_resp: Callable, path: str, who: Agent) -> Handler:
        """
        return a handler instance to handle a particular request to a path
        :param Mapping env:  the WSGI environment containing the request
        :param Callable start_resp:  the start_resp function to use initiate the response
        :param str path:     the path to the resource being requested.  This is usually 
                             relative to a parent path that this ServiceApp is configured to 
                             handle.  
        """
        raise NotImplementedError()
        
    def handle_path_request(self, env: dict, start_resp: Callable, path: str=None, who: Agent=None):
        """
        respond to a request on a particular (relative) URL path.
        :param Mapping env:  the WSGI environment containing the request
        :param Callable start_resp:  the start_resp function to use initiate the response
        :param str path:     the path to the resource being requested.  This is usually 
                             relative to a parent path that this ServiceApp is configured to 
                             handle.  If None, the value of env['PATH_INFO'] should be 
                             assumed.
        """
        if path is None:
            path = env.get('PATH_INFO', '')
        return self.create_handler(env, start_resp, path, who).handle()

    def __call__(self, env, start_resp):
        return self.handle_path_request(env, start_resp)

class Unauthenticated(Exception):
    """
    An exception indicating that a service client did not successfully authenticate itself.
    This may be because the credentials are required but none were provided by the client, or 
    because the credentials presented were not valid.

    Note that an implementation is not required to raise this exception, particularly if 
    credentials are optional.  Instead an identity can be returned that specifically represent
    an unauthenticated client.  
    """
    pass

class WSGIApp(metaclass=ABCMeta):
    """
    A WSGI application base class for wrapping one or more ServiceApp classes.  It provides a 
    common authentication authentication check.  

    This base implementation will leverage two parameters from the configuration:
    
    ``base_ep``
        _str_ (required).  The base endpoint URL for the web app given as a path starting with 
                           a forward slash, ``/``.  All resource path requests must start with 
                           this path; otherwise 403 (Forbidden) is returned.
    ``name``
        _str_ (required).  A short name to use to identify this web app (e.g. in log messages, 
                           authentication, authorization, etc.)

    WSGIApp subclasses may expand on this base set of configuration parameters.  
    """

    def __init__(self, config: Mapping, log: Logger, base_ep: str = None, name: str = None):
        """
        initialize the base information for the app.  
        :param dict config:  configuration data for the app.  (See 
                             :py:class:`the class documentation<WSGIApp>` as well as for 
                             subclasses for more information.)
        :param Logger  log:  the Logger this app should use to record log messages
        :param str base_ep:  the base endpoint URL for the suite of services.  If not provided,
                             the base URL is set by the configuration (via the ``base_ep`` 
                             parameter).
        :param str    name:  a name to use to identify this app for context (e.g. in logs, 
                             authentication, authorization, etc.) default: None.
        """
        self.log = log
        self.cfg = config
        self.name = name
        if not self.name:
            self.name = self.cfg.get("name", "")
        self.base_ep = None
        if not base_ep:
            base_ep = self.cfg.get("base_ep", "")
        base_ep = base_ep.strip('/')
        if base_ep:
            self.base_ep = '/%s/' % base_ep

    def authenticate(self, env) -> Union[object,str,None]:
        """
        determine and return the identity of the client.  This base method has loose constraints on 
        the form of the output identity: it can be a simple string or a more complex object.  This 
        implementation returns None, reflecting that by default authentication is not supported.  
        This method is called automatically by :py:meth:`handle_request`.

        This method may raise an :py:class:`Unauthenticated` exception.  If it does, 
        :py:meth:`handle_request` will immediately respond to the client with a 401 (Unauthorized) 
        error.  If this is not desired (because, say, this state is to be handled at the method 
        level), the implementation should return an identity that represents an unauthenticated 
        user.

        See also :py:class:`AuthenticatedWSGIApp`.
        
        :param Mapping env:  the WSGI request environment 
        :return:  a representation of the requesting user.  None can be returned if authentication is 
                  not supported or needed.  
        :raises Unauthenticated:  if the authentication process fails.   Note that an implementation 
                  is not required to raise this exception for an unauthenticated user; instead, the 
                  implementation may choose to return an identity that specifically represents an 
                  unauthenticated user.
        """
        return None

    def handle_request(self, env: Mapping, start_resp: Callable):
        path = re.sub(r'/+', '/', env.get('PATH_INFO', '/'))

        # determine who is making the request
        try:
            who = self.authenticate(env)
        except Unauthenticated as ex:
            self.log.debug("Authentication failure: %s", str(ex))
            self.send_error(401, "Authentication Failure")
        except Exception as ex:
            self.log.error("Unexpected failure while authenticating: %s", str(ex))
            self.send_error(500, "Internal Server Error")

        if self.base_ep:
            if path.startswith(self.base_ep):
                path = path[len(self.base_ep):]

            elif self.base_ep == path+'/':
                path = ''

            elif self.base_ep.startswith(path.rstrip('/')+'/'):
                # client asked for a parent resource of the base_ep
                return Handler(path, env, start_resp).send_error(403, "Forbidden")

            else:
                # path does not match the required base endpoint path at all
                return Handler(path, env, start_resp).send_error(404, "Not Found")

        return self.handle_path_request(path.strip('/'), env, start_resp, who)

    @abstractmethod
    def handle_path_request(self, path: str, env: Mapping, start_resp: Callable, who = None):
        """
        Dispatch a request on a resource path to a handler.
        :param str path:  the path requested by the client.  This path will be relative to the base
                          endpoint path for the service, and will not start with a slash.  Thus, if 
                          this WSGIApp was set with a base path, it will be stripped from this 
                          path.  
        :param dict env:  the WSGI environment containing all request information
        :param func start_resp:  the start-response function provided by the WSGI engine.
        :param      who:  a string or object that represents the client user.  
        """
        raise NotImplemented()

    def __call__(self, env, start_resp):
        return self.handle_request(env, start_resp)

class AuthenticatedWSGIApp(WSGIApp):
    """
    a WSGIApp base class with a NIST-OAR-specific model for authentication built around the 
    :py:class:`~nistoar.pdr.utils.prov.Agent` class as the representation for the client identity.  
    Subclasses provide a specific authentication mechanism via the :py:meth:`authenticate_user`
    method.  See :py:meth:`authenticate` for more details.  

    This WSGIApp subclass expands the set of parameters looked for the app configuration with the 
    following parameters:

    ``authentication``
        an object whose sub-parameters control the authentication process.  Beyond a base set, the 
        set of parameters expected in this object depends on the authentication implementation 
        provided by the specific ``AuthenticatedWSGIApp`` subclass.  See :py:meth:`authenticate`
        for a description of the base set of parameters.  
    """

    def authenticate(self, env) -> Agent:
        """
        determine and return the identity of the client as an :py:class:`~nistoar.pdr.utils.prov.Agent`
        instance.  This checks both user credentials and, if configured, the client application 
        identifier.  
        If client keys are configured and the client has not provided a recognized key or if the 
        user credentials are invalid in someway, then either an exception is thrown or an "invalid" 
        Agent (where the ``agent_class`` is set to "invalid") is returned, depending on the 
        configuration.  Also depending on the configuration, if no usr credentials are presented,
        either the exception is raised or an Agent.ANONYMOUS identity is returned.  

        This implementation will check for client application identifier which is expected to be 
        provided via the ``OAR-client-id`` HTTP header.  Clients may also provide a list of delegated 
        agents via the ``OAR-client-agents`` header; the agent identifiers should be space-separated.  
        If provided, these will be included in provenance recording.  Checking for user credentials 
        and creating the appropriate Agent instance to return is delegated to the 
        :py:meth:`authenticate_user` method.  

        This implementation is configured by the ``authentication`` parameter in the configuration 
        dictionary that was provided at construction time.  This parameter's value is an object that 
        can include the following sub-properties:

        ``client_agents``
            a map whose keys are secret client keys and, and each value is a list of agent 
            identifiers corresponding to the agents that were assigned this identifier.  The list 
            represents the agent delegation that was expected to have occured on the client side 
            leading to the current request.  This list will be set as the returned 
            :py:class:`~nistoar.pdr.utils.prov.Agent`'s ``delegated`` property.  If not 
            provided, this will default to the value of the presented client ID.  

        ``allowed_clients``
            a list of client identifiers that should be considered allowed to use this service.  If 
            this parameter is not set, the ``OAR-client-id`` HTTP header is not required to be set, 
            and all clients will be allowed.  Otherwise, the ``OAR-client-id`` value must be in the 
            this list or the authentication will be considered invalid. 

        ``raise_on_invalid``
            set to True if an exception should be raised if the credentials presented are found to 
            be invalid (this includes if client ID is not in the ``allowed_clients`` parameter).
            The default False will cause an Agent to be returned whose ``agent_class`` is set to 
            "invalid".

        ``raise_on_anonymous``
            set to True if an exception should be raised if user credentials are not provided by 
            the client.  The default False will cause an Agent to be returned whose ``actor`` 
            ID is set to Agent.ANONYMOUS and its ``agent_class`` set to "public".

        :param Mapping env:  the WSGI request environment 
        :return:  a representation of the requesting user.  None can be returned if authentication is 
                  not supported or needed.  
                  :rtype: Agent
        :raises Unauthenticated:  if the authentication process fails.   Note that an implementation 
                  is not required to raise this exception for an unauthenticated user; instead, the 
                  implementation may choose to return an identity that specifically represents an 
                  unauthenticated user.
        """
        authcfg = self.cfg.get('authentication', {})

        # get client id, if present
        client_id = env.get('HTTP_OAR_CLIENT_ID','(unknown)')
        agents = env.get('HTTP_OAR_CLIENT_AGENTS', '').split()
        if not agents:
            agents = authcfg.get('client_agents', {}).get(client_id, [client_id])
        allowed = authcfg.get('allowed_clients')
        if allowed is not None and client_id not in allowed:
            log.warning("Client %s is not recongized among %s", client_id, str(allowed))
            if authcfg.get('raise_on_invalid'):
                raise Unauthenticated("Unrecognized Client ID")
            return Agent(client_id, Agent.UNKN, Agent.ANONYMOUS, Agent.INVALID, agents,
                         invalid_reason=f"Unrecognized client ID: {client_id}")

        # this encapsulates the configured user authentication mechanisms
        return self.authenticate_user(env, agents, client_id)

    def authenticate_user(self, env: Mapping, agents: List[str]=None, client_id: str=None) -> Agent:
        """
        determine the authenticated user.  

        This implementation simply returns an Agent instance representing an anonymous user.  
        Subclasses requiring user authentication should override this method.  This module 
        provides a few functions that can provide different implementations, including
        :py:func:`authenticate_via_authkey`, :py:func:`authenticate_via_jwt`, and 
        :py:func:`authenticate_via_proxy_x509`.
        
        :param dict     env:  The WSGI environment with contains the request data
        :param [str] agents:  an optional list of agent strings to attach to output agent
        :param str client_id: an ID representing the OAR client being used to connect.  
                              This can be used to influence what groups are attached to 
                              the output agent.  If None, either an ID was not provided by
                              the client or it is otherwise not supported by the app.  
        :raises Unauthenticated:  if the authentication process fails.   Note that an implementation 
                  is not required to raise this exception for an unauthenticated user; instead, the 
                  implementation may choose to return an identity that specifically represents an 
                  unauthenticated user.
        """
        if self.cfg.get('authentication', {}).get('raise_on_anonymous'):
            raise Unauthenticated("Unauthenticated by default")
        if not client_id:
            client_id = "(unknown)"
        vehicle = self.name or client_id
        return Agent(vehicle, Agent.UNKN, Agent.ANONYMOUS, Agent.PUBLIC, agents)


def authenticate_via_authkey(svcname: str, env: Mapping, authcfg: Mapping, log: Logger,
                             agents: List[str]=None, client_id: str=None):
    """
    authenticate the user via a simple shared Bearer Authorization key.

    This authorization method simply requires the client to present an opaque key set as a
    Bearer token to the Authorization HTTP header.  The key must be provided in the given 
    configuration with in the ``authorized`` object which contains a list of client authentication 
    objects; each object contains the following parameters:

    ``auth_key``
       _str_ (required).  A recognized opaque key looked for as a Bearer Authorization token

    ``user``
       _str_ (required).  an identifier to set the returned Agent ``actor`` id to when the client 
       presents the associated ``auth_key``.

    ``client``
       _str_ (required).  a name for the client; this will be set as both the Agent ``vehicle`` and 
       ``agent_class``.  


    :param str   svcname: a name to provide as the agent software vehicle
    :param dict      env: the WSGI environment containing the request data
    :param dict  authcfg: the JWT decoding configuration (see above)
    :param Logger    log: the logger that can be used to record messages
    :param [str]  agents: an optional list of agent strings to attach to output agent
    :param str client_id: an ID representing the OAR client being used to connect.  If None,
                          either an ID was not provided or is otherwise not supported by the 
                          app.  
    :returns:  an :py:class:`Agent` instance representing the user
    """
    if not client_id:
        client_id = "(unknown)"
    if not svcname:
        svnname = client_id

    auth = env.get('HTTP_AUTHORIZATION', "x").split()
    if len(auth) < 2 or auth[0] != "Bearer" or not auth[1]:
        log.warning("Client %s did not provide a Bearer authentication token", str(client_id))
        if authcfg.get('raise_on_anonymous'):
            raise Unauthenticated("No auth token provided")
        return Agent(svcname, Agent.UNKN, Agent.ANONYMOUS, Agent.PUBLIC, agents)

    for client in authcfg.get('authorized'):
        if client.get("auth_key") == auth[1]:
            return Agent(svcname, Agent.AUTO, client.get('user','authorized'),
                         client.get('client', client_id), agents)

    log.warning("Unrecognized token from client %s", str(client_id))
    if authcfg.get('raise_on_invalid'):
        raise Unauthenticated("Unrecognized auth token")
    return Agent(svcname, Agent.UNKN, Agent.ANONYMOUS, Agent.INVALID, agents,
                 invalid_reason="Unrecognized auth token")

def authenticate_via_proxy_x509(svcname: str, env: Mapping, authcfg: Mapping, log: Logger,
                                agents: List[str]=None, client_id: str=None):
    """
    authenticate the user assuming that the client provided an X.509 client certificate
    that was validated by the service's reverse proxy server.  If the certificate is valid,
    the proxy server will have provided the following HTTP headers:
      * OAR_SSL_S_DN -- set to the distinguished name of the certificate's subject
      * Authorization -- set with a Bearer token with a shared secret, required if a 
                         ``proxy_key`` was provided in the configuration.

    This function will look for the following properties in the provided configuration dictionary:

    ``proxy_key``
        (str) _optional_.  The secret key shared with the proxy server configuration.  If set, 
        the proxy server must set this key as a Bearer token in the Authorization HTTP header
        in order for subject data to be considered valid.  If the keys do not match or is not 
        provided in the input request to this function, an invalid anonymous identity is returned.

    :param str   svcname: a name to provide as the agent software vehicle
    :param dict      env: the WSGI environment containing the request data
    :param dict   jwtcfg: the JWT decoding configuration (see above)
    :param Logger    log: the logger that can be used to record messages
    :param [str]  agents: an optional list of agent strings to attach to output agent
    :param str client_id: an ID representing the OAR client being used to connect.  If None,
                          either an ID was not provided or is otherwise not supported by the 
                          app.  
    :returns:  an :py:class:`Agent` instance representing the user
    """
    if not client_id:
        client_id = "(unknown)"
    if not svcname:
        svnname = client_id

    subj = env.get('HTTP_OAR_SSL_S_DN')
    if not subj:
        if authcfg.get('raise_on_anonymous'):
            raise Unauthenticated("OAR_SSL_S_DN not provided")
        return Agent(svcname, Agent.UNKN, Agent.ANONYMOUS, Agent.PUBLIC, agents)

    if authcfg.get('proxy_key'):
        # we're expecting a validation token from the proxy server
        auth = env.get('HTTP_AUTHORIZATION' 'x').split()
        if len(auth) < 2 or auth[0] != "Bearer":
            log.warning("Reverse proxy server did not provide an authentication token")
            if authcfg.get('raise_on_invalid'):
                raise Unauthenticated("required proxy key not provided")
            return Agent(svcname, Agent.UNKN, Agent.ANONYMOUS, Agent.INVALID, agents, 
                         invalid_reason="Missing proxy auth token")
        if authcfg['proxy_key'] != auth[1]:
            log.error("Reverse proxy server presented unrecognized authentication token")
            if authcfg.get('raise_on_invalid'):
                raise Unauthenticated("bad proxy key")
            return Agent(svcname, Agent.UNKN, Agent.ANONYMOUS, Agent.INVALID, agents, 
                         invalid_reason="bad proxy auth token")

    # parse the subject elements to construct an identity
    # TODO!
    return None

def authenticate_via_jwt(svcname: str, env: Mapping, jwtcfg: Mapping, log: Logger,
                         agents: List[str], client_id: str=None,
                         claim_to_agent_func: Callable=None):
    """
    authenticate the remote user assuming a JWT was provided as an Authorization Bearer token.

    This function will look for the following properties in the provided configuration dictionary:

    ``key``
        (str) _required_.  The secret key shared with the token generator (usually a separate 
        service) used to encrypt the token.

    ``algorithm``
        (str) _optional_.  The name of the encryption algorithm to encrypt the token.  Currently, 
        only one value is support (the default): "HS256".

    ``require_expiration``
        (bool) _optional_.  If True (default), any JWT token that does not include an expiration 
        time will be rejected, and the client user will be set to anonymous.

    :param str   svcname: a name to provide as the agent software vehicle
    :param dict      env: the WSGI environment containing the request data
    :param dict   jwtcfg: the JWT decoding configuration (see above)
    :param Logger    log: the logger that can be used to record messages
    :param [str]  agents: an optional list of agent strings to attach to output agent
    :param str client_id: an ID representing the OAR client being used to connect.  If None,
                          either an ID was not provided or is otherwise not supported by the 
                          app.  
    :param function claim_to_agent_func:  a function that takes a JWT claimset dictionary and 
                          returns an Agent instance.  If not provided, 
                          :py:func:`make_agent_from_nistoar_claimset` will be executed.
    :returns:  an :py:class:`Agent` instance representing the user
    """
    if not client_id:
        client_id = "(unknown)"
    if not svcname:
        svnname = client_id

    auth = env.get('HTTP_AUTHORIZATION', "x").split()
    if len(auth) < 2 or auth[0] != "Bearer":
        log.warning("Client %s did not provide an authentication token", str(client_id))
        if jwtcfg.get('raise_on_anonymous'):
            raise Unauthenticated("JWT token not provided")
        return Agent(svcname, Agent.UNKN, Agent.ANONYMOUS, Agent.PUBLIC, agents)

    try:
        userinfo = jwt.decode(auth[1], jwtcfg.get("key", ""),
                              algorithms=[jwtcfg.get("algorithm", "HS256")])
    except jwt.InvalidTokenError as ex:
        log.warning("Invalid token can not be decoded: %s", str(ex))
        if jwtcfg.get('raise_on_invalid'):
            raise Unauthenticated("Undecodable JWT token")
        return Agent(svcname, Agent.UNKN, Agent.ANONYMOUS, Agent.INVALID, agents,
                     invalid_reason="Invalid token can not be decoded")

    # make sure the token has an expiration date
    if jwtcfg.get('require_expiration', True) and not userinfo.get('exp'):
        # Note expiration was checked implicitly by the above jwt.decode() call
        log.warning("Rejecting non-expiring token for user %s", userinfo.get('sub', "(unknown)"))
        if jwtcfg.get('raise_on_invalid'):
            raise Unauthenticated("Non-expiring JWT token")
        return Agent(svcname, Agent.UNKN, Agent.ANONYMOUS, Agent.INVALID, agents,
                     invalid_reason=f"non-expiring token rejected")

    if not claim_to_agent_func:
        claim_to_agent_func = make_agent_from_nistoar_claimset
    return claim_to_agent_func(svcname, userinfo, log, agents)

def make_agent_from_nistoar_claimset(svcname: str, userinfo: Mapping, log: Logger, agents=None,
                                     client_id: str=None) -> Agent:
    """
    Create an Agent instance representing the end user given a JWT claim set assuming 
    it originated from a NIST-OAR JWT service.
    :param str   svcname:  a name to provide as the agent software vehicle
    :param dict userinfo:  a dictionary containing the JST claimset data
    :param Logger    log:  a Logger object that should be used to record warning messages
                           (e.g. if the claimset is misisng key data)
    :param list[str] agents:  a list of agents that the user described by the claim set is acting
                           on behalf of.  By default, if None or empty, no agents will be attached 
                           to the returned Agent.
    """
    subj = userinfo.get('sub')
    email = userinfo.get('userEmail')
    group = Agent.PUBLIC
    if not subj:
        log.warning("User token is missing subject identifier; defaulting to anonymous")
        subj = Agent.ANONYMOUS
    elif subj.endswith("@nist.gov"):
        group = "nist"
        subj = subj[:-1*len("@nist.gov")]
    elif email and email.endswith("@nist.gov"):
        group = "nist"

    umd = dict((k,v) for k,v in userinfo.items()
                         if k not in ["userEmail", "sub"])

    if not client_id:
        client_id = group
    return Agent(svcname, Agent.USER, subj, client_id, agents, email=email, **umd)


class WSGIAppSuite(AuthenticatedWSGIApp):
    """
    A WSGI application class that aggregates one or more :py:class:ServiceApp: instances.  This supports 
    a model where each ServiceApp represents a different logical service and each with its own base URL; 
    they are all brought together into a single WSGI application.
    """

    def __init__(self, config: Mapping, svcapps: Mapping[str, ServiceApp], log: Logger,
                 base_ep: str = None):
        """
        initialize the suite of web services
        :param dict  config:  the configuration for the suite of services
        :param dict svcapps:  a mapping of resource paths (relative to the base endpoint URL)
                              to the ServiceApp instances that should serve them. 
        :param Logger   log:  the base logger to use among the suite
        :param str  base_ep:  the base endpoint URL for the suite of services.  If not provided,
                              the base URL is set by the configuration (via the ``base_ep`` 
                              parameter).
        """
        super(WSGIAppSuite, self).__init__(config, log, base_ep)
        self.svcapps = dict(svcapps.items())

    def _set_service_route(self, path: str, svcapp: ServiceApp):
        """
        configure a resource path to be handled by a particular ServiceApp instance
        """
        self.svcapps[path] = svcapp

    def handle_path_request(self, path: str, env: Mapping, start_resp: Callable, who = None):
        """
        Dispatch a request on a resource path to a handler.
        :param str path:  the path requested by the client.  This path will be relative to the base
                          endpoint path for the service, and will not start with a slash.  Thus, if 
                          this WSGIApp was set with a base path, it will be stripped from this 
                          path.  
        :param dict env:  the WSGI environment containing all request information
        :param func start_resp:  the start-response function provided by the WSGI engine.
        :param      who:  a string or object that represents the client user.  
        """
        # Determine which ServiceApp should handle this request
        base = re.sub(r'/+', '/', path)
        apppath = ''
        svcapp = None
        isaparent = False
        while not svcapp:
            svcapp = self.svcapps.get(base)
            if svcapp:
                # Found!
                continue

            if not base:
                if isaparent:
                    return Handler(path, env, start_resp).send_error(403, "Forbidden")
                else:
                    return Handler(path, env, start_resp).send_error(404, "Not Found")

            elif not isaparent:
                isaparent = any([p.startswith(base+'/') for p in self.svcapps.keys()])            

            parts = base.rsplit('/', 1)
            if len(parts) < 2:
                parts = ['', base]
            apppath = "/".join([parts[1], apppath]).strip('/')
            base = parts[0]

        return svcapp.handle_path_request(env, start_resp, apppath, who)
    
class WSGIServiceApp(WSGIAppSuite):
    """
    a wrapper around a single ServiceApp instance.
    """

    def __init__(self, svcapp: ServiceApp, log: Logger, base_ep: str = None, config: Mapping={}):
        """
        wrap a single ServiceApp
        """
        super(WSGIServiceApp, self).__init__(config, {'': svcapp}, log, base_ep)

            
    
