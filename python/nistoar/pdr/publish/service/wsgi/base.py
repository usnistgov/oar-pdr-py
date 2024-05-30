"""
The base Handler class for handler implementations used by the PDP WSGI app
"""
import sys, re, json
from abc import ABCMeta, abstractmethod, abstractproperty
from typing import Callable, List
from functools import reduce
from logging import Logger
from urllib.parse import parse_qs
from collections import OrderedDict
from collections.abc import Mapping

from wsgiref.headers import Headers
# from urllib.parse import parse_qs

from nistoar.pdr.utils.prov import Agent
from nistoar.pdr import config as cfgmod
from nistoar.pdr.utils.web import order_accepts
from nistoar.web.rest import ServiceApp, Handler

class PDPHandler(Handler):
    """
    a request handler that also serves as a base class for programmatic data publishing (PDP)
    service handlers.  It adds support for an ``action`` query parameter to request an action 
    to be applied to the resource being requested (perhaps in addition to that implied by the HTTP 
    request method); see :py:meth:`get_action`.
    """
    default_agent = Agent("pdp", Agent.UNKN, Agent.ANONYMOUS, Agent.PUBLIC)
    ACTION_UPDATE   = ''
    ACTION_FINALIZE = "finalize"
    ACTION_PUBLISH  = "publish"

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

    def preauthorize(self):
        """
        determine if the client agent is authorized to access this endpoint.  This implementation 
        ensures that a valid client agent has been established.
        """
        return bool(self.who) and self.who.agent_class != Agent.INVALID \
                              and self.who.agent_class != Agent.PUBLIC

    def acceptable(self):
        """
        return True if the client's Accept request is compatible with this handler.

        This default implementation will return True if "*/*" is included in the Accept request
        or if the Accept header is not specified.
        """
        accepts = self.get_accepts()
        return not accepts or "*/*" in accepts or "application/json" in accepts

class Ready(ServiceApp):
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

    def create_handler(self, env: dict, start_resp: Callable, path: str, who: Agent) -> Handler:
        """
        return a handler instance to handle a particular request to a path
        :param Mapping env:  the WSGI environment containing the request
        :param Callable start_resp:  the start_resp function to use initiate the response
        :param str path:     the path to the resource being requested.  This is usually 
                             relative to a parent path that this SubApp is configured to 
                             handle.  
        """
        return self._Handler(path, env, start_resp, who, log=self.log)

