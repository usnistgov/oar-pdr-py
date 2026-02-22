"""
Support for JSON-formatted error content for HTTP responses.

Proper REST service clients should use the HTTP status value for determining if an HTTP request as 
resutled in an error; however, a service may want to provide more information about what went wrong 
than what can be fit into the HTTP status and reason fields, and in a machine-readable format.  This 
module provides functions and classes that provide a consistent model for returning error data as 
a JSON object.  It can include a detailed explanation of the error that can be a longer and more 
specific than the reason that is sent in the header; plus, implementations can augment the object with 
custom properties.  

At a minimum a JSON error message will contain at least the following properties:

``http:status``
     the HTTP status number (e.g. 400, 503, etc.) indicating the type error that occured.  This 
     should match the value given in the response header.

``http:reason``
     the text briefly describing the error that occurred.  This should match the value given in the 
     resonse header.  It need not be a standard message that usually accompanies an HTTP status (e.g.
     "Bad Input" for 400), but it shoud be comparably short.  

``oar:message``
     a longer message explaining what went wrong.

The function is :py:func:`is_error_msg` can be used by clients to recognized a response message that 
conforms the above model.  


"""
import json
from logging import Logger
from collections import OrderedDict
from typing import Mapping, Callable
from copy import deepcopy

from .base import Handler

def is_error_msg(msgobj: Mapping):
    """
    return True if the given dictionary represents a JSON-formatted error message
    """
    if not isinstance(msgobj, Mapping):
        return False
    return "http:status" in msgobj and "oar:message" in msgobj;

def make_message(code: int, reason: str, message: str=None, extra: Mapping={}):
    """
    create a compliant error message object from the inputs
    """
    out = OrderedDict([
        ("http:status", code),
        ("http:reason", reason),
        ("oar:message", message or reason)
    ])
    if extra:
        for k,v in extra.items():
            out[k] = v
    return out

class FatalError(Exception):
    """
    an exception that can be used to send data to be returned to the web client as an error 
    JSON message object up the call stack.  
    """
    def __init__(self, code: int, reason: str, explain=None, extra=None):
        """
        :param int    code:  the HTTP code to respond with 
        :param str  reason:  the reason to return as the HTTP status message
        :param str explain:  the more extensive explanation as to the reason for the error; 
                             this is returned only in the body of the message
        :param dict  extra:  a dictionary of additional properties to include in the output 
                             message object.
        """
        if not explain:
            explain = reason or ''
        super(FatalError, self).__init__(explain)
        self.code = code
        self.reason = reason
        self.explain = explain
        self.data = extra

    def data_update(self, props: Mapping):
        """
        add or update the extra data attached to this FatalError
        """
        if self.data is None:
            self.data = OrderedDict()
        self.data.update(props)

    def to_dict(self):
        return make_message(self.code, self.reason, self.explain, self.data)

    def to_json(self, indent=None):
        return json.dumps(self.to_dict(), indent=indent)

class ErrorHandling:
    """
    a Handler mixin class that provides extra methods for returning error message objects to 
    web clients.
    """

    def __init__(self):
        pass

    def send_error_obj(self, code: int, reason: str, explain=None, extra=None, ashead=False,
                       contenttype="application/json"):
        """
        send a JSON-formatted error message back to the web client
        :param int    code:  the HTTP code to respond with 
        :param str  reason:  the reason to return as the HTTP status message
        :param str explain:  the more extensive explanation as to the reason for the error; 
                             this is returned only in the body of the message
        :param dict  extra:  a dictionary of additional properties to include in the output 
                             message object.
        """
        return self.send_fatal_error(FatalError(code, reason, explain, extra), ashead, contenttype)

    def send_fatal_error(self, fatalex: FatalError, ashead=False, contenttype="application/json"):
        """
        report a FatalError as a JSON-formatted error message back to the web client
        :param FatalError fatalex:  the error data as a FatalError exception 
        :param bool        ashead:  True if the HTTP request was a HEAD request
        :param str    contenttype:  The JSON mime-type to affix to the response as the message 
                                    content type.  Default: "application/json"
        """
        return self.send_error(fatalex.code, fatalex.reason, fatalex.to_json(), contenttype, ashead)

class HandlerWithJSON(Handler, ErrorHandling):
    """
    a Handler that provides extra methods for returning to web clients error responses formatted in 
    JSON.  
    """

    def __init__(self, path: str, wsgienv: dict, start_resp: Callable, who=None, 
                 config: dict={}, log: Logger=None, app=None):
        Handler.__init__(self, path, wsgienv, start_resp, who, config, log, app)

        
        
