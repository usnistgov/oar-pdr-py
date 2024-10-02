"""
Reuseable classes for providing a proof-of-life endpoint in a NIST-OAR web app
"""
import json
from typing import Callable, Mapping
from logging import Logger
from collections import OrderedDict

from .base import ServiceApp, Handler
from ..formats import (Unacceptable, UnsupportedFormat, FormatSupport,
                       XHTMLSupport, TextSupport, JSONSupport)

class Ready(Handler):
    """
    a default handler for handling unsupported paths or proof-of-life responses.

    This handler supports only one method on its base path--GET--which simply returns a message
    indicating that the service it provides is alive and ready for use.  Clients may choose 
    plain text or HTML as the output format either via the ``ACCEPT`` HTTP header or the 
    ``format`` query parameter.
    """

    def __init__(self, path, wsgienv, start_resp, who=None, config={}, log=None, app=None,
                 deffmt: str="text"):
        """
        instantiate the handler
        """
        super(Ready, self).__init__(path, wsgienv, start_resp, who, config, log, app)

        self._set_format_qp("format")
        if deffmt not in [ TextSupport.FMT_TEXT, XHTMLSupport.FMT_HTML, JSONSupport.FMT_JSON ]:
            deffmt = TextSupport.FMT_TEXT

        fmtsup = FormatSupport()
        TextSupport.add_support(fmtsup, deffmt == TextSupport.FMT_TEXT)
        XHTMLSupport.add_support(fmtsup, deffmt == XHTMLSupport.FMT_HTML)
        JSONSupport.add_support(fmtsup, ["application/json", "text/json"], deffmt == JSONSupport.FMT_JSON)
        self._set_default_format_support(fmtsup)

    def do_GET(self, path, ashead=False, format=None):
        path = path.lstrip('/')
        if path:
            return self.send_error(404, "Not found")

        format = None
        try:
            format = self.select_format(format, path)
            if not format:
                if self.log:
                    self.log.failure("Failed to determine output format")
                return send_error(500, "Server Error")
        except Unacceptable as ex:
            return self.send_unacceptable(content=str(ex))
        except UnsupportedFormat as ex:
            return self.send_error(400, "Unsupported Format", str(ex))
            
        if format.name == XHTMLSupport.FMT_HTML:
            return self.get_ready_html(format.ctype, ashead)

        if format.name == JSONSupport.FMT_JSON:
            return self.get_ready_json(format.ctype, ashead)

        if format.name == TextSupport.FMT_TEXT:
            name = ""
            if self.app and self.app.name:
                name = self.app.name
            msg = f"{name} service is ready"
            return self.send_ok(msg, format.ctype, "Ready", ashead=ashead)

        self.send_error(400, "Unsupported format requested")

    def get_ready_html(self, contenttype, ashead=None):
        servicename = None
        if self.app:
            servicename = self.app.name
        if not servicename:
            servicename = ""
        servicename = servicename.capitalize()

        out = f"""
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
  <head>
    <title>{servicename} Service: Ready</title>
  </head>
  <body>
    <h1>{servicename} Service Is Ready</h1>
u  </body>
</html>
"""      
        return self.send_ok(out, contenttype, "Ready", ashead=ashead)

    def get_ready_json(self, contenttype, ashead=None):
        servicename = None
        if self.app:
            servicename = self.app.name
        if not servicename:
            servicename = ""

        out = OrderedDict([
            ("service", servicename),
            ("status",  "ready"),
            ("message", f"{servicename} service is ready.")
        ])
        return self.send_ok(json.dumps(out, indent=2), contenttype, ashead=ashead)
        
        
class ReadyApp(ServiceApp):
    """
    a WSGI sub-app that handles unsupported path or proof-of-life requests
    """

    def __init__(self, log: Logger, appname: str="Ready", config: Mapping=None, deffmt="text"):
        super(ReadyApp, self).__init__(appname, log, config)
        self._deffmt = deffmt

    def create_handler(self, env: dict, start_resp: Callable, path: str, who) -> Handler:
        """
        return a handler instance to handle a particular request to a path
        :param Mapping env:  the WSGI environment containing the request
        :param Callable start_resp:  the start_resp function to use initiate the response
        :param str path:     the path to the resource being requested.  This is usually 
                             relative to a parent path that this ServiceApp is configured to 
                             handle.  
        """
        return Ready(path, env, start_resp, who, log=self.log, app=self, deffmt=self._deffmt)

