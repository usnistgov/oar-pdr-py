"""
A RESTful web service interface to the PeopleService. This implementation is intended to be compatible
with the NIST Staff Directory (NSD) Service. 
"""
import logging, json, re
from collections import OrderedDict
from collections.abc import Mapping, Callable
from typing import List

from ..service import MongoPeopleService, PeopleService, NSDClientError
from nistoar.web.rest import ServiceApp, Handler, WSGIServiceApp, authenticate_via_jwt
from nistoar.pdr.utils.prov import Agent
from nistoar.base.config import ConfigurationException
from .. import system

deflog = logging.getLogger(system.system_abbrev)   \
                .getChild('wsgi')

DEF_BASE_PATH = "/"

class NSDHandler(Handler):
    """
    Base Handler class for NSD queries
    """

    def __init__(self, service: PeopleService, path: str, wsgienv: Mapping, start_resp: Callable, 
                 who=None, config: Mapping={}, log: logging.Logger=None, app=None):
        if not log:
            log = deflog
        super(NSDHandler, self).__init__(path, wsgienv, start_resp, who, config, log, app)

        self.svc = service
        self._format_qp = "format"

class OrgHandler(NSDHandler):
    """
    Handle requests for organization information
    """

    def do_GET(self, path):
        try:
            if path == "NISTOU":
                return self.send_json(self.svc.OUs())
            elif path == "NISTDivision":
                return self.send_json(self.svc.divs())
            elif path == "NISTGroup":
                return self.send_json(self.svc.groups())
            elif path:
                return self.send_error(404, "Not Found")
            else:
                return self.send_error(405, "Method Not Allowed") 
        except Exception as ex:
            self.log.error("%s: %s", path, str(ex))
            return self.send_error(500, "Internal Server Error")

    def do_OPTIONS(self, path):
        return self.send_options(["GET"],
                                 extra={'Access-Control-Allow-Headers': 'Authorization'})


class PeopleHandler(NSDHandler):
    """
    Handle people queries
    """

    def do_POST(self, path):
        """
        handle a people query
        """
        try:
            bodyin = self._env.get('wsgi.input')
            if bodyin is None:
                return self.send_error(400, "Missing Input")
            if self.log.isEnabledFor(logging.DEBUG):
                body = bodyin.read()
                query = json.loads(body, object_pairs_hook=OrderedDict)
            else:
                query = json.load(bodyin, object_pairs_hook=OrderedDict)

        except (ValueError, TypeError) as ex:
            if self.log.isEnabledFor(logging.DEBUG):
                self.log.error("Failed to parse input: %s", str(ex))
                self.log.debug("\n%s", body)
            return self.send_error(400, "Input not parseable as JSON")

        try:
            return self.send_json(list(self.svc.select_people(query)))
        except NSDClientError as ex:
            self.log.debug("client error: %s", str(ex))
            return self.send_error(400, "Bad input")
        except Exception as ex:
            self.log.exception("Failed to execute people query: %s", str(ex))
            return self.send_error(500, "Server error")

    def do_OPTIONS(self, path):
        return self.send_options(["POST"],
                                 extra={'Access-Control-Allow-Headers': 'Authorization'})


class ReadyHandler(NSDHandler):
    """
    Handle requests for organization information (e.g. everything under "/oar1/org")
    """
    def do_GET(self, path, ashead=False, format=None):
        try:
            format = self.select_format(format)
        except Unacceptable as ex:
            return self.send_unacceptable(content=str(ex))
        except UnsupportedFormat as ex:
            return self.send_error(400, "Unsupported Format", str(ex))

        return self.send_json(self.svc.status(), ashead=ashead)

    def do_OPTIONS(self, path):
        return self.send_options(["GET"],
                                 extra={'Access-Control-Allow-Headers': 'Authorization'})


class NSDServiceApp(ServiceApp):
    """
    A ServiceApp wrapper around the PeopleService that can be deployed into a larger WSGI app.
    """
    def __init__(self, config, log, appname=None, service: PeopleService=None):
        if not appname:
            appname = "nsd"
        super(NSDServiceApp, self).__init__(appname, log, config)

        if not service:
            dburl = self.cfg.get('db_url')
            if not dburl:
                raise ConfigurationException("Missing required config param: db_url")
            if not dburl.startswith("mongodb:"):
                raise ConfigurationException("Unsupported (non-MongoDB) database URL: "+dburl)

            service = MongoPeopleService(dburl)
        self.svc = service

    def create_handler(self, env, start_resp, path, who) -> Handler:
        if not path:
            return ReadyHandler(self.svc, path, env, start_resp, who, log=self.log, app=self)
        if path.startswith("People/"):
            return PeopleHandler(self.svc, path, env, start_resp, who, log=self.log, app=self)

        return OrgHandler(self.svc, path, env, start_resp, who, log=self.log, app=self)

    def load_from(self, datadir=None):
        """
        initialize the people database.  This will use the configuration under the ``data`` 
        parameter to control the loading (see :py:class:`~nistoar.nsd.service.PeopleService` 
        supported parameters).

        :param str datadir:   the directory to look for data files in, overriding the value
                              provided in the configuration.  

        :raises ConfigurationException:  if any of the configured files or directory does not exist
        """
        datacfg = self.cfg.get("data", {})
        datacfg.setdefault("dir", "/data/nsd")
        if datadir:
            datacfg = dict(self.cfg.items())
            datacfg['dir'] = str(datadir)
        self.svc.load(datacfg, self.log, True)

PeopleServiceApp = NSDServiceApp


class NSDApp(WSGIServiceApp):
    """
    A partial implementation of the NIST Staff Directory web service
    """

    def __init__(self, config: Mapping, base_ep: str=None, log: logging.Logger = deflog):
        """
        initialize the app
        :param Mapping config:  the collected configuration for the App
        :param str base_ep:     the resource path to assume as the base of all services provided by
                                this App.  If not provided, a value set in the configuration is 
                                used (which itself defaults to "").
        """
        if base_ep is None:
            base_ep = config.get('base_endpoint', DEF_BASE_PATH).strip('/')

        svcapp = NSDServiceApp(config, log)
        super(NSDApp, self).__init__(svcapp, log, base_ep, config)
                                    
        if not self.cfg.get('authentication'):  # formerly jwt_auth
            log.warning("JWT Authentication is not configured")
        else:
            if not isinstance(self.cfg['authentication'], Mapping):
                raise ConfigurationException("Config param, authentication, not a dictionary: "+
                                             str(self.cfg['authentication']))
            if not self.cfg['authentication'].get('require_expiration', True):
                log.warning("JWT Authentication: token expiration is not required")

    def authenticate_user(self, env: Mapping, agents: List[str]=None, client_id: str=None) -> Agent:
        """
        determine the authenticated user
        """
        authcfg = self.cfg.get('authentication', {})
        return authenticate_via_jwt("midas", env, authcfg, self.log, agents, client_id)


app = NSDApp

