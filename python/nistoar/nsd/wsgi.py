"""
A RESTful web service interface to the PeopleService
"""
import logging, json, re
from collections import OrderedDict
from collections.abc import Mapping, Callable

from .service import MongoPeopleService, PeopleService, NSDClientError
from nistoar.web.rest import ServiceApp, Handler, WSGIServiceApp, authenticate_via_jwt
from nistoar.base.config import ConfigurationException
from . import system

deflog = logging.getLogger(system.system_abbrev)   \
                .getChild('wsgi')

DEF_BASE_PATH = "/"

class NSDHandler(Handler):
    """
    Base Handler class for NSD queries
    """

    def __init__(self, service: PeopleService, path: str,
                 wsgienv: Mapping, start_resp: Callable, config: Mapping={}, log: logging.Logger=None):
        if not log:
            log = deflog
        super(NSDHandler, self).__init__(path, wsgienv, start_resp, config=config, log=log)

        self.svc = service

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
        return self.send_options(["GET"])


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
            return self.send_json(self.svc.select_people(query))
        except NSDClientError as ex:
            self.log.debug("client error: %s", str(ex))
            return self.send_error(400, "Bad input")
        except Exception as ex:
            self.log.exception("Failed to execute people query: %s", str(ex))
            return self.send_error(500, "Server error")

    def do_OPTIONS(self, path):
        return self.send_options(["POST"])


class PeopleApp(WSGIServiceApp):
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

        svcapp = self.SvcApp(config, log)
        super(PeopleApp, self).__init__(svcapp, log, base_ep, config)
                                    
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

    class SvcApp(ServiceApp):
        def __init__(self, config, log):
            ServiceApp.__init__(self, "nsd", log, config)

            dburl = self.cfg.get('db_url')
            if not dburl:
                raise ConfigurationException("Missing required config param: db_url")
            if not dburl.startswith("mongodb:"):
                raise ConfigurationException("Unsupported (non-MongoDB) database URL: "+dburl)

            self.svc = MongoPeopleService(dburl)

        def create_handler(self, env, start_resp, path, who) -> Handler:
            if path.startswith("People/"):
                return PeopleHandler(self.svc, path, env, start_resp)

            return OrgHandler(self.svc, path, env, start_resp)


app = PeopleApp

