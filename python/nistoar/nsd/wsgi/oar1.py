"""
A RESTful web service interface to the PeopleService designed to be optimized for use with OAR 
applications.  As it is intended to be populated from the NSD, it uses the NSD data schema for
records; however, the RESTful interface is slightly different and leverages 
:py:mod:`downloadable indexes <nistoar.midas.dbio.index>`
"""
import logging, json
from logging import Logger
from typing import Callable, List
from collections import OrderedDict
from collections.abc import Mapping
from urllib.parse import parse_qs

from .nsd1 import NSDHandler
from ..service import MongoPeopleService, PeopleService, NSDClientError
from nistoar.web.formats import (FormatSupport, Format, UnsupportedFormat, Unacceptable)
from nistoar.web.rest import ServiceApp, Handler, WSGIServiceApp, authenticate_via_jwt, NotFoundHandler
from nistoar.web.rest.jsonerr import ErrorHandling
from nistoar.pdr.utils.prov import Agent
from nistoar.midas.dbio import index
from nistoar.base.config import ConfigurationException

from .. import system

deflog = logging.getLogger(system.system_abbrev)   \
                .getChild('oar1')
jsonfmt = Format("json", "application/json")
csvfmt = Format("csv", "text/csv")

DEF_BASE_PATH = "/"

class OARNSDHandler(NSDHandler, ErrorHandling):
    """
    Handle requests for organization information
    """
    def __init__(self, service: PeopleService, path: str, wsgienv: Mapping, start_resp: Callable, 
                 who=None, config: dict={}, log: Logger=None, app=None):
        super(OARNSDHandler, self).__init__(service, path, wsgienv, start_resp, who, config, log, app)
        self._set_format_qp("format")
        self.def_pvals = {
            "like": "",
            "as": "records",
            "with": "",
            "format": "json"
        }
        self._qp = parse_qs(self._env.get('QUERY_STRING',""))

        self.id_fmt_supp = FormatSupport()
        self.id_fmt_supp.support(jsonfmt, ["application/json", "text/json"], True)
        self.sel_fmt_supp = FormatSupport()
        self.sel_fmt_supp.support(jsonfmt, ["application/json", "text/json"], True)
        self.sel_fmt_supp.support(csvfmt, ["text/csv"])

    def get_format_support(self, path: str, method: str="GET") -> FormatSupport:
        if not path or path.startswith(":"):
            return self.sel_fmt_supp
        return self.id_fmt_supp

    def get_like_selection(self):
        """
        return the value of the requested "like" selection (i.e. the value of the "like" query
        parameter).  If the parameter was not specified the by the client, an empty string is 
        returned. 

        Specifying the "like" parameter selects records where key record fields start with the 
        parameter value.  
        """
        return self._qp.get("like")

    def get_with_selection(self):
        """
        return the value of the requested "with" selection (i.e. the value of the "with" query
        parameter) which sets constraints on the records to select for return.  If the parameter was 
        not specified the by the client, an empty string is returned
        """
        out = {}
        for key in self._qp:
            if key.startswith("with_"):
                out[key[len("with_"):]] = self._qp[key]
        return out if out else None

    def preauthorize(self):
        """
        do an initial test to see if the client identity is authorized to access this service.  
        This method will get called prior to calling the specific method handling function (e.g. 
        ``do_GET()``).  In this implementation, this method will reject any client that has not 
        authenticated successfully with a token.  
        """
        # return True if the app was not configured for authentication
        if not self.app or not self.app.cfg.get('authentication'):
            self.log.debug("Authentication not configured")
            return True

        # otherwise, the user agent must be set and it can't be anonymous or invalid
        out = bool(self.who) and self.who.actor != Agent.ANONYMOUS and \
               self.agent_class != Agent.INVALID_AGENT_CLASS
        if not out:
            if self.who:
                self.log.info("Unauthorized user: %s", str(self.who))
            else:
                self.log.info("No user authenticated")
        return out
        

class PeopleHandler(OARNSDHandler):
    """
    Handle people queries (e.g. everything under "/oar1/People")
    """
    idxr = index.NSDPeopleResponseIndexer()

    def make_like_filter(self, likes):
        if not isinstance(likes, list):
            likes = [likes]
        likes = "|".join(likes)
        if "|" in likes:
            likes = f"({likes})"
        return {"$or": [{"lastName": { "$regex": f"^{likes}" }}, {"firstName": { "$regex": f"^{likes}" }}]}

    def do_GET(self, path, ashead=False, format=None):
        try:
            format = self.select_format(format)
            if not format:
                format = jsonfmt
        except Unacceptable as ex:
            return self.send_unacceptable(content=str(ex))
        except UnsupportedFormat as ex:
            return self.send_error(400, "Unsupported Format", str(ex))
            
        if path and path not in "select index".split():
            # path is taken as an identifier
            out = None
            try:
                out = self.svc.get_person(int(path))
            except ValueError as ex:
                pass
            except Exception as ex:
                self.log.exception("service error while retrieving person record by id: %s", str(ex))
            if not out:
                return self.send_error_obj(404, "Not Found")

            return self.send_json(out)

        # requesting a selection (a list of matching people)
        withfilt = self.get_with_selection()
        likefilt = self.get_like_selection()
        try:
            iter = self.svc.select_people(withfilt, likefilt)
        except NSDClientError as ex:
            self.log.debug("client error: %s", str(ex))
            return self.send_error_obj(400, "Bad input", str(ex))
        except Exception as ex:
            self.log.exception("Failed to execute people query (%s): %s", str(filter), str(ex))
            return self.send_error(500, "Server error")

        if path == "index":
            # return an index
            idx = self.idxr.make_index(iter)
            if format.name == "csv":
                return self.send_ok(self.export_as_csv(), format.ctype)
            else:
                return self.send_ok(idx.export_as_json(), jsonfmt.ctype)
                
        else:
            # return the records
            if format.name == "csv":
                return self.send_ok(self.to_csv(iter), format.ctype)
            else:
                return self.send_json(list(iter))

    def do_POST(self, path, format=None):
        try:
            format = self.select_format(format, path)
            if not format:
                format = jsonfmt
        except Unacceptable as ex:
            return self.send_unacceptable(content=str(ex))
        except UnsupportedFormat as ex:
            return self.send_error(400, "Unsupported Format", str(ex))

        if not path or path not in "select index".split():
            return self.send_error(405, "Method Not Supported")
            
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
            out = self.svc.select_people(query)
            if path == "index":
                return self.send_ok(self.idxr.make_index(iter).export_as_json(), jsonfmt.ctype)
            else:
                return self.send_json(list(out))
        except NSDClientError as ex:
            self.log.debug("client error: %s", str(ex))
            return self.send_error(400, "Bad input")
        except Exception as ex:
            self.log.exception("Failed to execute people query (%s): %s", str(query), str(ex))
            return self.send_error(500, "Server error")

    def do_OPTIONS(self, path):
        return self.send_options(["POST", "GET"],
                                 extra={'Access-Control-Allow-Headers': 'Authorization'})

        
class OrgHandler(OARNSDHandler):
    """
    Handle requests for organization information (e.g. everything under "/oar1/org")
    """
    idxr = index.NSDOrgResponseIndexer()
    toorglab = { "OU":    PeopleService.OU_ORG_TYPE,
                 "Div":   PeopleService.DIV_ORG_TYPE,
                 "Group": PeopleService.GRP_ORG_TYPE  }

    def do_GET(self, path, ashead=False, format=None):
        try:
            format = self.select_format(format)
            if not format:
                format = jsonfmt
        except Unacceptable as ex:
            return self.send_unacceptable(content=str(ex))
        except UnsupportedFormat as ex:
            return self.send_error(400, "Unsupported Format", str(ex))

        op = None
        orgtp = None
        if path:
            parts = path.split('/', 2)
            if parts[0] in self.toorglab:
                orgtp = self.toorglab[parts.pop(0)]
            if len(parts) > 0 and parts[0] in "select index".split():
                op = parts.pop(0)
            path = '/'.join(parts)

        if path:
            # path is taken as an identifier
            if op:
                # incompatible with an id
                return send_error_obj(404, "Not Found", ashead=ashead)

            out = None
            try: 
                if orgtp == self.svc.OU_ORG_TYPE:
                    out = self.svc.get_ou(int(path))
                elif orgtp == self.svc.DIV_ORG_TYPE:
                    out = self.svc.get_div(int(path))
                elif orgtp == self.svc.GRP_ORG_TYPE:
                    out = self.svc.get_group(int(path))
                else:
                    out = self.svc.get_org(int(path))
            except ValueError as ex:
                pass
            except Exception as ex:
                self.log.exception("service error while retrieving person record by id: %s", str(ex))
            if not out:
                return self.send_error_obj(404, "Not Found", ashead=ashead)

            return self.send_json(out, ashead=ashead)

        # requesting a selection (a list of matching people)
        withfilt = self.get_with_selection()
        likefilt = self.get_like_selection()
        try:
            iter = self.svc.select_orgs(withfilt, likefilt, orgtp)
        except NSDClientError as ex:
            self.log.debug("client error: %s", str(ex))
            return self.send_error(400, "Bad input")
        except Exception as ex:
            self.log.exception("Failed to execute org query (%s): %s", str(filter), str(ex))
            return self.send_error(500, "Server error")

        if op == "index":
            # return an index
            idx = self.idxr.make_index(iter)
            if format.name == "csv":
                return self.send_ok(self.export_as_csv(), format.ctype, ashead=ashead)
            else:
                return self.send_ok(idx.export_as_json(), jsonfmt.ctype, ashead=ashead)
                
        else:
            # return the records
            if format.name == "csv":
                return self.send_ok(self.to_csv(iter), format.ctype, ashead=ashead)
            else:
                return self.send_json(list(iter), ashead=ashead)

    def do_POST(self, path, format=None):
        try:
            format = self.select_format(format, path)
            if not format:
                format = jsonfmt
        except Unacceptable as ex:
            return self.send_unacceptable(content=str(ex))
        except UnsupportedFormat as ex:
            return self.send_error(400, "Unsupported Format", str(ex))

        if not path or path not in "select index".split():
            return self.send_error(405, "Method Not Supported")
            
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
            out = self.svc.select_orgs(query)
            if path == "index":
                return self.send_json(self.idxr.make_index(iter).export_as_json())
            else:
                return self.send_json(list(out))
        except NSDClientError as ex:
            self.log.debug("client error: %s", str(ex))
            return self.send_error(400, "Bad input")
        except Exception as ex:
            self.log.exception("Failed to execute people query (%s): %s", str(query), str(ex))
            return self.send_error(500, "Server error")

    def do_OPTIONS(self, path):
        return self.send_options(["POST", "GET"],
                                 extra={'Access-Control-Allow-Headers': 'Authorization'})

        
class ReadyHandler(OARNSDHandler):
    """
    Handle health (and load) requests ("/oar")
    """
    def do_GET(self, path, ashead=False, format=None):
        try:
            format = self.select_format(format)
        except Unacceptable as ex:
            return self.send_unacceptable(content=str(ex))
        except UnsupportedFormat as ex:
            return self.send_error(400, "Unsupported Format", str(ex))

        out = self.svc.status()
        if out.get("status") == "ready":
            return self.send_json(out, ashead=ashead)
        return self.send_json(out, "Not Ready", 503, ashead=ashead)

    def do_LOAD(self, path):
        allowed = self.cfg.get("allowed_loaders", [])
        if self.who.actor not in allowed and self.who.agent_class not in allowed:
            self.log.warning("Unauthorized user attempted reload NSD data: %s", self.who.actor)
            return self.send_error_obj(401, "Not Authorized",
                                       f"{self.who.actor} is not authorized to reload")

        try:
            if not self.cfg.get("quiet", False):
                self.log.info("NSD reload triggered by %s", str(self.who))
            self.app.load_from()
            return self.send_error_obj(200, "Data Reloaded", "Successfully reloaded NSD data")
        except ConfigurationException as ex:
            # raised if any of the configured data locations does not exist
            self.log.warning("Unable to reload NSD data: %s", str(ex))
            return self.send_error_obj(503, "NSD data not available for reloading")
        except Exception as ex:
            self.log.error("Failed to reload NSD data: %s", str(ex))
            return self.send_error_obj(500, "Internal Server Error")

    def do_OPTIONS(self, path):
        return self.send_options(["GET"],
                                 extra={'Access-Control-Allow-Headers': 'Authorization'})

class PeopleServiceApp(ServiceApp):
    """
    A ServiceApp wrapper around the PeopleService that can be deployed into a larger WSGI app.
    """
    def __init__(self, config, log, appname=None, service: PeopleService=None):
        if not appname:
            appname = "nsd"
        super(PeopleServiceApp, self).__init__(appname, log, config)

        if not service:
            dburl = self.cfg.get('db_url')
            if not dburl:
                raise ConfigurationException("Missing required config param: db_url")
            if not dburl.startswith("mongodb:"):
                raise ConfigurationException("Unsupported (non-MongoDB) database URL: "+dburl)

            service = MongoPeopleService(dburl)
        self.svc = service

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
            datacfg['datadir'] = datadir
        self.svc.load(datacfg, self.log, True)

    def create_handler(self, env, start_resp, path, who) -> Handler:
        parts = path.split('/', 1)
        what = parts.pop(0).lower()
        path = parts[0] if parts else ""

        if not what:
            return ReadyHandler(self.svc, path, env, start_resp, who, self.cfg, log=self.log, app=self)

        if what == "people":
            return PeopleHandler(self.svc, path, env, start_resp, who, self.cfg, 
                                 log=self.log.getChild("people"), app=self)

        if what == "orgs":
            return OrgHandler(self.svc, path, env, start_resp, who, self.cfg, 
                              log=self.log.getChild("people"), app=self)

        return NotFoundHandler(path, env, start_resp, self.cfg, log=self.log, app=self)


class PeopleApp(WSGIServiceApp):
    """
    A specialized implementation of the NIST Staff Directory web service
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

        svcapp = PeopleServiceApp(config, log)
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
        return authenticate_via_jwt("nsd", env, authcfg, self.log, agents, client_id)

    def load(self):
        """
        (re-)initialize the underlying database with data from the configured data directory
        """
        self.svcapps[''].load_from()


app = PeopleApp
