"""
Web service interface used by the legacy NPS review system to deliver review status updates on DAP 
records.

This interface provides a REST interface that is compatible with the legacy communication protocol
between MIDAS and NPS; in addition, it provides a fuller interface for a next generation protocol.  In 
the legacy protocol, NPS will only respond when a reviewer requests chagnes or the review completes 
to full approval.
"""

import logging, re, json
from logging import Logger
from collections import OrderedDict
from typing import List, Mapping, Callable

from nistoar.midas import dbio
from nistoar.midas.dbio.project import ProjectService, ProjectServiceFactory
from nistoar.midas.dap.service import mds3
from nistoar.pdr.utils.prov import Agent
from nistoar.web.rest import (ServiceApp, Handler, HandlerWithJSON, AuthenticatedWSGIApp,
                              authenticate_via_authkey, FatalError)
from ... import system

deflog = logging.getLogger(system.system_abbrev)   \
                .getChild('extrev').getChild('nps')

DEF_BASE_PATH = '/extrev/nps/leg/'
DEF_DBIO_CLIENT_FACTORY_CLASS = dbio.InMemoryDBClientFactory
DEF_DBIO_CLIENT_FACTORY_NAME  = "inmem"

class LegacyNPSFeedbackHandler(HandlerWithJSON):
    """
    the main handler receiving external review feedback
    """
    def __init__(self, project_service: ProjectService, path: str, wsgienv: dict, start_resp: Callable,
                 who=None, log: Logger=None, config: Mapping={}, app=None):
        super(LegacyNPSFeedbackHandler, self).__init__(path, wsgienv, start_resp, who, config, log, app)
        self._svc = project_service

    def get_json_body(self):
        try:
            bodyin = self._env.get('wsgi.input')
            if bodyin is None:
                raise FatalError(400, "Missing input", "Missing expected input JSON data")
            
            return json.load(bodyin, object_pairs_hook=OrderedDict)

        except (ValueError, TypeError) as ex:
            if self.log.isEnabledFor(logging.DEBUG):
                self.log.error("Failed to parse input: %s", str(ex))
                self.log.debug("\n%s", body)
            raise FatalError(400, "Input not parseable as JSON",
                             "Input document is not parse-able as JSON: "+str(ex))

    def do_POST(self, path):
        """
        receive feedback: either approval or rejection
        """
        if not path:
            return self.send_error_obj(404, "POST not allowed",
                                       "Cannot POST to this resource without an ID")
        id = path

        try:
            input = self.get_json_body()
        except FatalError as ex:
            return self.send_fatal_error(ex)

        try:
            if input.get('reviewResponse') is None:
                # interpret a missing response as indicating that the review has started
                self._svc.apply_external_review(id, "nps", "in progress", id)

            elif input.get('reviewResponse'):
                # review is approved
                self._svc.approve(id, "nps", id)

            else:
                # reviewer wants changes
                fb = [{ "type": "req", "description": "Visit NPS for reviewer comments" }]
                self._svc.apply_external_review(id, "nps", "paused", id, feedback=fb, request_changes=True)

            status = self._get_review_status_for(id)
        
        except dbio.NotAuthorized as ex:
            return self.send_error_obj(409, "Not Reviewable",
                                       "Record has not been submitted for review, yet", {"id": id})
        except dbio.ObjectNotFound as ex:
            return self.send_error_obj(404, "ID not found",
                                       "Record with requested identifier not found", {"id": id})

        return self.send_json(status)


    def do_GET(self, path, ashead=False):

        if path:
            try:
                return self.send_json(self._get_review_status_for(path))
            except dbio.NotAuthorized as ex:
                return self.send_unauthorized()
            except dbio.ObjectNotFound as ex:
                return self.send_error_obj(404, "ID not found",
                                           "Record with requested identifier not found", id)

        try:
            return self._select_open_reviews()
        except dbio.NotAuthorized as ex:
            return self.send_unauthorized()

    def _get_review_status_for(self, id):
        prec = self._svc.dbcli.get_record_for(id, dbio.ACLs.PUBLISH)  # may raise exc
        rev = prec.status.get_review_from("nps")
        if not rev:
            rev = {}
        return rev

    def _select_open_reviews(self):
        recs = self._svc.dbcli.select_records(dbio.ACLs.PUBLISH)

        out = []
        for prec in recs:
            rev = prec.status.get_review_from("nps")
            if rev and rev.get('phase') != "approved":
                out.append(rev)

        return self.send_json(out)

class LegacyNPSServiceApp(ServiceApp):
    """
    the web service to receive feedback from the legacy version of NPS
    """

    def __init__(self, service_factory: ProjectServiceFactory, log: Logger, config: Mapping={}):
        super(LegacyNPSServiceApp, self).__init__("legacyNPS", log, config)
        self.svcfact = service_factory

    def create_handler(self, env: dict, start_resp: Callable, path: str, who: Agent) -> Handler:
        """
        return a handler instance to handle a particular request to a path
        :param Mapping env:  the WSGI environment containing the request
        :param Callable start_resp:  the start_resp function to use initiate the response
        :param str    path:  the path to the resource being requested.  This is usually 
                             relative to a parent path that this ServiceApp is configured to 
                             handle.  
        :param Agent   who:  the authenticated user agent making the request
        """

        # create a service on attached to the user
        service = self.svcfact.create_service_for(who)

        return LegacyNPSFeedbackHandler(service, path, env, start_resp, who, self.log, self.cfg, self)

class ExternalReviewApp(AuthenticatedWSGIApp):

    DB_FACTORY_CLASSES = {
        "inmem":    dbio.InMemoryDBClientFactory,
        "fsbased":  dbio.FSBasedDBClientFactory,
        "mongo":    dbio.MongoDBClientFactory
    }

    def __init__(self, config: Mapping, dbio_client_factory: dbio.DBClientFactory=None, base_ep: str=None):
        log = deflog
        if config.get('base_log_name'):
            log = logging.getLogger(config['base_log_name'])
        baseep = base_ep or DEF_BASE_PATH
        if config.get('base_ep_path'):
            baseep = config['base_ep_path']
        
        super(ExternalReviewApp, self).__init__(config, log, baseep, "nps")

        if not dbio_client_factory:
            dbclsnm = self.cfg.get('dbio', {}).get('factory')
            if not dbclsnm:
                dbclsnm = DEF_DBIO_CLIENT_FACTORY_NAME
            dbcls = self.DB_FACTORY_CLASSES.get(dbclsnm)
            if dbcls:
                dbcls = DEF_DBIO_CLIENT_FACTORY_CLASS
            dbio_client_factory = dbcls(self.cfg.get('dbio', {}))

        self.svcfact = mds3.DAPServiceFactory(dbio_client_factory, self.cfg.get('dap_service', {}), log)

    def authenticate_user(self, env: Mapping, agents: List[str]=None, client_id: str=None) -> Agent:
        """
        determine the authenticated user
        """
        authcfg = self.cfg.get('authentication')
        if authcfg:
            return authenticate_via_authkey("midas", env, authcfg, self.log, agents, client_id)
        return None

    def handle_path_request(self, path: str, env: Mapping, start_resp: Callable, who = None):
        svcapp = LegacyNPSServiceApp(self.svcfact, self.log, self.cfg.get('dap', {}))
        return svcapp.create_handler(env, start_resp, path, who).handle()

