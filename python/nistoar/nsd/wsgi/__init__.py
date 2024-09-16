"""
Web Service interface implementations to a staff directory.  This module offers two takes on the service:
  * :py:mod:`~nistoar.nsd.wsgi.nsd1` -- a service intended to be compatible with the 
    NIST Staff Directory (NSD) Service.  Its purpose is to facilitate testing of NSD clients.
  * :py:mod:`~nistoar.nsd.wsgi.oar1` -- a service designed to be optimized for use with OAR 
    applications.  As it is intended to be populated from the NSD, it uses the NSD data schema for
    records; however, the RESTful interface is slightly different and leverages 
    :py:mod:`downloadable indexes <nistoar.midas.dbio.index>`
    for fast lookups.
"""
from logging import Logger, getLogger
from collections.abc import Mapping
from typing import List

from .oar1 import PeopleServiceApp
from .nsd1 import NSDServiceApp
from ..service import PeopleService, MongoPeopleService
from nistoar.web.rest import WSGIAppSuite, authenticate_via_jwt, ReadyApp
from nistoar.pdr.utils.prov import Agent


class PeopleWebServiceApp(WSGIAppSuite):
    """
    a web service for getting information about people and their organization affiliations.  This service
    provides two API versions:

    :py:mod:`~nistoar.nsd.wsgi.nsd1`
        an implementation with an API matching that of the NIST Staff Directory (NSD) service; this 
        is accessible via the "/nsd1" endpoint.

    :py:mod:`~nistoar.nsd.wsgi.oar1`
        an implementation with an API optimized for use by OAR MIDAS front-end applications; this 
        is accessible via the "/oar1" endpoint.

    Both implementations rely on the same underlying database and share a common record-level schema.
    """

    def __init__(self, config: Mapping, log: Logger=None, baseep: str="/",
                 appname: str=None, service: PeopleService=None):
        """
        Initialize the app with the two APIs

        :param dict config:  configuration data 
        :param Logger  log:  the Logger to use for messages; if None, one will be created using the 
                             value of ``appname``.
        :param str appname:  a name to give to the app for informational purposes.  The default will 
                             be taken from the configuration data (``name``).
        :param str  baseep:  that endpoint URL prefix path that all requested paths must start from;
                             if not provided, the default is "/".  
        :param PeopleService service:  the underlying PeopleService connected to the database that 
                             should be used.  This service will power both APIs.  If not provided,
                             one will be constructed using the database URL set in the configuration
                             (as ``db_url``).
        """
        if not appname:
            appname = config.get("name", "nsd")

        if not log:
            log = getLogger(appname)

        if not service:
            dburl = config.get('db_url')
            if not dburl:
                raise ConfigurationException("Missing required config param: db_url")
            if not dburl.startswith("mongodb:"):
                raise ConfigurationException("Unsupported (non-MongoDB) database URL: "+dburl)

            service = MongoPeopleService(dburl)

        # create subapp instances
        svcapps = {
            '': ReadyApp(log, appname, config, "json"),
            'oar1': PeopleServiceApp(config, log.getChild('oar1'), 'oar1', service),
            'nsd1': NSDServiceApp(config, log.getChild('nsd1'), 'nsd1', service),
        }
        
        super(PeopleWebServiceApp, self).__init__(config, svcapps, log, baseep)

    def authenticate_user(self, env: Mapping, agents: List[str]=None, client_id: str=None) -> Agent:
        """
        determine the authenticated user
        """
        authcfg = self.cfg.get('authentication')
        if authcfg:
            return authenticate_via_authkey("nsd", env, authcfg, self.log, agents, client_id)
        return None

    def load(self):
        """
        (re-)initialize the underlying database with data from the configured data directory
        """
        self.svcapps['oar1'].load_from()
        

app = PeopleWebServiceApp        
