"""
The web service providing the various flavored endpoints for programmatic data publishing (PDP).
"""
import os, sys, logging, json, re
from wsgiref.headers import Headers
from collections import OrderedDict
from typing import Mapping, List
from copy import deepcopy

from .pdp0 import PDP0App, PDP1App
from ... import PublishSystem, system, PublishException
from nistoar.pdr.utils.prov import Agent
from nistoar.base.config import ConfigurationException, merge_config
from nistoar.web.rest import ServiceApp, WSGIAppSuite, Unauthenticated
from nistoar.web.rest.ready import ReadyApp
from nistoar.pdr.preserve.service import PreservationService, AIP1PreservationService

syslog = logging.getLogger(system.system_abbrev)   \
                .getChild(system.subsystem_abbrev)

DEF_BASE_PATH = "/"

class PDPApp(WSGIAppSuite, PublishSystem):
    """
    a WSGI-compliant service app providing programmatic data publishing (PDP) services under various 
    conventions.  Each endpoint under the base URL handles a different convention.  

    An instance will look for the following parameters in the configuration provided at construction:
    :param str working_dir:  the default parent working directory for state data for all conventions.  
                             If a convention-level configuration does not set its own 'working_dir'
                             parameter, it will be set to a directory named after the convention within
                             the parent working directory specified here.
    :param list authorized:  a list of authorization configurations, one for each client authorized to
                             use this app (see below).
    :param dict conventions: a set of convention-specific configurations where each key is the name 
                             of a SIP convention (e.g. "pdp0") and its value, the configuration for 
                             PublishingService that will handle that service.  The service configuration
                             can also contain a 'override_config_for' parameter which can be used to 
                             combine the configuration with those of other conventions (see below).

    If a service configuration contains the 'override_config_for' parameter, its value must be a 
    string giving the name of another convention.  This indicates that the configuration for the 
    convention it names provides default values that should be overridden by the configuration that 
    refers to it.  The named configuration can also contain a 'override_config_for' parameter to chain 
    several configurations together.  When a circular reference is detected, the chain is broken.

    Each value in the 'authorized' parameter configures a particular authorized client of this web app
    and contains the following sub-parameters:
    :param str auth_key:  (required) the authorization bearer token that should be presented by the 
                          client.  
    :param str client:    (required) the name of the client group to use for clients that connect 
                          with the associated 'auth_key'.  If not provided, the app will ignore this 
                          client authorization, effectively disabling use of the authorization token.
                          Note that is value is used to determine which identifier shoulders the client
                          is allowed to publish under (see 
                          :py:class:`~nistoar.pdr.publish.service.pdp.PDPublishingService`).
    :param str user:      (optional) a default name to assume as the identity of client.  
    :param str type:      (optional) one of ('user', 'auto') indicating the type of agent the client should 
                          be classified as.  'auto' indicates that the client is a user-less system; 'user'
                          indicates the client action was initiated ultimately by an interactive user.
    """

    def __init__(self, config, log: logging.Logger=None, base_ep: str=None, workdir: str=None):
        if not log:
            log = syslog
        if not base_ep:
            base_ep = DEF_BASE_PATH
        if not workdir:
            workdir = config.get('working_dir')
        if not workdir:
            raise ConfigurationException("PDPApp: missing required parameters: working_dir")
        if config.get('repo_access'):
            if not config['repo_access'].get('working_dir'):
                config['repo_access']['working_dir'] = workdir
            elif not os.path.isabs(config['repo_access']['working_dir']):
                config['repo_access']['working_dir'] = \
                    os.path.join(workdir, config['repo_access']['working_dir'])

        pressvc = None
        if config.get('preservation'):
            cfg = config['preservation']
            cfg['working_dir'] = workdir
            cfg['sip_dir'] = os.path.join(workdir, "sipbags")   # Note: not used but required
            if config.get('repo_access'):
                cfg['repo_access'] = merge_config(cfg.get('repo_access', {}),
                                                  deepcopy(config['repo_access']))
            pressvc = AIP1PreservationService(cfg)

        svcapps = self.make_svc_apps(config, pressvc, log, workdir)
        
        WSGIAppSuite.__init__(self, config, svcapps, log, base_ep)

        self._id_map = self._make_id_map(config.get('authorized', {}))
        if not self._id_map:
            log.warning("Missing auth key configuration")

    _service_app_classes = {
        "pdp0": PDP0App,
        "pdp1": PDP1App
    }
        
    @classmethod
    def make_svc_apps(cls, config: Mapping, pressvc: PreservationService=None,
                      log: logging.Logger=None, workdir: str=None) -> Mapping[str, ServiceApp]:
        """
        interpret the given configuration dictionary to create the ServiceApp instances 
        it describes.  

        The configuration must comply with the schema describe in :py:class:`this class <PDPApp>`.
        A side effect of this function is that it may create subdirectories under the working directory 
        if needed by the services.  
        
        :param dict config:  the configuration to interpret
        :param PreservationService pressvc:  the preservation service to inject into the ServiceApp
                             instances.  If not provided, each ServiceApp is responsible for 
                             creating their own as needed.  
        :param Logger  log:  the parent log to use in the ServiceApps
        :returns: a dictionary whose keys are endpoint paths (relative to an unspecified 
                  base path) and the values are ServiceApp instances that should be used 
                  to handle that path.  
                  :rtype: Mapping[str, ServiceApp]
        """
        if not config.get('conventions'):
            raise ConfigurationException("PDPApp: required config parameter missing: conventions")
        if not isinstance(config['conventions'], Mapping):
            raise ConfigurationException("PDPApp: conventions: not a dict: "+
                                         str(type(config['conventions'])))
        if len(config['conventions'].keys()) == 0:
            raise ConfigurationException("PDPApp: no service endpoints configured under conventions")

        if not workdir:
            workdir = config.get('working_dir')
        if not workdir:
            raise ConfigurationException("PDPApp: missing required parameters: working_dir")
        if not os.path.isdir(workdir):
            raise ConfigurationException("PDPApp: working_dir does not exist as directory: "+workdir)

        defsvccfg = deepcopy(config)
        del defsvccfg['conventions']
        for prop in "clients authorized preservation":
            if prop in defsvccfg:
                del defsvccfg[prop]

        conventions = config['conventions']

        # normalize config as needed
        # first assemble bagger configs
        baggers = {}
        for conv in conventions.values():
            for shldr, cfg in conv.get('shoulders', {}).items():
                baggers[shldr] = cfg.get('bagger', {})

        # then copy shared bagger info
        for conv in conventions.values():
            for shldr, cfg in conv.get('shoulders', {}).items():
                if cfg.get('bagger', {}).get('override_config_for') and baggers.get(shldr):
                    # this bagger wants to build on another shoulder's bagger config
                    cfg['bagger'] = merge_config(cfg['bagger'], deepcopy(baggers.get(shldr)))

                if config.get('repo_access'):
                    # merge in global repo access config
                    cfg.setdefault('bagger', {})
                    cfg['repo_access'] = merge_config(cfg['bagger'].get('repo_access', {}),
                                                      config['repo_access'])

        pubroot = os.path.join(workdir, "publish")
        if not os.path.exists(pubroot):
            os.mkdir(pubroot)
            
        # now instantiate
        svcapps = {}
        for conv, cfg in config['conventions'].items():
            cfg = merge_config(cfg, deepcopy(defsvccfg))
            cfg['working_dir'] = os.path.join(pubroot, conv)
            if not os.path.exists(cfg['working_dir']):
                os.mkdir(cfg['working_dir'])

            tp = cfg.get('type', conv)   # service type label
            if not cls._service_app_classes.get(tp):
                raise ConfigurationException(f"PDP: conventions.{conv}.type: "
                                             f"{tp} not a recognized service type")
            try:
                svcapps[conv] = cls._service_app_classes[tp](log, cfg, pressvc)
            except ConfigurationException as ex:
                raise ConfigurationException(f"{tp} ServiceApp: "+str(ex)) from ex
            except Exception as ex:
                raise PublishException(f"Unable to create {tp} ServiceApp: {str(ex)}") from ex

        svcapps[''] = ReadyApp(log.getChild("ready"))
        return svcapps

    def _make_id_map(self, authcfg):
        out = {}
        for iden in authcfg:
            if not iden.get('auth_key'):
                if iden.get('user'):
                    self.log.warning("Missing authorization key for client=%s; skipping...",
                                     iden['client'])
                continue;
            if not isinstance(iden['auth_key'], str):
                raise ConfigurationException("auth_key has wrong type for client="+str(iden.get('client'))+
                                             ": "+type(iden['auth_key']))
            out[iden['auth_key']] = iden

        return out
        
    def authenticate_user(self, env: Mapping, agents: List[str]=None, client_id: str=None) -> Agent:
        """
        determine and return the identity of the client.  This is done by mapping a Bearer key to 
        an identity in the `authorized` configuration parameter.
        :rtype: Agent
        """
        auth = env.get('HTTP_AUTHORIZATION', "")
        authkey = None
        user = None
        auth = auth.split()
        if len(auth) > 1:
            if auth[0] == "Bearer":
                authkey = auth[1]
        if not authkey:
            self.log.warning("Client %s did not provide a Bearer authentication token", str(client_id))
            return Agent("pdp", Agent.UNKN, Agent.ANONYMOUS, Agent.PUBLIC, agents)
        
        client = deepcopy(self._id_map.get(authkey))
        if client:
            client.setdefault('user', 'authorized')
            client.setdefault('client', client_id)
            if not agents or agents == ["(unknown)"]:
                agents = [f"{client['client']}/{client['user']}"]
            return Agent("pdp", Agent.AUTO, client['user'], client['client'], agents)

        self.log.warning("Unrecognized token from client %s", str(client_id))
        raise Unauthenticated("Unrecognized auth token")

    def __call__(self, env, start_resp):
        return self.handle_request(env, start_resp)



