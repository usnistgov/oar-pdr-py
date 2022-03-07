"""
The web service providing the various flavored endpoints for programmatic data publishing (PDP).
"""
import os, sys, logging, json, re
from wsgiref.headers import Headers
from collections import OrderedDict, Mapping
from copy import deepcopy

from ... import ConfigurationException, PublishSystem, system
from ...prov import PubAgent
from .base import Ready
from .pdp0 import PDP0App

log = logging.getLogger(system.system_abbrev)   \
             .getChild(system.subsystem_abbrev) \
             .getChild('wsgi')

DEF_BASE_PATH = "/"

class PDPApp(PublishSystem):
    """
    a WSGI-compliant service app providing programmatic data publishing (PDP) services under various 
    conventions.  Each endpoint under the base URL handles a different convention.  

    Currently, only the "pdp0" convention is supported.
    """
    _agent_types = { "user": PubAgent.USER, "auto": PubAgent.AUTO }

    def __init__(self, config):
        level = config.get('loglevel')
        if level:
            log.setLevel(level)

        # load the authorized identities
        self._id_map = {}
        for iden in self.cfg.get('authorized',[]):
            if not iden.get('auth_key'):
                if iden.get('user'):
                    log.warning("Missing authorization key for user=%s; skipping...", iden['user'])
            if not isinstance(iden['auth_key'], str):
                raise ConfigurationException("auth_key has wrong type for user="+str(iden.get('user'))+
                                             ": "+type(iden['auth_key']))
            self._id_map[iden['auth_key']] = iden

        self.subapps = {
            "pdp0": PDP0App(self._config_for_convention("pdp0"), log),
            "":     ReadyApp(self.cfg.get('ready',{}), log)
        }

    def _config_for_convention(self, conv):
        # return a complete configuration for the handler covering a particular convention.
        # it resolves all 'override_config_for' directives

        cfg = deepcopy(self.cfg.get('conventions', {}).get(conv, {}))
        if not cfg:
            raise ConfigurationException("No configuration available for convention="+conv+"!")

        # if config inherits from another convention (via 'override_config_for'), combine them properly
        loaded = [conv]
        while 'override_config_for' in cfg:
            cnv = cfg.pop('override_config_for')
            if cnv in loaded:
                self.log.warning("Circular references found in configuration for %s convention", conv)
                break
            parent = deepcopy(self.cfg.get('conventions', {}).get(conv, {}))
            cfg = cfgmod.merge_config(cfg, parent)
            loaded.append(cnv)

        cfg['convention'] = conv
        return cfg

    def authenticate(self) -> PubAgent:
        """
        determine and return the identity of the client.  This is done by mapping a Bearer key to 
        an identity in the `authorized` configuration parameter.
        :rtype: PubAgent
        """
        auth = self._env.get('HTTP_AUTHORIZATION', "")
        authkey = None
        user = None
        parts = auth.split()
        if len(auth) > 1:
            if auth[0] == "Bearer":
                authkey = auth[1]
        
        client = self._id_map.get(authkey)
        if not client:
            return None

        if not user:
            user = client.get('user', 'anonymous')

        return PubAgent(client.get('group'), self._agent_types.get(client.get('type')), PubAgent.UNKN, user)
        

    def handle_request(self, env, start_resp):
        path = env.get('PATH_INFO', '/').strip('/')
        parts = path.split('/', 1)

        # determine who is making the request
        who = self.authenticate()

        subapp = None
        if parts[0] in self.subapps:
            # parts[0] is a convention (e.g. "pdp0")
            path = '/'
            if len(parts) > 1:
                path += parts[1]
            subapp = self.handlers.get(parts[0])

        if not subapp:
            subapp = self.handlers.get('')

        return subapp.handle_path_request(env, start_resp, path, who)

    def __call__(self, env, start_resp):
        return self.handle_request(env, start_resp)

app = PDPApp

