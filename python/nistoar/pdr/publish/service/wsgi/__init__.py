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

    If a service configuration can contains the 'override_config_for' parameter, its value must be a 
    string giving the name of another convention.  This indicates that the configuration for the 
    convention it names provides default values that should be overridden by the configuration that 
    refers to it.  The named configuration can also contain a 'override_config_for' parameter to chain 
    several configurations together.  When a circular reference is detected, the chain is broken.

    Each value in the 'authorized' parameter configures a particular authorized client of this web app
    and contains the following sub-parameters:
    :param str auth_key:  (required) the authorization bearer token that should be presented by the 
                          client.  
    :param str group:     (required) the name of the permission group to use for clients that connect 
                          with the associated 'auth_key'.  If not provided, the app will ignore this 
                          client authorization, effectively disabling use of the authorization token.
                          Note that is value is used to determine which identifier shoulders the client
                          is allowed to publish under (see 
                          :py:class:`~nistoar.pdr.publish.service.pdp.PDPublishingService`).
    :param str user:      (optional) a default name to assume as the identity of client when the client
                          does not provide one via the "X_OAR_USER" HTTP header item.  
    :param str type:      (optional) one of ('user', 'auto') indicating the type of agent the client should 
                          be classified as.  'auto' indicates that the client is a user-less system; 'user'
                          indicates the client action was initiated ultimately by an interactive user.
    """

    def __init__(self, config):
        self.cfg = config

        # load the authorized identities
        self._id_map = {}
        for iden in self.cfg.get('authorized',[]):
            if not iden.get('auth_key'):
                if iden.get('user'):
                    log.warning("Missing authorization key for group=%s; skipping...", iden['group'])
                continue;
            if not isinstance(iden['auth_key'], str):
                raise ConfigurationException("auth_key has wrong type for group="+str(iden.get('group'))+
                                             ": "+type(iden['auth_key']))
            self._id_map[iden['auth_key']] = iden

        # each convention is mapped to a "sub-app" that will handle its requests
        self.subapps = {
            "pdp0": PDP0App(log, self._config_for_convention("pdp0")),
            "":     Ready(log, self.cfg.get('ready',{}))
        }

    def _config_for_convention(self, conv):
        # return a complete configuration for the handler covering a particular convention.
        # it resolves all 'override_config_for' directives

        cfg = deepcopy(self.cfg.get('conventions', {}).get(conv, {}))
        if not cfg:
            raise ConfigurationException("No configuration available for convention="+conv+"!")

        # Note 'working_dir' should not be inherited via 'override_config_for'
        if 'working_dir' not in cfg and self.cfg.get('working_dir'):
            cfg['working_dir'] = os.path.join(self.cfg.get('working_dir'), conv)
            if not os.path.exists(cfg['working_dir']) and os.path.exists(self.cfg['working_dir']):
                try:
                    os.mkdir(cfg['working_dir'])
                except OSError as ex:
                    raise PublishingStateException("Unable to create working directory for "+conv+
                                                   " convention", cause=ex)

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

    def authenticate(self, env) -> PubAgent:
        """
        determine and return the identity of the client.  This is done by mapping a Bearer key to 
        an identity in the `authorized` configuration parameter.
        :param Mapping env:  the WSGI request environment 
        :rtype: PubAgent
        """
        auth = env.get('HTTP_AUTHORIZATION', "")
        authkey = None
        user = None
        auth = auth.split()
        if len(auth) > 1:
            if auth[0] == "Bearer":
                authkey = auth[1]
        
        client = self._id_map.get(authkey)
        if not client:
            return None

        user = env.get('HTTP_X_OAR_USER')
        patype = PubAgent.USER
        if not user:
            patype = PubAgent.AUTO
            user = client.get('user', 'anonymous')

        return PubAgent(client.get('group'), client.get('type', patype), user)
        

    def handle_request(self, env, start_resp):
        path = env.get('PATH_INFO', '/').strip('/')
        parts = path.split('/', 1)

        # determine who is making the request
        who = self.authenticate(env)

        subapp = None
        if parts[0] in self.subapps:
            # parts[0] is a convention (e.g. "pdp0")
            path = '/'
            if len(parts) > 1:
                path += parts[1]
            subapp = self.subapps.get(parts[0])

        if not subapp:
            subapp = self.subapps.get('')

        return subapp.handle_path_request(env, start_resp, path, who)

    def __call__(self, env, start_resp):
        return self.handle_request(env, start_resp)

app = PDPApp

