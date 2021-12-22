"""
The suite of web service endpoints that can resolve an identifier.  
"""
import os, sys, logging, json, re
from wsgiref.headers import Headers
from collections import OrderedDict, Mapping
from copy import deepcopy

from ... import ARK_NAAN
from . import system
from .handlers import Ready, PDRIDHandler, AIPHandler

log = logging.getLogger(system.system_abbrev)   \
             .getChild(system.subsystem_abbrev) \
             .getChild('wsgi')

DEF_BASE_PATH = "/"
ark_naan = ARK_NAAN

class ResolverApp(object):
    """
    a WSGI-compliant service app designed to resolve PDR-recognized identifiers

    This handles two main base endpoints:
    /id/
    /aip/
    Only GET methods are supported.
    """

    def __init__(self, config):
        level = config.get('loglevel')
        if level:
            log.setLevel(level)
        cfg = deepcopy(config)
        cfg.setdefault('id', {})
        cfg.setdefault('aip', {})
        cfg.setdefault('ready', {})
        if 'locations' in cfg:
            cfg['id'].setdefault('locations', cfg['locations'])
            cfg['aip'].setdefault('locations', cfg['locations'])
            cfg['ready'].setdefault('locations', cfg['locations'])

        self.handlers = {
            "id":  (PDRIDHandler, cfg.get('id', {})),
            "aip": (AIPHandler,   cfg.get('aip', {})),
            "":    (Ready,        cfg.get('ready', {}))
        }

    def handle_request(self, env, start_resp):
        path = env.get('PATH_INFO', '/').strip('/')
        parts = path.split('/', 1)

        handler = None
        if parts[0] in self.handlers:
            # parts[0] is one of 'id', 'aip', or ''; see self.handlers above
            path = '/'
            if len(parts) > 1:
                path += parts[1]
            handler = self.handlers.get(parts[0])

        if not handler:
            handler = self.handlers.get('')

        handler = handler[0](path, env, start_resp, handler[1], log)
        return handler.handle()

    def __call__(self, env, start_resp):
        return self.handle_request(env, start_resp)

app = ResolverApp



    
        
