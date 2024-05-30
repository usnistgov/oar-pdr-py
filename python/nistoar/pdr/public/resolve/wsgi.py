"""
The suite of web service endpoints that can resolve an identifier.  
"""
import os, sys, logging, json, re
from wsgiref.headers import Headers
from collections import OrderedDict, Mapping
from copy import deepcopy

from ... import ARK_NAAN
from . import system
from .handlers import ResolverReady, PDRIDHandler, AIPHandler
from nistoar.web.rest import ServiceApp

log = logging.getLogger(system.system_abbrev)   \
             .getChild(system.subsystem_abbrev) \
             .getChild('wsgi')

DEF_BASE_PATH = "/"
ark_naan = ARK_NAAN

class ResolverApp(ServiceApp):
    """
    a WSGI-compliant service app designed to resolve PDR-recognized identifiers

    This handles two main base endpoints:
    /id/
    /aip/
    Only GET methods are supported.
    """

    def __init__(self, config):
        cfg = deepcopy(config)
        cfg.setdefault('id', {})
        cfg.setdefault('aip', {})
        cfg.setdefault('ready', {})
        if 'locations' in cfg:
            cfg['id'].setdefault('locations', cfg['locations'])
            cfg['aip'].setdefault('locations', cfg['locations'])
            cfg['ready'].setdefault('locations', cfg['locations'])
        if 'APIs' in cfg:
            cfg['id'].setdefault('APIs', cfg['APIs'])
            cfg['aip'].setdefault('APIs', cfg['APIs'])
            cfg['ready'].setdefault('APIs', cfg['APIs'])

        super(ResolverApp, self).__init__("Resolver", log, cfg)
        
        level = self.cfg.get('loglevel')
        if level:
            self.log.setLevel(level)

        self.handlers = {
            "id":  (PDRIDHandler,  self.cfg.get('id', {})),
            "aip": (AIPHandler,    self.cfg.get('aip', {})),
            "":    (ResolverReady, self.cfg.get('ready', {}))
        }

    def create_handler(self, env, start_resp, path, who):
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

        return handler[0](path, env, start_resp, config=handler[1], log=log, app=self)


app = ResolverApp



    
        
