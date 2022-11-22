"""
A module that provides the top-level WSGI App providing access to the MIDAS services via the DBIO layer.

The :ref:class:`MIDASApp` class is an WSGI application class that provides the suite of MIDAS services.  
Which services are actually made available depends on the configuration provided at construction time.  
See the :py:module:`nistoar.midas.dbio.wsgi` module documentation for a description of the 
configuraiton schema.  

In addition to providing the :ref:class:`MIDASApp` class, this module provides a mechanism for plugging 
addition _project_ services, particularly new conventions of services.  The class constructor takes 
an optional dictionary parameter that provides in its values the 
:ref:class:`~nistoar.pdr.publish.service.wsgi.SubApp` class that implements a particular DBIO project
service.  The keys labels that correspond to the ``type`` parameter in the 
:py:module:`configuration <nistoar.midas.dbio.wsgi>` and which, by default, have the form 
_service_/_convention_ (e.g. ``dmp/mdm1``).  If this dictionary is not provided to the constructur, an 
default defined in this module, ``_MIDASSubApps`` is used.  Thus, the normal way to add a new service 
implementation to the suite is to add it to the internal ``_MIDASSubApps`` dictionary.  

This module also provides two other classes that are used internally to initialize a :ref:class:`MIDASApp` 
instance: :ref:class:`SubAppFactory` and :ref:class:`About`.  :ref:class:`SubAppFactory` is used within 
:ref:class:`MIDASApp` to instantiate all of the ``SubApp`` classes given in the above mentioned 
dictionary; however, it also provides functions to instantiate a ``SubApp`` individually and to extract
the configuration parameters needed to do that instantiation.  :ref:class:`About` is a ``SubApp`` that 
returns through the web interface information about the MIDAS services.  :ref:class:`SubAppFactory`
injects instances of this class into :ref:class:`MIDASApp` to respond to GET requests on the MIDAS 
service's parent resources.  
"""
import os, sys, logging, json, re
from logging import Logger
from wsgiref.headers import Headers
from collections import OrderedDict
from collections.abc import Mapping, MutableMapping, Callable
from copy import deepcopy

from ... import system
from . import project as prj, SubApp, Handler, DBIOHandler
from ..base import DBClientFactory
from ..inmem import InMemoryDBClientFactory
from ..fsbased import FSBasedDBClientFactory
from ..mongo import MongoDBClientFactory
from nistoar.pdr.publish.prov import PubAgent
from nistoar.base.config import ConfigurationException, merge_config

log = logging.getLogger(system.system_abbrev)   \
             .getChild(system.subsystem_abbrev) \
             .getChild('wsgi')

DEF_BASE_PATH = "/midas/"
DEF_DBIO_CLIENT_FACTORY_CLASS = InMemoryDBClientFactory
DEF_DBIO_CLIENT_FACTORY_NAME  = "inmem"

class SubAppFactory:
    """
    a factory for creating MIDAS WSGI SubApps based on a configuration.  Individual SubApps can be 
    instantiated on demand or all at once (for :py:class:`MIDASApp`).  
    """

    def __init__(self, config: Mapping, subapps: Mapping):
        """
        :param Mapping subapps:  a mapping of type names (referred to in the configuration) to 
                                 a SubApp class (factory function that produces a SubApp) that 
                                 takes four arguments: an application name, a ``Logger`` instance, 
                                 a :py:class:`~nistoar.midas.dbio.DBIOClientFactory` instance, 
                                 and the complete convention-specific configuration appropriate 
                                 for the SubApp type referred to in by the type name.  (See also 
                                 :py:method:`register_subapp`.)
        :param Mapping config:   the configuration for the full collection of MIDAS sub-apps that 
                                 be included in the output.  
        """
        self.cfg = config
        if "services" not in self.cfg:
            raise ConfigurationException("Missing required config parameter: services")
        if not isinstance(self.cfg["services"], Mapping):
            raise ConfigurationException("Config parameter type error: services: not a dictionary: "+
                                         type(self.cfg["services"]))
        self.subapps = subapps

    def register_subapp(self, typename: str, factory: Callable):
        """
        Make a SubApp class available through this factory class via a given type name
        :param str typename:     the type name by which the factory function can accessed
        :param str cls_or_fact:  a SubApp class or other factory callable that produces a SubApp
                                 that accepts four arguments:  an application name, a ``Logger`` instance, 
                                 a :py:class:`~nistoar.midas.dbio.DBIOClientFactory` instance, 
                                 and the complete convention-specific configuration appropriate 
                                 for the SubApp type referred to in by the type name.  (See also 
                                 :py:method:`register_subapp`.
        """
        self.subapps[typename] = factory


    def config_for_convention(self, appname: str, convention: str, typename: str = None) -> MutableMapping:
        """
        Find the convention-specific subapp configuration, merge in its app-level defaults, and 
        return it as a complete convention-specific configuration, or None if the convention is not 
        configured.  
        :param str appname:    the name of the MIDAS app to be configured.  (Examples are "dmp",
                               "pdr")
        :param str convention: the name of the API convention that is desired in the configuration.  A 
                               special name "def" refers to the convention that is configured as the 
                               default for the app; an empty string and None behaves in the same way.
        :param str typename:   a app type name to assign to this configuration, overriding the name 
                               that might be in configuration by default.  This name should be used to
                               select the SubApp factory function in the set of SubApp provided at 
                               construction time.
        """
        svccfg = self.cfg["services"]
        if appname not in svccfg:
            return None
        if not convention:
            convention = "def"

        appcfg = deepcopy(svccfg[appname])
        if "conventions" in appcfg:
            cnvcfg = deepcopy(appcfg.get("conventions", {}).get(convention))
            if not cnvcfg and convention == "def" and appcfg.get("default_convention"):
                convention = appcfg["default_convention"]
                cnvcfg = appcfg.get("conventions", {}).get(convention)

            del appcfg["conventions"]
            if "about" in appcfg:
                del appcfg["about"]
            if cnvcfg:
                appcfg = merge_config(cnvcfg, appcfg)

        if typename:
            appcfg['type'] = typename
        elif not appcfg.get('type'):
            appcfg['type'] = "%s/%s" % (appname, convention)
        appcfg.setdefault("project_name", appname)

        return appcfg

    def create_subapp(self, log: Logger, dbio_client_factory: DBClientFactory,
                      appconfig: Mapping, typename: str=None) -> SubApp:
        """
        instantiate a SubApp as specified by the given configuration
        :param Logger        log:  the Logger instance to inject into the SubApp
        :param Mapping appconfig:  the convention-specific SubApp configuration to initialize the 
                                   SubApp with
        :param str      typename:  the name to use to look-up the SubApp's factory function.  If not 
                                   provided, the value of the configuration's ``type`` property will be 
                                   used instead.
        :raises ConfigurationException:  if the type name is not provided and is not otherwise set in 
                                   the configuration
        :raises KeyError:  if the type name is not recognized as registered SubApp
        """
        if not typename:
            typename = appconfig.get('type')
        if typename is None:
            raise ConfigurationException("Missing configuration parameter: type")
        factory = self.subapps[typename]

        return factory(appconfig.get('project_name', typename), log, dbio_client_factory, appconfig)

    def create_suite(self, log: Logger, dbio_client_factory: DBClientFactory) -> MutableMapping:
        """
        instantiate all of the MIDAS subapps found configured in the configuration provided at 
        construction time, returning them as a map of web resource paths to SubApp instances.  
        The path for a SubApp will be of the form "[appname]/[convention]".  Also included will 
        be About SubApps that provide information and proof-of-life for parent paths.  
        """
        out = OrderedDict()
        about = About(log, self.cfg.get("about", {}))

        for appname, appcfg in self.cfg['services'].items():
            if not isinstance(appcfg, Mapping):
                # wrong type; skip
                continue

            aboutapp = About(log, appcfg.get('about', {}))
            
            if "conventions" in appcfg:
                if not isinstance(appcfg["conventions"], Mapping):
                    raise ConfigurationException("Parameter 'conventions' not a dictionary: "+
                                                 type(appcfg["conventions"]))
                
                for conv in appcfg.get("conventions", {}):
                    cnvcfg = self.config_for_convention(appname, conv)
                    if isinstance(cnvcfg, Mapping):

                        path = "%s/%s" % (appname, conv)
                        try:
                            out[path] = self.create_subapp(log, dbio_client_factory, cnvcfg)
                        except KeyError as ex:
                            if self.cfg.get("strict", False):
                                raise ConfigurationException("MIDAS app type not recognized: "+str(ex))
                            else:
                                log.warning("Skipping unrecognized MIDAS app type: "+str(ex))
                                continue
                        except ConfigurationException as ex:
                            ex.message = "While creating subapp for %s: %s" % (path, str(ex))
                            raise

                        # Add an entry into the About SubApp
                        aboutapp.add_version(conv, cnvcfg.get("about", {}))

                        # if so configured, set as default
                        if appcfg.get("default_convention") == conv:
                            out["%s/def" % appname] = out[path]
                        elif not appcfg.get("default_convention") and len(appcfg["conventions"]) == 1:
                            out["%s/def" % appname] = out[path]

            else:
                # No conventions configured for this app name; try to create an app from the defaults
                cnvcfg = self.config_for_convention(appname, "def")
                path = "%s/def" % appname
                try:
                    out[path] = self.create_subapp(log, dbio_client_factory, cnvcfg)
                    aboutapp.add_version("def", cnvcfg.get("about", {}))
                except KeyError as ex:
                    if self.cfg.get("strict", False):
                        raise ConfigurationException("MIDAS app type not recognized: "+str(ex))
                    else:
                        log.warning("Skipping unrecognized MIDAS app type: "+str(ex))
                        continue
                except ConfigurationException as ex:
                    raise ConfigurationException("While creating subapp for %s: %s" % (path, str(ex)),
                                                 cause=ex)

            out[appname] = aboutapp
            about.add_service(appname, appcfg.get('about', {}))

        out[""] = about
        return out


class About(SubApp):
    """
    a SubApp intended to provide information about the endpoints available as part of the overall 
    MIDAS WSGI App.  

    This SubApp only supports a GET response, to which it responds with a JSON document containing 
    data provided to this SubApp at construction time and subsequently added to via ``add_*`` methods.  
    This document might look something like this:

    .. code-block::
       :caption:  An example About response document describing the MIDAS API suite

       {
           "message":  "Services are available",
           "title": "MIDAS Authoring Suite",
           "describedBy": "https://midas3.nist.gov/midas/apidocs"
           "services": {
               "dmp": {
                   "title": "Data Management Plan Authoring API",
                   "describedBy": "https://midas3.nist.gov/midas/apidocs/dmp",
                   "href": "http://midas3.nist.gov/midas/dmp"
               },
               "dap": {
                   "title": "Digital Asset Publication Authoring API",
                   "describedBy": "https://midas3.nist.gov/midas/apidocs/dap",
                   "href": "http://midas3.nist.gov/midas/dmp/mdm1"
               }
           }
       }

    """

    def __init__(self, log, base_data: Mapping=None):
        """
        initialize the SubApp.  Some default properties may be added to base_data.
        :param Mapping base_data:  the initial data the should appear in the GET response JSON object
        """
        super(About, self).__init__("about", log, {})
        if not base_data:
            base_data = OrderedDict()
        self.data = self._init_data(base_data)

    def _init_data(self, data: Mapping):
        data = deepcopy(data)
        if "message" not in data:
            data["message"] = "Service is available"
        return data

    def add_component(self, compcat, compname, data):
        """
        append data for a named component of the about information to return.  Within GET responses, 
        components are listed by its category by its name (e.g. "services") which is an object; each 
        key in that object is the component's name.  This method provides the implementation for 
        :py:method:`add_service` and :py:method:`add_version`.  

        :param str compcat:  the component category name to add the data to (e.g. "services"); if a 
                             property does not exist in the base data with this name, it will be added.
        :param str compname: the name of the component; the data will be added within the ``compcls``
                             object property as the value of a subproperty with this name.  If this
                             subproperty already exists, it will be overridden.
        :param Mapping data: the data to add for the component
        :raises ValueError:  if the ``compcls`` property already exists in the base data but is not an 
                             object.
        """
        if compcat not in self.data:
            self.data[compcat] = OrderedDict()
        if not isinstance(self.data[compcat], MutableMapping):
            raise ValueError("Category property is not an object: %s: %s" % (comcat, type(self.data[comcat])))

        self.data[compcat][compname] = data

    def add_service(self, name, data):
        """
        add a named description to the ``services`` property
        """
        self.add_component("services", name, data)

    def add_version(self, name, data):
        """
        add a named description to the ``versions`` property
        """
        self.add_component("versions", name, data)

    class _Handler(DBIOHandler):

        def __init__(self, parentapp, path: str, wsgienv: Mapping, start_resp: Callable, who=None,
                     config: Mapping={}, log: Logger=None):
            Handler.__init__(self, path, wsgienv, start_resp, who, config, log)
            self.app = parentapp

        def handle(self):
            # no sub resources are supported via this SubApp
            if self._path.strip('/'):
                return self.send_error(404, "Not found")

            return super().handle()

        def do_GET(self, path, ashead=False):
            path = path.strip('/')
            if path:
                # only the root path is supported
                return self.send_error(404, "Not found")

            return self.send_json(self.app.data, ashead=ashead)
    
    def create_handler(self, env: dict, start_resp: Callable, path: str, who: PubAgent) -> Handler:
        """
        return a handler instance to handle a particular request to a path
        :param Mapping env:  the WSGI environment containing the request
        :param Callable start_resp:  the start_resp function to use initiate the response
        :param str path:     the path to the resource being requested.  This is usually 
                             relative to a parent path that this SubApp is configured to 
                             handle.  
        """
        return self._Handler(self, path, env, start_resp, who, log=self.log)
    


_MIDASSubApps = {
    "dmp/mdm1":  prj.MIDASProjectApp
}

class MIDASApp:
    """
    A complete WSGI App implementing the suite of MIDAS APIs.  The MIDAS applications that are included 
    are driven by the configuration.  The Groups application (used to define access groups) is always 
    included. 
    """

    DB_FACTORY_CLASSES = {
        "inmem":    InMemoryDBClientFactory,
        "fsbased":  FSBasedDBClientFactory,
        "mongo":    MongoDBClientFactory
    }

    def __init__(self, config: Mapping, dbio_client_factory: DBClientFactory=None,
                 base_ep: str=None, subapp_factory_funcs: Mapping=None):
        """
        initial the App
        :param Mapping config:  the collected configuration for the App (see the 
                                :py:module:`wsgi module documentation <nistoar.midas.dbio.wsgi>` 
                                for the schema
        :param DBClientFactory dbio_client_factory:  the DBIO client factory to use to create
                                clients used to access the DBIO storage backend.  If not specified,
                                the in-memory client factory will be used.  
        :param str base_ep:     the resource path to assume as the base of all services provided by
                                this App.  If not provided, a value set in the configuration is 
                                used (which itself defaults to "/midas/").
        :param Mapping subapp_factory_funcs: a map of project service names to ``SubApp`` classes 
                                that implement the MIDAS Project services that can be included in 
                                this App.  The service name (which gets matched to the ``type``)
                                configuration parameter, normally has the form "_service_/_convention_".
                                If not provided (typical), an internal map is used.
        """
        self.cfg = config
        if not self.cfg.get("services"):
            raise ConfigurationException("No MIDAS apps configured (missing 'services' parameter)")

        if base_ep is None:
            base_ep = self.cfg.get('base_endpoint', DEF_BASE_PATH)
        self.base_ep = base_ep.strip('/').split('/')

        # Load MIDAS project servies based on what's in the configuration (i.e. if only the dmp app
        # is configured, only that app will be available; others will return 404)
        if not subapp_factory_funcs:
            subapp_factory_funcs = _MIDASSubApps

        if not dbio_client_factory:
            dbclsnm = self.cfg.get('dbio', {}).get('factory')
            if not dbclsnm:
                dbclsnm = DEF_DBIO_CLIENT_FACTORY_NAME
            dbcls = self.DB_FACTORY_CLASSES.get(dbclsnm)
            if dbcls:
                dbcls = DEF_DBIO_CLIENT_FACTORY_CLASS
            dbio_client_factory = dbcls(self.cfg.get('dbio', {}))

        factory = SubAppFactory(self.cfg, subapp_factory_funcs)
        self.subapps = factory.create_suite(log, dbio_client_factory)

        # Add the groups endpoint
        # TODO

    def authenticate(self, env) -> PubAgent:
        """
        determine and return the identity of the client.  This checks both user credentials and, if 
        configured, the client application key.  If client keys are configured and the client has not 
        provided a recognized key, an exception is thrown.  Otherwise, if the request has not presented 
        authenticable credentials, the returned PubAgent will represent an anoymous user.
        
        :param Mapping env:  the WSGI request environment 
        :return:  a representation of the requesting user
                  :rtype: PubAgent
        """
        # TODO: support JWT cookie for authentication

        # TODO: support optional client 

        # anonymous user
        return PubAgent("public", PubAgent.UNKN, "anonymous")

    def handle_request(self, env, start_resp):
        path = env.get('PATH_INFO', '/').strip('/').split('/')
        if path == ['']:
            path = []

        # determine who is making the request
        who = self.authenticate(env)

        if self.base_ep:
            if len(path) < len(self.base_ep) or path[:len(self.base_ep)] != self.base_ep:
                # path does not match the required base endpoint path
                return Handler(path, env, start_resp).send_error(404, "Not Found")

            # lop off the base endpoint path
            path = path[len(self.base_ep):] 

        # Determine which subapp should handle this request
        subapp = None
        if len(path) > 1:
            sapath = '/'.join(path[:2])
            subapp = self.subapps.get(sapath)
            if subapp:
                path = path[2:]
        if not subapp and len(path) > 0:
            subapp = self.subapps.get(path[0])
            if subapp:
                path = path[1:]
        if not subapp:
            # this will handle any other non-existing paths
            subapp = self.subapps.get('')

        return subapp.handle_path_request(env, start_resp, "/".join(path), who)

    def __call__(self, env, start_resp):
        return self.handle_request(env, start_resp)



