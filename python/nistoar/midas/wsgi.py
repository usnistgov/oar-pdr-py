"""
A module that assembles all of the different endpoints of the MIDAS API into one WSGI App.

The :ref:class:`MIDASApp` class is an WSGI application class that provides the suite of MIDAS services.  
Which services are actually made available depends on the configuration provided at construction time.  
See the :py:mod:`nistoar.midas.dbio.wsgi` module documentation for a description of the 
configuraiton schema.  In particular, :ref:class:`MIDASApp` application can provide, when so configured,
the following endpoints:

  * ``/dmp/mdm1/`` -- the Data Management Plan (DMP) Authoring API, for creating and editing DMPs
    (according to the "mdm1" convention)
  * ``/dap/mds3/`` -- the Digital Assets Publication (DAP) Authoring API, for drafting data and 
    software publications (according to the mds3 convention) to be submitted to the Public Data 
    Repository (PDR)
  * ``/groups/`` -- the API for creating and managing access permission groups for collaborative 
    authoring.

These endpoint send and receive data stored in the backend database through the common 
:py:mod:` DBIO layer <nistoar.midas.dbio>`.  

The app configuration determines which endpoints that are actually available.  The authoring API 
endpoints follow a common pattern:

   /_service_/_convention_/

where _service_ is MIDAS service name (like "dmp" or "dap") and _convention_ is name that represents 
the version of the service interface.  Usually, there is one convention available called "def", which 
serves as a synonym for the convention that is considered the default convention.  Through the 
configuration, it is possible, then, to create additional authoring services or conventions of services.  

The configuration that is expected by ``MIDASApp`` is a (JSON) object with the following properties:

``base_endpoint``
    (str) _optional_.  the URL resource path where the base of the base of the service suite is accessed.
    The default value is "/midas/".  An empty string is equivalent to "/", the root path.  
``strict``
    (bool) _optional_.  if False and if a service type (see below) given in this configuration is not 
    recognized, a ``ConfiguraitonException`` will be raised.
``about``
    (object) _optional_.  an object of data describing this suite of services that should be returned 
    when the base path is requested.  (See the :py:class:`~nistoar.midas.dbio.wsgi.wsgiapp.About` class 
    for an example.)  There are no requirements on the properties in this object except that it should 
    _not_ include "services" or "versions".  
``services``
    (object) _required_.  an object in which each property is a service name (as referred to above in 
    the API endpoint pattern--e.g., "dmp" or "dap"), and its value is the configuration for that service.
``dbio``
    (object) _recommended_.  an object that provides configuration for the DBIO client; typically, this 
                          includes a ``factory`` property whose string value identifies the type of 
                          backend storage to use ("mongo", "fsbased", or "inmem").  The other properties
                          are the parameters that are specific to the backend storage.
``jwt_auth``
    (object) an object that provides configuration related to JWT-based authentication to the service
    endpoints.  If set, a JWT token (presented via the Authorization HTTP header) will be used to 
    determine the client user identity and attributes; if a token is not included with requests, the 
    user will be set to "anonymous".  If this configuration is not set, all client users will be 
    considered anonymous.  

The supported subproperties for ``jwt_auth`` are as follows:

``key``
    (str) _required_.  The secret key shared with the token generator (usually a separate service) used to 
    encrypt the token.

``algorithm``
    (str) _optional_.  The name of the encryption algorithm to encrypt the token.  Currently, only one value 
    is support (the default): "HS256".

``require_expiration``
    (bool) _optional_.  If True (default), any JWT token that does not include an expiration time will be 
    rejected, and the client user will be set to anonymous.

Most of the properties in a service configuration object will be treated as default configuration 
parameters for configuring a particular version, or _convention_, of the service.  Convention-level 
configuration will be merged with these properties (overriding the defaults) to produce the configuration 
that is passed to the service SubApp that handles the service.  The properties supported are 
service-specific.  In addition to the service-specific properties, three special-purpose properties are 
supported:

``about``
    (object) _optional_.  an object of data describing the service catagory that should be returned 
    when the service name endpoint is requested.  (See the 
    :py:class:`~nistoar.midas.dbio.wsgi.wsgiapp.About` class for an example.)  There are no requirements 
    on the properties in this object except that it should _not_ include "services" or "versions".  
``conventions``
    (object) _optional_.  an object in which each property is a convention name supported for the service
    (as referred to above in the API endpoint pattern--e.g., "mdm1" for the DMP service), and its value is 
    the configuration for that convention (i.e. version) of the service.  Any properties given here 
    override properties of the same name given at the service level, as discussed above.  The properties
    can be service- or convention-specific, apart from the required property, ``type`` (defined below).  
``default_convention``
    (str) _optional_.  the name of the convention (one of the names specified as a property of the 
    ``conventions`` field described above) that should be considered the default convention.  If a client
    requests the special convention name "def", the request will be routed to the version of the service 
    with that name.  

There are a few common properties that can appear in either the service or convention level (or both, where 
the convention level takes precedence): 

``type``
    (str) _optional_.  a name that serves as an alias for the Python ``SubApp`` class that implements 
    the service convention.  The default value is the service and convention names combined as 
    "_service_/_convention_".  
``project_name``
    (str) _optional_.  a name indicating the type of DBIO project the service manages.  This name 
    corresponds to a DBIO project collection name.  If provided, it will override the collection used 
    by default ``SubApp`` specified by the ``type`` parameter.  
``clients``
    (object) _required_.  the configuration parameters restrict the scope of the clients that connect to 
    the web service.  This is passed to the :py:class:`~nistoar.midas.dbio.project.ProjectService` 
    configured for the convention.
``dbio``
    (object) _recommended_.  the configuration parameters for the DBIO client which are specific to the 
    project service type (see below).  In particular, this includes the authorization configurations;
    see the :py:mod:`dbio module documentation <nistoar.midas.dbio>` for this schema. This is passed to 
    the :py:class:`~nistoar.midas.dbio.project.ProjectService` configured for the convention.

In addition to providing the :ref:class:`MIDASApp` class, this module provides a mechanism for plugging 
in addition _project_ services, particularly new conventions of services.  The class constructor takes 
an optional dictionary parameter that provides in its values the 
:ref:class:`~nistoar.pdr.publish.service.wsgi.SubApp` class that implements a particular DBIO project
service.  The keys labels that correspond to the ``type`` parameter in the 
:py:mod:`configuration <nistoar.midas.dbio.wsgi>` and which, by default, have the form 
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

import jwt

from . import system
from .dbio.base import DBClientFactory
from .dbio.wsgi import project as prj, SubApp, Handler, DBIOHandler
from .dap.service import mdsx, mds3
from .dbio.inmem import InMemoryDBClientFactory
from .dbio.fsbased import FSBasedDBClientFactory
from .dbio.mongo import MongoDBClientFactory
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
                               "dap")
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

        return factory(dbio_client_factory, log, appconfig, appconfig.get('project_name'))

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
                        defdesc = None
                        if appcfg.get("default_convention") == conv:
                            defdesc = out[path]
                        elif not appcfg.get("default_convention") and len(appcfg["conventions"]) == 1:
                            defdesc = out[path]
                        if defdesc:
                            out["%s/def" % appname] = defdesc
                            aboutdesc = deepcopy(cnvcfg.get("about", {}))
                            if aboutdesc.get('href') and isinstance(aboutdesc['href'], str):
                                aboutdesc['href'] = re.sub(r'/%s/?$' % conv, '/def', aboutdesc['href'])
                            aboutapp.add_version("def", aboutdesc)

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
#    "dmp/mdm1":  mdm1.DMPApp,
    "dmp/mdm1":  prj.MIDASProjectApp.factory_for("dmp"),
    "dap/mdsx":  mdsx.DAPApp,
    "dap/mds3":  mds3.DAPApp
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
                                :py:mod:`wsgi module documentation <nistoar.midas.dbio.wsgi>` 
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

        if not self.cfg.get('jwt_auth'):
            log.warning("JWT Authentication is not configured")
        else:
            if not isinstance(self.cfg['jwt_auth'], Mapping):
                raise ConfigurationException("Config param, jwt_auth, not a dictionary: "+
                                             str(self.cfg['jwt_auth']))
            if not self.cfg['jwt_auth'].get('require_expiration', True):
                log.warning("JWT Authentication: token expiration is not required")

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
        # get client id, if present
        client_id = env.get('HTTP_OAR_CLIENT_ID','(unknown)')
        agents = self.cfg.get('client_agents', {}).get(client_id, [client_id])
        allowed = self.cfg.get('allowed_clients')
        if allowed is not None and client_id not in allowed:
            log.warning("Client %s is not recongized among %s", client_id, str(allowed))
            return PubAgent("invalid", PubAgent.UNKN, "anonymous", agents)

        # ensure an authenticated identity
        auth = env.get('HTTP_AUTHORIZATION', "x").split()
        jwtcfg = self.cfg.get('jwt_auth')
        if jwtcfg and auth[0] == "Bearer":
            try:
                userinfo = jwt.decode(auth[1], jwtcfg.get("key", ""),
                                      algorithms=[jwtcfg.get("algorithm", "HS256")])
            except jwt.InvalidTokenError as ex:
                log.warning("Invalid token can not be decoded: %s", str(ex))
                return PubAgent("invalid", PubAgent.UNKN, "anonymous", agents)

            # make sure the token has an expiration date
            if jwtcfg.get('require_expiration', True) and not userinfo.get('exp'):
                # Note expiration was checked implicitly by the above jwt.decode() call
                log.warning("Rejecting non-expiring token for user %s", userinfo.get('sub', "(unknown)"))
                return PubAgent("invalid", PubAgent.UNKN, "anonymous", agents)

            return self._agent_from_claimset(userinfo, agents)

        # anonymous user
        if jwtcfg and auth[0] != "Bearer":
            log.warning("Client %s did not provide an authentication token as expected", client_id)
        return PubAgent("public", PubAgent.UNKN, "anonymous", agents)

    def _agent_from_claimset(self, userinfo: dict, agents=None):
        subj = userinfo.get('sub')
        email = userinfo.get('userEmail','')
        group = "public"
        if not subj:
            log.warning("User token is missing subject identifier; defaulting to anonymous")
            subj = "anonymous"
        elif subj.endswith("@nist.gov"):
            group = "nist"
            subj = subj[:-1*len("@nist.gov")]
        elif email.endswith("@nist.gov"):
            group = "nist"
        return PubAgent(group, PubAgent.USER, subj, agents)

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

app = MIDASApp


