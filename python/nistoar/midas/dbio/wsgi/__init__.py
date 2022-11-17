"""
The WSGI interface to the DBIO layer.

The :py:class:`~nistoar.midas.dbio.wsgi.wsgiapp.MIDASApp` WSGI application provides DBIO collections 
and data and serves as the API for the suite of available MIDAS services.  In particular, when so 
configured, this application can provide the following endpoints:

  * ``/dmp/mdm1/`` -- the Data Management Plan (DMP) Authoring API, for creating and editing DMPs
    (according to the "mdm1" convention)
  * ``/dap/mds3/`` -- the Digital Assets Publication (DAP) Authoring API, for drafting data and 
    software publications (according to the mds3 convention) to be submitted to the Public Data 
    Repository (PDR)
  * ``/groups/`` -- the API for creating and managing access permission groups for collaborative 
    authoring.

These endpoint send and receive data stored in the backend database through the common 
:py:module:` DBIO layer <nistoar.midas.dbio>`.  

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

Most of the properties in a service configuration object will be treated as default configuration 
parameters for configuring a particular version, or _convention_, of the service.  Convention-level 
configuration will be merged with these properties (overriding the defaults) to produce the configuration 
that is passed to the service SubApp that handles the service.  The properties supported are 
service-specific.  In addition to the service-specific properties, two special-purpose properties are 
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

There are two common properties that can appear in either the service or convention level (or both, where 
the convention level takes precedence): ``project_name`` and ``type``.  These optional properties are 
defined as follows:

``project_name``
    (str) _optional_.  a name indicating the type of DBIO project the service manages.  This name 
    corresponds to a DBIO project collection name.  It defaults to the value of the name associated with 
    the configuration under the ``services`` property (described above).
``type``
    (str) _optional_.  a name that serves as an alias for the Python ``SubApp`` class that implements 
    the service convention.  The default value is the service and convention names combined as 
    "_service_/_convention_".  

"""
from .base import SubApp, Handler, DBIOHandler
from .wsgiapp import MIDASApp

app = MIDASApp
