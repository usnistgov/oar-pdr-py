"""
Framework classes for creating REST web interfaces via WSGI 

The small framework provided by this module is provides foundation classes for RESTful web APIs
that wrap around NIST-OAR services.  The framework allows for a strict approach to RESTful 
service design via the following features:
  *  a resource-based model for handling requests.  The :py:class:`~nistoar.web.rest.base.Handler` 
     class is implemented to handle a single resource (given by a path).  A Handler can either 
     handle all of its sub-resources itself or pass requests for them to other Handlers.  
     Routing is explicitly in the hands of the service implementation.
  *  the ability to compose multiple resources into a single WSGI application via the 
     :py:class:`~nistoar.web.rest.base.ServiceApp` class.
  *  full but simple control over the returned HTTP status for proper error handling
  *  support for client-specified return formats either via query-parameters or the ``Accept``
     HTTP request header.
  *  extra convenience support for JSON-formatted responses

The NIST-OAR approach to web services is to provide a thin web service layer over a business 
service class (which features a class name like [X]Service).  The business service class captures 
all of the business logic of the service to be offered; however, it is only accessed via its 
Python programming API and contains no knowledge of the web layer.  The web layer is provided via 
a :py:class:`~nistoar.web.rest.base.ServiceApp` subclass that wraps around business class.  When responding 
to a web request, the :py:class:`~nistoar.web.rest.base.ServiceApp` creates a 
:py:class:`~nistoar.web.rest.base.Handler` subclass based on the requested resource path; that handler
then provides access to different functions of the service.  

A :py:class:`~nistoar.web.rest.base.ServiceApp` instance is a compliant WSGI application by itself.  
However, it is typical to wrap it an additional layer, :py:class:`~nistoar.web.rest.base.WSGIApp`, to 
enable some additional features:
   * it can combine several ``ServiceApp`` instances to be combined into a single WSGI application 
     (via :py:class:`~nistoar.web.rest.base.WSGIAppSuite`)
   * a base URL path can be defined to be prepended to paths handled by a ``ServiceApp`` or 
     ``ServiceApp``s.  
   * it provide user authentication for all wrapped services

"""

from .base import *
