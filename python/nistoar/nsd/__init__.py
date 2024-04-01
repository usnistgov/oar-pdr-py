"""
Support for the NIST Staff directory (NSD) service
"""
from nistoar.base import OARException

class NSDException(OARException):
    """
    An general base class for exceptions that occur while using or providing an NSD service
    """
    pass

class NSDServerError(NSDException):
    """
    an exception indicating an error occurred on the server-side while 
    trying to access the distribution service.  

    This exception includes three extra public properties, `status`, `reason`, 
    and `resource` which capture the HTTP response status code, the associated 
    HTTP response message, and (optionally) a name for the record being 
    submitted to it.  
    """

    def __init__(self, resource=None, http_code=None, http_reason=None, 
                 message=None, cause=None):
        super(NSDServerError, self).__init__("distribution", resource,
                                             http_code, http_reason, message, cause)
                                                 
class NSDClientError(NSDException):
    """
    an exception indicating an error occurred on the client-side while 
    trying to access the distribution service.  

    This exception includes three extra public properties, `status`, `reason`, 
    and `resource` which capture the HTTP response status code, the associated 
    HTTP response message, and (optionally) a name for the record being 
    submitted to it.  
    """

    def __init__(self, resource, http_code, http_reason, message=None,
                 cause=None):
        if not message:
            message = "client-side distribution error occurred"
            if resource:
                message += " while processing " + resource
            message += ": {0} {1}".format(http_code, http_reason)
          
        super(NSDClientError, self).__init__("distribution", resource,
                                             http_code, http_reason, message, cause)
                                                 

class NSDResourceNotFound(NSDClientError):
    """
    An error indicating that a requested resource is not available via the
    distribution service.
    """
    def __init__(self, resource, http_reason=None, message=None,
                 cause=None):
        if not message:
            message = "Requested distribution resource not found"
            if resource:
                message += ": "+resource
        
        super(NSDClientError, self).__init__("distribution", resource, 404, 
                                             http_reason, message, cause)




