"""
Support for the NIST Staff directory (NSD) service
"""
from nistoar.base import OARException, SystemInfoMixin

try:
    from .version import __version__
except ImportError:
    __version__ = "(unset)"

_NSDSYSNAME = "NIST Staff Directory"
_NSDSYSABBREV = "NSD"

class NSDSystem(SystemInfoMixin):
    """
    A SystemInfoMixin representing the overall PDR system.
    """
    def __init__(self, subsysname="", subsysabbrev=""):
        super(NSDSystem, self).__init__(_NSDSYSNAME, _NSDSYSABBREV, subsysname, subsysabbrev, __version__)

system = NSDSystem()

class NSDException(OARException):
    """
    An general base class for exceptions that occur while using or providing an NSD service
    """
    pass

class NSDServiceException(NSDException):
    """
    an exception indicating a problem using the NSD service.
    """

    def __init__(self, resource=None, http_code=None, http_reason=None, message=None, cause=None):
        if not message:
            if resource:
                message = f"Trouble accessing {resource} from the NSD service"
            else:
                message = f"Problem accessing the NSD service"
            if http_code or http_reason:
                message += ":"
                if http_code:
                    message += " "+str(http_code)
                if http_reason:
                    message += " "+str(http_reason)
            elif cause:
                message += ": "+str(cause)

        super(NSDServiceException, self).__init__(message)
        self.resource = resource
        self.code = http_code
        self.status = http_reason


class NSDServerError(NSDServiceException):
    """
    an exception indicating an error occurred on the server-side while 
    trying to access the NSD service.  

    This exception includes three extra public properties, `status`, `reason`, 
    and `resource` which capture the HTTP response status code, the associated 
    HTTP response message, and (optionally) a name for the record being 
    submitted to it.  
    """

    def __init__(self, resource=None, http_code=None, http_reason=None, message=None, cause=None):
        super(NSDServerError, self).__init__(resource, http_code, http_reason, message, cause)
                                                 
class NSDClientError(NSDServiceException):
    """
    an exception indicating an error occurred on the client-side while 
    trying to access the NSD service.  

    This exception includes three extra public properties, `status`, `reason`, 
    and `resource` which capture the HTTP response status code, the associated 
    HTTP response message, and (optionally) a name for the record being 
    submitted to it.  
    """

    def __init__(self, resource, http_code, http_reason, message=None, cause=None):
        if not message:
            message = "client-side NSD error occurred"
            if resource:
                message += " while processing " + resource
            message += ": {0} {1}".format(http_code, http_reason)
          
        super(NSDClientError, self).__init__(resource, http_code, http_reason, message, cause)
                                                 

class NSDResourceNotFound(NSDClientError):
    """
    An error indicating that a requested resource is not available via the
    NSD service.
    """
    def __init__(self, resource, http_reason=None, message=None,
                 cause=None):
        if not message:
            message = "Requested NSD resource not found"
            if resource:
                message += ": "+resource
        
        super(NSDClientError, self).__init__(resource, 404, http_reason, message, cause)




