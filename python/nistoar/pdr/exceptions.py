"""
Exceptions and warnings for the PDR system
"""
from . import system as pdrsys
from ..base import OARException, OARWarning
from ..base.config import ConfigurationException
from ..nerdm.exceptions import NERDError, PODError

class PDRWarning(OARWarning):
    """
    a base class for warnings generated by the preservation system
    """
    def __init__(self, msg=None, cause=None):
        """
        create the base warning.

        :param msg    str:  a specific warning message
        :param Exception cause:  a caught but handled Exception that is the 
                            cause of the warning
        """
        if not msg and not cause:
            msg = "Unspecified Warning"
        super(PDRWarning, self).__init__(msg, cause, pdrsys)

class PDRException(Exception):
    """
    a base class for exceptions occuring in the PDR system
    """
    def __init__(self, msg=None, cause=None, sys=None):
        """
        create the exception.

        :param msg    str:  a specific warning message
        :param cause Exception:  a caught but handled Exception that is the 
                            cause of the warning
        :param sys SystemInfo:  a SystemInfo instance that can provide 
                            information as to the cause of the 
        """
        if not msg and not cause:
            msg = "Unknown PDR Error"
        if not sys:
            sys = pdrsys.get_global_system() or pdrsys
        super(PDRException, self).__init__(msg, cause, sys)

class StateException(PDRException):
    """
    a class indicating that the PDR system or environment is in 
    an uncorrectable state preventing proper processing
    """
    pass

class PDRServiceException(PDRException):
    """
    an exception indicating a problem using a PDR service.
    """

    def __init__(self, service_name, resource=None, http_code=None,
                 http_status=None, message=None, cause=None, sys=None):
        if not message:
            if resource:
                message = "Trouble accessing {0} from the {1} service". \
                          format(resource, service_name)
            else:
                message = "Problem accessing the {0} service". \
                          format(service_name)
            if http_code or http_status:
                message += ":"
                if http_code:
                    message += " "+str(http_code)
                if http_status:
                    message += " "+str(http_status)
            elif cause:
                message += ": "+str(cause)
                
        super(PDRServiceException, self).__init__(message, cause, sys)
        self.service = service_name
        self.resource = resource
        self.code = http_code
        self.status = http_status
                                        
class PDRServerError(PDRServiceException):
    """
    an exception indicating a problem using a PDR service.
    """

    def __init__(self, service_name, resource=None, http_code=None,
                 http_status=None, message=None, cause=None, sys=None):
        if not message:
            if resource:
                message = "Server-side error occurred while accessing " + \
                          resource + " from the " + service_name + " service"
            else:
                message = "Server-side error occurred while accessing the " + \
                          service_name + " service"
            if http_code or http_status:
                message += ":"
                if http_code:
                    message += " "+str(http_code)
                if http_status:
                    message += " "+str(http_status)
            elif cause:
                message += ": "+str(cause)
        super(PDRServerError, self).__init__(service_name, resource, http_code,
                                             http_status, message, cause, sys)

class PDRServiceClientError(PDRServiceException):
    """
    an exception indicating a problem using a PDR service due to a user/client 
    error.  
    """

    def __init__(self, service_name, resource=None, http_code=None,
                 http_status=None, message=None, cause=None, sys=None):
        if not message:
            if resource:
                message = "Client-side error occurred while accessing " + \
                          resource + " from the " + service_name + " service"
            else:
                message = "Client-side error occurred while accessing the " + \
                          service_name + " service"
            if http_code or http_status:
                message += ":"
                if http_code:
                    message += " "+str(http_code)
                if http_status:
                    message += " "+str(http_status)
            elif cause:
                message += ": "+str(cause)
        super(PDRServiceClientError, self).__init__(service_name, resource, http_code,
                                                    http_status, message, cause, sys)

class PDRServiceAuthFailure(PDRServiceException):
    """
    an exception indicating a failure using a service due to incorrect or lack of 
    authorization credentials.
    """

    def __init__(self, service_name, resource=None, http_status=None, 
                 message=None, cause=None, http_code=401, sys=None):
        if not message:
            if resource:
                message = "Client not properly authorized to access " + \
                          resource + " from the " + service_name + " service"
            else:
                message = "Client not properly authorized to access the " + \
                          service_name + " service"
            if http_code or http_status:
                message += ":"
                if http_code:
                    message += " "+str(http_code)
                if http_status:
                    message += " "+str(http_status)
            elif cause:
                message += ": "+str(cause)
        super(PDRServiceAuthFailure, self).__init__(service_name, resource, http_code,
                                                    http_status, message, cause, sys)

class IDNotFound(PDRException):
    """
    An error indicating a request for an identifier that is not recognized 
    as existing in the PDR system.
    """
    def __init__(self, id, message=None, cause=None, sys=None):
        if not message:
            if id:
                message = "{0}: Identifier not recognized".format(id)
            else:
                message = "Requested unrecognized identifier"
            if cause:
                message += " ("+str(cause)+")"
        super(IDNotFound, self).__init__(message, cause, sys)

