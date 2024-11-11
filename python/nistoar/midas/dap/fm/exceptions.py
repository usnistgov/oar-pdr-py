"""
Customized exceptions that allow code to handle error conditions
"""

class FileManagerException(Exception):
    """
    an exception indicting a problem interacting with the generic file-manager service.

    This class serves as a base class for all exceptions raised in this code
    """

    def __init__(self, message: str=None):
        if not message:
            message = "Unspecified problem accessing the file-manager"
        super(FileManagerException, self).__init__(message)


class FileManagerServiceError(FileManagerException):
    """
    an exception indicating an error occurred while access a file-manager service endpoint.

    This class serves as a base class for more specific service access errors.
    """

    def __init__(self, message: str=None, ep: str=None, code: int=0, resptext: str=None):
        """
        create the exception

        :param str message:  an explanation of the cause of the error
        :param str ep:       the service endpoint that was being accessed
        :param int code:     the HTTP response code that was returned (if service responded)
        :param str resptext: the erroroneous response body that was returned, as text (if service responded)
        """
        if not message:
            message = "Error accessing file-manager"
            if ep:
                message += f" at {ep}"
            if code:
                message += f" ({str(code)})"
            if resptext:
                message += f"; unhandlable response:\n{resptext}"
        super(FileManagerServiceError, self).__init__(message)
        self.ep = ep
        self.code = code or 0
        self.response = resptext


class FileManagerCommError(FileManagerServiceError):
    """
    an error indicating a failure communicating with the remote file-manager service.  This error
    typically covers network related errors, like failures to connect, dropped connection, DNS 
    errors, etc.  Typically, the remote service did not get a chance to respond directly to the 
    request.
    """
    def __init__(self, message: str=None, ep: str=None):
        """
        create the exception

        :param str message:  an explanation of the cause of the error
        :param str ep:       the service endpoint that was being accessed
        """
        if not message:
            message = "File manager service communication failure"
            if ep:
                message += f" while accessing {ep}"
        super(FileManagerCommError, self).__init__(message, ep)


class FileManagerServerError(FileManagerServiceError):
    """
    an error indicating a server-side error during a request to the remote file-manager service.
    This error is typically the fault of the remote server (i.e. code >= 500) and not due to 
    improper use of the service by the client.
    """

    def __init__(self, code: int=0, ep: str=None, resptext: str=None, message: str=None):
        if not message:
            message = "Unexpected file manager server error"
            if ep:
                message += f" while accessing {ep}"
            if code:
                message += f": HTTP code: {str(code)}"
        super(FileManagerServerError, self).__init__(message, ep)


class UnexpectedFileManagerResponse(FileManagerServerError):
    """
    an error that indicates that the remote file manager responded with unexpected or 
    erroneous content.  The code may reflect a successful operation, but the returned content 
    cannot be processed (e.g. due to format errors).  
    """

    def __init__(self, message: str=None, ep: str=None, resptext: str=None, code: int=0):
        """
        create the exception
        :param str message:  an explanation of the cause of the error
        :param str ep:       the service endpoint that was being accessed
        :param str resptext: the erroroneous response body that was returned, as text
        :param int code:     the HTTP response code that was returned
        """
        if not message:
            message = "Unexpected content returned from file manager service"
            if ep:
                message += f" while accessing {ep}"
            if resptext:
                message += f"; unhandlable response:\n{resptext}"
        super(UnexpectedFileManagerResponse, self).__init__(message, ep, code, resptext)


class FileManagerClientError(FileManagerServiceError):
    """
    an error indicating a client-side error during a request to the remote file-manager service.
    This error is typically indicates that the client is using the service improperly, such as 
    providing bad input data (i.e. code >= 400, < 500).
    """

    def __init__(self, message: str=None, code: int=0, ep: str=None, resptext: str=None):
        """
        create the exception
        :param str message:  an explanation of the cause of the error
        :param int code:     the HTTP response code that was returned
        :param str ep:       the service endpoint that was being accessed
        :param str resptext: the erroroneous response body that was returned, as text
        """
        if not message:
            message = "Bad request made to file manager service"
            if code:
                message += f" ({str(code)})"
            if ep:
                message += f" at {ep}"
        super(FileManagerClientError, self).__init__(message, ep, code, resptext)

class FileManagerResourceNotFound(FileManagerClientError):
    """
    an error indicating that the resource (file or directory) requested from the file manager
    service does not exist.  This typically exception captures a 404 response.
    """

    def __init__(self, ep: str=None, message: str=None, resptext: str=None, code: int=404):
        """
        create the exception
        :param str ep:       the service endpoint that was being accessed
        :param str message:  an explanation of the cause of the error
        :param str resptext: the erroroneous response body that was returned, as text
        :param int code:     the HTTP response code that was returned (default: 404)
        """
        if not message:
            message = "Requested resource not found"
            if ep:
                message += f": {ep}"
        super(FileManagerResourceNotFound, self).__init__(message, code, ep, resptext)

class FileManagerUserUnauthorized(FileManagerClientError):
    """
    an error indicating that the user represented by the supplied credentials is not authorized 
    to access the resource as requested.  This typically exception captures a 403 response.
    """

    def __init__(self, ep: str=None, message: str=None, resptext: str=None, code: int=0):
        """
        create the exception
        :param str ep:       the service endpoint that was being accessed
        :param str message:  an explanation of the cause of the error
        :param str resptext: the erroroneous response body that was returned, as text
        :param int code:     the HTTP response code that was returned (default: 403)
        """
        if not message:
            message = "User is not authorized for access as requested"
            if ep:
                message += f": {ep}"
        super(FileManagerUserUnauthorized, self).__init__(message, code, ep, resptext)
