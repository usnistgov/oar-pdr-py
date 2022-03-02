"""
Provide tools creating and updating PDR data publications.  

The primary aim of this package is to produce complete SIPs that can be preserved into long-term 
storage (after conversion to an AIP) and ingested into the PDR.  Typically, the SIP takes the form 
of an unserialized BagIt bag that conforms to the NIST Preservation Bag Profile; however, this 
publishing model does not require this.  
"""
from ..exceptions import *
from ..preserve import CorruptedBagError
from ... import pdr as _pdr

_PUBSUBSYSNAME = "Publishing"
_PUBSUBSYSABBREV = "Pub"

class PublishSystem(_pdr.PDRSystem):
    """
    a SystemInfoMixin providing static information about the Preservation system
    """
    def __init__(self):
        super(PublishSystem, self).__init__(_PUBSUBSYSNAME, _PUBSUBSYSABBREV)

system = PublishSystem()

class PublishException(PDRException):
    """
    An exception indicating an error in the processing an SIP or SIP draft
    within a publishing process.
    """
    def __init__(self, msg=None, cause=None, sys=None):
        """
        create the exception.

        :param str   msg:  A message to override the default.
        :param Exception cause:  a caught exception that represents the underlying cause of the problem.  
        :param SystemInfoMixin sys: a SystemInfoMixin instance for the system under which the exception 
                           occurred
        """
        if not msg and not cause:
            msg = "Unknown publishing error"
        if not sys:
            sys = pdrsys.get_global_system() or system
        super(PublishException, self).__init__(msg, cause, sys)

class PublishingStateException(PublishException):
    """
    An exception indicating that a publishing process (or specifically, an SIP) is in an
    illegal or unexpected state, preventing an operation.
    """
    def __init__(self, msg=None, cause=None):
        """
        create the exception.

        :param str   msg:  A message to override the default.
        :param Exception cause:  a caught exception that represents the underlying cause of the problem.  
        """
        if not msg:
            if cause:
                msg = "Publishing state error: " + str(cause)
            else:
                msg = "Unknown Publishing System state error"
        super(PublishingStateException, self).__init__(msg, cause)

class BadSIPInputError(PublishException):
    """
    An exception indicating that a publishing client provided illegal or incompatable input as 
    part of an SIP or SIP creation.
    """
    def __init__(self, msg=None, cause=None):
        """
        create the exception.

        :param str   msg:  A message to override the default.
        :param Exception cause:  a caught exception that represents the underlying cause of the problem.  
        """
        if not msg:
            msg = "Bad SIP input"
            if cause:
                msg += ": " + str(cause)
        super(BadSIPInputError, self).__init__(msg, cause)

class SIPStateException(PublishingStateException):
    """
    An exception indicating that an SIP is in an illegal or unexpected state, preventing an operation.
    """
    def __init__(self, sipid, msg=None, cause=None):
        """
        create the exception.

        :param str sipid:  The ID for the SIP in the bad state
        :param str   msg:  A message to override the default.
        :param Exception cause:  a caught exception that represents the underlying cause of the problem.  
        """
        self.sipid = sipid
        if not msg:
            msg = "State error with SIP=" + sipid
            if cause:
                msg += ": " + str(cause)
            else:
                msg = "Unknown " + msg
        super(SIPStateException, self).__init__(msg, cause)

class SIPConflictError(SIPStateException):
    """
    An exception indicating that although the SIP appears to be in a legal state, that state is not 
    compatible with a requested operation.
    """
    def __init__(self, sipid, msg, cause=None):
        """
        create the exception.

        :param str sipid:  The ID for the SIP in the bad state
        :param str   msg:  A message describing the conflict
        :param Exception cause:  a caught exception that represents the underlying cause of the problem.  
        """
        super(SIPConflictError, self).__init__(sipid, msg, cause)
    
class SIPNotFoundError(SIPStateException):
    """
    An exception indicating the SIP cannot be found in the scope of current processing.  
    """
    def __init__(self, sipid, msg=None, cause=None):
        """
        create the exception.

        :param str sipid:  The ID for the SIP in the bad state
        :param str   msg:  A message to override the default.
        :param Exception cause:  a caught exception that represents the underlying cause of the problem.  
        """
        if not msg:
            msg = "SIP not found: " + sipid
            if cause:
                msg += "({0})".format(str(cause))
        super(SIPNotFoundError, self).__init__(sipid, msg, cause)
    
class UnauthorizedPublishingRequest(BadSIPInputError):
    """
    An exception indicating that a client is not authorized to publish a given SIP as 
    requested 
    """
    def __init__(self, msg=None, cause=None):
        """
        create the exception.

        :param str   msg:  A message to override the default.
        :param Exception cause:  a caught exception that represents the underlying cause of the problem.  
        """
        if not msg:
            msg = "Unauthorized publishing request"
            if cause:
                msg += ": " + str(cause)
        super(UnauthorizedPublishingRequest, self).__init__(msg, cause)
