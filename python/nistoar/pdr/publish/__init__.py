"""
Provide tools creating and updating PDR data publications
"""
from ..exceptions import *
from ... import pdr as _pdr
from .. import PDRSystem

_PRESSYSNAME = _pdr._PDRSYSNAME
_PRESSYSABBREV = _pdr._PDRSYSABBREV
_PRESSUBSYSNAME = "Publishing"
_PRESSUBSYSABBREV = "Pub"

class PublishSystem(PDRSystem):
    """
    a mixin providing static information about the publishing system
    """
    @property
    def system_name(self): return _PRESSYSNAME
    @property
    def system_abbrev(self): return _PRESSYSABBREV
    @property
    def subsystem_name(self): return _PRESSUBSYSNAME
    @property
    def subsystem_abbrev(self): return _PRESSUBSYSABBREV

sys = PublishSystem()

class PublishException(PDRException):
    """
    An exception indicating an error in the processing an SIP or SIP draft
    within a publishing process.
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
        if not sys or not isinstance(sys, SystemInfoMixin):
            sys = PublishSystem()

        if not msg:
            if cause:
                msg = str(cause)
            else:
                msg = "Unknown {0} System Error".format(sys.subsystem_abbrev)
        Exception.__init__(self, msg, cause, sys)

class PublishingStateException(PDRException):
    """
    An exception indicating that a publishing process (or specifically, an SIP) is in an
    illegal or unexpected state, preventing an operation.
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
        if not sys or not isinstance(sys, SystemInfoMixin):
            sys = PublishSystem()

        if not msg:
            if cause:
                msg = "Publishing state error: " + str(cause)
            else:
                msg = "Unknown {0} System State Error".format(sys.subsystem_abbrev)
        Exception.__init__(self, msg, cause, sys)

class SIPDirectoryError(PublishException):
    """
    a class indicating a problem with the given directory containing 
    the submission data.
    """
    def __init__(self, dir=None, problem=None, cause=None, msg=None, sys=None):
        """
        initial the exception.  By default the exception message will
        be formatted by combining the directory name and the problem statement.
        This can be overridden by providing a verbatim message via the msg
        parameter.

        If no arguments are provided, it is assumed that the problem is that 
        an SIP directory was not provided.

        :param dir  str:   the directory giving the problem
        :param problem str:   a statement of what the problem is; this should not
                           include the name of the directroy.
        :param cause Exception:  a caught exception that represents the 
                           underlying cause of the problem.  
        :param msg  str:   a fully formatted to string to use as the exception
                           message instead of one formed by combining the 
                           directory name and its problem.
        :param sys SystemInfo:  a SystemInfo instance that can provide 
                        information as to the cause of the 
        """
        self.dir = dir
        if not problem:
            if cause:
                problem = str(cause)
            elif not dir:
                problem = "SIP directory not provided"
        if not msg:
            if dir:
                msg = "Problem with SIP directory, {0}: {1}".format(dir, problem)
            else:
                msg = problem
        super(SIPDirectoryError, self).__init__(msg, cause, sys)
        self.problem = problem
                    
class SIPDirectoryNotFound(SIPDirectoryError):
    """
    An exception indicating the SIPDirectory does not exist
    """
    def __init__(self, dir=None, cause=None, msg=None, sys=None):
        """
        :param dir  str:   the directory giving the problem
        :param cause Exception:  a caught exception that represents the 
                           underlying cause of the problem.  
        :param msg  str:   A message to override the default.
        """
        prob = "directory not found"
        super(SIPDirectoryNotFound, self).__init__(dir, prob, cause, msg, sys)



