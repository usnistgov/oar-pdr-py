"""
a subpackage that defines an interface and implementations for interacting with external 
review systems

The :py:class:`ExternalReviewClient` defines the interface.
"""
from abc import ABC, abstractmethod

from nistoar.midas import MIDASException

__all__ = [ "ExternalReviewClient", "ExternalReviewException" ]

class ExternalReviewException(MIDASException):
    """
    a base exception for issues when interacting with an external review system
    """
    pass

class ExternalReviewClient(ABC):
    """
    an interface for interacting with a remote DAP review system.  
    """

    def __init__(self, config=None):
        """
        initialize the client
        """
        self.cfg = config or {}

    @abstractmethod
    def submit(self, id: str, submitter: str, version: str=None, **options):
        """
        submit a specified DAP to this system for review.  The options supported depend on 
        the implementation (and implementations should ignore any option parameters that 
        it does not recognize; however, support for the following parameters are defined as 
        follows:
        ``instructions``
             (*[str]*) a list of statements that should be passed as special instructions to 
             the reviewers.
        ``changes``
             (*[str]*) provided when a DAP revision is being requested for review, this is a 
             list of statements indicating what has changed since the previous publication.
        ``reviewers``
             (*[dict]*) a list of designations of reviewers requested to be included 
             among the full set of assigned reviewers.  
        ``security_review``
             (*bool*) if True, this DAP includes content (e.g. software) that requires 
             review IT security review.  

        :param str id:         the identifier for the DAP to submit to the review system
        :param str submitter:  the identifier for the user submitting the DAP (this is 
                               usually the owner of the record).
        :param str version:    the version of the DAP being submitted.  This should be 
                               provided when revising a previously published DAP; otherwise,
                               the implementation may assume that the initial version is being 
                               submitted.  
        :param Mapping options:  extra implementation-specific keyword parameters
        """
        raise NotImplemented()

