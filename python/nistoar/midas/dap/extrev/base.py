"""
base and common classes for for managing external review clients
"""
from abc import ABC, abstractmethod
from typing import Mapping

from nistoar.midas import MIDASException

__all__ = [ "ExternalReviewClient", "ExternalReviewException" ]

class ExternalReviewException(MIDASException):
    """
    a base exception for issues when interacting with an external review system
    """
    def __init__(self, message: str, sysname: str=None, statuscode: int=None):
        """
        initialize the exception

        :param str message:  the description of what caused the exception
        :param str  system:  the name of the external review system being engaged
        :param int statuscode:  the HTTP status code that the service responded with (when applicable)
        """
        super(ExternalReviewException, self).__init__(message)
        self.system = sysname
        self.status = statuscode


class ExternalReviewClient(ABC):
    """
    an interface for interacting with a remote DAP review system.  
    """
    system_name = "unspecified"

    def __init__(self, config=None):
        """
        initialize the client
        """
        self.cfg = config or {}

    @abstractmethod
    def submit(self, id: str, submitter: str, version: str=None, **options) -> Mapping:
        """
        submit a specified DAP to this system for review.  

        The options supported depend on the implementation (and implementations should ignore 
        any option parameters that it does not recognize); however, support for the following 
        parameters are defined as follows:
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

        :param str          id:  the identifier for the DAP to submit to the review system
        :param str   submitter:  the identifier for the user submitting the DAP (this is 
                                 usually the owner of the record).
        :param str     version:  the version of the DAP being submitted.  This should be 
                                 provided when revising a previously published DAP; otherwise,
                                 the implementation may assume that the initial version is being 
                                 submitted.  
        :param Mapping options:  extra implementation-specific keyword parameters
        :return:  a dictionary containing the data supported by :py:meth:`get_status`.
                  :rtype: dict
        """
        raise NotImplemented()

    @abstractmethod
    def resubmit(self, id: str, revid: str, **options) -> Mapping:
        """
        resubmit an updated DAP (in response to reviewer feedback) to continue in previously 
        started review process.

        The options supported depend on the implementation (and implementations should ignore 
        any option parameters that it does not recognize); however, support for the following 
        parameters are defined as follows:
        ``comments``
             (*[str]*) a list of statements that should be passed as response to the reviewers'
             feedback.  It can, for example, summarize the changes applied or how the changes 
             addressed the feedback.  

        :param str          id:  the identifier for the DAP to submit to the review system
        :param str       revid:  an identifier for the open review process to resubmit to.  
        :param Mapping options:  extra implementation-specific keyword parameters
        :return:  a dictionary containing the data supported by :py:meth:`get_status`.
                  :rtype: dict
        """
        raise NotImplemented()

    @abstractmethod
    def get_status(self, id: str, revid: str=None) -> Mapping:
        """
        request and return the status of the review for a given dataset as a dictionary.  

        The exact contents for the returned dictionary is implementation-specific, but it should 
        support the following properties:

        ``id``
            the MIDAS identifier for the record being reviewed
        ``revid``
            the identifier assigned by the review system for the current review being processed 
            on the record.
        ``phase``
            an implementation-specific name for the phase that the review is currently in.
        ``requestChanges``
            True if the review is paused because changes were requested of the record's submitter
        ``seeURL``
            A URL that can be visited via a web browser to view the status of a review.
        ``details``
            a dictionary providing implementation-specific details about the review

        :param str          id:  the identifier for the DAP to submit to the review system
        :param str       revid:  an identifier for the open review process it is part of.  If not 
                                 provided, an attempt to discover the process should be attempted.
        """
        raise NotImplemented()

