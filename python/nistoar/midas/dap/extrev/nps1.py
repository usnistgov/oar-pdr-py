"""
An implementation of the ExternalReviewClient that talks to the NPS (version 1)
"""
from nistoar.base.config import ConfigurationException
from nistoar.nsd.service import PeopleService
from . import *

class NPSExternalReviewClient(ExternalReviewClient):
    """
    an ExternalReviewClient implementation that connects to the legacy NPS service
    (consistent with MIDAS v3).  

    This implementation will look for the following parameters from the configuration 
    dictionary provided at construction time:

    ``draft_url_template``
        (*str*) *required*. a string template for forming a URL where a reviewer can view the draft 
        landing page for the DAP.  The template should include one "%s" insert point where the draft 
        DAP ID can be inserted.
    ``published_url_template``
        (*str*) *required*. a string template for forming a URL where the DAP will be viewable once 
        it is published.  The template should include one "%s" insert point where the public 
        DAP ID can be inserted.
    """

    def __init__(self, config, peopsvc: PeopleService=None):
        """
        initialize the client

        :param dict config:  the configuration for this client
        :param PeopleService peopsvc:  a PeopleService instance to use to resolve a submitter ID
                             to a full description of the user (i.e. first and last names, email).
        """
        super(NPSExternalReviewClient, self).__init__(config)
        self.ps = peopsvc  # may be None

        # get templates for the home page URL from config
        self._drafturl_tmpl = get.get("draft_url_template")
        if not self._drafturl_tmpl:
            raise ConfigurationException("Missing required config param: draft_url_template")

        self._puburl_tmpl = self.cfg.get("published_url_template")
        if not self._drafturl_tmpl:
            raise ConfigurationException("Missing required config param: published_url_template")

    def submit(self, id: str, submitter: str, version: str=None, **options=None):
        """
        submit a specified DAP to this system for review.  This implementation supports the 
        following extra options:
        ``title``
             (*str*) the title assigned to the DAP (for display purposes)
        ``description``
             (*str*) the abstract for the DAP (for display purposes)
        ``pubid``
             (*str*) the identifier that will be assigned to the DAP once it is published
        ``instructions``
             (*[str]*) a list of statements that should be passed as special instructions to 
             the reviewers.
        ``changes``
             (*[str]*) a list of statements or phrases describing the reason for review.  
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
        pass

    _review_reasons = {
        "NEWREC":  "New Record",
        "MDUP":    "Metadata Update",
        "MINDATA": "Data change (minor)",
        "MAJDATA": "Data change (major)",
        "NEWFILE": "New file addition",
        "RMFILE":  "Distribution removal"
    ]

    def select_review_reason(self, changes: List[str] = None, version: str = None) -> str:
        """
        Return the string to use as the review reason
        """
        if not changes:
            return self._review_reasons['NEWREC']

        # This part depends on the DAP tool phrases used;

        # default to assuming this is a major change
        return self._review_reasons['MAJDATA']

