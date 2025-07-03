"""
An implementation of the ExternalReviewClient that talks to the NPS (version 1)
"""
import json
from typing import List, Dict, Any
import requests

from nistoar.base.config import ConfigurationException
from nistoar.nsd.service import PeopleService
from nistoar.nsd.sync.syncer import get_nsd_auth_token
from nistoar.midas.dap.extrev import ExternalReviewClient, ExternalReviewException


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

    _review_reasons = {
        "NEWREC":  "New Record",
        "MDUP":    "Metadata Update",
        "MINDATA": "Data change (minor)",
        "MAJDATA": "Data change (major)",
        "NEWFILE": "New file addition",
        "RMFILE":  "Distribution removal",
        "DEACT":   "Record deactivation",
    }

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
        self._drafturl_tmpl = self.cfg.get("draft_url_template")
        if not self._drafturl_tmpl:
            raise ConfigurationException("Missing required config param: draft_url_template")

        self._puburl_tmpl = self.cfg.get("published_url_template")
        if not self._puburl_tmpl:
            raise ConfigurationException("Missing required config param: published_url_template")

        self.nps_endpoint = self.cfg.get("nps_endpoint")
        if not self.nps_endpoint:
            raise ConfigurationException("Missing required config param: nps_endpoint")

        # Okta token config (for API authentication)
        self.token_service = self.cfg.get("tokenService")

    def _get_token(self):
        if not self.token_service:
            raise ConfigurationException("No tokenService config provided for NPS API auth.")
        return get_nsd_auth_token(self.token_service)

    def _build_urls(self, record_id, pubid=None):
        draft_url = self._drafturl_tmpl % record_id
        pub_url = self._puburl_tmpl % (pubid if pubid else record_id)
        return draft_url, pub_url

    def select_review_reason(self, changes: List[str] = None, version: str = None) -> str:
        """
        Return the string to use as the review reason
        """
        if not changes:
            return self._review_reasons["NEWREC"]
        # Just use "Data change (major)" if not mapped, as before
        # (Custom logic could go here if needed)
        return self._review_reasons["MAJDATA"]

    def _prepare_reviewer(self, user, is_owner=False):
        """
        Prepare a reviewer dictionary as required by NPS.
        :param dict user: user dict (must include 'nistId', 'firstName', 'lastName', 'eMail')
        :param bool is_owner: True if this is the record owner.
        """
        reviewer = {
            "nistId": user["nistId"],
            "firstName": user["firstName"],
            "lastName": user["lastName"],
            "eMail": user["eMail"],
            "contactTypeId": 7 if is_owner else 21
        }
        return reviewer

    def submit(self, id: str, submitter: str, version: str=None, **options):
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
        # Gather options
        title = options.get("title", "")
        description = options.get("description", "")
        pubid = options.get("pubid")  # Will use id if not provided
        instructions = options.get("instructions", [])
        changes = options.get("changes", [])
        reviewers_opt = options.get("reviewers", [])
        security_review = options.get("security_review", False)
        review_reason = options.get("reviewReason")
        # URLs
        draft_url, pub_url = self._build_urls(id, pubid)

        # Compose the reviewers list
        reviewers = []
        # First reviewer: record owner (submitter)
        submitter_info = None
        if reviewers_opt:
            # If explicit reviewers are provided, first one is owner
            submitter_info = reviewers_opt[0]
        elif self.ps:
            # Try to use PeopleService to resolve
            submitter_info = self.ps.get_person(submitter)
        else:
            # Fallback to submitter as is
            submitter_info = {
                "nistId": submitter,
                "firstName": "",
                "lastName": "",
                "eMail": ""
            }
        reviewers.append(self._prepare_reviewer(submitter_info, is_owner=True))
        # Add any other reviewers as tech reviewers (contactTypeId=21)
        for user in reviewers_opt[1:]:
            reviewers.append(self._prepare_reviewer(user, is_owner=False))

        # Set the review reason if not given
        if not review_reason:
            review_reason = self.select_review_reason(changes, version)

        # Build the request payload
        payload = {
            "dataSetID": id,
            "itSecurityReview": bool(security_review),
            "submitterID": submitter_info["nistId"],
            "pdR_URL": pub_url,
            "dataPub_URL": draft_url,
            "title": title,
            "description": description,
            "reviewReason": review_reason,
            "reviewers": reviewers
        }

        if instructions:
            payload["instructions"] = instructions

        # NPS expects a POST to {nps_endpoint}/review/{record_id}
        url = f"{self.nps_endpoint.rstrip('/')}/review/{id}"

        # Send the request
        token = self._get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        try:
            resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
        except Exception as ex:
            raise ExternalReviewException(f"Failed to POST to NPS: {ex}")

        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 401:
            raise ExternalReviewException("Unauthorized: Check the NPS API token and permissions.")
        elif resp.status_code == 403:
            raise ExternalReviewException("Forbidden: Record is not currently in review.")
        else:
            raise ExternalReviewException(
                f"NPS API error: {resp.status_code} {resp.reason}\n{resp.text}"
            )
