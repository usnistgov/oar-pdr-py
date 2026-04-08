"""
An implementation of the ExternalReviewClient that talks to the NPS (version 1)
"""
import json, re, logging
from typing import List, Dict, Any
from collections import OrderedDict

import requests

from nistoar.base.config import ConfigurationException
from nistoar.nsd.service import PeopleService
from nistoar.nsd.sync.syncer import get_nsd_auth_token
from nistoar.midas.dap.extrev import ExternalReviewClient, ExternalReviewException
from nistoar.nsd.service import PeopleService, NSDException

mdsid_re = re.compile(r'')

class NPSExternalReviewClient(ExternalReviewClient):
    """
    an ExternalReviewClient implementation that connects to the legacy NPS service
    (consistent with MIDAS v3).

    This implementation will look for the following parameters from the configuration
    dictionary provided at construction time:

    ``draft_url_template``
        _str_ (required). a string template for forming a URL where a reviewer can view the draft
        landing page for the DAP.  The template should include one "%s" insert point where the draft
        DAP ID can be inserted.
    ``published_url_template``
        _str_ (required). a string template for forming a URL where the DAP will be viewable once
        it is published.  The template should include one "%s" insert point where the public
        DAP ID can be inserted.
    ``nps_endpoint``
        _str_ (required). the endpoint URL for the NPS service
    ``tokenService``
        _dict_ (required).  The configuration data that describes the service for retrieving an 
        authentication token for use with the NPS service.  The sub-properties looked for in this 
        dictionary the same as those documented for 
        :py:func:`~nistoar.nsd.sync.syncer.get_nsd_auth_token`.  
    """
    system_name = "nps1"
    log_name = "ExternalReviewClient."+system_name

    _review_reasons = {
        "NEWREC":  "New Record",
        "MDUP":    "Metadata Update",
        "MINDATA": "Data change (minor)",
        "MAJDATA": "Data change (major)",
        "NEWFILE": "New file addition",
        "RMFILE":  "Distribution removal",
        "DEACT":   "Record deactivation",
    }

    _changes = OrderedDict([    # ordered most major to most minor
        ("deactivate",   "DEACT"),
        ("change_major", "MAJDATA"),
        ("remove_files", "RMFILE"),
        ("add_files",    "NEWFILE"),
        ("add_readmes",  "NEWFILE"),
        ("change_minor", "MINDATA"),
        ("metadata",     "MDUP")
    ])

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

    def _get_token(self):
        service_config = self.cfg.get("tokenService")
        if not service_config:
            raise ConfigurationException("No tokenService config provided for NPS API auth.")
        return get_nsd_auth_token(service_config)

    def _build_urls(self, record_id, pubid=None):
        draft_url = self._drafturl_tmpl % record_id
        pub_url = self._puburl_tmpl % (pubid if pubid else record_id)
        return draft_url, pub_url

    @property
    def people_service(self):
        """
        the instance of a PeopleService that will be used for resolving user IDs, or None if not 
        available.
        """
        return self.ps

    @people_service.setter
    def people_service(self, svc: PeopleService):
        self.ps = svc

    def select_review_reason(self, changes: List[str] = None, version: str = None) -> str:
        """
        Return the string to use as the review reason
        """
        if not changes:
            # There should be no changes listed if this is a new record
            return self._review_reasons["NEWREC"]

        # choose the most extreme change provided in the changes list
        for ch in self._changes:
            if ch in changes:
                return self._review_reasons.get(self._changes[ch], "MAJDATA")

        # If none of the changes labels are recognized, assume a major change
        return self._review_reasons["MAJDATA"]

    def _prepare_reviewer(self, user, is_owner=False):
        """
        Prepare a reviewer dictionary as required by NPS.
        :param dict user: user dict (must include 'nistId', 'firstName', 'lastName', 'eMail')
        :param bool is_owner: True if this is the record owner.
        """
        reviewer = {
            "nistId": user["nistId"],
            "firstName": user.get("firstName", ""),
            "lastName": user.get("lastName", ""),
            "eMail": user.get("eMail", ""),
            "contactTypeId": 7 if is_owner else 21
        }
        if self.ps:
            # override with our own query to the people db
            try:
                if isinstance(reviewer['nistId'], int):
                    # id is the integer person record ID
                    person = self.get_person(reviewer['nistId'])
                elif isinstance(reviewer['nistId'], str):
                    # id is the enterprise ID
                    person = self.ps.get_person_by_eid(reviewer['nistId'])
                    reviewer['nistId'] = person['peopleID']
                else:
                    raise ValueError("user['nistId']: unsupported type: "+type(user['nistId']))

                reviewer['firstName'] = person.get('firstName', reviewer['firstName'])
                reviewer['lastName'] = person.get('lastName', reviewer['lastName'])
                reviewer['eMail'] = person.get('emailAddress', reviewer['eMail'])

            except NSDException as ex:
                log = logging.getLogger(self.log_name)
                log.error("Trouble accessing people service info for NPS: "+str(ex))

        elif isinstance(reviewer['nistId'], str):
            raise NSDException("No people service configured available to lookup int people ID")

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
        if reviewers_opt:
            # If explicit reviewers are provided, first one is owner
            reviewers.append(self._prepare_reviewer(reviewers_opt[0], is_owner=True))
        else:
            reviewers.append(self._prepare_reviewer({"nistId": submitter}, is_owner=True))
        for user in reviewers_opt[1:]:
            reviewers.append(self._prepare_reviewer(user, is_owner=False))

        # Set the review reason if not given
        if not review_reason:
            review_reason = self.select_review_reason(changes, version)

        m = re.search(r':\d+$', id)
        if m:
            # NPS1: use only record number portion of ID
            try:
                id = int(id.rsplit(':', 1)[-1])
            except ValueError as ex:
                # should not happen
                pass

        # Build the request payload
        payload = {
            "dataSetID": id,
            "itSecurityReview": bool(security_review),
            "submitterID": reviewers[0]['nistId'],
            "pdR_URL": pub_url,
            "dataPub_URL": draft_url,
            "title": title,
            "description": description,
            "reviewReason": review_reason,
            "reviewers": reviewers
        }

        if instructions:
            payload["instructions"] = instructions

        # NPS expects a POST to {nps_endpoint}/DataSet/SubmitDataset
        url = f"{self.nps_endpoint.rstrip('/')}/DataSet/SubmitDataset"

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
            raise ExternalReviewException(f"Failed to POST to NPS: {ex}", self.system_name) from ex

        if resp.status_code == 200:
            try:
                return resp.json()
            except requests.exception.InvalidJSONError as ex:
                log = logging.getLogger(self.log_name)
                log.exception(ex)
                log.warning("Unexpected error decoding JSON response:\n  %s", resp.text)
                raise ExternalReviewException("Unexpected JSON-decoding error in response to successful "+
                                              "submission: "+str(ex)) from ex
        elif resp.status_code == 401:
            raise ExternalReviewException("Unauthorized: Check the NPS API token and permissions.",
                                          self.system_name, resp.status_code)
        elif resp.status_code == 403:
            raise ExternalReviewException("Forbidden: Record is not currently in review.",
                                          self.system_name, resp.status_code)
        else:
            if resp.status_code == 400:
                log = logging.getLogger(self.log_name)
                log.info("POSTed input: \n%s", json.dumps(payload))
            raise ExternalReviewException(
                f"NPS API error: {resp.status_code} {resp.reason}\n  {url}",
                self.system_name, resp.status_code
            )
