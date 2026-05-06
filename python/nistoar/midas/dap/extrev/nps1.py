"""
An implementation of the ExternalReviewClient that talks to the NPS (version 1)
"""
import json, re, logging
from typing import List, Dict, Any, Union, Mapping
from collections import OrderedDict
from urllib.parse import urlparse

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
        self._token = None

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

        self.fbcli = None
        fbcfg = self.cfg.get("nps_feedback")
        if fbcfg:
            self.fbcli = ExternalReviewFeedbackClient(fbcfg)

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

    def submit(self, id: str, submitter: str, version: str=None, **options) -> Mapping:
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
        if not self._token:
            self._token = self._get_token()
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        try:
            resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
        except Exception as ex:
            raise ExternalReviewException(f"Failed to POST to NPS: {ex}", self.system_name) from ex

        if resp.status_code == 401:
            raise ExternalReviewException("Unauthorized: Check the NPS API token and permissions.",
                                          self.system_name, resp.status_code)
        elif resp.status_code == 403:
            raise ExternalReviewException("Forbidden: Record is not currently in review.",
                                          self.system_name, resp.status_code)
        elif resp.status_code >= 300 or resp.status_code < 200:
            if resp.status_code == 400:
                log = logging.getLogger(self.log_name)
                log.info("POSTed input: \n%s", json.dumps(payload))
            raise ExternalReviewException(
                f"Unexpected NPS API error: {resp.status_code} {resp.reason}\n  {url}",
                self.system_name, resp.status_code
            )

        # Looks successful; now get the updated status
        return self._refresh_status(id)

    def get_status(self, id: Union[str, int], revid: str=None) -> Mapping:
        """
        request and return the status of the review for a given dataset

        :param str          id:  the identifier for the DAP to submit to the review system
        :param str       revid:  an identifier for the open review process to resubmit to
        """
        if isinstance(id, str):
            m = re.search(r':\d+$', id)
            if m:
                # NPS1: use only record number portion of ID
                try:
                    id = int(id.rsplit(':', 1)[-1])
                except ValueError as ex:
                    # should not happen
                    pass

        if not revid and revid != 0:
            npsstat = self._discover_status(id)
            revid = npsstat.get('taskID')
        else:
            npsstat = self._get_status("/DataSet/GetByID", id, revid)
        
        out = {
            "id": id,
            "systemID": revid,
            "phase": npsstat.get('taskDescription', 'in progress'),
            "requestChanges": False
        }
        if self.cfg.get('review_url_template'):
            out['seeURL'] = self.cfg['review_url_template'] % {'dataSetID': id, 'taskID': revid}
            
        if npsstat.get('submitterID') == npsstat.get('assigneeNistId'):
            out['phase'] = "changes requested"
            out["requestChanges"] = True

        out['details'] = npsstat
        return out

    def _discover_status(self, id: int):
        tasks = self._get_status("/DataSet/GetActiveTasks", id)
        if len(tasks) == 0:
            raise ExternalReviewException("Not Found: Record does not currently have a review task",
                                          self.system_name, resp.status_code)
        return tasks[-1]

    def _get_status(self, relurl: str, id: int, revid: str=None):
        if not self._token:
            self._token = self._get_token()
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json"
        }
        
        url = f"{self.nps_endpoint.rstrip('/')}{relurl}?dataSetID={id}"
        if revid:
            url += f"&taskID={revid}"

        try:
            resp = requests.get(url, headers=headers)
        except Exception as ex:
            raise ExternalReviewException(f"Failed to POST to NPS: {ex}", self.system_name) from ex

        if resp.status_code == 200:
            if resp.text:
                try:
                    return resp.json()
                except requests.exceptions.InvalidJSONError as ex:
                    log = logging.getLogger(self.log_name)
                    log.exception(ex)
                    log.warning("Unexpected error decoding JSON response:\n  %s", resp.text)
                    raise ExternalReviewException("Unexpected JSON-decoding error in response to successful "+
                                                  "submission: "+str(ex)) from ex
            else:
                log = logging.getLogger(self.log_name)
                log.exception(ex)
                log.warning("Unexpected error decoding JSON response:\n  %s", resp.text)
                raise ExternalReviewException("Unexpected JSON-decoding error in response to successful "+
                                              "request: "+str(ex)) from ex
                    
        elif resp.status_code == 401:
            raise ExternalReviewException("Unauthorized: Check the NPS API token and permissions.",
                                          self.system_name, resp.status_code)
        elif resp.status_code == 403:
            raise ExternalReviewException("Forbidden: Record is not currently in review.",
                                          self.system_name, resp.status_code)
        elif resp.status_code == 404:
            raise ExternalReviewException("Not Found: Record is not currently in review",
                                          self.system_name, resp.status_code)
        else:
            raise ExternalReviewException(
                f"NPS API error: {resp.status_code} {resp.reason}\n  {url}",
                self.system_name, resp.status_code
            )

    def resubmit(self, id: Union[str,int], revid: Union[str,int]=None, **options) -> Mapping:

        if isinstance(id, str):
            m = re.search(r':\d+$', id)
            if m:
                # NPS1: use only record number portion of ID
                try:
                    id = int(id.rsplit(':', 1)[-1])
                except ValueError as ex:
                    # should not happen
                    pass

        if not revid:
            status = self.get_status(id)
            revid = status.get('taskId')

        comments = options.get('comments', ["Resubmitting for further review"])
        if isinstance(comments, list):
            comments = "\n\n".join(comments)

        data = {
            "taskID": revid,
            "comments": comments
        }
        
        if not self._token:
            self._token = self._get_token()
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        url = self.nps_endpoint.rstrip('/') + "/DataSet/RevisionCompleted"
        try:
            resp = requests.post(url, headers=headers, json=data)
        except Exception as ex:
            raise ExternalReviewException(f"Failed to POST to NPS: {ex}", self.system_name) from ex

        if resp.status_code == 401:
            raise ExternalReviewException("Unauthorized: Check the NPS API token and permissions.",
                                          self.system_name, resp.status_code)
        elif resp.status_code == 403:
            raise ExternalReviewException("Forbidden: Record is not currently in review.",
                                          self.system_name, resp.status_code)
        elif resp.status_code >= 300 or resp.status_code < 200:
            if resp.status_code == 400:
                log = logging.getLogger(self.log_name)
                log.info("POSTed input: \n%s", json.dumps(payload))
            raise ExternalReviewException(
                f"Unexpected NPS API error: {resp.status_code} {resp.reason}\n  {url}",
                self.system_name, resp.status_code
            )

        # Looks successful; now get the updated status
        return self._refresh_status(id, revid)

    def _refresh_status(self, id: str, revid: str=None) -> Mapping:
        out = self.get_status(id, revid)

        feedback = None
        if out['requestChanges']:
            feedback = { 'description': "Changes requested; visit NPS site for details" }

        if self.fbcli:
            try:
                self.fbcli.send_feedback(out['id'], out['phase'], revid, feedback, out['requestChanges'])
            except Exception as ex:
                logging.getLogger(self.log_name).exception(ex)

        return out

    def refresh_status(self, id: str, revid: str=None) -> bool:
        """
        pull the latest status from NPS and push it into the DAP record via the feedback service.
        
        This method essentially calls the feedback service on behalf of the legacy NPS service, 
        which cannot do this on its own.  For this to work, this client must have been configured 
        with details on the feedback service via the ``nps_feedback`` configuration parameter.  

        :param str          id:  the identifier for the DAP to submit to the review system
        :param str       revid:  an identifier for the open review process it is part of.  If not 
                                 provided, an attempt to discover the process should be attempted.
        :return:  True if the status was successfully updated.
        """
        if not self._fbcli:
            return False

        try:
            status = self._refresh_status(id, revid)
            return True
        except ExternalReviewException as ex:
            # log issue
            log = logging.getLogger(self.log_name)
            log.error("Failed to push latest review status as requested: "+str(ex))
            return False
        except Exception as ex:
            # log issue
            log = logging.getLogger(self.log_name)
            log.error("Unexpected error while pushing latest review status: "+str(ex))
            return False

class ExternalReviewFeedbackClient:
    """
    A client to the :py:mod:`external review feedback service<nistoar.midas.dap.extrev.wsgi>` provided
    specifically for the legacy NPS service.  

    The legacy NPS service does not have the ability to respond back to MIDAS about review status updates 
    except when review is successfully completed.  This client is used by the 
    :py:class:`NPSExternalReviewClient` to instead pull back the status to MIDAS on NPS's behalf.  
    """

    def __init__(self, config):
        self.ep = config.get('service_endpoint')
        if not self.ep:
            raise ConfigurationException("ExternalReviewFeedbackClient: Missing required param: "+
                                         "service_endpoint")
        if not self.ep.endswith('/'):
            self.ep += '/'

        self.sysname = "nps1"
        try:
            urlp = urlparse(self.ep)
            path = urlp.path.strip('/').split('/')
            if len(path) > 1:
                self.sysname = '/'.join(path[-2:])
            elif len(path) > 0:
                self.sysname = path[-1]
        except ValueError as ex:
            raise ConfigurationException("ExternalReviewFeedbackClient: service_endpoint: not a legal URL: "+
                                         str(self.ep))

        self.hdrs = {}
        if config.get('auth_key'):
            self.hdrs['Authorization'] = f"Bearer {config.get('auth_key')}"

    def send_feedback(self, id: str, phase: str, feedback: Union[Mapping, List]=None,
                      request_changes: bool=False, info_url: str=None, revid: int=None):
        """
        update the status of a review.

        The main purpose of this function is to allow for updating the label indicating the phase 
        of that the review is currently in: the review system would send such a message each time 
        the review enters a new phase or step.  The update can also optionally provide specific 
        instructions for requested changes, provided as a list of dictionaries where each dictionary 
        can have the following properties (corresponding to those supported by 
        :py:meth:`Status.pubreview() <nistoar.midas.dbio.status.pubreview>`):

        ``description``
            (str) _required_.  text describing the request or comment
        ``reviewer``
            (str) _recommended_.  a user identifier or full name of the reviewer or other origin of 
            this instruction
        ``type``
            (str) _optional_.  a label indicating the type of feedback.  Special values include, 
            ``req`` (required to be addressed for approval), ``warn`` (not required but of potentially
            serious concern or otherwise strongly recommended), ``rec`` (recommended to be addressed),
            and ``comment`` (just a comment with no explicit recommendation being made).  Other values
            are allowed (as defined by the external system) but will be interpreted by default as 
            comments.

        :param str        id:  the identifier for the records being reviewed
        :param str     phase:  the current phase or step that the review is currently in
        :param dict|list feedback:  instructions or comments to be provided back to the record's authors.
                               A single piece of feedback can be provided in dictionary form (see above); 
                               Multiple feedback instructions are given as an list of such dictionaries.
        :param bool request_changes:  an indicator as to whether the reviewer(s) require that changes
                               be made according to the information in the ``feedback`` parameter.  
                               If True, the record permissions will be reset to allow authors to 
                               make corrections; when False (or not provided), the information provided
                               in ``feedback`` can be considered optional or commentary.  
        :param str  info_url:  a URL that authors can access to see the state of the review of the 
                               record within the NPS system.
        :param int     revid:  the identifier that NPS uses to track this review.  It is expected that 
                               this is the ID that the NPS API requires for interacting with the review 
                               state. 
        """
        if not revid:
            revid = self._surmise_revid(id)

        msg = {
            "systemID": revid,
            "phase": phase
        }
        if info_url is not None:
            msg['info_at'] = info_url
        if feedback:
            if not isinstance(feedback, (Mapping, List)):
                raise TypeError(f"send_feedback: bad type for feedback ({type(feedback)}): not list or dict")
            if not isinstance(feedback, List):
                feedback = [ feedback ]
            msg['feedback'] = feedback
        if request_changes is True:
            msg['changesRequested'] = True

        return self._send(id, msg, 'PUT')

    def _surmise_revid(self, id: str):
        revid = -1
        
        m = re.match("^mds\d+:0*(\d+)$", id)
        if m:
            try:
                revid = int(m.group(1))
            except ValueError as ex:
                # should not happen
                pass

        return revid

    def legacy_approve(self, id: str):
        """
        send an approval message using the legacy MIDAS-NPS API interface
        """
        return self._send(id, { "reviewResponse": True }, 'POST')

    def approve(self, id: str, info_url: str=None, revid: int=None):
        """
        send an approval message using the new standard feedback API
        """
        if not revid:
            revid = self._surmise_revid(id)

        msg = {
            "systemID": revid,
            "phase": "approved"
        }
        if infor_url:
            msg['info_at'] = info_url
        return self._send(id, msg, 'PUT')

    def cancel(self, id: str, info_url: str=None, revid: int=None):
        """
        request that the review process be canceled 
        """
        if not revid:
            revid = self._surmise_revid(id)

        msg = {
            "systemID": revid,
            "phase": "canceled"
        }
        if info_url:
            msg['info_at'] = info_url
        return self._send(id, msg, 'PUT')

    def _send(self, id: str, message: Mapping, method='PUT'):
        url = self.ep + id
        emsg = "Failed to send review feedback: "

        try:
            resp = requests.request(method, url, json=message, headers=self.hdrs)
        except Exception as ex:
            raise ExternalReviewException(emsg + "comm failure: " + str(ex))

        return self._extract_reply_data(resp)

    def _extract_reply_data(self, resp):
        emsg = "Failed to send review feedback: "

        reply = { "oar:message": resp.reason }
        try:
            reply = resp.json()
        except Exception as ex:
            if resp.status_code >= 200 and resp.status_code < 300:
                reply = { "oar:message": "Corrupted response message" }

        if resp.status_code == 401:
            raise ExternalReviewException(emsg + f"authentication failure ({reply['oar:message']})",
                                          self.sysname, resp.status_code)
        elif resp.status_code == 404:
            raise ExternalReviewException(emsg +f"record not found ({reply['oar:message']})",
                                          self.sysname, resp.status_code)
        elif resp.status_code >= 500:
            raise ExternalReviewException(emsg +f"Unexpected server failure: ({str(resp.status_code)}) " +
                                          reply['oar:message'], self.sysname, resp.status_code)
        elif resp.status_code >= 300 or resp.status_code < 200:
            raise ExternalReviewException(emsg + f"Unexpected {str(resp.status_code)} response: " +
                                          reply['oar:message'], self.sysname, resp.status_code)

        resp.close()
        return reply

    def get_review(self, id: str):
        """
        return the summary of the review of the DAP with the given identifier
        """
        url = self.ep + id
        
        try:
            resp = requests.get(url, headers=self.hdrs)
        except Exception as ex:
            raise ExternalReviewException("Unable to retrieve review summary due to comm failure: " +
                                          str(ex))

        try:
            return self._extract_reply_data(resp)
        finally:
            resp.close()

            
    
        

            
