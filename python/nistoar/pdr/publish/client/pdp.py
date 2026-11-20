"""
Support for a client to the Programmatic Data Publishing (PDP) web service.

The PDR's programmatic publishing service (PDP) allows a client to submit an approved data 
asset publication (DAP) to the PDR for release.  This service assumes that the publication is 
pre-approved for release, either because it went through an explicit review via an upstream 
system (e.g. MIDAS) or the client is automatically pre-approved to publish by policy.  Its
work is intended to be fully automatic (i.e. without human intervention) though asynchronous 
(i.e. it may take a long time to publish its submission).  It encapsulates preservation of the 
publishing artifacts and their delivery to the PDR's public system.  In other words, the PDP
handles the "in-press" part of the publishing process.  

The clients provided here interact with services implemented in 
:py:mod:`nistoar.pdr.publish.service.wsgi.pdp`.  
"""
import json, re
from typing import Mapping, List
from pathlib import Path

import requests

from nistoar.base.config import ConfigurationException
from nistoar.pdr.exceptions import (PDRServiceException, PDRServiceClientError,
                                    PDRServiceAuthFailure, PDRServerError)
from .. import (PublishException, SIPNotFoundError, SIPConflictError,
                BadSIPInputError, UnauthorizedPublishingRequest)
from ..service import status

arkpfx = re.compile(r'^ark:/\d+/')

class PDPPublishingClient:
    """
    A web client that understands the PDP0 and PDP1 publishing web service interfaces.

    The PDR's programmatic publishing service (PDP) allows a :py:mod:`client to submit an approved 
    data asset publication for publication<nistoar.pdr.publish.client>` in the PDR, and it can 
    support multiple _conventions_ (i.e. different API versions, input requirements, and workflows) 
    for accomplishing that, each with their own base service endpoint URL.  The two conventions 
    supported by this client are PDP0 and PDP1.  The difference between them is that PDP1 allows 
    uploaded data files to be made part of a publication; PDP0 does not.  PDP0 is intended for 
    clients publishing DAPs that either do not include data (e.g. they describe software or a 
    service) or point to data products provided by a repository or system external to the PDR.  
    PDP1 is used by clients that may need to provide data products as part of the publication.  

    In both conventions, clients use the interface to build up a _submission information package_ 
    (SIP).  Clients using the PDP0 convention can generally do this by sending a complete NERDm 
    metadata descripion of the DAP in a single call to :py:meth:`publish_sip`.  More complicated 
    publications, incuding those with files submitted via PDP1, can use multiple calls to build 
    up SIP on the publishing server and then submit the package for publication.  That workflow 
    works as follows:

    1. The client creates an initial SIP on the server with a call to :py:meth:`create_sip`.  The 
       submitted metadata can either be a complete or partial description of a NERDm publication
       resource.  The returned metadata (a transformed version of the input) includes the property,
       ``pdr:sipid``, which is used in subsequent calls.  
    2. NERDm components can be added to the SIP with subsequent calls to :py:meth:`add_component`.
    3. At any point after creating the SIP, the client can get back the current state of the 
       DAP (NERDm) metadata via a call to :py:meth:`get_sip`.  
    4. If the SIP will include files (requiring use of the PDP1 convention), the client must 
       declare this intention by calling :py:meth:`get_data_folder`; this will return the name 
       of a folder on the client's local filesystem where the files should be copied.  Note that 
       this folder is specified as a relative directory path that is relative to a parent that is
       _already known to the client_.  
    5. The client tranfers (out-of-band from this service) the files that are described in the 
       submitted NERDm metadata to the data folder.
    6. After files have been delivered, the client can _optionally_ call :py:meth:`import_files`
       to direct the server to incorporate the transfered files into the SIP.  (Only those files
       described in the NERDm metadata will be incorporated.)  Generally, calling this method is 
       not necessary as the files will otherwise be imported automatically at submission time.  
    7. After all metadata and files have been delevered, the client can _optionally_ call 
       :py:meth:`finalize` which applies final massaging of the SIP metadata just prior to 
       publication.  This gives the client one last look before final submission (allowing it 
       to make further updates if needed).  If not called, this finalization is applied 
       automatically at submission time.
    8. The completed SIP is committed for publication with a call to :py:meth:`submit`; this 
       launches the publication process on the server which happens asynchronously.  Further 
       updates can not be done until publication is complete.
    9. If the client can check on the progress of the SIP publication via :py:meth:`get_status`
       with returns a dictionary of information.  When its ``state`` property equals "published",
       publication is complete.

    Even though an SIP can be built up and deliver incrementally to the publishing server via the 
    above process, the PDP service fundementally assumes that an SIP is fully realized when it is 
    first created on the server, allowing the client's submission to be fully automated.  The 
    interface is not designed to support human-driven interaction.  Nevertheless, a partially 
    delivered SIP can be aborted with a call to :py:meth:`delete`.  

    Once publication is complete (as indicated in the response from :py:meth:`get_status`), a 
    client can submit a revision with a new call to :py:meth:`publish_sip` or :py:meth:`create_sip`.
    To be recognized as a revision, the metadata must include a ``pdr:sipid`` property providing
    the same SIP identifier.  PDP1 clients need only transfer new files being added to the 
    publication or files that have changed.  To be recognized as an updated file, it must have the 
    exact same file path and name as the file previously published that it should replace.  A file
    is deleted in the revision if it is not included as a component in the revised NERDm metadata.
    """
    NOT_FOUND  = status.NOT_FOUND
    AWAITING   = status.AWAITING
    PENDING    = status.PENDING
    PROCESSING = status.PROCESSING
    FINALIZED  = status.FINALIZED
    SUBMITTED  = status.SUBMITTED
    PUBLISHED  = status.PUBLISHED
    FAILED     = status.FAILED
    ONHOLD     = status.ONHOLD

    def __init__(self, config: Mapping, service_ep: str=None):
        """
        set up the client to talk to the publishing service
        """
        if not service_ep:
            service_ep = config.get('service_endpoint')
        if not service_ep:
            raise ConfigurationException("PDPPublishingClient: Missing required config parameter: "
                                         "service_endpoint")
        self.baseurl = service_ep.rstrip('/') + '/'
        self.authcfg = config.get('auth', {})
        self.token = None

    def _get_token(self, token_ep: str, req_headers: Mapping = {}, **kwargs):
        """
        retrieve an authentication token via a means prescribed in the instance configuration.
        If this client is not configured for this type of configuration, None is returned.
        :raises NotAuthorized:  if authentication fails or the authenticated user is otherwise 
                                not authorized to access this service.
        """
        # not yet implemented
        return None

    def _handle_request(self, meth="GET", sipid: str=None, path: str=None, input: Mapping=None):
        url = self.baseurl
        if path:
            url += path

        output = None
        hdrs = {}
        if meth in ("POST", "PUT", "PATCH"):
            hdrs = { "Content-type": "application/json" }
        else:
            input = None

        # set up authentication
        kwargs = {}
        if self.authcfg.get('client_cert'):
            # configured to send X.509 certificate
            kwargs['cert'] = (self.authcfg['client_cert'], self.athcfg['client_key'])
        elif self.authcfg.get('auth_key'):
            # configured to send a bearer auth key
            hdrs['Authorization'] = f"Bearer {self.authcfg['auth_key']}"
        elif self.authcfg.get('user'):
            if self.authcfg.get('pw'):
                # username/password required
                kwargs['auth'] = (self.authcfg['user'], self.authcfg['pw'])
            else:
                # Assume the user is an admin account using an internal service
                hdrs.update({ "X_CLIENT_VERIFY": "SUCCESS", "X_CLIENT_CN": self.authcfg['user'] })

        if self.authcfg.get('token_service_endpoint'):
            # a separately retrieved token is required
            if not self.token:
                self.token = self._get_token(self.authcfg['token_service_endpoint'], hdrs, **kwargs)
            if not self.token:
                raise PDRServiceAuthFailure("PDP", resource,
                                            message="Failed to get temporarty auth token")
            hdrs['Authentication'] = f"Bearer {self.token}"
            kwargs = {}

        # now send request
        try:
            resp = requests.request(meth, url, headers=hdrs, json=input, **kwargs)
            msg = resp.reason
            if meth in ["HEAD", "DELETE"]:
                output = resp.status_code >= 200 and resp.status_code < 300
            else:
                try:
                    output = resp.json()
                    if isinstance(output, Mapping) and output.get('pdr:message'):
                        msg = output['pdr:message']
                except Exception as ex:
                    if resp.status_code >= 200 and resp.status_code < 300:
                        raise

            if resp.status_code == 404:
                if path and "No data sources set" in resp.reason:
                    raise SIPConflictError(sipid, msg)
                elif sipid:
                    if msg == resp.reason:
                        msg = "SIP not found"
                    raise SIPNotFoundError(sipid, msg)
                else:
                    msg = "Resource not found (is base URL correct?)"
                    raise PDRServiceClientError("PDP", respath, resp.status_code, message=msg)

            elif resp.status_code == 401:
                raise UnauthorizedPublishingRequest(msg)

            elif input and resp.status_code == 400:
                raise BadSIPInputError(msg)

            elif input and resp.status_code == 409:
                raise SIPConflictError(msg)

            elif resp.status_code > 500:
                raise PDRServerError("PDP", path, resp.status_code, resp.reson)

            elif resp.status_code < 200 or resp.status_code >= 300:
                raise PDRServerError("PDP", path, resp.status_code, resp.reason)

            return output

        except requests.exceptions.JSONDecodeError as ex:
            raise PDRServerError("PDP", path,
                                 message="Expected JSON response; got "+resp.text) from ex
            
        except requests.RequestException as ex:
            raise PDRServiceException("PDP", "Failed to connect to remote publishing service: "+
                                       str(ex)) from ex

    def create_sip(self, resmd: Mapping) -> Mapping:
        """
        create an SIP on the server that can be updated and later published.

        Note that the server may modify metadata before saving it to ensure its compliance 
        with the PDP SIP conventions (e.g. "pdp0", "pdp1").  

        :param dict resmd:  the NERDm Resource metaadata describing the SIP to publish
        :return:  the modified NERDm description of the created SIP
                  :rtype: Mapping

        :raises PDRServiceAuthFailure:  if invalid authentication credentials were submitted or 
                  the attempt to obtain authentication credentials fails
        :raises UnauthorizedPublishingRequest:  if the authenticated caller is not authorized to 
                  submit this particular SIP for publication.  Generally, users are restricted 
                  to submitting an SIP with an SIP identifier with certain configured prefixes.  
        :raises BadSIPInputError:  if the submitted metadata is invalid or does not describe a 
                  valid resource to publish.  
        :raises SIPConflictError:  if this request occurs while the SIP is already in the process of
                  being published.  When this occurs, the user must wait until the SIP reaches 
                  "published" status before submitting a revision.  
        :raises PDRServerError:  if an unexpected server error occured 
        :raises PDRServiceException:  if this client failed to connect to the remote service.  This 
                  can occur if the `service_endpoint` configuration paramter is incorrect, or if the
                  service is down.
        """
        return self._create(resmd, False)

    def _create(self, resmd: Mapping, publish: bool=False):
        sipid = resmd.get('pdr:sipid')
        pdrid = resmd.get('@id')
        if not sipid:
            if not pdrid:
                raise BadSIPInputError("Input SIP Resource metadata missing both '@id' and " +
                                       "'pdr:sipid' properties")
            sipid = arkpfx.sub('', pdrid)
            resmd['pdr:sipid'] = sipid

        path = sipid
        if publish:
            path +="?action=publish"

        return self._handle_request("PUT", sipid, path, resmd)  # may raise exception

    def get_sip(self, sipid) -> Mapping:
        """
        return the metadata description of the SIP established so far.  

        The returned metadata will reflect all previous calls to :py:meth:`create_sip`, 
        :py:meth:`update_sip`, :py:meth:`add_component`, and :py:meth:`finalize`.  

        :param str   sipid:  the SIP identifier previously created via :py:meth:`create_sip`.
        :return: the resource metadata for the SIP.
                 :rtype: Mapping

        :raises SIPNotFoundError:  if the given SIP has not yet been received/created.  
        :raises PDRServiceAuthFailure:  if invalid authentication credentials were submitted or 
                  the attempt to obtain authentication credentials fails
        :raises UnauthorizedPublishingRequest:  if the authenticated caller is not authorized to 
                  update this particular SIP for publication.  A client can only update an SIP
                  it created.
        :raises PDRServerError:  if an unexpected server error occured 
        :raises PDRServiceException:  if this client failed to connect to the remote service.  This 
                  can occur if the `service_endpoint` configuration paramter is incorrect, or if the
                  service is down.
        """
        return self._handle_request("GET", sipid, sipid)  # may raise exception

    def add_component(self, sipid: str, compmd: Mapping):
        """
        add a component to an SIP

        :param str   sipid:  the SIP identifier previously created via :py:meth:`create_sip`.
        :param dict compmd:  the NERDm component metadata to add
        """
        return self._handle_request("POST", sipid, sipid, compmd)  # may raise exception

    def get_status(self, sipid: str) -> Mapping:
        """
        return the status of a previously created SIP.  

        The returned dictionary will contain a variety of information regarding the status of the 
        SIP, including the following key properties:

        ``state``
            a label indicating the state of the SIP in the publishing process.  See below for the 
            definition of the possible values; note that a value of ``awaiting`` or ``pending``
            is a normal state prior to submission.
        ``message``
            a more detailed description of the SIP state.  If the SIP has been submitted, this will 
            describe where in the publishing process the SIP currently is.  
        ``published``
            a flag (``True`` or ``False``) indicating whehter this SIP has been published before 
            (regardless of the value of ``state``).  If set to ``True`` and the ``state`` is set to
            anything other than ``published``, then the current SIP represents a revision on a 
            previously published DAP.  If this property is not present, ``False`` should be assumed. 

        The supported values of ``state`` are as follows:

        ``not found``
             The requested SIP has not been created yet
        ``awaiting``
             The SIP has been created but requires further update before it can be published
        ``pending``
             The SIP has been created and can potentially be submitted for publication.
        ``finalized``
             A call to :py:meth:`finalize` has been made to the SIP and its metadata is in the
             form that will get submitted.  The SIP can be submitted or further updates can be 
             made.
        ``submitted``
             The SIP has been submitted for publication; no further updates are allowed.  The 
             ``message`` property contains a more detailed description of where the SIP is in 
             its publishing process.  
        ``failed``
             The SIP was submitted (or possibly just finalized) but a failure occurred due to an 
             illegal state or condition in the SIP.  The ``message`` property should contain a 
             description of the problem.  To proceed, the SIP must be updated (or possibly rebuilt 
             from scratch) and then resubmitted.  One condition that can cause this state if the 
             metadata describes a file component that was not previously published and not provided 
             for import.  
        ``on-hold``
             Processing has been paused, usually due to an internal issue or system error (and not 
             due to a correctable problem with the SIP).  Administrators have been alerted to this 
             status to address the issue.
        """
        return self._handle_request("GET", sipid, sipid+"/:status")  # may raise exception

    def delete(self, sipid: str):
        """
        cancel the publishing of the given SIP.

        This can be called any time between a call to :py:meth:`create_sip` and :py:meth:`submit`.   
        Once an SIP has been submitted, it cannot be deleted.  If the SIP has already be deleted
        (or never created), this call does nothing.

        :raises PDRServiceAuthFailure:  if invalid authentication credentials were submitted or 
                  the attempt to obtain authentication credentials fails
        :raises UnauthorizedPublishingRequest:  if the authenticated caller is not authorized to 
                  update this particular SIP for publication.  A client can only update an SIP
                  it created.
        :raises PDRServerError:  if an unexpected server error occured 
        :raises PDRServiceException:  if this client failed to connect to the remote service.  This 
                  can occur if the `service_endpoint` configuration paramter is incorrect, or if the
                  service is down.
        """
        try:
            return self._handle_request("DELETE", sipid, sipid)  # may raise exception
        except SIPNotFoundError:
            pass

    def get_data_folder(self, sipid) -> Path:
        """
        return the path to a folder on a local filesystem where data files to be included in an 
        SIP can be imported from.

        After calling this method, the client can move files into the folder; the files must be 
        have the same names and organized into the same hierarchy as defined by the previously 
        loaded component metadata (via the components' ``filepath`` properties).  When all files 
        are in place, the client can call :py:meth:`import_files` to load them into the SIP on 
        the server.  Alternatively, the client can just call :py:meth:`submit` and the files will 
        be imported explicitly.  Multiple calls to this function will always return the same folder.  

        :param str sipid:  the identifier of the existing SIP that files will be added to.  A call 
                           to :py:meth:`create_sip` must already have been made to create the SIP on 
                           the server.
        :return:  a dictionary describing the data folder where files can be transfered to.  The 
                  "location" property indicates the name of the folder.  

        :raises SIPNotFoundError:  if the given SIP has not yet been received/created.  
        :raises PDRServiceAuthFailure:  if invalid authentication credentials were submitted or 
                  the attempt to obtain authentication credentials fails
        :raises UnauthorizedPublishingRequest:  if the authenticated caller is not authorized to 
                  update this particular SIP for publication.  A client can only update an SIP
                  it created.
        :raises PDRServerError:  if an unexpected server error occured 
        :raises PDRServiceException:  if this client failed to connect to the remote service.  This 
                  can occur if the `service_endpoint` configuration paramter is incorrect, or if the
                  service is down.
        """
        return self._handle_request("PUT", sipid, sipid+"/:data")  # defaults to type=fs

    def delete_data_folder(self, sipid):
        """
        delete the data import folder established by a previous call to :py:meth:`get_data_folder`
        along with any files previously copied into it.  

        :raises SIPNotFoundError:  if the given SIP has not yet been received/created.  
        :raises PDRServiceAuthFailure:  if invalid authentication credentials were submitted or 
                  the attempt to obtain authentication credentials fails
        :raises UnauthorizedPublishingRequest:  if the authenticated caller is not authorized to 
                  update this particular SIP for publication.  A client can only update an SIP
                  it created.
        :raises PDRServerError:  if an unexpected server error occured 
        :raises PDRServiceException:  if this client failed to connect to the remote service.  This 
                  can occur if the `service_endpoint` configuration paramter is incorrect, or if the
                  service is down.
        """
        return self._handle_request("DELETE", sipid, sipid+"/:data")  # may raise exception

    def import_files(self, sipid) -> List[str]:
        """
        import data files found in the import folder returned by a previous call to to 
        :py:meth:`get_data_folder`.  

        Only those files that have a component description defined (via previous calls to 
        :py:meth:`create_sip`, :py:meth:`update_sip`, or :py:meth:`add_component`) will be 
        loaded.  (Note that the files must be organized with the same names and in the same 
        hierarchy as defined by the NERDm component metadata via the ``filepath`` properties 
        or they will not be imported.)  Those files that have been successfully imported will 
        be removed from this directory.  

        Note that there is likely a practical upper limit to the number of files that can be 
        imported before the request times out and this method raises with an exception.  If this 
        limit is known, clients can transfer the files to the import folder in smaller batches
        and call this method multiple times.  Typically, the likelihood of a time-out is _not_ 
        affected by the size of the files.  

        :param str sipid:  the identifier for the SIP that the files should be imported into
        :return: a list of the file paths for files imported from the import folder
        :raises SIPConflictError:  if this method is call before the folder has been established
                                   via a previous call to :py:meth:`get_data_folder`. 

        :raises SIPNotFoundError:  if the given SIP has not yet been received/created.  
        :raises PDRServiceAuthFailure:  if invalid authentication credentials were submitted or 
                  the attempt to obtain authentication credentials fails
        :raises UnauthorizedPublishingRequest:  if the authenticated caller is not authorized to 
                  submit this particular SIP for publication.  A client can only submit the SIPs
                  it created.
        :raises PDRServerError:  if an unexpected server error occured 
        :raises PDRServiceException:  if this client failed to connect to the remote service.  This 
                  can occur if the `service_endpoint` configuration paramter is incorrect, or if the
                  service is down.
        """
        resp = self._handle_request("PATCH", sipid, sipid+"?action=import")  # may raise exception
        return resp.get('pdr:imported', [])

    def finalize(self, sipid: str) -> Mapping:
        """
        Make any final modifications to the SIP metadata assuming that no further updates will 
        be made and return it.  

        Note that calling this method is optional as finalization will be applied automatically 
        as part of the call to :py:meth:`submit`.  However, calling this method provides the client 
        with the exact version of the SIP that will be submitted prior to that call.  The client may
        choose to make further updates to the SIP after calling this function.  

        :param str sipid:  the identifier of the existing SIP to be finalized.  At a minimum, a call 
                           to :py:meth:`create_sip` must already have been made to create the SIP on 
                           the server.  
        :return:  the finalized version of the SIP metadata that was actually submitted for 
                  publication (including any automatic modifications made by the server), 
                  augmented with status information.
        :raises SIPNotFoundError:  if the given SIP has not yet been received/created.  
        :raises PDRServiceAuthFailure:  if invalid authentication credentials were submitted or 
                  the attempt to obtain authentication credentials fails
        :raises UnauthorizedPublishingRequest:  if the authenticated caller is not authorized to 
                  submit this particular SIP for publication.  A client can only submit the SIPs
                  it created.
        :raises SIPConflictError:  if this request occurs while the SIP is already in the process of
                  being published.  When this occurs, the user must wait until the SIP reaches 
                  "published" status before submitting a revision.  
        :raises PDRServerError:  if an unexpected server error occured 
        :raises PDRServiceException:  if this client failed to connect to the remote service.  This 
                  can occur if the `service_endpoint` configuration paramter is incorrect, or if the
                  service is down.
        """
        return self._handle_request("PATCH", sipid, sipid+"?action=finalize")  # may raise exception

    def submit(self, sipid) -> Mapping:
        """
        Submit the assembled SIP for preservation and publication.  

        Upon successful submission, the SIP is locked from further updates.  Processing of the SIP 
        is handled asynchronlously; calls to :py:meth:`get_status` can be made to find out the status 
        of that processing.  The ``state`` property will be set to ``published`` when the publishing 
        process is complete.  Upon the successful publication, the SIP is removed from the server; to
        revise the publication, the :py:meth:`create_sip` must be called again.  

        Note that the server will automatically apply the functions provided by :py:meth:`import_files`
        and :py:meth:`finalized`.  (See :py:meth:`import_files` for note on how to avoid timeout 
        failures with large numbers of files.)

        :param str sipid:  the identifier of the existing SIP to be submitted.  At a minimum, a call 
                           to :py:meth:`create_sip` must already have been made to create the SIP on 
                           the server.  
        :return:  the finalized version of the SIP metadata that was actually submitted for 
                  publication (including any automatic modifications made by the server), 
                  augmented with status information (via its ``pdr:status`` property).
        :raises SIPNotFoundError:  if the given SIP has not yet been received/created.  
        :raises PDRServiceAuthFailure:  if invalid authentication credentials were submitted or 
                  the attempt to obtain authentication credentials fails
        :raises UnauthorizedPublishingRequest:  if the authenticated caller is not authorized to 
                  submit this particular SIP for publication.  A client can only submit the SIPs
                  it created.
        :raises SIPConflictError:  if this request occurs while the SIP is already in the process of
                  being published.  When this occurs, the user must wait until the SIP reaches 
                  "published" status before submitting a revision.  
        :raises PDRServerError:  if an unexpected server error occured 
        :raises PDRServiceException:  if this client failed to connect to the remote service.  This 
                  can occur if the `service_endpoint` configuration paramter is incorrect, or if the
                  service is down.
        """
        return self._handle_request("PATCH", sipid, sipid+"?action=publish")  # may raise exception

    def publish_sip(self, resmd: Mapping):
        """
        create and immediately submit an SIP.

        This method provides a one-call way to submit a simple SIP.  The provided metadata must be 
        a complete NERDm resource description that includes all needed components.  No data files 
        can be included in the SIP; thus, the components cannot include ``DataFile`` types unless 
        _(a)_ the files have ``downloadURL`` properties point to an approved non-PDR URL, or _(b)_
        the files have already been published in a previous version of the publication.  If these 
        conditions are not met, SIP status state (retrievable via :py:meth:`get_status`) will be 
        set to failed, and the SIP must be fixed via the other methods available in this client 
        interface.  

        See also :py:meth:`submit`.  

        :param dict resmd:  the NERDm Resource metaadata describing the SIP to publish
        :return:  the finalized version of the SIP metadata that was actually submitted for 
                  publication (including any automatic modifications made by the server), 
                  augmented with status information (via its ``pdr:status`` property).

        :raises PDRServiceAuthFailure:  if invalid authentication credentials were submitted or 
                  the attempt to obtain authentication credentials fails
        :raises UnauthorizedPublishingRequest:  if the authenticated caller is not authorized to 
                  submit this particular SIP for publication.  Generally, users are restricted 
                  to submitting an SIP with an SIP identifier with certain configured prefixes.  
        :raises BadSIPInputError:  if the submitted metadata is invalid or does not describe a 
                  valid resource to publish.  
        :raises SIPConflictError:  if this request occurs while the SIP is already in the process of
                  being published.  When this occurs, the user must wait until the SIP reaches 
                  "published" status before submitting a revision.  
        :raises PDRServerError:  if an unexpected server error occured 
        :raises PDRServiceException:  if this client failed to connect to the remote service.  This 
                  can occur if the `service_endpoint` configuration paramter is incorrect, or if the
                  service is down.
        """
        return self._create(resmd, True)

