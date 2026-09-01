"""
This module provides publishing service implementations based around assembling a preservation bag 
as the Archive Information Package (AIP).  This includes those supporting Submission Information Package
(SIP) conventions pdp0 and pdp1.  

The pdp0 convention is based on the following assumptions and requirements:
  * An SIP is provided in the form of a Bagit bag that conforms to the NIST Bagit Profile
  * An SIP can be assembled via submitted NERDm metadata documents
  * The SIP cannot include actual data files (i.e. in the bag's ``data`` folder)

The pdp1 convention modifies the pdp0 in that it supports including data files in the SIP.  This 
convention is intended for use by the MIDAS DAP service.  
"""
import os, re, importlib, inspect, shutil
from copy import deepcopy
from typing import Mapping, List, Union
from abc import abstractmethod, abstractproperty
from logging import Logger
from pathlib import Path

from ... import constants as const
from ....nerdm import constants as nrdconst
from ....pdr import config as cfgmod, utils
from ....pdr.preserve import PreservationInProgress
from .base import SimpleNerdmPublishingService
from .. import (PublishingStateException, SIPConflictError, SIPNotFoundError, BadSIPInputError,
                ConfigurationException, UnauthorizedPublishingRequest)
from ..bagger import SIPBagger, SIPBaggerFactory, PDPBagger
from ..bagger.prepupd import UpdatePrepService
from ...utils.prov import Agent, Action
from ..idmint import PDPMinter, PDP0Minter
from nistoar.id.minter import IDMinter
from ....nerdm import utils as nerdutils
from ....nerdm.validate import ValidationError
from . import status

ARK_PFX_RE = re.compile(const.ARK_PFX_PAT)
ARK_ID_RE = re.compile(const.ARK_ID_PAT)
SIP_PFX_RE = re.compile(r'^(\w+):')

def _pdrid2sipid(pdrid, shoulder_only=False):
    if not ARK_PFX_RE.match(pdrid):
        raise ValueError("pdrid: Not a valid PDR ID: "+pdrid)
    # it's a pdrid; turn it into an sipid
    sipid = ARK_PFX_RE.sub('', pdrid)               # lop off ark:/NNNNN/
    sipid = re.sub(r'-', ':', sipid)                # convert - to :
    sipid = re.sub(r'(:\d+)[sp]\w$', r'\1', sipid)  # remove any check digits
    if shoulder_only:
        colon = ":" if ":" in sipid else ""
        sipid = sipid.split(":", 1)[0] + colon
    return sipid


class BagBasedPublishingService(SimpleNerdmPublishingService):
    """
    A base class for PublishingService implementations for SIP processing conventions based around 
    assembling a preservation bag that becomes the AIP.  The assembly is largely driven by submission 
    of NERDm metadata.  Generally, these service implementations are intended for assembling bags from 
    complete SIP inputs; they are not intended to support user-mediated creation and update.

    This base class will look for the following parameters in the configuration:
    :param str working_dir:      The path to the overall working directory for this service.  Unless 
                                 otherwise specified, all other specific state directories that part 
                                 of this configuration will be placed within this directory.  
    :param str sip_bags_dir:     The path to the parent directory where the SIP bags will be created.
                                 If the path is relative, it will be taken to be relative to the 
                                 working directory.
    :param str sip_status_dir:   The path to the directory where SIP status state is persisted.  
                                 If the path is relative, it will be taken to be relative to the 
                                 working directory.
    :param str sip_submitted_dir:   The path to the directory where SIP bags are moved to when 
                                 submitted for preservation.  If the path is relative, it will be 
                                 taken to be relative to the working directory.
    :param bool validate_nerdm:  If True (default), input NERDm metadata will be validated before 
                                 being accepted, raising a ValidationError exception if the 
                                 metadata is not valid.  
    :param str nerdm_schema_dir: the path to the directory containing NERDm schema files used to 
                                 validate input metadata; if not set, the default OAR schema 
                                 directory (e.g. the OAR system's etc/schemas directory).
    """

    def __init__(self, config: Mapping, convention: str, baselog: Logger=None, workdir: str=None, 
                 bagdir: str=None, statusdir: str=None, submitdir: str=None, pressvc=None):
        """
        initialize the service.

        :param dict    config:  the configuration parameters for this service
        :param str convention:  the label indicating the SIP convention implemented by this class.
                                (This is usually supplied by the subclass.)
        :param Logger baselog:  the Logger to derive this instance's Logger from; this constructor will 
                                call getChild() on this log to instantiate its Logger.
        :param str    workdir:  the default location for this instance's internal data (over-riding
                                what's specified in config).  It will be used as the parent directory
                                bagdir, statusdir, and idregdir if these are not specified, either as 
                                parameter or within config.
        :param str     bagdir:  the directory where bags are assembled 
                                (over-riding what's specified in config)
        :param str  statusdir:  the directory for recording SIP status 
                                (over-riding what's specified in config)
        :param str  submitdir:  the directory to move SIPs submitted for preservation
                                (over-riding what's specified in config)
        :param PreservationService pressvc: the preservation service to use to publish the resulting AIP
        """
        super(BagBasedPublishingService, self).__init__(convention, config, baselog)

        if not workdir:
            workdir = self.cfg.get("working_dir")   # typeically the "pdr" directory
        self.workdir = workdir

        self.bagparent = self._resolve_dir('sip_bags_dir', bagdir, self.workdir, 'sipbags')
        self.statusdir = self._resolve_dir('sip_status_dir', statusdir, self.workdir, 'status')
        self.submitdir = self._resolve_dir('sip_submit_dir', submitdir, self.workdir, 'submitted')

        self.pressvc = pressvc
        if not self.pressvc:
            self.pressvc = self._create_preservation_service()

        self._baggers = {}

    def _resolve_dir(self, cfgkey, injectedval, defbasedir, defsubdir=None):
        # resolve the path to use for the cfgkey directory
        # The injectedval takes precendence if set; if not, self.cfg is consulted using cfgkey.
        # If not present in the config, a value is formed from the defbasedir and defsubdir
        out = injectedval
        if not out:
            out = self.cfg.get(cfgkey)
        if not out:
            if not defbasedir:
                raise ConfigurationException("Missing needed config param: working_dir", sys=self)
            if not defsubdir:
                return defbasedir
            out = defsubdir

        if not os.path.isabs(out):
            # specified directory is relative, make it relative to the workdir
            if not defbasedir:
                raise ConfigurationException("Missing needed config parameter: working_dir", sys=self)
            if not os.path.isdir(defbasedir):
                raise PublishingStateException("Publishing work directory does not exist: "+defbasedir)

            notapath = os.sep not in out
            out = os.path.join(defbasedir, out)
            if notapath and not os.path.exists(out):
                # create the directory if it doesn't exist and if is directly below the workdir
                os.mkdir(out)

        if not os.path.isdir(out):
            raise PublishingStateException(cfgkey + " directory does not exist as a directory: " + out)

        return out

    def status_of(self, sipid):
        """
        return the current status  of the SIP with the given identifier
        :param str sipid:  the identifier for the SIP of interest
        :return: an object describing the current status of the idenfied SIP
        :rtype: SIPStatus
        """
        return status.SIPStatus(sipid, self.statusdir)

    @abstractmethod
    def _get_id_shoulder(self, who: Agent, sipid: str, create: bool):
        """
        determine the ID shoulder to be associated with a service request.  The ID shoulder (the prefix 
        to the local part of our identifiers) serves as a particular account for the client under which 
        this request will operate.  It determines the configuration used by the bagger that will assemble 
        the SIP.  This should raise an UnauthorizedPublishingRequest if client (given by who) is requesting
        a shoulder (as specified by sipid) they are not authorized for.  

        :param Agent    who:  the user agent making the request
        :param str    sipid:  the requested SIP ID
        :param bool  create:  True if the user is requesting the publishing of a new SIP; False if 
                              requesting an update to a previously published SIP.
        """
        raise NotImplementedError()

    @abstractmethod
    def _set_identifiers(self, nerdm, minter, sipid):
        """
        update nerdm with SIP and PDR (and any others, like AIP) identifiers using the minter.
        If sipid is non-None, mint a new one.  The actual sipid is returned 
        """
        raise NotImplementedError()

    @abstractmethod
    def _get_minter(self, shoulder):
        """
        return a minter to be used to mint SIP/PDR identifiers.  This is usually constructed based 
        on configuration data.
        """
        raise NotImplementedError()
        
    def _get_bagger_for(self, shoulder, sipid, idorminter=None):
        if sipid not in self._baggers:
            out = self._create_bagger(shoulder, sipid, idorminter)
            if idorminter: 
                self._baggers[sipid] = out
            return out
        return self._baggers[sipid]

    @abstractmethod
    def _create_bagger(self, shoulder, sipid, idorminter=None):
        raise NotImplementedError()

    @abstractmethod
    def _create_preservation_service(self):
        raise NotImplementedError()

    def accept_resource_metadata(self, nerdm: Mapping, who: Agent=None, sipid: str=None, create:
                                 bool=None) -> str:
        """
        create or update an SIP for submission.  

        By default, a new SIP will be created if the input record is does not have an "@id" property, 
        and an identifier is assigned to it; otherwise, the metadata provided will be considered an 
        update to the SIP with that identifier.  This behavior can be overridden with the ``sipid`` and 
        ``create`` parameters.  When creating, ``sipid`` is a requested SIP identifier; if the client
        is allowed to specify its SIP ID, it will be taken as such and a the "@id" property assigned
        to the new record will be based on it.  If the client is not so allowed, an exception is raised.
        
        The resource metadata that is actually persisted may be modified from the submitted metadata 
        according to the SIP convention.  The metadata that is actually persisted may be modified from 
        the submitted metadata according to the SIP convention.

        The SIP must not be in the PROCESSING nor FAILED state when this method is called.  

        :param dict nerdm:  a NERDm Resource object; this must include an "@type" property that includes 
                            the "Resource" type.
        :param who:         an actor identifier object, indicating who is requesting this action.  This 
                            will get recorded in the history data.  If None, an internal administrative 
                            identity will be assumed.  This identity may affect the identifier assigned.
        :param str sipid:   If provided, assume this to be the SIP's identifier.  If creating a new SIP,
                            then the value does not require the ARK prefix; the actual SIP assigned 
                            maybe modified from this input (see response).  If not provided, the SIP ID
                            will be discerned from the "@id" property or minted anew if "@id" is not set.
        :param bool create: if True, assume this is a request to create a new SIP; if an SIP with the 
                            specified ID already exists, an error is raised.  If False, assume this is
                            an update; if the SIP doesn't exist, an error is raised.  If not provided,
                            the intent is determined based on whether an SIP ID is specified (either 
                            via the sipid parameter or the "@id" property in the nerdm object).

        :return: the SIP ID that should be used to send updates for this SIP
                 :rtype: str

        :raises NERDError:  if the input metadata cannot be interpreted as proper NERDm Resource metadata
        :raises PublishingStateException:  if the SIP is not in a correct state to accept the metadata
        """
        if not sipid:
            sipid = nerdm.get("pdr:sipid") 
        if not sipid and nerdm.get('@id'):
            # in some cases, we can use the value of @id in the given NERDm metadata
            if not ARK_PFX_RE.match(nerdm['@id']) and SIP_PFX_RE.match(nerdm['@id']):
                # nerdm['@id'] looks like an SIPID; let's use it
                sipid = nerdm.get("@id", '')
            elif ARK_PFX_RE.match(nerdm['@id']):
                # if it is a valid pdrid in format, use just the sipid prefix (PFX:)
                sipid = _pdrid2sipid(nerdm['@id'], shoulder_only=True)

        if create is None:
            # if sipid is not provided, assume we're creating a new SIP (rather than updating)
            create = not bool(sipid)
        if not create and not sipid:
            raise SIPConflictError("unknown", "Requested update without providing SIP ID")

        nerdm = deepcopy(nerdm)

        # transform the resource metadata (filter, map, and/or enhance) to what will actually
        # get saved (apart from the possible assignment of an identifier)
        # self._moderate_res_metadata(nerdm)

        # validate the input
        if self.cfg.get('validate_nerdm', True):
            nerdm.setdefault('@id', "unassigned")

            # ensure that input record has all necessary schema designations
            self._tweak_for_validation(nerdm)

            # will raise ValidationError or NERDError if not valid
            self.validate_res_nerdm(nerdm)

        # The ID shoulder (the prefix to the local part of our identifiers) serves as a particular
        # account for the client under which this request will operate.  It determines the configuration
        # used by the bagger that will assemble the SIP.
        shoulder = self._get_id_shoulder(who, sipid, create)  # may raise UnauthorizedPublishingRequest
        if sipid and sipid.endswith(':'):
            sipid = shoulder+":"

        minter = self._get_minter(shoulder)
        pdrid = None
        sts = None
        if sipid and not sipid.endswith(':'):
            sts = self.status_of(sipid)
            if sts.data['user'].get('pdrid'):
                pdrid = sts.data['user']['pdrid']   # caution: how reliable is this?
        sipid = self._set_identifiers(nerdm, minter, sipid, pdrid)  # nerdm gets updated
        if not sts:
            sts = self.status_of(sipid)

        if create:
            if sts.state != status.NOT_FOUND and sts.state != status.PUBLISHED:
                raise SIPConflictError(sipid, "Unable to create SIP {0}: already in process ({1}: {2})"
                                       .format(sipid, sts.siptype, sts.state))

            bagger = self._get_bagger_for(shoulder, sipid, nerdm['@id'])
            bagger.delete(who, "New SIP requested: clearing out any previously existing SIP")
            sts.start(self.convention, who.agent_class)

        else:
            sts = self.status_of(sipid)
            if sts.state == status.PROCESSING:
                raise SIPConflictError(sipid, "Unable to update SIP {0}: already in process ({1}: {2})"
                                       .format(sipid, sts.siptype, sts.state))
            if create is False and (sts.state == status.NOT_FOUND or sts.state == status.PUBLISHED):
                # Caller explicitly says they are expecting this SIP to exist already
                raise SIPConflictError(sipid, "Unable to update SIP {0}: SIP not established, yet"
                                       .format(sipid))

            bagger = self._get_bagger_for(shoulder, sipid, nerdm['@id'])
            if sts.state == status.NOT_FOUND or sts.state == status.PUBLISHED:
                sts.start(self.convention, who.agent_class)
            elif sts.siptype != self.convention:
                raise SIPConflictError(sipid, "SIP is already being handled under a different convention: "+
                                       sts.siptype)
            else:
                sts.update(status.PROCESSING)

        try:
            ct = Action.PUT if os.path.exists(bagger.bagdir) else Action.CREATE
            act = Action(ct, sipid, who, "Submit resource metadata")
            bagger.prepare(who=who, _action=act)
            bagger.set_res_nerdm(nerdm, who, True, _action=act);
            bagger.record_history(act)
            if bagger.id and not sts.data['user'].get('pdrid'):
                sts.data['user']['pdrid'] = bagger.id
            sts.update(status.PENDING)

        except Exception as ex:
            self.log.error("Failed to set resource metadata: "+str(ex))
            sts.update(status.FAILED, sysdata={'errors': [str(ex)]})
            raise ex

        return sipid

    def upsert_component_metadata(self, sipid: str, cmpmd: Mapping, who: Agent=None):
        """
        add or update a component of the NERDm resource with the provided metadata.  If the record does not 
        have an "@id" property, a new component will be created and a component identifier will be assigend
        to it; otherwise, the metadata typically be considered an update to the component with the identifier 
        given by the "@id" property.  Some implementations may allow the caller to create a new component with 
        the given identifier if the component does not already exist; if this is not allowed, an exception is 
        raised.  The metadata that is actually persisted may be modified from the submitted metadata 
        according to the SIP convention.

        The SIP must be already established, either via a previous call to accept_resource_metadata()
        or otherwise having been published before.  Typically, the current state should either AWAITING
        or PENDING.  If the state is NOT_FOUND, the SIP must have been published before so that a base
        bag can be established and updated with the given component.  If the state is currently set to 
        PROCESSING, an exception is raised; the client must wait until processing is complete.  The SIP 
        may be in any of the other states (FINALIZED, PUBLISHED, or FAILED) when this method is called;
        after successful completion, the state will be returned to either the AWAITING or (more typically) 
        PENDING state.

        :param str sipid:   the identifier for the SIP resource to be updated
        :param dict cmpmd:  the metadata describing the resource component.  It must have an "@type" 
                            property with a recognized type.
        :param who:         an actor identifier object, indicating who is requesting this action.  This 
                            will get recorded in the history data.  If None, an internal administrative 
                            identity will be assumed.  This identity may affect the identifier assigned.

        :return:  the relative id assigned to the component.   

        :raises NERDError:  if the input metadata cannot be interpreted as proper NERDm Component metadata
        :raises PublishingStateException:  if the SIP is not in a correct state to accept the metadata
        """
        sts = self.status_of(sipid)
        if sts.state == status.PROCESSING:
            raise SIPConflictError(sipid, 
                "Unable to update SIP {0} with component data: already in process ({1}: {2})"
                .format(sipid, sts.siptype, sts.state)
            )

        # validate the input
        if self.cfg.get('validate_nerdm', True):
            # will raise ValidationError if not valid
            self.validate_comp_nerdm(cmpmd)

        shoulder = self._get_id_shoulder(who, sipid, False)  # may raise UnauthorizedPublishingRequest
        bagger = self._get_bagger_for(shoulder, sipid)

        if sts.state == status.PUBLISHED:
            bagger.delete(who, "Updating previously published SIP: clearing any out previous one")
            sts.start(self.convention, who.agent_class)

        elif sts.state == status.NOT_FOUND:
            if not bagger.get_prepper().aip_exists():
                raise SIPConflictError(sipid,
                    "Unable to update SIP {0} with component data: SIP not yet created ({1}: {2})"
                    .format(sipid, sts.siptype, sts.state)
                )
            sts.start(self.convention, who.agent_class)

        try:
            bagger.prepare(who=who)
            cmpid = bagger.set_comp_nerdm(cmpmd, who)
            if bagger.id and not sts.data['user'].get('pdrid'):
                stat.data['user']['pdrid'] = bagger.id
            sts.update(status.PENDING)

        except Exception as ex:
            self.log.error("Failed to set component metadata: "+str(ex))
            sts.update(status.FAILED, sysdata={'errors': [str(ex)]})
            raise ex
                
        return cmpid

    def remove_component(self, sipid: str, cmpid: str, who: Agent=None):
        """
        remove the identified component from the SIP.  

        :param str sipid:  the identifier for the SIP of interest
        :param str cmpid:  the relative ID of the component to remove
        :param who:        an actor identifier object, indicating who is requesting this action.  This 
                           will get recorded in the history data.  If None, an internal administrative 
                           identity will be assumed.  This identity may affect the identifier assigned.
        :rtype: bool
        :returns:  True if the component was found and removed; False, otherwise
        :raises SIPConflictError:     if the SIP is currently be processed or is being handled via 
                                         a different convention
        :raises PublishingException:  if the deletion operation otherwise fails
        """
        sts = self.status_of(sipid)
        if sts.state == status.PROCESSING:
            raise SIPConflictError(sipid, "Requested SIP is currently being processed: "+sipid)
        if sts.state == status.NOT_FOUND:
            raise SIPNotFoundError(sipid)

        shoulder = self._get_id_shoulder(who, sipid, False)  # may raise UnauthorizedPublishingRequest
        bagger = self._get_bagger_for(shoulder, sipid)

        if os.path.exists(bagger.bagdir):
            return bagger.bagbldr.remove_component("@id:"+cmpid)
        return False
            

    def delete(self, sipid: str, who: Agent=None):
        """
        delete the presence of the SIP from this service.  This will be called automatically by the 
        publish() method after successful submission of the SIP for publication; however, clients can 
        call this in advance of this.  This will purge any unpublished artifacts of the SIP from the 
        service's internal cache and revert its state back to either PUBLISHED or NOT_FOUND, depending 
        on its previous publishing status.  If there are no such artifacts, this method does nothing. 

        :param str sipid:  the identifier for the SIP of interest
        :param who:        an actor identifier object, indicating who is requesting this action.  This 
                           will get recorded in the history data.  If None, an internal administrative 
                           identity will be assumed.  This identity may affect the identifier assigned.
        :rtype: bool
        :returns:  True if artifacts were found and removed; False, otherwise
        :raises SIPConflictError:     if the SIP is currently be processed or is being handled via 
                                         a different convention
        :raises PublishingException:  if the deletion operation otherwise fails
        """
        sts = self.status_of(sipid)
        if sts.state == status.PROCESSING:
            raise SIPConflictError(sipid, "Requested SIP is currently being processed: "+sipid)
        if sts.state != status.NOT_FOUND and sts.state != status.PUBLISHED and sts.siptype != self.convention:
            raise SIPConflictError(sipid, "SIP is already being handled under a different convention: "+
                                   sts.siptype)
        
        shoulder = self._get_id_shoulder(who, sipid, False)  # may raise UnauthorizedPublishingRequest
        bagger = self._get_bagger_for(shoulder, sipid)

        if os.path.exists(bagger.bagdir):
            bagger.delete(who)
            if sts.state != status.PUBLISHED:
                sts.revert()
            if bagger.isrevision and sts.state == NOT_FOUND:
                sts.update(status.PUBLISHED)
            del self._baggers[sipid]

        elif sts.status != state.NOT_FOUND and sts.status != state.PUBLISHED:
            sts.revert()

        else:
            return False

        return True

    def finalize(self, sipid: str, who: Agent=None) -> SIPBagger:
        """
        process all SIP input to get it ready for publication.  The SIP metadata will be updated 
        accordingly (which will affect what is returned from :py:method:`describe`).  
        In this convention, finalization is expected to be quick and therefore can be handled 
        synchronously.  Upon successful completion, the state will be set to FINALIZED.  If an 
        error caused by the collected SIP input occurs, the state will be set to FAILED to 
        indicate that the client must provide updated input to fix the problem and make the 
        SIP publishable.  

        :param str sipid:  the identifier for the SIP of interest
        :raises SIPNotFoundError:   if the SIP is in the NOT_FOUND state
        :raises SIPStateException:  if the SIP is not in the PENDING or FINALIZED state
        """
        sts = self.status_of(sipid)
        if sts.state == status.NOT_FOUND:
            raise SIPNotFoundError(sipid)

        shoulder = self._get_id_shoulder(who, sipid, False)  # may raise UnauthorizedPublishingRequest
        bagger = self._get_bagger_for(shoulder, sipid)
        
        if sts.state == status.FINALIZED:
            self.log.info("SIP %s is already finalized (skipping)", sipid)
            return bagger
        if sts.state != status.PENDING:
            raise SIPConflictError(sipid, "SIP {0} is not ready for finalizing: {1}"
                                          .format(sipid, sts.message))
        if sts.siptype != self.convention:
            raise SIPConflictError(sipid, "SIP {0} is being handled by a different convention: {1}"
                                          .format(sipid, sts.message))

        try:
            bagger.finalize(who)

            userdata = None
            md = bagger.bag.nerd_metadata_for('', True)
            if md.get('doi'):
                userdata = {'doi': md.get('doi')}

            sts.update(status.FINALIZED, userdata=userdata)
            
        except Exception as ex:
            self.log.error("Failed to publish SIP {0}: {1}".format(sipid, str(ex)))
            sts.update(status.FAILED, sysdata={'errors': [str(ex)]})
            raise ex

        return bagger


    def publish(self, sipid: str, who: Agent=None):
        """
        submit the SIP for ingest and preservation into the PDR archive.  The SIP needs to be in 
        the PENDING state.  

        This implementations will call delete() after successful submission of the SIP.

        :param str sipid:  the identifier for the SIP of interest
        :param who:        an actor identifier object, indicating who is requesting this action.  This 
                           will get recorded in the history data.  If None, an internal administrative 
                           identity will be assumed.  This identity may affect the identifier assigned.
        :raises SIPNotFoundError:   if the SIP is in the NOT_FOUND state
        :raises SIPConflictError:   if the SIP is not in the PENDING state or was prepared via 
                                       a different SIP convention 
        :raises PreservationInProgress:  if it apears that preservation of the SIP (usually, a previous 
                                       version) is already in progress
        """
        sts = self.status_of(sipid)
        if sts.state == status.NOT_FOUND:
            raise SIPNotFoundError(sipid)
        if sts.state != status.PENDING and sts.state != status.FINALIZED:
            raise SIPConflictError(sipid, "SIP {0} is not ready for publishing: {1}"
                                          .format(sipid, sts.message))
        if sts.siptype != self.convention:
            raise SIPConflictError(sipid, "SIP {0} is being handled by a different convention: {1}"
                                          .format(sipid, sts.message))

        try:
            bagger = self.finalize(sipid, who)
            sts.update(status.PROCESSING)

            # move the bag to the submitted dir
            submittedbag = bagger.bagdir
            if os.path.isdir(self.submitdir):
                submittedbag = os.path.join(self.submitdir, os.path.basename(bagger.bagdir))
                if os.path.exists(submittedbag):
                    raise PreservationInProgress(sipid)
                shutil.move(bagger.bagdir, submittedbag)

            if self.pressvc:
                # generally, preservation is asynchronous
                self.pressvc.preserve_from(submittedbag, sts, startover=True)
                sts.update(status.SUBMITTED)
            else:
                self.log.warning("No preservation service configured; holding SIP in PROCESSING state")

        except Exception as ex:
            self.log.error("Failed to publish SIP {0}: {1}".format(sipid, str(ex)))
            sts.update(status.FAILED, sysdata={'errors': [str(ex)]})
            raise ex

    def describe(self, id: str, withcomps=True):
        """
        returns a NERDm description of the entity with the given identifier.  

        If the identifier points to a resource, a NERDm Resource record is returned.  If it refers 
        to a component of an SIP, a Component record is returned.  (The NERDm metadata could be 
        incomplete if it hasn't been set yet.)  Extra status information is added, including the 
        information returned by :py:meth:`status_of` in a property called ``pdr:pub_status``, the 
        current publishing state (``pdr:state``) and possibly a ``pdr:message``.  

        :param str id:   an identifier identifying the SIP.  This is typically an SIP-ID, but it can 
                         also be a PDR-ID.
        :param bool withcomps:  if True, and the ID points to a resource, then the member component
                         metadata will be included.
        :rtype Mapping:
        :raises SIPNotFoundError: if an open or published SIP with the given ID does not exist
        """
        reqid = id
        m = ARK_ID_RE.match(id)
        if m:
            # a PDR-ID was provided; convert it to an SIP-ID
            pdrid = id[:m.end(2)]
            aipid = m.group(2)
            shldr = re.sub(r'-.*', '', aipid)

            # look up the SIP-ID
            shldrcfg = self.cfg.get('shoulders', {}).get(shldr)
            if not shldrcfg:
                self.log.warning("Request for unconfigured shoulder: %s", shldr)
                raise SIPNotFoundError(aipid, "Unrecognized ID shoulder: "+shldr)

            minter = self._get_minter(shldr)
            idmd = minter.datafor(pdrid)
            if idmd and idmd.get('sipid'):
                sipid = idmd.get('sipid')
            else:
                sipid = aipid
            id = sipid + id[m.end(2):]
        else:
            sipid = re.sub(r'/.*$', '', id)
            shldr = re.sub(r'[:\-].*$', '', sipid)

        if not sipid:
            msg = "BagBasedPublishingService.describe(): SIP identifier not specified"
            if id:
                msg += ": " + id
            raise ValueError(msg)
        sts = self.status_of(sipid)
        if sts.state == status.NOT_FOUND:
            raise SIPNotFoundError(sipid)

        bagger = self._get_bagger_for(shldr, sipid)

        if os.path.exists(bagger.bagdir):
            parts = id.split('/', 1)
            if len(parts) == 1 or not parts[1]:
                # resource-level requested
                if withcomps:
                    out = bagger.bag.nerdm_record(True)
                else:
                    out = bagger.bag.describe("pdr:r")

            else:
                # component item requested
                out = bagger.bag.describe(parts[1])
                if not out:
                    # component has not been created yet
                    out = {}

        else:
            # not currently active
            out = { '@id': sts.data['user'].get('pdrid') or bagger.id }
            if out.get('@id'):
                if sts.state == status.PUBLISHED:
                    # this has been published before; try to find a published record
                    if self.cfg.get('nerdm_cache'):
                        # look for a cached record
                        aipid = ARK_PFX_RE.sub('', out['@id'])
                        nerdf = os.path.join(self.cfg['nerdm_cache'], aipid+".json")
                        try:
                            out = utils.read_nerd(nerdf)
                        except:
                            pass
                    else:
                        # consult metadata service, if we can
                        prepper = bagger._get_prepper()
                        if prepper and prepper.mdcli:
                            try:
                                out = prepper.mdcli.describe(id)
                            except:
                                pass
            else:
                # we don't know much about it
                if 'pdrid' in sts.data['user']:
                    out['@id'] = sts.data['user']['pdrid']
                if 'doi' in sts.data['user']:
                    out['doi'] = sts.data['user']['doi']

            out['pdr:message'] = "SIP was published"
            if out.get('@id'):
                out['pdr:message'] += " as "+out['@id']
                    
        out.update({ "pdr:sipid": sipid, "pdr:status": sts.state,   # pdr:status is deprecated
                     "pdr:state": sts.state, "pdr:pub_status": sts.user_export() })
        return out

    def _tweak_for_validation(self, nerdmd):
        """
        this will update the `@type` and `_extensionSchemas` properties to ensure that the input
        record validates deeply against the key extension schemas for this service.
        """
        if nerdutils.is_type(nerdmd, "Resource") or 'contactPoint' in nerdmd:
            # it's a resource
            self._tweak_resource_for_validation(nerdmd)
        elif nerdutils.is_type(nerdmd, "Component") or nerdutils.is_type(nerdmd, "Distribution"):
            self._tweak_component_for_validation(nerdmd)
        else:
            raise ValidationError("@type is missing or insufficient to interpret as an SIP submission")

    def _tweak_resource_for_validation(self, resmd):
        if not resmd.get('ediid'):
            resmd['ediid'] = resmd.get('@id')

        types = resmd.setdefault('@type', [])
        extschs = set(resmd.setdefault('_extensionSchemas', []))
        
        if not nerdutils.is_type(resmd, "PDRSubmission"):
            types.append('nrds:PDRSubmission')
            extschema = nrdconst.SIP_SCHEMA_URI + "#/definitions/PDRSubmission"
            if extschema not in extschs:
                extschs.add(extschema)

        if not nerdutils.is_type(resmd, "Resource"):
            types.append('nrd:Resource')

        if not nerdutils.is_type(resmd, "ExperimentalData") and self._has_exp_md(resmd):
            nerdutils._insert_before_val(types, 'nrde:ExperimentalData', 'nrdp:DataPublication',
                                         'nrds:PDRSubmission', 'nrd:PublicDataResource', 'nrd:Resource')
            extschema = nrdconst.EXP_SCHEMA_URI + "#/definitions/ExperimentalContext"
            altextschema = nrdconst.EXP_SCHEMA_URI + "#/definitions/AcquisitionActivity"
            if extschema not in extschs and altextschema not in extschs:
                extschs.add(extschema)

        resmd["_extensionSchemas"] = list(extschs)

        if 'components' in resmd:
            for comp in resmd['components']:
                self._tweak_component_for_validation(comp)

    def _has_exp_md(self, md):
        isexp = False
        for prop in "instrumentsUsed isPartOfProjects acquisitionStartTime hasAcquisitionStart acquisitionEndTime hasAcquisitionEnd".split():
            if prop in md:
                isexp = True
                break
        return isexp

    def _tweak_component_for_validation(self, cmpmd):
        types = cmpmd.setdefault('@type', [])
        extschs = cmpmd.setdefault('_extensionSchemas', [])
        
        if not nerdutils.is_type(cmpmd, "Component"):
            types.append('nrd:Component')

        if not nerdutils.is_type(cmpmd, "AcquisitionActivity") and self._has_exp_md(cmpmd):
            nerdutils._insert_before_val(types, 'nrde:AcquisitionActivity', 'nrdp:AccessPage',
                                         'nrd:Component')
            extschema = nrdconst.EXP_SCHEMA_URI + "#/definitions/ExperimentalContext"
            altextschema = nrdconst.EXP_SCHEMA_URI + "#/definitions/AcquisitionActivity"
            if extschema not in extschs and altextschema not in extschs:
                extschs.add(extschema)

class UploadMethodNotSupported(BadSIPInputError):
    """
    An exception indicating that a requested upload method is not recognized or not supported
    """

    def __init__(self, method: str, msg: str=None):
        """
        create the exceptions

        :param str method:  the name for the requested upload method
        :param str    msg:  a message to override the default
        :param Exception cause:  a caught exception that represents the underlying cause of the problem.  
        """
        if not msg:
            msg = "Requested uploads method is not supported: "+str(method)
        super(UploadMethodNotSupported, self).__init__(msg)
        self.method = method

class PDPublishingService(BagBasedPublishingService):
    """
    This PublishingService implements the base, level-0 assumptions of the PDR's Programmatic
    Data Publishing framework.  A Level-0 PDP publication contains no files served directly by the 
    PDR; only externally-served data is allowed.  

    This service can support several SIP input types; however, all are intended to be NIST-bag based.  
    In particular, different clients may require different behaviors in processing the SIP inputs into 
    published data packages.  The different SIP handling channels are identified by an ID shoulder--the 
    prefix to the local component of the dataset's identifier (e.g. as in "pdp0" in 
    "ark:/88434/pdp0-2341sp".)  Different client groups are authorized to publish under different 
    ID shoulders.  Usually a client will have a single shoulder that it publishes under; however, a 
    client may have multiple shoulders assigned to it.  

    A client can be configured to allow it to specify its own local id portion of an identifier (i.e. part 
    of the portion appearing after the shoulder).  In such a case, the client is responsible for ensuring
    that the local id is unique within the shoulder it is submitted to.  Such a client may also have 
    multiple shoulders available to it, and it can specify which shoulder along with the requested 
    local id by providing a value of the form "SHOULDER:LOCALID" as the value of the "@id" property in 
    resource-level metadata it submits to accept_resource_metadata().  

    A client that is not authorized to choose its own local ID can only submit to its single, configured 
    shoulder--its default shoulder.  In this case, IDs are formed based on an incremented sequence 
    number.  

    This base class will look for the following parameters in the configuration:
    :param str working_dir:      The path to the overall working directory for this service.  Unless 
                                 otherwise specified, all other specific state directories that part 
                                 of this configuration will be placed within this directory.  
    :param str sip_bags_dir:     The path to the parent directory where the SIP bags will be created.
                                 If the path is relative, it will be taken to be relative to the 
                                 working directory.
    :param str sip_status_dir:   The path to the directory where SIP status state is persisted.  
                                 If the path is relative, it will be taken to be relative to the 
                                 working directory.
    :param str id_registry_dir:  The path to the directory where ID minting registries are stored
                                 If the path is relative, it will be taken to be relative to the 
                                 working directory.  This serves as a default path that can be overridden
                                 in the configuration of IDMinter for a specific shoulder (see below).
    :param bool validate_nerdm:  If True (default), input NERDm metadata will be validated before 
                                 being accepted, raising a ValidationError exception if the 
                                 metadata is not valid.  
    :param str nerdm_schema_dir: the path to the directory containing NERDm schema files used to 
                                 validate input metadata; if not set, the default OAR schema 
                                 directory (e.g. the OAR system's etc/schemas directory).
    :param Mapping clients:      a configuration of the clients the names of the client groups that 
                                 are authorized to use this service.  Each key is the name of an
                                 authorized group, and its value is the configuration of the 
                                 authorization.  See below for the subparameters looked for.  
    :param Mapping shoulders:    a configuration of the SIP handlers named after the ID shoulders
                                 that identifier them.  Each key is a shoulder (i.e. a prefix to 
                                 the ARK ID's local-id portion), and its value the configuration 
                                 for that handler.  See below for the subparameters looked for.  
    :param str default_bagger_factory:  the same kind of value as supported by the 
                                 "shoulders.*.bagger.factory_function" parameter (see below)
                                 indicating the function that should be used by default to create
                                 a bagger instance for a shoulder that has not specified its own
                                 "bagger.factory_function" parameter. 
    :param Mapping repo_access:  a configuration of the PDR APIs

    As described above, the 'clients' parameter contains configurations for each of the authorized 
    groups for this service; each key under 'clients' is a group's name, and each value is the 
    configuration for that group, within which the following parameters are supported:
    :param str default_shoulder: the default shoulder that should be applied to the submissions from 
                                 the client group that don't otherwise specify the shoulder to use
                                 (see 'localid_provider')
    :param bool localid_provider:  if True, the client group is authorized to request the localid that 
                                 should be used in forming a record identifier.  This also must be True
                                 to allow the client to specify which shoulder to submit the input to.  
                                 So-authorized clients can provide the local-id value as the '@id' value
                                 for the input NERDm Resource metadata, prefixed by the desired shoulder 
                                 name, delimited by a colon (':').  The desired shoulder must also be 
                                 listed in the 'allowed_shoulders' parameter (see below) for the input 
                                 to be accepted.  False is the default value.
    :param str auth_key:         A token used by clients to authorize themselves; a client that presents 
                                 this token will be considered as part of the the client group that these
                                 parameters configure.  This parameter is not actually used by this service
                                 class but rather by a wrapping (e.g. web service) interface which handles
                                 authentication.  

    As described above, the 'shoulders' parameter contains configurations for each SIP handler that the 
    named shoulder is associated with.  Each key is a shoulder (i.e. a prefix to the ARK ID's local-id 
    portion), and its value is the configuration of the associated handler, in which the following 
    parameters are supported:
    :param List[str] allowed_clients:  the group names (a subset of the keys of the 'clients' parameter
                                 described above) that are authorized to publish under this shoulder.
    :param Mapping bagger:       the configuration for the SIPBagger that should be used process the inputs
                                 into a working SIP bag.  (See also default_bagger_factory and 
                                 bagger.factory_function.)
    :param str bagger.override_config_for:  if set, it gives the name of another shoulder whose 
                                 configuration should inherited from--that is, used as default values for 
                                 this one.  
    :param str bagger.factory_function:  the fully-qualified python name for a factory function that 
                                 should be used to instantiate the SIPBagger.  The callable function 
                                 must accept four named arguments that are sufficient for instantiation:
                                   sipid -- the identifier of the SIP to operate on
                                   siptype -- the shoulder 
                                   config -- the bagger configuration to use
                                   minter -- an PDPMinter instance to use to mint IDs
                                 The name, therefore, can point to one of following four types of python 
                                 entities: 
                                 (a) a stand-alone function conforming to the API,
                                 (b) a class whose constructor conforms to the API,
                                 (c) a static or class method of a class that conform to the API, or
                                 (d) a class that has a class method called "create" that conforms to 
                                     the API.
    :param Mapping id_minter:    the configuration for a PDPIDMinter to use with this shoulder (see below 
                                 for details).

    IDMinter objects are used within this service to assign identifiers to SIPs and the subsequent published
    dataset.  Different minters (or differently configured minters) can be associated with different 
    shoulders.  The "id_minter" configuration defined above supports the following parameters:
    :param str factory_function: the fully-qualified python name for a factory function that 
                                 should be used to instantiate a PDPMinter.  See above for the various 
                                 function types that this name can refer to.
    :param bool based_on_sipid:  If True, minted PDR identifiers will be based on the SIP ID already 
                                 assigned to the SIP; this must be set to true to allow clients to provide
                                 their own local IDs.  False is default.
    :param int sequence_start:   When creating a sequence-based local-ID, the sequence that is nominally 
                                 started with this number.  (Sequence numbers that have already been 
                                 issued will be skipped over.)
    :param str clientid_flag:    A string value (usually one character) to use as a delimiter between 
                                 the client-provided local ID and the trailing check-character.  
                                 Default: 'p'.
    :param str seqid_flag:       A string value (usually one character) to use as a delimiter between 
                                 the sequence-based local ID and the trailing check-character.
                                 Default: 's'.
    :param str naan:             The Name Assigning Authority Number (NAAN) to be used in the PDR 
                                 identifiers (e.g. as is, "88434" in "ark:/88434/mds2-2234").  If not 
                                 provided, the default value set in the nistoar.pdr.constants module 
                                 will be used.  
    :param Mapping registry:     The configuration for the registry that stores the issued identifiers
                                 and their associated data.  
    :param str registry.id_store_file:  the name of the file to store the registered ID associated with
                                 the shoulder.  If not set, an appropriate default based on the shoulder
                                 will be used.  
    :param str registry.store_dir:  the directory to store the registry file in; if not specified, the 
                                 'id_registry_dir' value set above will be used.  
    """
    
    def __init__(self, config: Mapping, convention: str="pdp0", baselog: Logger=None, workdir: str=None, 
                 bagdir: str=None, status_dir: str=None, idregdir: str=None, pressvc=None):
        """
        initialize the service.

        :param dict    config:  the configuration parameters for this service
        :param str convention:  the label indicating the SIP convention implemented by this class.
                                It defaults to "pdp0".  A non-default value can reflect a different 
                                configuration on than what is otherwise expected for "pdp0".  
        :param Logger baselog:  the Logger to derive this instance's Logger from; this constructor will 
                                call getChild() on this log to instantiate its Logger.
        :param str    workdir:  the default location for this instance's internal data (over-riding
                                what's specified in config).  It will be used as the parent directory
                                bagdir, statusdir, and idregdir if these are not specified, either as 
                                parameter or within config.
        :param str     bagdir:  the directory where bags are assembled 
                                (over-riding what's specified in config)
        :param str  statusdir:  the directory for recording SIP status 
                                (over-riding what's specified in config)
        :param str   idregdir:  the default directory for persisting ID registries
                                (over-riding what's specified in config)

        :param PreservationService pressvc: the service to use to publish the resulting AIP
        """
        if not convention:
            convention = "pdp0"
        super(PDPublishingService, self).__init__(config, convention, baselog, workdir, bagdir,
                                                  status_dir, pressvc)
        self.idregdir = self._resolve_dir('id_registry_dir', idregdir, self.workdir, 'idregs')
        self._minters = {}

        if self.workdir and self.cfg.get('repo_access'):
            if not self.cfg['repo_access'].get('working_dir'):
                self.cfg['repo_access']['working_dir'] = self.workdir
            elif not os.path.isabs(self.cfg['repo_access']['working_dir']):
                self.cfg['repo_access']['working_dir'] = \
                    os.path.join(workdir, self.cfg['repo_access']['working_dir'])
            

    def _get_id_shoulder(self, who, sipid: str, create: bool):
        """
        determine the ID shoulder to be associated with a service request.  The ID shoulder (the prefix 
        to the local part of our identifiers) serves as a particular account for the client under which 
        this request will operate.  It determines the configuration used by the bagger that will assemble 
        the SIP.  This will raise an UnauthorizedPublishingRequest if client (given by who) is requesting
        a shoulder (as specified by sipid) they are not authorized for.  

        :param Agent    who:  the user agent making the request
        :param str    sipid:  the requested SIP ID
        :param bool  create:  True if the user is requesting the publishing of a new SIP; False if 
                              requesting an update to a previously submitted SIP.
        """
        # return an ID shoulder to mint an ID under given the permissions configured for the
        # given client (who)

        # The agent's class will corresponds to a class of clients allowed create publications under
        # particular ID shoulders.  The supported clients are specified in the configuration.
        out = None
        client_ctl = self.cfg.get('clients', {}).get(who.agent_class)
        if client_ctl is None:
            client_ctl = self.cfg.get('clients', {}).get("default")
        if client_ctl is None:
            raise UnauthorizedPublishingRequest("No default permissions available for client agent, "+
                                                who.agent_class)

        if sipid:
            # sipid must begin with a shoulder name (or the form NAME: or NAME-)
            m = re.search(r'^([a-zA-Z]\w+):', sipid)
            if not m:
                raise BadSIPInputError("Illegal SIP identifier requested: "+sipid)
            out = m.group(1)

            # is client allowed to specify its own local id portion to mint?
            if create and not sipid.endswith(':') and not client_ctl.get('localid_provider'):
                raise UnauthorizedPublishingRequest(
                    "Client group, %s, is not allowed to request new SIP ID: %s"
                    % (who.agent_class, sipid)
                )
        else:
            # client is requesting a shoulder to be assigned
            out = client_ctl.get('default_shoulder')
            if not out:
                raise UnauthorizedPublishingRequest(
                    "No default shoulder permitted for %s under SIP-type=%s"
                    % (who.agent_class, self.convention)
                )

        shoulder = self.cfg.get('shoulders', {}).get(out)
        if not shoulder:
            self.log.warning("No handler configured for SIP shoulder=%s", out)
        if not shoulder or who.agent_class not in shoulder.get('allowed_clients', []):
            isdefault = "default " if out == client_ctl.get('default_shoulder') else ""
            raise UnauthorizedPublishingRequest(
                "Client group '%s' is not permitted to publish to %sSIP shoulder, %s"
                % (who.agent_class, isdefault, out)
            )

        return out

    def _set_identifiers(self, nerdm, minter, sipid, pdrid=None):
        if pdrid and not ARK_PFX_RE.match(pdrid):
            raise PublishingStateException("Given PDR-ID has invalid form: "+pdrid)

        if sipid and sipid.endswith(':') and not pdrid and \
           nerdm.get('@id') and ARK_PFX_RE.match(nerdm['@id']):
            # the sipid only has a validated shoulder, we don't have a validated pdrid
            # but the nerdm record has legit ARK id:
            # allow client to request previously minted pdrid via the record
            if minter.issued(nerdm['@id']):
                _id = _pdrid2sipid(nerdm['@id'])
                if _id.startswith(sipid):
                    # the @id has the right shoulder
                    pdrid = nerdm['@id']
            sipid = None

        data = {'sipid': sipid}
        if not pdrid and sipid:
            matches = minter.search(data)
            if len(matches) > 1:
                raise PublishingStateException("Multiple IDs have been registered for sipid="+sipid)
            elif len(matches) > 0:
                pdrid = matches[0]

        if not pdrid:
            pdrid = minter.mint(data)

        nerdm['@id'] = pdrid
        aipid = ARK_PFX_RE.sub('', pdrid)
        if not sipid:
            iddata = minter.datafor(pdrid)
            if iddata:
                if iddata.get('sipid'):
                    sipid = iddata.get('sipid')
                if iddata.get('aipid'):
                    aipid = iddata['aipid']
            else:
                sipid = _pdrid2sipid(pdrid)
                if minter.registry:
                    minter.registry.registerID(pdrid, {'sipid': sipid, 'aipid': aipid})

        nerdm['pdr:sipid'] = sipid
        nerdm['pdr:aipid'] = aipid

        return sipid

    def _create_preservation_service(self):
        if self.cfg.get('preservation'):
            from nistoar.pdr.preserve.service import AIP1PreservationService

            log = self.log.getChild('preserve')
            prescfg = self.cfg['preservation']
            if not prescfg.get('working_dir'):
                prescfg['working_dir'] = self.workdir
            if self.cfg.get('repo_access') is not None:
                if not prescfg.get('repo_access'):
                    prescfg['repo_access'] = self.cfg['repo_access']
                else:
                    prescfg['repo_access'] = cfgmod.merge_config(prescfg['repo_access'],
                                                                 deepcopy(self.cfg['repo_access']))
            prescfg['sip_dir'] = self.bagparent
            
            return AIP1PreservationService(prescfg, log)

        self.log.warning("No preservation service configured!")
        return None

    def _create_bagger(self, shoulder: str, sipid: str, idorminter=None):

        # build the bagger configuration
        bgrcfg = self.cfg.get('shoulders',{}).get(shoulder)
        if bgrcfg is None:
            # FYI, we should have caught this error in get_id_shoulder()
            raise PublishingStateException("Missing configuration for shoulder: "+shoulder)

        bgrcfg = deepcopy(bgrcfg.get('bagger', {}))
        loaded = [shoulder]
        while 'override_config_for' in bgrcfg:
            shldr = bgrcfg.pop('override_config_for')
            if shldr in loaded:
                break
            parent = deepcopy(self.cfg.get('shoulders',{}).get(shldr, {}).get('bagger', {}))
            bgrcfg = cfgmod.merge_config(bgrcfg, parent)
            loaded.append(shldr)

        for prop in "working_dir store_dir repo_access default_bagger_factory".split():
            if prop in self.cfg:
                bgrcfg.setdefault(prop, self.cfg[prop])

        # determine the bagger factory
        factoryid = bgrcfg.get('factory_function', self.cfg.get('default_bagger_factory'))
        if isinstance(factoryid, str):
            # factory names a python symbol that must be loaded
            if factoryid.startswith('bagger.'):
                factoryid = 'nistoar.pdr.publish.' + factoryid

            # load specified factory
            factory = self._load_factory_function(factoryid)

        else:
            factory = factoryid
        if not factory:
            factory = PDPBaggerFactory(self.cfg).create
        if not factory:
            raise ConfigurationException("No bagger factory function configured for shoulder="+shoulder)

        # call the factory function
        try:
            return factory(sipid=sipid, siptype=shoulder, config=bgrcfg, idorminter=idorminter)
        except TypeError as ex:
            raise ConfigurationException("factory_function: Does not resolve to an API-compliant callable: "+
                                         str(factoryid)+": "+str(ex))
        
    def _load_factory_function(self, factoryid):
        funcid = ''
        modid = factoryid
        mod = None
        while '.' in modid:
            parts = modid.rsplit('.', 1)
            if not parts[0] or not parts[1]:
                raise ConfigurationException("factory_function: Unimportable factory function: "+
                                             factoryid)
            funcid += '.' + parts[1]
            modid = parts[0]
            try:
                mod = importlib.import_module(modid)
                break
            except ImportError:
                pass
        if not mod:
            raise ConfigurationException("factory_function: Unable to find importable module in "+
                                         "factory name: "+factoryid)
        if not funcid:
            funcid = factoryid
        else:
            funcid = funcid[1:]

        func = mod
        while funcid:
            parts = funcid.split('.', 1)
            if not hasattr(func, parts[0]):
                raise ConfigurationException("factory_function: Unable to resolve %s within %s"
                                             % (funcid, factoryid[:-1*(len(funcid)+1)]))
            func = getattr(func, parts[0])
            funcid = (len(parts) > 1 and parts[1]) or None

        if inspect.isclass(func) and hasattr(func, 'create') and \
           hasattr(getattr(func, 'create'), '__call__'):
            factory = func(self.cfg)
            func = getattr(factory, 'create')
            factoryid += ".create"
        if not hasattr(func, '__call__'):
            raise ConfigurationException("factory_function: Does not resolve to a callable: "+factoryid)

        return func
            
    def _get_minter(self, shoulder: str):
        if shoulder not in self._minters:
            self._minters[shoulder] = self._create_minter(shoulder)
        return self._minters[shoulder]
    
    def _create_minter(self, shoulder: str):
        # find the minter configuration; create the minter
        mntrcfg = self.cfg.get('shoulders', {}).get(shoulder, {}).get("id_minter")
        if not mntrcfg:
            raise ConfigurationException("required id_minter parameter not specified for shoulder="+
                                         shoulder)
        mntrcfg = deepcopy(mntrcfg)
        if 'id_shoulder' not in mntrcfg:
            mntrcfg['id_shoulder'] = shoulder

        regdir = mntrcfg.setdefault('store_dir', self.idregdir)
        if not os.path.isabs(regdir):
            regdir = os.path.join(self.workdir, regdir)
            if not os.path.exists(regdir) and os.path.exists(self.workdir):
                try:
                    os.makedirs(regdir)
                except OSError as ex:
                    raise PublishingStateException("Unable to create ID registry directory: "+regdir+
                                                   ": "+str(ex))
        if not os.path.isdir(regdir):
            raise PublishingStateException("ID registry directory does not exist (as a directory): " + regdir)

        func = None
        if 'factory_function' in mntrcfg:
            func = mntrcfg.get('factory_function')
        if func and isinstance(func, str):
            func = self._load_factory_function(func)
            try:
                return func(mntrcfg, shoulder)
            except TypeError as ex:
                raise ConfigurationException("factory_function: Doesn't resolve to an API-compliant callable: "
                                             +factoryid+": "+str(ex))

        return PDP0Minter(mntrcfg, shoulder)

PDP0Service = PDPublishingService

class PDP1Service(PDPublishingService):
    """
    This :py:class:`~nistoar.pdr.publish.service.base.PublishingService` extends the 
    :py:class:`PDP0Service <PDPublishingService>` to allow including data files in the SIP.  

    It does this by adding methods that set up and manage an _upload space_ where data files 
    can be delivered and then imported into the SIP.  Often (and by default), the data is 
    delivered to the space "out of band"; once the files are in place, the client may request 
    to this service that files be imported; otherwise, the files will be imported automatically 
    during finalization (and publishing).  See :py:meth:`add_data_source` and :py:meth:`import_files`
    for more details.  

    This class supports the same configuration as described for :py:class:`PDPublishingService` with
    an additional parameter (facilitating the so-called _fs_ method for data file uploads):

    ``uploads_dir``
         (str) _optional_.  the path to a local directory which is also accesible to the client 
                            where upload directories will be created on request via 
                            :py:meth:`init_data_upload`.  If not provided, data uploads will not 
                            be enabled, and this service will behave essentially like the
                            :py:class:`PDP0Service <PDPublishingService>`.
    """

    def __init__(self, config: Mapping, baselog: Logger=None, working_dir: str=None, bagdir: str=None, 
                 status_dir: str=None, idregdir: str=None, pressvc=None,
                 uploadsroot: Union[str,Path]=None, convention: str="pdp1"):
        """
        initialize the service.

        :param dict    config:  the configuration parameters for this service
        :param Logger baselog:  the Logger to derive this instance's Logger from; this constructor will 
                                call getChild() on this log to instantiate its Logger.
        :param str    workdir:  the default location for this instance's internal data (over-riding
                                what's specified in config).  It will be used as the parent directory
                                bagdir, statusdir, and idregdir if these are not specified, either as 
                                parameter or within config.
        :param str     bagdir:  the directory where bags are assembled 
                                (over-riding what's specified in config)
        :param str  statusdir:  the directory for recording SIP status 
                                (over-riding what's specified in config)
        :param str   idregdir:  the default directory for persisting ID registries
                                (over-riding what's specified in config)
        :param PreservationService pressvc: the service to use to publish the resulting AIP
        :param str|Path uploadsroot:  the root directory to use for uploads, overriding what's in the 
                                configuration.  Use this to enable uploads with a
                                :py:class:`PDP0Service <PDPublishingService>` configuration.
        :param str convention:  the label indicating the SIP convention implemented by this class.
                                (This is usually supplied by the subclass.)  It defaults to "pdp1".
        """
        if not convention:
            convention = "pdp1"
        super(PDP1Service, self).__init__(config, convention, baselog, working_dir, bagdir, status_dir,
                                          idregdir, pressvc)
        if not uploadsroot:
            if not self.cfg.get('uploads_dir'):
                self.log.warning("PDP1Service: uploads_dir config param not set; "
                                 "data file uploads not supported.")
            else:
                uploadsroot = self.cfg['uploads_dir']
                if not os.path.isabs(uploadsroot):
                    uploadsroot = os.path.join(self.workdir, uploadsroot)
                    if not os.path.isdir(self.workdir):
                        raise ConfigurationException("PDP1Service: %s: working_dir does not exists as directory",
                                                     self.workdir)
                    if not os.path.isdir(uploadsroot):
                        os.mkdir(uploadsroot)
        if isinstance(uploadsroot, str):
            uploadsroot = Path(uploadsroot)

        if uploadsroot and not uploadsroot.is_dir():
            raise ConfigurationException("PDP1Service: uploads_dir does exist as a directory: "+
                                         str(uploadsroot))
        self.uplparent = uploadsroot

    def init_data_upload(self, sipid: str, method: str, who: Agent=None) -> Mapping:
        """
        prepare a space for uploading data files that should be a part of the publication.

        This service supports one method for uploading named 'fs`: the client and this service 
        are assumed to have both have direct filesystem access to an uploads directory.  In 
        particular, this service and the client are configured with root directory on a shared
        filesystem where uploads can occur.  (The server and the client may have different paths 
        to that same root directory.)  When this method is called with method='fs', a 
        dedicated subdirectory will be created under that root uploads directory; its path
        relative to the shared root is returned to the client.  The client then uploads the data
        files--using the same hierarchical organization as is desired for within the bag--to the 
        dedicated subdirectory.  After all files are uploaded, the client is free to call 
        :py:meth:`finalize` or :py:meth:`publish` to submit the SIP.  

        .. seealso the documentation for the parent abstract method, 
        :py:meth:`~nistoar.pdr.publish.service.base.SimpleNerdmPublishingService.upsert_component_metadata`,
        for more information about how this method should be used by the client in concert with 
        other methods used to submit the SIP.  

        :param str  sipid:  the identifier for the SIP being prepared
        :param str method:  the name of the mechanism that will be engaged to upload files; currently,
                            only 'fs' is supported.
        :param who:         an actor identifier object, indicating who is requesting this action.  This 
                            will get recorded in the history data.  If None, an internal administrative 
                            identity will be assumed.  This identity may affect the identifier assigned.
        :raises UploadMethodNotSupported: if ``method`` is not recognized as a supported upload method
        """
        sts = self.status_of(sipid)
        if sts.state == status.NOT_FOUND:
            raise SIPNotFoundError(sipid)
        if sts.state != status.PENDING and sts.state != status.FINALIZED and sts.stat != status.AWAITING:
            raise SIPConflictError(sipid, "SIP {0} is not ready for receiving data: {1}"
                                          .format(sipid, sts.message))
        if sts.siptype != self.convention:
            raise SIPConflictError(sipid, "SIP {0} is being handled by a different convention: {1}"
                                          .format(sipid, sts.message))

        if method != 'fs':
            raise UploadMethodNotSupported(method)
        if not self.uplparent:
            raise UploadMethodNotSupported(method, "fs uploads are not configured in this service")

        upldir = self.uplparent/sipid
        try:
            if not upldir.exists():
                upldir.mkdir()
        except Exception as ex:
            raise PublishingStateException(f"{sipid}: Unable to create uploads directory: {str(ex)}") \
                from ex

        shoulder = self._get_id_shoulder(who, sipid, False)  # may raise UnauthorizedPublishingRequest
        bagger = self._get_bagger_for(shoulder, sipid)
        if not os.path.exists(bagger.bagdir):
            # should not happen; NotFound should have been raised above
            raise PublishingStateException(f"{sipid}: unable to register uploads dir: missing bag")

        if upldir not in [s.get('location','') for s in bagger.get_data_sources()]:
            bagger.add_data_source("fs:"+str(upldir), who)
        
        return { "type": 'fs', "location": sipid }

    def get_upload_space(self, sipid: str, who: Agent=None) -> Mapping:
        """
        return a description of the space that has been set up for data uploads or None one 
        has not yet be initialized.  

        :param str sipid:  the identifier for the SIP being assembled
        :param Agent who:  the user requesting this information; this is used to determine 
                           authorization to get this info.
        """
        sts = self.status_of(sipid)
        if sts.state == status.NOT_FOUND:
            raise SIPNotFoundError(sipid)
        if not self.uplparent:
            return None

        shoulder = self._get_id_shoulder(who, sipid, False)  # may raise UnauthorizedPublishingRequest
        bagger = self._get_bagger_for(shoulder, sipid)

        srcs = bagger.get_data_sources()
        if not srcs or not srcs[0].get('location'):
            return None
        srcs[0]['location'] = os.path.relpath(srcs[0]['location'], self.uplparent)
        return srcs[0]

    def cancel_upload_space(self, sipid: str, who: Agent=None) -> bool:
        """
        delete the space previously set up for uploads.  

        Caution: Any data previously upload will be deleted.

        :param str sipid:  the identifier for the SIP being assembled
        :param Agent who:  the user requesting this information; this is used to determine 
                           authorization to get this info and record provenance
        :return:  True if the space had been set-up and was deleted; False if no such space existed
        """
        sts = self.status_of(sipid)
        if sts.state == status.NOT_FOUND:
            raise SIPNotFoundError(sipid)
        if not self.uplparent:
            return False

        shoulder = self._get_id_shoulder(who, sipid, False)  # may raise UnauthorizedPublishingRequest
        bagger = self._get_bagger_for(shoulder, sipid)

        removed = False
        upldir = os.path.join(self.uplparent, sipid)
        if os.path.isdir(upldir):
            shutil.rmtree(upldir)
            removed = True
        bagger.remove_data_sources()
        return removed

    def import_files(self, sipid: str, who: Agent=None) -> List[str]:
        """
        import all data found in the registered data source
        :param str sipid:  the identifier for the SIP being assembled
        :param Agent who:  the user requesting this information; this is used to determine 
                           authorization to get this info and record provenance
        """
        sts = self.status_of(sipid)
        if sts.state == status.NOT_FOUND:
            raise SIPNotFoundError(sipid)
        if sts.state != status.PENDING and sts.state != status.FINALIZED and sts.stat != status.AWAITING:
            raise SIPConflictError(sipid, "SIP {0} is not ready for receiving data: {1}"
                                          .format(sipid, sts.message))

        shoulder = self._get_id_shoulder(who, sipid, False)  # may raise UnauthorizedPublishingRequest
        bagger = self._get_bagger_for(shoulder, sipid)

        return bagger.ensure_data_files(who=who)



class PDPBaggerFactory(SIPBaggerFactory):
    """
    an SIPBaggerFactory class for creating baggers supporting the PDP convention.  It is intended for 
    use as the default factory for the :py:class:`PDPublishingService`.  

    This implementation will always create an instance of PDPBagger.  

    The following configuration parameters (provided to the constructor) are supported; see 
    :py:class:`PDPublishingService` for more information on definitions:
    :param Mapping repo_access:   configuration for an UpdatePrepService instance that will be 
                                  provided to each generated bagger.
    :param Mapping bagger:        a partial bagger configuration; the values here will _override_
                                  any values passed into the :py:method:`create` method.  
    """

    def __init__(self, config=None):
        """
        initialize this factory.  

        :param Mapping config:  the factory configuration 
        """
        super(PDPBaggerFactory, self).__init__(config)

        self.prepsvc = None
        pcfg = self.cfg.get('repo_access')
        if pcfg:
            if 'metadata_service' not in pcfg:
                raise ConfigurationException("missing required repo_access parameter: metadata_service",
                                             sys=self)
            self.prepsvc = UpdatePrepService(pcfg)

    def supports(self, siptype: str) -> bool:
        """
        return True if this factory can instantiate an SIPBagger that supports the given convention 
        or False, otherwise.  This always returns True; thus, it can, in principle, be used for any
        specified shoulder, assuming the basic PDP conventions are intended.
        :rtype: bool
        """
        return True

    def create(self, sipid: str, siptype: str, config: Mapping=None, 
               idorminter: Union[str,IDMinter]=None) -> SIPBagger:
        """
        create a new instantiation of an SIPBagger that can process an SIP of the given type.  If config
        is provided, it may get merged in some way with the configuration set at construction time before
        being applied to the bagger.

        :param           sipid:  the ID for the SIP to create a bagger for; this is usually a str, 
                                 subclasses may support more complicated ID types.
        :param str     siptype:  the name given to the SIP convention supported by the SIP reference 
                                 by sipid
        :param Mapping  config:  bagger configuration parameters that should override the default
        :param str|PDPMinter idorminter:  either the resource identifier (a str) to assign to the bag 
                                 or an IDMinter instance to use to create an identifier when the bag
                                 is eventually created.  Note that for this factory, this minter must 
                                 be a PDPMinter
        """
        bgrcfg = self.cfg.get('bagger')
        if bgrcfg:
            config = cfgmod.merge_config(bgrcfg, deepcopy(config))

        id = None
        minter = None
        if isinstance(idorminter, str):
            id = idorminter
        else:
            minter = idorminter
            if minter and not isinstance(minter, PDPMinter):
                raise TypeError("PDPBaggerFactory.create: idorminter must be str or a PDPMinter "
                                "(not an IDMinter)")
        
        return PDPBagger(sipid, config, minter, self.prepsvc, siptype, id)

