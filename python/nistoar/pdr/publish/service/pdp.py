"""
This module provides publishing service implementations based around assembling a preservation bag 
as the Archive Information Package (AIP).  This includes those supporting Submission Information Package
(SIP) conventions PDP1 and PDP2.  
"""
import os, re, importlib, inspect
from copy import deepcopy
from collections.abc import Mapping
from abc import abstractmethod, abstractproperty

from ... import constants as const
from ....pdr import config as cfgmod
from .base import SimpleNerdmPublishingService
from .. import (PublishingStateException, SIPConflictError, SIPNotFoundError, BadSIPInputError,
                ConfigurationException, UnauthorizedPublishingRequest)
from ..bagger import SIPBagger, SIPBaggerFactory, PDPBagger
from ..prov import PubAgent
from ..idmint import PDP0Minter
from . import status

ARK_PFX_RE = re.compile(const.ARK_PFX_PAT)
ARK_ID_RE = re.compile(const.ARK_ID_PAT)

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
    :param bool validate_nerdm:  If True (default), input NERDm metadata will be validated before 
                                 being accepted, raising a ValidationError exception if the 
                                 metadata is not valid.  
    :param str nerdm_schema_dir: the path to the directory containing NERDm schema files used to 
                                 validate input metadata; if not set, the default OAR schema 
                                 directory (e.g. the OAR system's etc/schemas directory).
    """

    def __init__(self, config: Mapping, convention: str, workdir: str=None, bagdir: str=None, 
                 statusdir: str=None, ingestsvc: str=None):
        """
        initialize the service.

        :param dict    config:  the configuration parameters for this service
        :param str convention:  the label indicating the SIP convention implemented by this class.
                                (This is usually supplied by the subclass.)
        :param str    workdir:  the default location for this instance's internal data (over-riding
                                what's specified in config).  It will be used as the parent directory
                                bagdir, statusdir, and idregdir if these are not specified, either as 
                                parameter or within config.
        :param str     bagdir:  the directory where bags are assembled 
                                (over-riding what's specified in config)
        :param str  statusdir:  the directory for recording SIP status 
                                (over-riding what's specified in config)
        :param IngestService ingestsvc: the ingest service to use to publish the resulting AIP
        """
        super(BagBasedPublishingService, self).__init__(convention, config)

        if not workdir:
            workdir = self.cfg.get("working_dir")
        self.workdir = workdir

        self.bagparent = self._resolve_dir('sip_bags_dir', bagdir, self.workdir, 'sipbags')
        self.statusdir = self._resolve_dir('sip_status_dir', statusdir, self.workdir, 'status')

        self.ingestsvc = ingestsvc
        if not self.ingestsvc:
            self.ingestsvc = self._create_ingest_service()

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
        return status.SIPStatus(sipid, {"cachedir": self.statusdir})

    @abstractmethod
    def _get_id_shoulder(self, who: PubAgent, sipid: str, create: bool):
        """
        determine the ID shoulder to be associated with a service request.  The ID shoulder (the prefix 
        to the local part of our identifiers) serves as a particular account for the client under which 
        this request will operate.  It determines the configuration used by the bagger that will assemble 
        the SIP.  This should raise an UnauthorizedPublishingRequest if client (given by who) is requesting
        a shoulder (as specified by sipid) they are not authorized for.  

        :param PubAgent who:  the user agent making the request
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
        
    def _get_bagger_for(self, shoulder, sipid, minter=None):
        if sipid not in self._baggers:
            out = self._create_bagger(shoulder, sipid, minter)
            if minter: 
                self._baggers[sipid] = out
            return out
        return self._baggers[sipid]

    @abstractmethod
    def _create_bagger(self, shoulder, sipid, minter=None):
        raise NotImplementedError()

    @abstractmethod
    def _create_ingest_service(self):
        raise NotImplementedError()

    def accept_resource_metadata(self, nerdm: Mapping, who: PubAgent=None, sipid: str=None, create:
                                 bool=None) -> str:
        """
        create or update an SIP for submission.  By default, a new SIP will be created if the input 
        record is does not have an "@id" property, and an identifier is assigned to it; otherwise,
        the metadata provided will be considered an update to the SIP with that identifier.  This 
        behavior can be overridden with the sipid and create parameters.  Some implementations may 
        allow the caller to create a new SIP with the given identifier if it does not exist; if this 
        is not allowed, an exception is raised.  The metadata that is actually persisted may be 
        modified from the submitted metadata according to the SIP convention.  The metadata that is 
        actually persisted may be modified from the submitted metadata according to the SIP convention.

        The SIP must not be in the PROCESSING nor FAILED state when this method is called.  

        :param dict nerdm:  a NERDm Resource object; this must include an "@type" property that includes 
                            the "Resource" type.
        :param who:         an actor identifier object, indicating who is requesting this action.  This 
                            will get recorded in the history data.  If None, an internal administrative 
                            identity will be assumed.  This identity may affect the identifier assigned.
        :param str sipid:   If provided, assume this to be the SIP's identifier.  If creating a new SIP,
                            then the value does not require the ARK prefix; the actual SIP assigned 
                            maybe modified from this input (see response).  If not provided, the SIP ID
                            will be taken from the "@id" property.
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
            # assume that if the @id contains a value, it represents an SIP ID
            sipid = nerdm.get("@id")
        if create is None:
            # if sipid is not provided, assume we're creating a new SIP (rather than updating)
            create = not bool(sipid)
        if not create and not sipid:
            raise SIPConflictError("Requested update without providing SIP ID")

        nerdm = deepcopy(nerdm)
        if not sipid:
            nerdm["@id"] = "unassigned"

        # transform the resource metadata (filter, map, and/or enhance) to what will actually
        # get saved (apart from the possible assignment of an identifier)
        # self._moderate_res_metadata(nerdm)

        # validate the input
        if self.cfg.get('validate_nerdm', True):
            # will raise ValidationError or NERDError if not valid
            self.validate_res_nerdm(nerdm)

        # The ID shoulder (the prefix to the local part of our identifiers) serves as a particular
        # account for the client under which this request will operate.  It determines the configuration
        # used by the bagger that will assemble the SIP.
        shoulder = self._get_id_shoulder(who, sipid, create)  # may raise UnauthorizedPublishingRequest

        minter = self._get_minter(shoulder)
        sipid = self._set_identifiers(nerdm, minter, sipid)  # nerdm gets updated
        sts = self.status_of(sipid)

        if create:
            if sts.state == status.PROCESSING:
                raise SIPConflictError("Unable to create SIP {0}: already in process ({1}: {2})"
                                       .format(sipid, sts.siptype, sts.state))

            bagger = self._get_bagger_for(shoulder, sipid, minter)
            bagger.delete()
            sts.start(self.convention)

        else:
            sts = self.status_of(sipid)
            if sts.state == status.PROCESSING:
                raise SIPConflictError(sipid, "Unable to update SIP {0}: already in process ({1}: {2})"
                                       .format(sipid, sts.siptype, sts.state))
            if create is False and (sts.state == status.NOT_FOUND or sts.state == status.PUBLISHED):
                # Caller explicitly says they are expecting this SIP to exist already
                raise SIPConflictError(sipid, "Unable to update SIP {0}: SIP not established, yet"
                                       .format(sipid))

            bagger = self._get_bagger_for(shoulder, sipid, minter)
            if sts.state == status.NOT_FOUND or sts.state == status.PUBLISHED:
                sts.start(self.convention)
            elif sts.siptype != self.convention:
                raise SIPConflictError("SIP is already being handled under a different convention: "+
                                       sts.siptype)
            else:
                sts.update(status.PROCESSING)

        try:
            bagger.prepare()
            bagger.set_res_nerdm(nerdm, who, True);
            sts.update(status.PENDING)

        except Exception as ex:
            self.log.error("Failed to set resource metadata: "+str(ex))
            sts.update(status.FAILED, sysdata={'errors': [str(ex)]})
            raise ex

        return sipid

    def upsert_component_metadata(self, sipid: str, cmpmd: Mapping, who: PubAgent=None):
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
            raise SIPConflictError(
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
            bagger.delete()
            sts.start(self.convention)

        elif sts.state == status.NOT_FOUND:
            if not bagger.get_prepper().aip_exists():
                raise SIPConflictError(
                    "Unable to update SIP {0} with component data: SIP not yet created ({1}: {2})"
                    .format(sipid, sts.siptype, sts.state)
                )
            sts.start(self.convention)

        try:
            bagger.prepare()
            cmpid = bagger.set_comp_nerdm(cmpmd, who)
            sts.update(status.PENDING)

        except Exception as ex:
            self.log.error("Failed to set component metadata: "+str(ex))
            sts.update(status.FAILED, sysdata={'errors': [str(ex)]})
            raise ex
                
        return cmpid

    def remove_component(self, sipid: str, cmpid: str, who: PubAgent=None):
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
            

    def delete(self, sipid: str, who: PubAgent=None):
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
            raise SIPConflictError("SIP is already being handled under a different convention: "+
                                   sts.siptype)
        
        shoulder = self._get_id_shoulder(who, sipid, False)  # may raise UnauthorizedPublishingRequest
        bagger = self._get_bagger_for(shoulder, sipid)

        if os.path.exists(bagger.bagdir):
            bagger.delete()
            sts.revert()
            if bagger.isrevision and sts.state == NOT_FOUND:
                sts.update(status.PUBLISHED)
            del self._baggers[sipid]

        elif sts.status != state.NOT_FOUND and sts.status != state.PUBLISHED:
            sts.revert()

        else:
            return False

        return True

    def finalize(self, sipid: str, who: PubAgent=None):
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
        if sts.state == status.FINALIZED:
            self.log.info("SIP %s is already finalized (skipping)", sipid)
            return
        if sts.state != status.PENDING:
            raise SIPConflictError(sipid, "SIP {0} is not ready for finalizing: {1}"
                                          .format(sipid, sts.message))
        if sts.siptype != self.convention:
            raise SIPConflictError(sipid, "SIP {0} is being handled by a different convention: {1}"
                                          .format(sipid, sts.message))

        shoulder = self._get_id_shoulder(who, sipid, False)  # may raise UnauthorizedPublishingRequest
        bagger = self._get_bagger_for(shoulder, sipid)
        try:
            bagger.finalize(who)
            sts.update(status.FINALIZED)
        except Exception as ex:
            self.log.error("Failed to publish SIP {0}: {1}".format(sipid, str(ex)))
            sts.update(status.FAILED, sysdata={'errors': [str(ex)]})
            raise ex


    def publish(self, sipid: str, who: PubAgent=None):
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
        """
        sts = self.status_of(sipid)
        if sts.state == status.NOT_FOUND:
            raise SIPNotFoundError(sipid)
        if sts.state != status.PENDING:
            raise SIPConflictError(sipid, "SIP {0} is not ready for publishing: {1}"
                                          .format(sipid, sts.message))
        if sts.siptype != self.convention:
            raise SIPConflictError(sipid, "SIP {0} is being handled by a different convention: {1}"
                                          .format(sipid, sts.message))

        try:
            self.finalize(sipid, who)
            # sts.update(status.PROCESSING)
            # self.ingester.ingest(sipid, who)
            sts.update(status.PUBLISHED)
        except Exception as ex:
            self.log.error("Failed to publish SIP {0}: {1}".format(sipid, str(ex)))
            sts.update(status.FAILED, sysdata={'errors': [str(ex)]})
            raise ex

    def describe(self, id: str, withcomps=True):
        """
        returns a NERDm description of the entity with the given identifier.  If the identifier 
        points to a resource, A NERDm Resource record is returned.  If it refers to a component
        of an SIP, a Component record is returned.  
        :param str id:   an identifier identifying the SIP.  This is typically an SIP-ID, but it can 
                         also be a PDR-ID.
        :param bool withcomps:  if True, and the ID points to a resource, then the member component
                         metadata will be included.
        :rtype Mapping:
        :raises SIPNotFoundError: if an open SIP does not exist
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
            msg = "BagBasedPublishingService.describe(): SIP identifier not specied"
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
                    return bagger.bag.nerdm_record(True)
                return bagger.bag.describe("pdr:r")

            else:
                # component item requested
                out = bagger.bag.describe(parts[1])
                if not out:
                    # component has not been created yet
                    out = {}
                return out

        else:
            # this is all we know about it
            return { "@id": id, "pdr:sipid": sipid }


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

    
    def __init__(self, config: Mapping, convention: str, working_dir: str=None, bagdir: str=None, 
                 status_dir: str=None, idregdir: str=None, ingestsvc=None):    # : IngestService
        """
        initialize the service.

        :param dict    config:  the configuration parameters for this service
        :param str convention:  the label indicating the SIP convention implemented by this class.
                                (This is usually supplied by the subclass.)
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
        :param IngestService ingestsvc: the ingest service to use to publish the resulting AIP
        """
        super(PDPublishingService, self).__init__(config, convention, working_dir, bagdir,
                                                  status_dir, ingestsvc)
        self.idregdir = self._resolve_dir('id_registry_dir', idregdir, self.workdir, 'idregs')
        self._minters = {}

    def _get_id_shoulder(self, who, sipid: str, create: bool):
        """
        determine the ID shoulder to be associated with a service request.  The ID shoulder (the prefix 
        to the local part of our identifiers) serves as a particular account for the client under which 
        this request will operate.  It determines the configuration used by the bagger that will assemble 
        the SIP.  This will raise an UnauthorizedPublishingRequest if client (given by who) is requesting
        a shoulder (as specified by sipid) they are not authorized for.  

        :param PubAgent who:  the user agent making the request
        :param str    sipid:  the requested SIP ID
        :param bool  create:  True if the user is requesting the publishing of a new SIP; False if 
                              requesting an update to a previously published SIP.
        """
        # return an ID shoulder to mint an ID under given the permissions configured for the
        # given client (who)

        out = None
        client_ctl = self.cfg.get('clients', {}).get(who.group)
        if client_ctl is None:
            client_ctl = self.cfg.get('clients', {}).get("default")
        if client_ctl is None:
            raise UnauthorizedPublishingRequest("No default permissions available for client group, "+
                                                who.group)

        if sipid:
            # sipid must begin with a shoulder name (or the form NAME: or NAME-)
            m = re.search(r'^([a-zA-Z]\w+)([:\-])', sipid)
            if not m:
                raise BadSIPInputError("Illegal SIP identifier requested: "+sipid)
            out = m.group(1)
            isclientid = m.group(2) == ':'

            # is client allowed to specify its own local id portion to mint?
            if isclientid and create and not client_ctl.get('localid_provider'):
                raise UnauthorizedPublishingRequest(
                    "Client group, %s, is not allowed to request new SIP ID: %s"
                    % (who.group, sipid)
                )
        else:
            # client is requesting a shoulder to be assigned
            out = client_ctl.get('default_shoulder')
            if not out:
                raise UnauthorizedPublishingRequest(
                    "No default shoulder permitted for %s under SIP-type=%s"
                    % (who.group, self.convention)
                )

        shoulder = self.cfg.get('shoulders', {}).get(out)
        if not shoulder:
            self.log.warning("No handler configured for SIP shoulder=%s", out)
        if not shoulder or who.group not in shoulder.get('allowed_clients', []):
            isdefault = "default " if out == client_ctl.get('default_shoulder') else ""
            raise UnauthorizedPublishingRequest(
                "Client group '%s' is not permitted to publish to %sSIP shoulder, %s"
                % (who.group, isdefault, out)
            )

        return out

    def _set_identifiers(self, nerdm, minter, sipid):
        data = {'sipid': sipid}
        pdrid = None
        if sipid:
            matches = minter.search(data)
            if len(matches) > 1:
                raise PublishingStateException("Multiple IDs have been registered for sipid="+sipid)
            elif len(matches) > 0:
                pdrid = matches[0]

        if not pdrid:
            pdrid = minter.mint(data)

        nerdm['@id'] = pdrid
        if not sipid:
            iddata = minter.datafor(pdrid)
            if iddata.get('sipid'):
                sipid = iddata.get('sipid')
            else:
                sipid = ARK_PFX_RE.sub('', pdrid)

        nerdm['pdr:sipid'] = sipid
        nerdm['pdr:aipid'] = ARK_PFX_RE.sub('', pdrid)

        return sipid

    def _create_ingest_service(self, ):
        return None

    def _create_bagger(self, shoulder: str, sipid: str, minter=None):

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
            return factory(sipid=sipid, siptype=shoulder, config=bgrcfg, minter=minter)
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

        if inspect.isclass(func) and hasattr(func, 'create') and hasattr(getattr(func, 'create'), '__call__'):
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
        if not os.path.abspath(regdir):
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

    def create(self, sipid, siptype: str, config: Mapping=None, minter=None) -> SIPBagger:
        """
        create a new instantiation of an SIPBagger that can process an SIP of the given type.  If config
        is provided, it may get merged in some way with the configuration set at construction time before
        being applied to the bagger.

        :param           sipid:  the ID for the SIP to create a bagger for; this is usually a str, 
                                 subclasses may support more complicated ID types.
        :param str     siptype:  the name given to the SIP convention supported by the SIP reference by sipid
        :param Mapping  config:  bagger configuration parameters that should override the default
        :param IDMinter minter:  an IDMinter instance that should be used to mint a new PDR-ID
        """
        bgrcfg = self.cfg.get('bagger')
        if bgrcfg:
            config = cfgmod.merge_config(bgrcfg, deepcopy(config))
        return PDPBagger(sipid, config, minter, self.prepsvc, siptype)

