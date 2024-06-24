"""
Base classes for implementing the publishing process according to a particular convention.
"""
import re, logging
from abc import ABCMeta, abstractmethod, abstractproperty
from collections.abc import Mapping
from ...utils.prov import Agent
from .. import PublishSystem, PublishingStateException, ConfigurationException
from ....nerdm.constants import core_schema_base as NERDM_SCHEMA_BASE, CORE_SCHEMA_URI
from ....nerdm import validate
from .. import PublishSystem
from .... import pdr

RESOURCE_SCHEMA_TYPE = "#/definitions/Resource"
COMPONENT_SCHEMA_TYPE = "#/definitions/Component"

class PublishingService(PublishSystem, metaclass=ABCMeta):
    """
    the base class for all publishing services.  This interface models an SIP as a resource that:
      * is identified uniquely within the PDR system via an SIP identifier
      * can be created in an unpublished state
      * can be published as a separate step out to the PDR archive
      * can be updated prior to or after the publication; an update returns the SIP to an unpublished state

    This model necessitates that the state of the SIP must be persisted between calls to this service;
    typically, it needs to be persisted across instantiations of the service.  

    All transitions are accompanied by an actor object that identfies who is responsible for the update
    """

    def __init__(self, convention, config=None, baselog=None):
        """
        initialize the base class data for a particular SIP convention.  

        This implementation creates the JSON-schema validator used to validate input NERDm metadata.

        :param str convention:  the label for the SIP convention (also referred to as the SIP type) that 
                                this class supports.
        :param dict config:     the service configuration
        :param Logger baselog:  the Logger to derive this instance's Logger from; this constructor will 
                                call getChild() on this log to instantiate its Logger.
        """
        PublishSystem.__init__(self)
        if not config:
            config = {}
        if not isinstance(config, Mapping):
            raise ValueError("PrePubMetadataService: config argument not a " +
                             "dictionary: " + str(config))
        self.cfg = config
        self._conv = convention

        if not baselog:
            baselog = logging.getLogger()
        self.log = baselog.getChild(self.subsystem_abbrev).getChild(self._conv)

    @property
    def convention(self):
        return self._conv

    def state_of(self, sipid):
        """
        return the current state of the SIP with the given identifier
        :param str sipid:  the identifier for the SIP of interest
        :return: a status state value, one of NOT_FOUND, AWAITING, PENDING, PROCESSINNG, FIMALIZED, 
                 PUBLISHED, or FAILED
        :rtype: str
        """
        return self.status_of(sipid).state

    @abstractmethod
    def status_of(self, sipid):
        """
        return the current status  of the SIP with the given identifier
        :param str sipid:  the identifier for the SIP of interest
        :return: an object describing the current status of the idenfied SIP
        :rtype: SIPStatus
        """
        raise NotImplementedError()

    def history_of(self, sipid):
        """
        return the known history of the SIP.  This should go back at least to just after it was 
        last published.  
        :param str sipid:  the identifier for the SIP of interest
        """
        return []

    @abstractmethod
    def publish(self, sipid):
        """
        submit the SIP for ingest and preservation into the PDR archive.  The SIP needs to be in 
        the PENDING state.  

        Implementations should call finalize() (if necessary) before submission and delete() after 
        successful submission of the SIP.

        :param str sipid:  the identifier for the SIP of interest
        :raises SIPNotFoundError:   if the SIP is in the NOT_FOUND state
        :raises SIPStateException:  if the SIP is not in the PENDING state
        """
        raise NotImplementedError()

    @abstractmethod
    def finalize(self, sipid):
        """
        process all SIP input to get it ready for publication.  This may cause the SIP metadata to be 
        updated, which will subsequently effect what is returned by :py:method:`describe`.  This may 
        take a while, during which the SIP should be in the PROCESSING state.  Upon successful completion, 
        the state should be set to FINALIZED.  If an error caused by the collected SIP input occurs, the 
        state should be set to FAILED to indicate that the client must provide updated input to fix the 
        problem and make the SIP publishable.  

        :param str sipid:  the identifier for the SIP of interest
        :raises SIPNotFoundError:   if the SIP is in the NOT_FOUND state
        :raises SIPStateException:  if the SIP is not in the PENDING or FINALIZED state
        """
        raise NotImplementedError()

    @abstractmethod
    def delete(self, sipid):
        """
        delete the presence of the SIP from this service.  This should be called automatically by the 
        publish() method after successful submission of the SIP for publication; however, clients can 
        call this in advance of this.  This will purge any unpublished artifacts of the SIP from the 
        service's internal cache and revert its state back to either PUBLISHED or NOT_FOUND, depending 
        on its previous publishing status.  If there are no such artifacts, this method does nothing. 

        :param str sipid:  the identifier for the SIP of interest
        :raises SIPNotFoundError:   if the SIP is in the NOT_FOUND state
        :raises PublishingException:  if the deletion operation otherwise fails
        """
        raise NotImplementedError()

    def describe(self, id, withcomps=True):
        """
        returns a NERDm description of the entity with the given identifier.  If the identifier 
        points to a resource, A NERDm Resource record is returned.  If it refers to a component
        of an SIP, a Component record is returned.  
        :rtype Mapping:
        """
        return {}

class SimpleNerdmPublishingService(PublishingService):
    """
    a PublishingService that is updated by submitting NERDm metadata.  By itself, this interface
    does not support adding data files to the SIP.  Data file components are restricted to those 
    with a downloadURL pointing outside the PDR.  

    This base class will look for the following parameters in the configuration:
    :param bool validate_nerdm:  If True (default), input NERDm metadata will be validated before 
                                 being accepted, raising a ValidationError exception if the 
                                 metadata is not valid.  
    :param str nerdm_schema_dir: the path to the directory containing NERDm schema files used to 
                                 validate input metadata; if not set, the default OAR schema 
                                 directory (e.g. the OAR system's etc/schemas directory).
    """

    def __init__(self, convention, config=None, baselog=None, minnerdmver=(0, 4)):
        """
        initialize the base class data for a particular SIP convention.  

        This implementation creates the JSON-schema validator used to validate input NERDm metadata.

        :param str convention:  the label for the SIP convention (also referred to as the SIP type) that 
                                this class supports.
        :param dict config:     the service configuration
        :param Logger baselog:  the Logger to derive this instance's Logger from; this constructor will 
                                call getChild() on this log to instantiate its Logger.
        :param tuple minnerdmver:  a tuple of ints specifying the minimum version of the NERDm schema
                                that provided NERDm records must be compliant with.
        """
        super(SimpleNerdmPublishingService, self).__init__(convention, config)

        self._schemadir = self.cfg.get('nerdm_schema_dir', pdr.def_schema_dir)
        self._valid8r = None
        if self.cfg.get('validate_nerdm', True):
            if not self._schemadir:
                raise ConfigurationException("'validate_nerdm' is set but cannot find schema dir")
            self._valid8r = validate.create_validator(self._schemadir, "_")

        self._minnerdmver = minnerdmver
    

    @abstractmethod
    def accept_resource_metadata(self, nerdm: Mapping, who: Agent=None, sipid: str=None, create: bool=None):
        """
        create or update an SIP for submission.  By default, a new SIP will be created if the input 
        record is does not have an "@id" property, and an identifier is assigned to it; otherwise,
        the metadata provided will be considered an update to the SIP with that identifier.  This 
        behavior can be overridden with the sipid and create parameters.  Some implementations may 
        allow the caller to create a new SIP with the given identifier if it doesnot exist; if this 
        is not allowed, an exception is raised.  The metadata that is actually persisted may be 
        modified from the submitted metadata according to the SIP convention.  The metadata that is 
        actually persisted may be modified from the submitted metadata according to the SIP convention.

        The SIP must not be in the PROCESSING state when this method is called.  

        :param dict nerdm:  a NERDm Resource object; this must include an "@type" property that includes 
                            the "Resource" type.
        :param who:         an actor identifier object, indicating who is requesting this action.  This 
                            will get recorded in the history data.  If None, an internal administrative 
                            identity will be assumed.  This identity may affect the identifier assigned.
        :param str sipid:   If provided, assume this to be the SIP's identifier.  If creating a new SIP,
                            then the value does not require the ARK prefix; the actual SIP assigned 
                            maybe modified from this input (see response).  If not provided, the SIP ID
                            will taken from the "@id" property.
        :param bool create: if True, assume this is a request to create a new SIP; if an SIP with the 
                            specified ID already exists, an error is raised.  If False, assume this is
                            an update; if the SIP doesn't exist, an error is raised.  If not provided,
                            the intent is determined based on whether an SIP ID is specified (either 
                            via the sipid parameter or the "@id" property in the nerdm object).

        :return:  the actual SIP ID assigned to the SIP; if the call to this method was an update, 
                  the value will be the same was that provide in the input.

        :raises NERDError:  if the input metadata cannot be interpreted as proper NERDm Resource metadata
        :raises SIPConflictError:  if the SIP is in the PROCESSING state when this method was called
        :raises SIPStateException:  if the SIP is not in an illegal state to accept the metadata
        """
        raise NotImplementedError()

    @abstractmethod
    def upsert_component_metadata(self, sipid: str, cmpmd: Mapping, who: Agent=None):
        """
        add or update a component of the NERDm resource with the provided metadata.  If the record does not 
        have an "@id" property, a new component will be create and a component identifier will be assigend
        to it; otherwise, the metadata typically be considered an update to the component with the identifier 
        given by the "@id" property.  Some implementations may allow the caller to create a new component with 
        the given identifier if the component does not already exist; if this is not allowed, an exception is 
        raised.  The metadata that is actually persisted may be modified from the submitted metadata 
        according to the SIP convention.

        The SIP must not be in the NOT_FOUND, PROCESSING, nor FAILED state when this method is called.  

        :param str sipid:   the identifier for the SIP resource to be updated
        :param dict cmpmd:  the metadata describing the resource component.  It must have a "@type" 
                            property with a recognized type.
        :param who:         an actor identifier object, indicating who is requesting this action.  This 
                            will get recorded in the history data.  If None, an internal administrative 
                            identity will be assumed.  This identity may affect the identifier assigned.

        :return:  the relative id assigned to the component.   

        :raises NERDError:  if the input metadata cannot be interpreted as proper NERDm Component metadata
        :raises SIPConflictError:  if the SIP is in the wrong state for this method to accept metadata
        :raises SIPStateException:  if the SIP is not in an illegal state to accept the metadata
        """
        raise NotImplementedError()

    def validate_json(self, json, schemauri):
        """
        validate the given JSON data record against the give schema, raising an exception if it 
        is not valid.

        :param dict json:      the (parsed) JSON data to validate
        :param str schemauri:  the JSONSchema URI to validate the input against, Typically, this is 
                                  either NERDM_RESOURCE_SCHEMAA or NERDM_COMPONENT_SCHEMAA, 
                                  depending on whether the input is resource- or component-level 
                                  metadata.

        :raises ValidationError: if the given (parsed) JSON data is not compliant with the given schema
        """
        if self._valid8r:
            self._valid8r.validate(json, schemauri=schemauri, strict=True, raiseex=True)
        else:
            self.log.warning("Unable to validate submitted NERDm data")

    def validate_res_nerdm(self, nerdm):
        """
        validate the given NERDm resource metadata, raising an exception if it is not for the 
        current SIP convention.

        This implementation does the basic JSONSchema-based validation.  Subclasses may override to 
        do additional checks appropriate for the SIP convention supported by this class.  

        :param dict nerdm:     the NERDm metadata to validate

        :raises ValidationError: if the given (parsed) JSON data is not a valid NERDm Resource record
        :raises NERDError: if the given record is otherwise insufficient for the SIP convention.  This
                           implementation raises this exception if the input record does not label itself
                           as a Resource record in its "_schema" property.  
        """
        schemauri = self.check_schema_uri(nerdm, RESOURCE_SCHEMA_TYPE)
        self.validate_json(nerdm, schemauri)

    def validate_comp_nerdm(self, nerdm):
        """
        validate the given NERDm component metadata, raising an exception if it is not for the 
        current SIP convention.

        This implementation does the basic JSONSchema-based validation.  Subclasses may override to 
        do additional checks appropriate for the SIP convention supported by this class.  

        :param dict nerdm:     the NERDm metadata to validate

        :raises ValidationError: if the given (parsed) JSON data is not a valid NERDm Component record
        :raises NERDError: if the given record is otherwise insufficient for the SIP convention.  This
                           implementation does not actually raise this exception type; however, subclasses
                           may.
        """
        schemauri = self.check_schema_uri(nerdm, COMPONENT_SCHEMA_TYPE)
        self.validate_json(nerdm, schemauri)

    def check_schema_uri(self, nerdm, schematype):
        """
        ensure that the schema type encoded in the given NERDm record (via its "_schema" property) is 
        compatible with the given NERDm object type and return the fully qualified schema URI.  If 
        "_schema" is not set, the latest supported version is assumed.  

        In this implementation, the schema specified in the "_schema" property (if set) must match
        the core NERDm schema base ("https://data.nist.gov/od/dm/nerdm-schema/").  It must also have 
        a minimum version of v0.4; subclasses may insist on a tighter restriction.

        :param dict nerdm:      the input NERDm record to examine
        :param str schematype:  the type definition pointer, usually of the form "#/definitions/[typename]"

        :raises NERDError:  if the implied schema found not sufficiently compatible with the requested type
        """
        schema = nerdm.get("_schema")
        if not schema:
            schema = CORE_SCHEMA_URI
        schema = schema.rstrip("#")

        m = re.search(r"^"+NERDM_SCHEMA_BASE+"v(\d+(\.\d+)*)", schema)
        if not m:
            raise NERDError("Unsupported base schema specified: "+schema)
        try:
            ver = [int(v) for v in m.group(1).split(".")]
        except ValueError:
            raise NERDError("Unsupported NERDm schema version: v"+m.group(1))

        for i in range(len(ver)):
            if i >= len(self._minnerdmver):
                break
            if ver[i] < self._minnerdmver[i]:
                raise NERDError("Specified NERDm schema version, " + m.group(1) +
                                " does not meet minimum requirement of " + ".".join(self._minnerdmver))

        if '#' not in schema:
            schema += schematype
        elif not schema.endswith(schematype):
            raise NERDError("Specified record type NERDm schema is incompatible with "+
                            schematype + ": " + schema)

        return schema


