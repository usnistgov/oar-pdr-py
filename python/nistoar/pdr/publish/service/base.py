"""
Base classes for implementing the publishing process according to a particular convention.
"""
from abc import ABCMeta, abstractmethod, abstractproperty

PubState = {
    NOT_FOUND:  "not found",    # SIP has not been created
    AWAITING:   "awaiting",     # SIP requires an update before it can be published
    PENDING:    "pending",      # SIP has been created/updated but not yet published
    PROCESSING: "processing",   # The SIP contents are being processed; further actions are not possible
                                #  until processing completes.
    PUBLISHED:  "published",    # SIP was successfully published
    FAILED:     "failed"        # an attempt to publish was made but failed due to an unexpected state
}

class PublishingService(object, metaclass=ABCMeta):
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

    @abstracemethod
    def state_of(self, sipid):
        """
        return the current state of the SIP with the given identifier
        :param str sipid:  the identifier for the SIP of interest
        :return: a PubState value
        :rtype: str
        """
        raise NotImplementedError()

    def history_of(self, sipid):
        """
        return the known history of the SIP.  This should go back at least to just after it was 
        last published.  
        :param str sipid:  the identifier for the SIP of interest
        """
        return []

    def publish(self, sipid):
        """
        submit the SIP for ingest and preservation into the PDR archive.  The SIP needs to be in 
        the PENDING state.  
        :param str sipid:  the identifier for the SIP of interest
        :raises PublishingStateException:  if the SIP is not in the PENDING state
        """
        raise NotImplementedError()

    def _initialize(self, sipid, who=None):
        """
        initialize the persisted presence of the SIP.  The SIP needs to be in either the NOT_FOUND or 
        PUBLISHED state.
        :param str sipid:  the identifier for the SIP of interest
        :param who:         an actor identifier object, indicating who is requesting this action.  This 
                            will get recorded in the history data.  If None, an internal administrative 
                            identity will be assumed.  
        :raises PublishingStateException:  if the SIP is not in the PENDING state
        """
        raise NotImplementedError()

    def _accept_update(self, sipid):
        """
        process the latest updates to the SIP.  The status after completion should be PENDING
        :param str sipid:  the identifier for the SIP of interest
        """
        pass

    def describe(self, id, withcomps=True):
        """
        returns a NERDm description of the entity with the given identifier.  If the identifier 
        points to a resource, A NERDm Resource record is returned.  If it refers to a component
        of an SIP, a Component record is returned.  
        :rtype object:
        """
        return {}

class SimpleNerdmPublishingService(PublishingService):
    """
    a PublishingService that is updated by submitting NERDm metadata.  By itself, this interface
    does not support adding data files to the SIP.  Data file components are restricted to those 
    with a downloadURL pointing outside the PDR.  
    """

    @abstractmethod
    def accept_resource_metadata(self, nerdm, who=None):
        """
        create or update an SIP for submission.  If the record does not have an "@id" property,
        a new SIP will be created and an identifier will be assigned to it; otherwise, the metadata
        will typically be considered an update to the SIP with the identifier given by the "@id" property.  
        Some implementations may allow the caller to create a new SIP with the given identifier if the 
        SIP does not already exist; if this is not allowed, an exception is raised.  The metadata that is 
        actually persisted may be modified from the submitted metadata according to the SIP convention.

        The SIP must not be in the PROCESSING FAILED state when this method is called.  

        :param dict nerdm:  a NERDm Resource object; this must include an "@type" property that includes 
                            the "Resource" type.
        :param who:         an actor identifier object, indicating who is requesting this action.  This 
                            will get recorded in the history data.  If None, an internal administrative 
                            identity will be assumed.  This identity may affect the identifier assigned.
        :raises NERDError:  if the input metadata cannot be interpreted as proper NERDm Resource metadata
        :raises PublishingStateException:  if the SIP is not in a correct state to accept the metadata
        """
        raise NotImplementedError()

    @abstractmethod
    def upsert_component_metadata(self, sipid, cmpmd, who=None):
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
        :raises NERDError:  if the input metadata cannot be interpreted as proper NERDm Component metadata
        :raises PublishingStateException:  if the SIP is not in a correct state to accept the metadata
        """
        raise NotImplementedError()

