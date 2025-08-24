"""
a subpackage that defines an interface and implementations for interacting with external 
review systems

The :py:class:`~nistoar.midas.dap.extrev.base.ExternalReviewClient` base class defines 
the interface.
"""
from .base import *
from .nps1 import NPSExternalReviewClient
# from .sim import SimulatedExternalReviewClient
from nistoar.base.config import ConfigurationException

__all__ = [ "ExternalReviewClient", "ExternalReviewException", "create_external_review_client" ]

_client_classes = {
    NPSExternalReviewClient.system_name:        NPSExternalReviewClient
#   SimulatedExternalReviewClient.system_name:  SimulatedExternalReviewClient
}

def create_external_review_client(config: Mapping) -> ExternalReviewClient:
    """
    a factory function that creates a client for submitting DAP records for external review.  

    This is called by :py:meth:`create_service_for` to inject an 
    :py:class:`~nistoar.midas.dap.extrev.ExternalReviewClient` into its 
    :py:class:`DAPService`.  If ``config`` is not give or is empty, None is returned;
    this will cause external review to be disabled and not required for record publication.

    If non-empty, the given configuration must include the ``name`` parameter, which identifies
    which class will be instantiated.  All other configuration paramters are class-dependent.

    :raises ConfigurationException: if the given, non-empty configuration lacks a ``name`` or its
                                    value is unrecognized.
    parameter.  
    """
    if not config:
        return None

    if not config.get('name'):
        raise ConfigurationException("external_review: missing required name parameter")

    cls = _client_classes.get(config['name'])
    if not cls:
        raise ConfigurationException("external_review: name not supported: "+config['name'])

    return cls(config)


