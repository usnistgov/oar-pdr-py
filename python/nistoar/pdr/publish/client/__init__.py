"""
Client classes that understand the PDR publishing web services.  

Currently, this only includes support for the Programmatic Data Publishing (PDP) web service.
"""
from collections.abc import Mapping

from .pdp import PDPPublishingClient

_client_classes = {
    "pdp": PDPPublishingClient
}
_client_classes['def'] = _client_classes['pdp']

def create_publishing_client(config: Mapping):
    """
    a factory function that creates a publishing web service client.

    In its given configuration, this factory function looks for the ``type`` property which 
    provides a name for the type of client to create.  If ``type`` is not specified, a
    default type is assumed.  

    Currently, only the :py:class:`~nistoar.pdr.publish.client.pdp.PDPPublishingClient` is 
    supported via the type name, ``pdp`` (which is also the default).

    :raises ConfigurationException: if the given configuration is missing properties need for 
                                    the implied type.
    :return: the publishing client instance 
             (:py:class:`~nistoar.pdr.publish.client.pdp.PDPPublishingClient`) or None, if 
             the configuration is empty or None or the type is not recognized.
    """
    if not config:
        return None

    type = config.get('type')
    if not type:
        type = 'def'

    cls = _client_classes.get(type)
    if not cls:
        raise ConfigurationException("publish: type not supported: "+type)

    return cls(config)
    
        
    
    
