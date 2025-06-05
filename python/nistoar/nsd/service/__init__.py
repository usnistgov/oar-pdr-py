"""
module providing implementations of an staff directory service.  A 
:py:class:`~nistoar.nsd.service.base.PeopleService` speaks directly to a backend storage system
to return staff and organization metadata.  The :py:mod:`.wsgi module<nistoar.nsd.wsgi>` is responsible 
for exposing a service through a web interface.  
"""
from collections.abc import Mapping

from .base import PeopleService
from .mongo import MongoPeopleService

def create_people_service(config):
    """
    instantiate a :py:class:`PeopleService` instance based on the given configuration
    """
    if not isinstance(config, Mapping):
        raise ConfigurationException("people_service config: not a dictionary: "+str(config))

    if config.get("factory") == "mongo":
        dburl = config.get("db_url")
        if not dburl:
            raise ConfigurationException("Missing required config param: people_service.db_url")
        # logging.getLogger("nsd.create_people_service").debug("Creating a MongoPeopleService")
        return MongoPeopleService(dburl)

    elif config.get("factory") == "files":
        from .files import FilesBasedPeopleService
        return FilesBasedPeopleService(config)

    elif config.get("factory"):
        raise ConfigurationException("people_service.factory type not supported: "+config["factory"])

    return None

