"""
a module provides specialized :py:class:`~nistoar.midas.dbio.restore.ProjectRestorer` implementations 
specifically for the DAP service.
"""
from __future__ import annotations
from collections import OrderedDict
from typing import Mapping
from logging import Logger

from nistoar.pdr.utils.prov import Action, Agent
from nistoar.base.config import ConfigurationException
from ..dbio.base import (ProjectRecord, DBClient, ACLs, DBIOException, DBIORecordException,
                         NotAuthorized, ObjectNotFound)
from ..dbio.restore import ProjectRestorer, URLRestorer

class AIPRestorer(ProjectRestorer):
    """
    a project restorer that can restore DAP records based on its AIP id.

    This implementation pulls the latest version of the NERDm data from the public repository.

    _Note: a future version may alternatively pull from the archive AIP (specifically, the latest
    archived head bag)._

    This implementation looks for the following configuration parameters:

    ``nerdm_resolver``
         a dictionary contain the configuration for the resolver service that can return 
         the last published NERDm metadata record for a given ID.  Currently, only one subparameter
         expected--``service_endpoint``--that provides the service base URL.  If this is not 
         provided, ``pdr_home_url`` must be provided.  
    ``pdr_home_url``
         a base URL for the public home of the PDR.  If this is provided instead of ``nerdm_resolver``,
         the resolver endpoint will be determined assuming the PDR's public resolver.  
    """

    def __init__(self, aipid: str, config: Mapping, log: Logger=None):
        """
        instantiate the restorer given an AIP ID
        """
        super(AIPRestorer, self).__init__()
        self._aipid = aipid
        self.cfg = config

        ep = None
        if self.cfg.get('nerdm_resolver', {}).get('service_endpoint'):
            ep = self.cfg['nerdm_resolver']['service_endpoint'].rstrip('/')
        elif self.cfg('pdr_home_url'):
            ep = self.cfg['pdr_home_url'].rstrip('/')
            ep += "/od/id"
        else:
            raise ConfigurationException("Missing required configuration parameter: "+
                                         "nerdm_resolver.service_endpoint")

        # Note that we have chosen an delegation pattern here (rather than inheritance) because
        # a future implementation may in corporate a second delegate along side this one.
        self._urestorer = URLRestorer(ep+'/'+self._aipid, self._aipid, log)

    @property
    def aipid(self):
        """
        the AIP identifier that this restorer is set to restore
        """
        return self._aipid

    def recover(self):
        self._urestorer.recover()

    def free(self):
        self._urestorer.free()

    def get_data(self):
        return self._urestorer.get_data()

    def restore(self, prec: ProjectRecord, dofree: bool=False) -> bool:
        self._urestorer.restore(prec, dofree)

    @classmethod
    def from_archived_at(cls, locurl: str, dbcli: DBClient,
                         config: Mapping, log: Logger=None) -> AIPRestorer:
        """
        instantiate a DBIORestorer given an ``archived_at`` URL.  

        :param str locurl:  the ``archived_at`` URL for the published project record to restore.  
                            This _must_ have the form, "aip:_aipid_"
        :param DBClient dbcli:  the DBClient for the draft project record that will be restored;
                            In this implementation, this is ignored and can, thus, can be None.
        :param dict config: The configuration for the restorer
        :param Logger log:  a Logger to use for messages; if not provided, a default will be used
                            if needed
        :rtype: DBIORestorer
        :raises ValueError:  if locurl does not comply with the proper URL form
        """
        if not locurl.startswith('aip:'):
            raise ValueError("Not an AIP URL: "+locurl)
        return cls(locurl[len('aip:'):], config, log)

    

    
