"""
a module for accessing public metadata about PDR objects via the Resource 
Metadata Manager (RMM).  
"""
import os, sys, shutil, logging, json, re
from collections import OrderedDict
from collections.abc import Mapping

import requests

from . import altbig, rmm
from ..exceptions import PDRServiceException, PDRServerError, IDNotFound, StateException
from .. import constants as const

_ark_id_re = re.compile(const.ARK_ID_PAT)
VER_DELIM  = const.RELHIST_EXTENSION

class MetadataClient(object):
    """
    a client interface for retrieving metadata via the RMM or from a file.  This addresses a 
    MongoDB limitation on the size of a document within a collection (given the RMM's current 
    collection model).  If a NERDm record is too big for the RMM, a component-less version is 
    stored instead and the full version is kept on disk.  This client will leverage both 
    `MetadataClient` instances from the :py:module:`~nistoar.pdr.describe.rmm` and 
    :py:module:`~nistoar.pdr.describe.altbig` modules to deliver complete record.  
    """

    def __init__(self, baseurl: str, cachedir: str=None):
        """
        setup the client
        :param str baseurl:   the base URL for the RMM service
        :param str cachedir:  the root directory for the alternate file cache, if not provided
                              records will only be retrieved from RMM.
        """
        self._rmmcli = rmm.MetadataClient(baseurl)
        self._altcli = None
        if cachedir:
            self._altcli = altbig.MetadataClient(cachedir)

    def alt_record_exists(self, id: str, version: str=None):
        if not self._altcli:
            return False
        return self._altcli.exists(id, version)

    def describe(self, id: str, version: str=None) -> Mapping:
        """
        return the NERDm metadata describing the data entity with the given ID.  The identifier
        can refer to a dataset, a version of a dataset, a dataset release history, or a dataset
        component (e.g. a file in a dataset).  
        :param str id:  the identifier for the desired item.  It must begin with an "ark:" prefix
                        unless it is an (old-style, >30-char) EDI ID.
        :param str version:  a particular version of the dataset.  If the given `id` already 
                        refers to a particular version, this parameter is ignored.  If not given,
                        then the ID determines if a particular version or the latest version is 
                        retrieved.  
        :return:  the NERDm metadata describing the identified thing
                  :rtype: Mapping
        :raises IDNotFound:  if the identifier is unknown
        """
        vers_specified = (bool(version) and version != 'latest') or VER_DELIM in id
        if not vers_specified and self.alt_record_exists(id, version):
            return self._altcli.describe(id, version)
        out = self._rmmcli.describe(id, version)
        if vers_specified and not id.endswith(VER_DELIM) and \
           self.alt_record_exists(id, out.get('version', '0')):
            return self._altcli.describe(id, version)
        return out

    def search(self, query=None, latest=True):
        """
        return the NERDm record matching a given query.  All records are returned if a query is not 
        provided.  

        [Document RMM search paramters]

        :param dict query:  a dictionary whose properties specify the RMM search query parameters by name
        :param bool latest: if True (default), only the latest versions of resources will be returned; 
                            otherwise, all matching versions will be returned.
        """
        return self._rmmcli.search(query, latest)
