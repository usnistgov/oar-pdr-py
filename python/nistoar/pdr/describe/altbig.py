"""
a module for accessing public metadata about PDR objects via the Resource 
Metadata Manager (RMM).  
"""
import os, sys, shutil, logging, json, re
from collections import OrderedDict
from collections.abc import Mapping

import requests

from ..exceptions import PDRServiceException, PDRServerError, IDNotFound, StateException
from .. import constants as const
from nistoar.nerdm.utils import Version
from nistoar.pdr.utils import read_json

_ark_id_re = re.compile(const.ARK_ID_PAT)
_dlurl_ver_re = re.compile(r'/_v/\d+\.\d+\.\d+')
VER_DELIM  = const.RELHIST_EXTENSION
FILE_DELIM = const.FILECMP_EXTENSION
LINK_DELIM = const.LINKCMP_EXTENSION
AGG_DELIM  = const.AGGCMP_EXTENSION
OLD_COMP_DELIM = "/cmps"

class MetadataClient(object):
    """
    a client interface for retrieving metadata from flat JSON files on disk.  The purpose of this 
    "database" is to hold records that are too big for the MongoDB database with its current 
    collection model.  This will work best when it only contains a few records.  

    In this implementation, the files are found under a single directory.  Each file contains a
    full NERDm Resource record (with components) with a file name of the form "[PDRID]-v[VER].json"
    (where VER is an _-delimited version).  
    """
    _fname_re = re.compile(r'-v(\d+_\d+_\d+).json$')

    def __init__(self, cachedir: str):
        """
        :param str cachedir:  directory where the NERDm JSON files can be found.
        """
        self._root = cachedir
        if not os.path.isdir(self._root):
            raise StateException("Metadata directory: %s: not found" % self._root)

        # index the contents
        self._versions = {}
        self._ediids = {}
        # self._index_cache()

    def _index_cache(self):
        latest = {}
        for k in self._versions:
            if 'latest' in self._versions[k]:
                f = os.path.basename(self._versions[k]['latest'])
                m = self._fname_re(f)
                if m:
                    latest[k] = Version(m.group(1).replace('_', '.'))

        for f in os.listdir(self._root):
            m = self._fname_re.search(f)
            if m:
                id = f[:m.start(0)]
                ver = m.group(1).replace('_', '.')
                f = os.path.join(self._root, f)
                if id not in self._versions:
                    self._versions[id] = {}
                self._versions[id][ver] = f

                ver = Version(ver)
                if id not in latest or latest[id] < ver:
                    latest[id] = ver

                if len(id) > 30:
                    # Note: ediid used in name; we should avoid this issue for performance reasons
                    ediid = id
                    try:
                        nerdm = read_json(f)
                        idm = _ark_id_re.match(nerdm.get('@id',''))
                        if idm:
                            id = idm.group(const.ARK_ID_DS_GRP)
                            self._ediids[id] = id
                            if id not in self._versions:
                                self._versions[id] = {}
                            self._versions[id][str(ver)] = f

                            if id not in latest or latest[id] < ver:
                                latest[id] = ver
                    except (ValueError, TypeError):
                        # skip corrupted file
                        pass

        for id in latest:
            if id not in self._versions:
                self._versions[id] = {}
            if str(latest[id]) in self._versions[id]:
                self._versions[id]['latest'] = self._versions[id][str(latest[id])]

    def describe(self, id: str, version: str=None) -> Mapping:
        """
        return the NERDm metadata describing the data entity with the given ID.  The identifier
        can refer to a dataset, a version of a dataset, a dataset release history, or a dataset
        component (e.g. a file in a dataset).  
        :param str id:  the identifier for the desired item.  If it does not start with "ark:",
                        the respository's native ARK base is assumed. 
        :param str version:  a particular version of the dataset.  If the given `id` already 
                        refers to a particular version, this parameter is ignored.  If not given,
                        then the ID determines if a particular version or the latest version is 
                        retrieved.  
        :return:  the NERDm metadata describing the identified thing
                  :rtype: Mapping
        :raises IDNotFound:  if the identifier is unknown
        """
        find = id.rstrip('/')  # trailing slash treated as superfluous
        if find.endswith(FILE_DELIM):
            # for now, treat an ID ending in "/pdr:f" equivalently to a dataset id
            find = find[:-1*len(FILE_DELIM)]

        if not find.startswith("ark:"):
            find = "ark:/" + const.ARK_NAAN + '/' + find

        idm = _ark_id_re.match(find)
        if not idm:
            # don't bother if it's not a compliant ARK ID
            raise IDNotFound(id)

        if not idm.group(const.ARK_ID_PATH_GRP) and not idm.group(const.ARK_ID_PART_GRP):
            # it appears to be simply a dataset ID (there's nothing past the dataset part)
            return self._describe_version(idm.group(const.ARK_ID_DS_GRP), version, id)

        if idm.group(const.ARK_ID_PATH_GRP) == VER_DELIM:
            # ends with "/pdr:v" 
            return self._describe_releases(idm.group(const.ARK_ID_DS_GRP), id)

        if not idm.group(const.ARK_ID_PART_GRP) and idm.group(const.ARK_ID_PATH_GRP).startswith(VER_DELIM+'/'):
            fields = idm.group(const.ARK_ID_PATH_GRP).split('/', 3)
            version = fields[2]
            if len(fields) < 4:
                # we want a version of a dataset
                return self._describe_version(idm.group(const.ARK_ID_DS_GRP), version, id)

        return self._describe_component(idm, version, id)

    def _describe_releases(self, id, reqid=None):
        if not reqid:
            reqid = id

        rec = self._describe_latest_ds(id, reqid)

        if not rec or not 'releaseHistory' in rec:
            raise IDNotFound(reqid)

        if 'components' in rec:
            del rec['components']
        rec['hasRelease'] = rec['releaseHistory'].get('hasRelease',[])
        rec['@id'] = rec['@id'] + const.RELHIST_EXTENSION
        return rec
        

    def _describe_version(self, id, version, reqid=None):
        if not reqid:
            reqid = id

        if not version:
            version = 'latest'

        out = self._get(id, version, reqid)
        idm = _ark_id_re.match(out.get('@id',''))
        if version == 'latest':
            if idm and idm.group(const.ARK_ID_PATH_GRP):
                out['@id'] = out['@id'][:idm.start(const.ARK_ID_PATH_GRP)]
            for cmp in out.get('components', []):
                if cmp.get('downloadURL'):
                    cmp['downloadURL'] = _dlurl_ver_re.sub('', cmp['downloadURL'])
        elif idm and not idm.group(const.ARK_ID_PATH_GRP):
            out['@id'] += VER_DELIM + '/' + version
            latest_dl_re = re.compile(r'/od/ds/(ark:/\d+/)*[\w\-]+/')
            for cmp in out.get('components', []):
                m = latest_dl_re.search(cmp.get('downloadURL', ''))
                if m and cmp['downloadURL'][m.end():m.end()+3] != "v_/":
                    cmp['downloadURL'] = cmp['downloadURL'][:m.end()] + "_v/"+version+'/' + \
                                         cmp['downloadURL'][m.end():]

        return out

    def _describe_latest_ds(self, id, reqid=None):
        return self._describe_version(id, 'latest', reqid)
        
    def _describe_component(self, idm, version, reqid=None):
        if not reqid:
            reqid = idm.group()

        dsid = idm.group()[:idm.start(const.ARK_ID_PATH_GRP)]
        cmpid = idm.group()[idm.start(const.ARK_ID_PATH_GRP):]
        id = idm.group(const.ARK_ID_DS_GRP)

        if idm.group(const.ARK_ID_PATH_GRP).startswith(VER_DELIM+'/'):
            parts = idm.group(const.ARK_ID_PATH_GRP).split('/', 3)
            if len(parts) > 2 and parts[1] == VER_DELIM.lstrip('/'):
                version = parts[2]
                cmpid = idm.group(const.ARK_ID_PART_GRP) or ''
                if len(parts) > 3:
                    cmpid = '/' + parts[3] + cmpid

        dsmd = self._describe_version(id, version)

        # extract the requested component
        find = cmpid.lstrip('/')
        cmpmd = [c for c in dsmd.get('components',[]) if c.get('@id') == find]

        # try some alternatives (support old file component delimiter)
        if len(cmpmd) == 0:
            find = None
            if cmpid.startswith(FILE_DELIM+'/'):
                find = "cmps" + cmpid[len(FILE_DELIM):]
            elif cmpid.startswith('/cmps/'):
                find = FILE_DELIM + cmpid[len('/cmps'):]
                find = find.lstrip('/')
            if find:
                cmpmd = [c for c in dsmd.get('components',[]) if c.get('@id') == find]

        if len(cmpmd) == 0:
            raise IDNotFound(reqid)

        # tweak the meta-metadata on its way out the door
        if version:
            dsid += VER_DELIM + '/' + version
        cmpmd[0]['isPartOf'] = dsid
        if cmpmd[0]['@id'][0] != '/' and cmpmd[0]['@id'][0] != '#':
            dsid += '/'
        cmpmd[0]['@id'] = dsid + cmpmd[0]['@id']
        if '@context' in dsmd:
            cmpmd[0]['@context'] = dsmd['@context']
        if 'version' in dsmd and 'version' not in cmpmd[0]:
            cmpmd[0]['version'] = dsmd['version']
        return cmpmd[0]

    def exists(self, id, version: str=None) -> bool:
        """
        return true if the specified dataset exists in the "database"
        """
        if id.startswith('ark:'):
            idm = _ark_id_re.match(id)
            if idm:
                id = idm.group(const.ARK_ID_DS_GRP)
            if idm.group(const.ARK_ID_PATH_GRP) and \
               idm.group(const.ARK_ID_PATH_GRP).startswith(VER_DELIM):
                fields = idm.group(const.ARK_ID_PATH_GRP).split('/')
                if len(fields) > 2:
                    version = fields[2]

        if not self._versions:
            if version and version != 'latest':
                fpath = os.path.join(self._root, "%s-v%s.json" % (id, version.replace('.', '_')))
                if os.path.isfile(fpath):
                    return True
            self._index_cache()

        if id not in self._versions:
            return False
        if version: 
            return version in self._versions[id]
        return True

    def _get(self, aipid, version, reqid):
        if not self._versions:
            if version != 'latest':
                fpath = os.path.join(self._root, "%s-v%s.json" % (aipid, version.replace('.', '_')))
                if os.path.isfile(fpath):
                    return self._read_file(fpath, reqid)
            self._index_cache()

        return self._read_file(self._versions.get(aipid, {}).get(version), reqid)
            
    def _read_file(self, fpath, reqid):
        if not fpath or not os.path.isfile(fpath):
            raise IDNotFound(reqid)

        try:
            return read_json(fpath)
        except (ValueError, IOError) as ex:
            raise RMMServerError(id, message="Failed to read NERDm record as JSON: " % str(ex))

    
        
