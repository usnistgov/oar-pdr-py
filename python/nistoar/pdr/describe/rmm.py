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

_ark_id_re = re.compile(const.ARK_ID_PAT)
VER_DELIM  = const.RELHIST_EXTENSION
FILE_DELIM = const.FILECMP_EXTENSION
LINK_DELIM = const.LINKCMP_EXTENSION
AGG_DELIM  = const.AGGCMP_EXTENSION
OLD_COMP_DELIM = "/cmps"

class MetadataClient(object):
    """
    a client interface for retrieving metadata from the RMM.
    """
    COLL_LATEST   = "records"
    COLL_VERSIONS = "versions"
    COLL_RELEASES = "releaseSets"

    def __init__(self, baseurl: str):
        self.baseurl = baseurl
        if not self.baseurl.endswith('/'):
            self.baseurl += '/'

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
        find = id.rstrip('/')  # trailing slash treated as superfluous
        if find.endswith(FILE_DELIM):
            # for now, treat an ID ending in "/pdr:f" equivalently to a dataset id
            find = find[:-1*len(FILE_DELIM)]

        if not find.startswith("ark:"):
            # it's an EDI-ID
            return self._describe_ediid(find, version, id)

        idm = _ark_id_re.match(find)
        if not idm:
            # don't bother if it's not a compliant ARK ID
            raise IDNotFound(id)

        if not idm.group(const.ARK_ID_PATH_GRP) and not idm.group(const.ARK_ID_PART_GRP):
            # it appears to be simply a dataset ID (there's nothing past the dataset part)
            if version:
                return self._describe_version(find + VER_DELIM +'/'+ version)
            return self._describe_latest_ds(find, id)

        if idm.group(const.ARK_ID_PATH_GRP) == VER_DELIM:
            # ends with "/pdr:v" 
            return self._describe_releases(find, id)

        if not idm.group(const.ARK_ID_PART_GRP) and idm.group(const.ARK_ID_PATH_GRP).startswith(VER_DELIM+'/'):
            fields = idm.group(const.ARK_ID_PATH_GRP).split('/', 3)
            if len(fields) < 4:
                # we want a version of a dataset
                return self._describe_version(find, id)

        return self._describe_component(idm, version, id)

    def _describe_ediid(self, ediid, version, reqid=None):
        if not reqid:
            reqid = ediid

        url = self.baseurl
        if version:
            url += "%s?version=%s&ediid=" % (self.COLL_VERSIONS, version)
        else:
            url += "%s?ediid=" % self.COLL_LATEST
        url += ediid

        return self._get(url, reqid)

    def _describe_releases(self, id, reqid=None):
        if not reqid:
            reqid = id

        url = "%s%s?@id=%s" % (self.baseurl, self.COLL_RELEASES, id)
        out = self._get(url, reqid)
        if 'releasesets data' in out:
            if len(out['releasesets data']) == 0:
                raise IDNotFound(reqid or id)
            out = out['releasesets data'][0]
            if '_id' in out:
                del out['_id']
        return out

    def _describe_version(self, id, reqid=None):
        if not reqid:
            reqid = id

        url = "%s%s?@id=%s" % (self.baseurl, self.COLL_VERSIONS, id)
        out = self._get(url, reqid)

        # inject the latest version history into this record
        idm = _ark_id_re.match(id)
        if not idm:
            raise StateException("id=id seems suddenly invalid!")
        relid = id[:idm.end(const.ARK_ID_DS_GRP)] + VER_DELIM
        try:
            relset = self._describe_releases(relid)
            if 'hasRelease' in relset:
                if not 'releaseHistory' in out:
                    out['releaseHistory'] = OrderedDict([
                        ("@id", relset['@id']),
                        ("@type", "nrdr:ReleaseHistory"),
                        ("label", "Release History")
                    ])
                out['releaseHistory']['hasRelease'] = relset['hasRelease']
                if 'versionHistory' in out:
                    del out['versionHistory']
                    
        except IDNotFound:
            # record does not have a corresponding release set record (shouldn't happen)
            pass
            
        return out

    def _describe_latest_ds(self, id, reqid=None):
        if not reqid:
            reqid = id

        url = "%s%s?@id=%s" % (self.baseurl, self.COLL_LATEST, id)
        return self._get(url, reqid)

    def _describe_component(self, idm, version, reqid=None):
        if not reqid:
            reqid = idm.group()

        dsid = idm.group()[:idm.start(const.ARK_ID_PATH_GRP)]
        cmpid = idm.group()[idm.start(const.ARK_ID_PATH_GRP):]

        dsmd = None
        if idm.group(const.ARK_ID_PATH_GRP).startswith(VER_DELIM+'/'):
            parts = idm.group(const.ARK_ID_PATH_GRP).split('/', 3)
            if len(parts) > 2 and parts[1] == VER_DELIM.lstrip('/'):
                dsid += VER_DELIM + '/' + parts[2]
                cmpid = idm.group(const.ARK_ID_PART_GRP) or ''
                if len(parts) > 3:
                    cmpid = '/' + parts[3] + cmpid
                dsmd = self._describe_version(dsid)
        elif version:
            dsid += VER_DELIM + '/' + version
            dsmd = self._describe_version(dsid)

        if not dsmd:
            dsmd = self._describe_latest_ds(dsid)

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
        cmpmd[0]['isPartOf'] = dsid
        if cmpmd[0]['@id'][0] != '/' and cmpmd[0]['@id'][0] != '#':
            dsid += '/'
        cmpmd[0]['@id'] = dsid + cmpmd[0]['@id']
        if '@context' in dsmd:
            cmpmd[0]['@context'] = dsmd['@context']
        if 'version' in dsmd and 'version' not in cmpmd[0]:
            cmpmd[0]['version'] = dsmd['version']
        return cmpmd[0]
            
    def _get(self, url, reqid):
        out = self._retrieve(url, reqid)

        if "PageSize" in out:
            # output is wrapped in RMM search envelope; unwrap it
            datakey = [k for k in out.keys() if k.lower().endswith('data')]
            if len(datakey) == 0:
                out = []
            else:
                out = out[datakey[0]]
            if len(out) == 0:
                raise IDNotFound(id)
            out = out[0]

        if "_id" in out:
            # remove this RMM artifact
            del out['_id']

        return out

    def _retrieve(self, url, id):
        hdrs = { "Accept": "application/json" }
        try:
            resp = requests.get(url, headers=hdrs)

            if resp.status_code >= 500:
                raise RMMServerError(id, resp.status_code, resp.reason)
            elif resp.status_code == 404:
                raise IDNotFound(id)
            elif resp.status_code == 406:
                raise RMMClientError(id, resp.status_code, resp.reason,
                                     message="JSON data not available from"+
                                         " this URL (is URL correct?)")
            elif resp.status_code >= 400:
                raise RMMClientError(id, resp.status_code, resp.reason)
            elif resp.status_code != 200:
                raise RMMServerError(id, resp.status_code, resp.reason,
                               message="Unexpected response from server: {0} {1}"
                                        .format(resp.status_code, resp.reason))

            # This gets around an incorrect implementation of the RMM service
            out = resp.json()
            if "Message" in out and "ResultData" not in out:
                if "No record available" in out['Message']:
                    # RMM should have responded with 404!
                    raise IDNotFound(id)
                raise RMMServerError(id, message="Unexpected response: "+
                                     out['Message'])
                
            return out
        except ValueError as ex:
            if resp.text and ("<body" in resp.text or "<BODY" in resp.text):
                raise RMMServerError(id,
                                     message="HTML returned where JSON "+
                                     "expected (is service URL correct?)")
            else:
                raise RMMServerError(id,
                                     message="Unable to parse response as "+
                                     "JSON (is service URL correct?)")
        except requests.RequestException as ex:
            raise RMMServerError(id,
                                 message="Trouble connecting to distribution"
                                 +" service: "+ str(ex), cause=ex)

    def search(self, query=None, latest=True):
        """
        return the NERDm record matching a given query.  All records are returned if a query is not 
        provided.  

        [Document RMM search paramters]

        :param dict query:  a dictionary whose properties specify the RMM search query parameters by name
        :param bool latest: if True (default), only the latest versions of resources will be returned; 
                            otherwise, all matching versions will be returned.
        """
        qstr = ""
        if query:
            qstr = "&".join(["%s=%s" % i for i in query.items()])
        url = self.baseurl + \
              ((latest and "records?") or "versions?") + \
              qstr

        hdrs = { "Accept": "application/json" }
        try:
            resp = requests.get(url, headers=hdrs)

            if resp.status_code >= 500:
                raise RMMServerError("records", resp.status_code, resp.reason)
            elif resp.status_code >= 400 and (resp.status_code != 404 or resp.status_code != 406):
                raise RMMClientError("records", resp.status_code, resp.reason)
            elif resp.status_code != 200:
                raise RMMServerError("records", resp.status_code, resp.reason,
                               message="Unexpected response from server: {0} {1}"
                                        .format(resp.status_code, resp.reason))

            # This gets around an incorrect implementation of the RMM service
            out = resp.json()
            if "Message" in out and "ResultData" not in out:
                if "No record available" in out['Message']:
                    # RMM should have responded with 404!
                    return []
                raise RMMServerError("records", message="Unexpected response: "+
                                     out['Message'])

            return out['ResultData']
        except ValueError as ex:
            if resp.text and ("<body" in resp.text or "<BODY" in resp.text):
                raise RMMServerError(id,
                                     message="HTML returned where JSON "+
                                     "expected (is service URL correct?)")
            else:
                raise RMMServerError(id,
                                     message="Unable to parse response as "+
                                     "JSON (is service URL correct?)")
        except requests.RequestException as ex:
            raise RMMServerError(id,
                                 message="Trouble connecting to metadata"
                                 +" service: "+ str(ex), cause=ex)

        
            
class RMMServerError(PDRServerError):
    """
    an exception indicating an error occurred on the server-side while 
    trying to access the distribution service.  

    This exception includes three extra public properties, `status`, `reason`, 
    and `resource` which capture the HTTP response status code, the associated 
    HTTP response message, and (optionally) a name for the record being 
    submitted to it.  
    """

    def __init__(self, resource=None, http_code=None, http_reason=None, 
                 message=None, cause=None):
        super(RMMServerError, self).__init__("rmm-metadata", resource,
                                         http_code, http_reason, message, cause)
                                                 

class RMMClientError(PDRServiceException):
    """
    an exception indicating an error occurred on the client-side while 
    trying to access the distribution service.  

    This exception includes three extra public properties, `status`, `reason`, 
    and `resource` which capture the HTTP response status code, the associated 
    HTTP response message, and (optionally) a name for the record being 
    submitted to it.  
    """

    def __init__(self, resource, http_code, http_reason, message=None,
                 cause=None):
        if not message:
            message = "client-side distribution error occurred"
            if resource:
                message += " while processing " + resource
            message += ": {0} {1}".format(http_code, http_reason)
          
        super(RMMClientError, self).__init__("rmm-metadata", resource,
                                          http_code, http_reason, message, cause)
                                                 

