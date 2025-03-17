"""
Implementation of the sync capabilities
"""
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Union
from io import TextIOBase

from nistoar.base.config import ConfigurationException
from .. import NSDException, NSDServiceException

import requests

ORGANIZATIONS = "NISTOUDivisionGroup"
PEOPLE = "People/list"
PEOP_OUORGID_SEL="oU_ORG_ID"
PEOP_PAGE_SZ=100
TOOMANY=6000
OU_LEV=1

class NSDSyncer:
    """
    A class that can connect the NIST-wide staff directory service and retrieve directory data 
    to update the local OAR mirror.

    This class expects the following configuration:
        ``dir``
            _str_ (required) the directory where the retrieved data should be written
        ``person_file``
            _str_ (optional) the name of the file to contain records describing people;
            default: ``people.json``
        ``org_file``
            _str_ (optional) the name of the file to contain records describing that organizations 
            the people are assigned to; default: ``orgs.json``
        ``source``
            _dict_ (required) a dictionary of parameters that describe the service from which to 
            pull the staff data
    
    This class expects the following parameters in ``source`` dictionary:
        ``service_endpoint``
            _str_ (required) The base endpoint URL for the NSD service to pull data from
        ``token``
            _str_ (optional) an authentication (Bearer) token to use to access the NSD service.
            If not provided, a token will be retrieved using ``tokenService`` (if provided)
        ``tokenService``
            _dict_ (optional) a dictionary configuring use of a token retrieval service.  See 
            :py:func:`get_nsd_token` for details on its contents.  If not provided, it will be
            assumed that a token is not required to access the NSD service.  
    """

    def __init__(self, config: Mapping={}):
        """
        initialize the syncer
        """
        self.cfg = config
        src = self.cfg.get('source', {})
        if not src:
            raise ConfigurationException("Missing required configuration: source")
        if not src.get("service_endpoint"):
            raise ConfigurationException("Missing required configuration: source.service_endpoint")
        self.token = src.get("token")

    def get_token(self):
        """
        Retrieve an authentication token from a token service.
        """
        return get_nsd_auth_token(self.cfg.get("source", {}).get("tokenService", {}))

    def cache_data(self, dir: Union[str,Path]=None, forous=None):
        """
        fetch the data needed by the OAR staff directory from the NIST Staff directory service
        and cache it to files on disk.  Configuration controls the names of the output files 
        (see :py:class:`class documentation <NSDSyncer>` for details).
        :param str|Path dir:  the directory to store the data into; if None, the value set in
                              the configuration is used.  
        :param list[str] forous:  a list of abbreviations for OUs to restrict the people data to
        """
        baseep = self.cfg["source"]["service_endpoint"]
        if not dir:
            dir = self.cfg.get("dir")
        if not dir:
            raise ConfigurationException("Missing required configuration: dir")
        if isinstance(dir, str):
            dir = Path(dir)

        token = self.token
        if not token:
           token = self.get_token()

        try:
            filepath = dir / self.cfg.get("org_file", "orgs.json")
            orgs = get_nsd_orgs(baseep, token)
            with open(filepath, 'w') as fd:
                json.dump(orgs, fd, indent=2)

            if forous is None:
                forous = [ou['orG_ID'] for ou in orgs if ou['orG_LVL_ID'] == OU_LEV]
            filepath = dir / self.cfg.get("person_file", "people.json")
            with open(filepath, 'w') as fd:
                out = _JSONListWriter(fd)
                _write_nsd_people(out, baseep, forous, token)
                out.done()

        except NSDException:
            raise
        except Exception as ex:
            raise NSDException(f"Failed to cache data to {filepath.name}: {str(ex)}")


def get_nsd_people(baseurl: str, forous=None, token: str=None) -> list[Mapping]:
    """
    return the complete list of people belonging to specified OUs from the NIST Staff Directory
    :param str baseurl:  the base URL for all NSD service endpoints
    :param str  forous:  a list of the OUs to restrict the people to.  Each element is either a 
                         an integer or a string.  A integer indicates an internal database ID for 
                         an OU; a string indicates the enterprise OU ID (e.g. "64" for MML).  
                         If None, all staff from all OUs are returned
    :param str   token:  the authentication token to use to access the service.  If None,
                         it will be assumed that no token is needed.
    """
    out = []
    _write_nsd_people(out, baseurl, forous, token)
    return out

class _JSONListWriter:

    def __init__(self, fd):
        self.ostrm = fd
        self._cnt = 0

    @property
    def count(self):
        return self._cnt

    def __len__(self):
        return self.count

    def append(self, item):
        out = json.dumps(item)
        if self.count > 0:
            self.ostrm.write(",\n  ")
        else:
            self.ostrm.write("[\n  ")
        self._cnt += 1
        self.ostrm.write(out)

    def extend(self, items):
        for item in items:
            self.append(item)

    def done(self):
        self.ostrm.write("\n]\n")
    

def _write_nsd_people(out: Union[list,_JSONListWriter], baseurl: str, forous=None, token: str=None):
    if not forous or any([isinstance(ou, str) for ou in forous]):
        # need normalize to a list of OU IDs; need a OU lookup list
        ous = [ou for ou in get_nsd_orgs(baseurl, token) if ou['orG_LVL_ID'] == OU_LEV]
        if not forous:
            # want all OUs
            forous = [ou['orG_ID'] for ou in ous]
        else:
            for i in range(len(forous)):
                if isinstance(forous[i], str):
                    match = [ou['orG_ID'] for ou in ous if ou['orG_CD'] == forous[i]]
                    forous[i] = match[0] if len(match) > 0 else -1

    # make sure the list of OU IDs is unique
    forous = list(set([ou for ou in forous if ou >= 0]))

    if not baseurl.endswith('/'):
        baseurl += '/'
    fullurl = baseurl+PEOPLE
    for ou in forous:
        _write_nsd_ou_people(out, fullurl, ou, token)


def _write_nsd_ou_people(out: Union[list,_JSONListWriter], fullurl: str, ouid: int, token: str=None):
    page = 1
    needmore = True
    togo = None
    while needmore:
        data = _get_nsd_people_page(fullurl, ouid, page, token)
        if 'userInfo' not in data:
            raise NSDException(f"Unexpected {PEOPLE} output: missing 'userInfo' property: {data.keys()}")
      
        out.extend(data['userInfo'])
        page += 1
        needmore = (data.get('totalCount', len(data['userInfo'])) - len(out)) > 0
        if len(out) > TOOMANY:
            raise RuntimeException(f"Possible programming error: unable to escape People-fetch loop")

    return out

def _get_nsd_people_page(fullurl: str, ouid: int, page=1, token: str=None) -> Mapping:
    hdrs = { "Accept": "application/json", "Content-type": "application/json" }
    if token:
        hdrs["Authorization"] = f"Bearer {token}"

    payload = {
        "pageSize": PEOP_PAGE_SZ,
        "pageIndex": page,
        PEOP_OUORGID_SEL: [ ouid ]
    }

    try:
        resp = requests.post(fullurl, data=json.dumps(payload).encode('utf-8'), headers=hdrs)
        if resp.status_code >= 300:
            raise NSDServiceException(fullurl, resp.status_code, resp.reason,
                  f"Failed to obtain people from {PEOPLE}: {resp.reason} ({resp.status_code})")
        elif resp.status_code == 204:
            # No Content
            return {'totalCount': 0, 'userInfo': []}
            
        return resp.json()
    except NSDServiceException:
        raise
    except Exception as ex:
        raise NSDServiceException(fullurl,
                                  message="Trouble accessing remote NSD service: "+str(ex)) from ex


def get_nsd_orgs(baseurl: str, token: str=None) -> list[Mapping]:
    """
    return the complete list of organizations from the NIST Staff Directory
    :param str baseurl:  the base URL for all NSD service endpoints
    :param str   token:  the authentication token to use to access the service
    """
    if not baseurl.endswith('/'):
        baseurl += '/'
    fullurl = baseurl+ORGANIZATIONS
    hdrs = { "Accept": "application/json" }
    if token:
        hdrs["Authorization"] = f"Bearer {token}"

    try:
        resp = requests.get(fullurl, headers=hdrs)
        if resp.status_code >= 300:
            raise NSDServiceException(fullurl, resp.status_code, resp.reason,
                  f"Failed to obtain orgs from {ORGANIZATIONS}: {resp.reason} ({resp.status_code})")
        return resp.json()
    except NSDServiceException:
        raise
    except Exception as ex:
        raise NSDServiceException(fullurl,
                                  message="Trouble accessing remote NSD service: "+str(ex)) from ex

def get_nsd_auth_token(tscfg: Mapping):
    """
    Retrieve an authentication token required to access the NIST Staff Directory service.

    This function requires a configuration dictionary with the following parameters:

    ``service_endpoint``
         the endpoint URL for the (OpenID/OAuth) token web service
    ``client_id``
         a previously issued client ID representing :py:mod:`nistoar.nsd` client
    ``secret``
         the associated client secret 
    """
    need = "service_endpoint client_id secret".split()
    missing = [p for p in need if not tscfg.get(p)]
    if missing:
        raise ConfigurationException("Missing configuration needed to get token: tokenService." +
                                     ", tokenService.".join(missing))

    payload = {
        "grant_type": "client_credentials",
        "client_id": tscfg['client_id'],
        "client_secret": tscfg['secret']
    }
    hdrs = {
        "accept": "application/json",
        "cache-control": "no-cache",
        "content-type": "application/x-www-form-urlencoded"
    }

    try: 
        url = tscfg['service_endpoint']
        resp = requests.post(url, data=payload, headers=hdrs)
        if resp.status_code >= 300:
            raise NSDServiceException(url, resp.status_code, resp.reason,
                    f"Failed to obtain NSD auth token from {url}: {resp.reason} ({resp.status_code})")
        data = resp.json()

    except NSDServiceException:
        raise
    except Exception as ex:
        raise NSDServiceException(url, message="Trouble accessing token service: "+str(ex)) from ex

    if "access_token" not in data:
        raise NSDServiceException(url, resp.status_code,
                                  message="Unexpected response: missing access_token property:\n  "+
                                      str(data)[:70])
    return data["access_token"]

