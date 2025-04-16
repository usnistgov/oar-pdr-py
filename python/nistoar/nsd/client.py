"""
A client library for querying the NIST Staff Directory (NSD) service 
"""
import os, sys, json
from collections import namedtuple
from collections.abc import Mapping
from typing import List

import urllib.request, urllib.parse, urllib.error
import requests

from . import NSDException, NSDServerError, NSDClientError, NSDResourceNotFound
from nistoar.base.config import ConfigurationException

NISTOrg = namedtuple('NISTOrg', "id title abbrev number")
_org_prop_nm = {
    "id":     "orG_ID",
    "title":  "orG_Name",
    "abbrev": "orG_ACRNM",
    "number": "orG_CD"
}

class NSDClient:
    """
    a client class for querying the NIST Staff Directory (NSD) service 
    """
    OU_EP = "/NISTOU"
    DIV_EP = "/NISTDivision"
    GROUP_EP = "/NISTGroup"
    PEOPLE_EP = "/People/list"

    def __init__(self, baseurl, authconfig: Mapping=None, restrict_ou: List[str]=None):
        """
        initialize the client
        :param str     baseurl:  the base URL for the service.  This should include everything up to 
                                 (and including) the version field (e.g. "https://..../.../v1").
        :param str  authconfig:  a dictionary providing credentials for connecting to the service; if 
                                 not provided, it will be assumed that authentication is not required.
        :param str restrict_ou:  if non-None, restrict people search results to those who are 
                                 members of OUs in the given list of OU abbreviated names (e.g. "MML"). 
        """
        self.baseurl = baseurl.rstrip('/')
        if restrict_ou and not isintance(restrict_ou, list):
            restrict_ou = [restrict_ou]
        self._def_ou = restrict_ou
        self._ou_nums = None

        self._authkw = {}
        self._authhdr = {}
        self._setup_auth(authconfig)

    def _setup_auth(self, config: Mapping=None):
        # erase any previously set-up authentication
        self._authkw = {}
        self._authhdr = {}

        if not config or config.get('type', '') is None:
            return      # no authentication required

        authtype = config.get('type', 'bearer')
        if isinstance(authtype, str):
            authtype = authtype.lower()

        if authtype == "none":
            pass      # no authentication required

        elif authtype == "userpass":
            self._authkw = { "user": (config.get('user'), config.get('pass')) }
            if not all(self._authkw["user"]):
                raise ConfigurationException("NSDClient: authentication type userpass requires both "+
                                             "'user' and 'pass' config parameters")

        elif authtype == "bearer":
            token = config.get("token")
            if not token:
                raise ConfigurationException("NSDClient: authentication type bearer requires "+
                                             "'token' config parameter")
            self._authhdr = { "Authorization": f"Bearer {token}" }

        elif authtype == "cert":
            self._authkw = { 'cert': (config.get('client_cert_path'), config.get('client_key_path')) }
            if not all(self._authkw["cert"]):
                raise ConfigurationException("NSDClient: authentication type userpass requires both "+
                                             "'user' and 'pass' config parameters")

            unreadable = []
            for cfile in self._authkw['cert']:
                try:
                    with open(cfile) as fd:
                        pass
                except OSError as ex:
                    unreadable.append(f"{cfile} ({str(ex)})")
            if unreadable:
                s = "s" if len(unreadable) > 1 else ""
                raise ConfigurationException("NSDClient: certificate file%s unreadable:\n  %s" %
                                             (s, "\n  ".join(unreadable)))

        else:
            raise ConfigurationException("NSDClient: authentication 'type' param value not supported: "+
                                         authtype)
            

    def _get(self, relurl):
        if not relurl.startswith('/'):
            relurl = '/'+relurl
        hdrs = self._authhdr
        
        try:
            resp = requests.get(self.baseurl+relurl, headers=hdrs, **self._authkw)

            if resp.status_code >= 500:
                raise NSDServerError(relurl, resp.status_code, resp.reason)
            elif resp.status_code == 404:
                raise NSDResourceNotFound(relurl, resp.reason)
            elif resp.status_code == 406:
                raise NSDClientError(relurl, resp.status_code, resp.reason,
                                     message="JSON data not available from"+
                                     " this URL (is URL correct?)")
            elif resp.status_code >= 400:
                raise NSDClientError(relurl, resp.status_code, resp.reason)
            elif resp.status_code != 200:
                raise NSDServerError(relurl, resp.status_code, resp.reason,
                                     message="Unexpected response from server: {0} {1}"
                                     .format(resp.status_code, resp.reason))

            return resp.json()

        except ValueError as ex:
            if resp and resp.text and \
               ("<body" in resp.text or "<BODY" in resp.text):
                raise NSDServerError(relurl,
                                     message="HTML returned where JSON "+
                                     "expected (is service URL correct?)", cause=ex)
            else:
                raise NSDServerError(relurl,
                                     message="Unable to parse response as "+
                                     "JSON (is service URL correct?)", cause=ex)
        except requests.RequestException as ex:
            raise NSDServerError(relurl, cause=ex)

    def _query(self, relurl, filter):
        if not relurl.startswith('/'):
            relurl = '/'+relurl
        hdrs = { "Content-type": "application/json" }
        hdrs.update(self._authhdr)
        
        try:
            resp = requests.post(self.baseurl+relurl, headers=hdrs, json=filter, **self._authkw)

            if resp.status_code >= 500:
                raise NSDServerError(relurl, resp.status_code, resp.reason)
            elif resp.status_code == 404:
                raise NSDResourceNotFound(relurl, resp.reason)
            elif resp.status_code == 406:
                raise NSDClientError(relurl, resp.status_code, resp.reason,
                                     message="JSON data not available from"+
                                     " this URL (is URL correct?)")
            elif resp.status_code >= 400:
                raise NSDClientError(relurl, resp.status_code, resp.reason)
            elif resp.status_code != 200:
                raise NSDServerError(relurl, resp.status_code, resp.reason,
                                     message="Unexpected response from server: {0} {1}"
                                     .format(resp.status_code, resp.reason))

            return resp.json()

        except TypeError as ex:
            raise NSDClientError(relurl, message="Unable to encode filter: "+str(ex))
        except ValueError as ex:
            if resp and resp.text and \
               ("<body" in resp.text or "<BODY" in resp.text):
                raise NSDServerError(relurl,
                                     message="HTML returned where JSON "+
                                     "expected (is service URL correct?)", cause=ex)
            else:
                raise NSDServerError(relurl,
                                     message="Unable to parse response as "+
                                     "JSON (is service URL correct?)", cause=ex)
        except requests.RequestException as ex:
            raise NSDServerError(relurl, cause=ex)

    def OUs(self) -> List[NISTOrg]:
        """
        return a list of the Organization Units (OU) in the directory
        :returns"  a list of records
                   :rtype: [NISTOrg}
        """
        p = _org_prop_nm
        try:
            return [NISTOrg(r[p['id']], r[p['title']], r[p['abbrev']], r[p['number']])
                    for r in self._get(self.OU_EP)]
        except KeyError as ex:
            raise NSDServerError(self.OU_EP, message="Unexpected org description: missing properties"
                                 " (has service changed?)")

    def divs(self) -> List[NISTOrg]:
        """
        return a list of the divisions in the directory
        """
        p = _org_prop_nm
        try:
            return [NISTOrg(r[p['id']], r[p['title']], r[p['abbrev']], r[p['number']])
                    for r in self._get(self.DIV_EP)]
        except KeyError as ex:
            raise NSDServerError(self.DIV_EP, message="Unexpected org description: missing properties"
                                 " (has service changed?)")

    def groups(self) -> List[NISTOrg]:
        """
        return a list of the Organization Units (OU) in the directory
        """
        p = _org_prop_nm
        try: 
            return [NISTOrg(r[p['id']], r[p['title']], r[p['abbrev']], r[p['number']])
                    for r in self._get(self.GROUP_EP)]
        except KeyError as ex:
            raise NSDServerError(self.GROUP_EP, message="Unexpected org description: missing properties"
                                 " (has service changed?)")

    def select_people(self, name=None, lname=None, fname=None, ou=None, email=None,
                      filter=None) -> List[Mapping]:
        """
        search for people records given constraints
        :param str|[str]  name:  a name or partial name or a list of the same to compare against 
                                 the first and last names
        :param str|[str] lname:  a name or partial name or a list of the same to compare against
                                 the last names
        :param str|[str] fname:  a name or partial name or a list of the same to compare against
                                 the first names
        :param str|[str] email:  the email address or a list of the same to compare against people's
                                 email addresses
        :param str|int|[str]|[int] ou:  an OU or list of OUs, given either as string representing
                                 the OU abbreviation or an integer representing its number,
                                 to compare against people's OU affiliation
        :param Mapping filter:   the raw NSD people filter object to use to select people.  If 
                                 provided, this will be combined with the constraints provided 
                                 by the other input parameters.
        """
        if name is not None and not isinstance(name, list):
            name = [name]
        if fname is not None and not isinstance(fname, list):
            fname = [fname]
        if lname is not None and not isinstance(lname, list):
            lname = [lname]
        if ou is not None and not isinstance(ou, list):
            ou = [ou]
        if email is not None and not isinstance(email, list):
            email = [email]            
        if filter is None:
            filter = {}

        if fname:
            filter['firstName'] = list(set(filter.get('firstName',[])) | set(fname))
        if lname:
            filter['lastName']  = list(set(filter.get('lastName', [])) | set(lname))
        if name:
            filter['firstName'] = list(set(filter.get('firstName',{})) | set(name))
            filter['lastName']  = list(set(filter.get('lastName', [])) | set(name))
        if email:
            filter['emailAddress'] = list(set(filter.get('emailAddress', [])) | set(name))

        if ou:
            filter['ouNumber'] = []
            ounums = set((str(u) if u > 9 else f"0{u}") for u in ou if isinstance(u, int))
            if ounums:
                filter['ouNumber'].extend(ounums)
            ouabbs = set(u for u in ou if isinstance(u, str))
            if ouabbs:
                if not self._ou_nums:
                    self._load_oulu(self.OUs())
                filter['ouNumber'].extend(self._ou_nums[u] for u in ouabbs)

        elif self._def_ou:
            # set default ou filter
            if not self._ouNums:
                self._load_oulu()
            filter['ouNumber'] = [self._ouNums[u] for u in ouabbs]

        return self._query(self.PEOPLE_EP, filter)

    def get_person(self, id) -> Mapping:
        """
        resolve a record identifier into a description of a person
        """
        if not isinstance(id, int):
            raise ValueError(f"get_person(): id is not an int: {str(id)}")

        hit = self._query(self.PEOPLE_EP, filter={"peopleID": [ id ]})
        if not hit:
            return None
        if isinstance(hit, (list, tuple)):
            return hit[0]
        raise NSDException("get_person(): Unexpected JSON from service")

    def _load_oulu(self, ous: List[NISTOrg]):
        self._ou_nums = {}
        for u in ous:
            self._ou_nums[u.abbrev] = u.number
