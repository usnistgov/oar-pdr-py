"""
an implementation of :py:class:`~nistoar.nsd.service.base.PeopleService` using JSON-formatted files
as the backend database.  This is intended primarily for testing purposes (e.g. in unittests)
"""
import json, os, re
from collections import OrderedDict
from collections.abc import Mapping
from typing import List, Iterator
from pathlib import Path

from .base import PeopleService
from .. import NSDException, NSDServerError, NSDClientError, NSDResourceNotFound
from nistoar.base.config import ConfigurationException
from nistoar.pdr.utils import read_json

class FilesBasedPeopleService(PeopleService):
    """
    an implementation of :py:class:`~nistoar.nsd.service.base.PeopleService` using JSON-formatted files
    as the backend database.  The format of the files is the same as those that are used to load 
    the MongoDB database.  

    The class constructor accepts a configuration dictionary to locate the source data files, and two 
    schema variations are accepted.  First, schema described by 
    :py:meth:`.mongo.MongoPeopleService.load() <nistoar.nsd.service.mongo.MongoPeopleService.load>`
    is supported for indicating the directory where the data files are found and the names of the 
    files.  However, if the given configuration dictionary contains a ``data`` parameter whose value
    is a dictionary, that parameter's contents will be taken as the data configuration using the same 
    schema; this option makes it consistent with the Mongo-based configration schema.  
    """
    OU_LVL_ID = 1
    DIV_LVL_ID = 2
    GRP_LVL_ID = 3
    org_lvl = { PeopleService.OU_ORG_TYPE:   OU_LVL_ID,
                PeopleService.DIV_ORG_TYPE: DIV_LVL_ID,
                PeopleService.GRP_ORG_TYPE: GRP_LVL_ID }

    ORG_ID_PROP    = "orG_ID"
    PERSON_ID_PROP = "peopleID"
    EMAIL_PROP     = "emailAddress"
    ORCID_PROP     = "orcid"
    EID_PROP       = "nistUsername"


    def __init__(self, config: Mapping, data_dir: str = None, exactmatch=False):
        """
        initialize the service around a file directory containing the staff data
        :param dict  config:  the configuration to use.  
                              See :py:class:`class documentation <FilesBasedPeopleService>` for details.
        :param str data_dir:  the local file directory where the staff data files are stored.  This 
                              value overrides what is given in the configuration.  By default, the files 
                              expected in this directory are ``person.json`` and ``orgs.json``, but 
                              these names can be overridden via the configuration.  
        """
        self.exact = exactmatch
        if isinstance(config.get('data'), Mapping):
            config = config['data']

        if not data_dir:
            data_dir = config.get('dir')
        if not data_dir:
            raise ConfigurationException("FilesBasedPeopleService: missing config parameter: dir")

        ddir = Path(data_dir)
        if not ddir.is_dir():
            raise ConfigurationException("FilesBasedPeopleService: %s: does not exist as a directory" %
                                         data_dir)

        self.peoplef = ddir / config.get('person_file', 'person.json')
        self.orgsf = ddir / config.get('orgs_file', 'orgs.json')
        if not self.peoplef.is_file():
            raise ConfigurationException("FilesBasedPeopleService: %s: does not exist as a file" %
                                         str(self.peoplef))
        if not self.orgsf.is_file():
            raise ConfigurationException("FilesBasedPeopleService: %s: does not exist as a file" %
                                         str(self.orgsf))

    def orgs(self) -> List[Mapping]:
        """
        return all organization records as JSON list
        """
        try:
            return read_json(self.orgsf)
        except IOError as ex:
            raise NSDException(f"Failed to read org source data from {str(self.orgsf)}: {str(ex)}") from ex
        except ValueError as ex:
            raise NSDException(f"JSON format error in {str(self.orgsf)}: {str(ex)}") from ex

    def people(self) -> List[Mapping]:
        """
        return all organization records as JSON list
        """
        try:
            return read_json(self.peoplef)
        except IOError as ex:
            raise NSDException(f"Failed to read org source data from {str(self.peoplef)}: {str(ex)}") from ex
        except ValueError as ex:
            raise NSDException(f"JSON format error in {str(self.orgsf)}: {str(ex)}") from ex

    def OUs(self) -> List[Mapping]:
        return [g for g in self.orgs() if g['orG_LVL_ID'] == self.OU_LVL_ID]

    def divs(self) -> List[Mapping]:
        return [g for g in self.orgs() if g['orG_LVL_ID'] == self.DIV_LVL_ID]

    def groups(self) -> List[Mapping]:
        return [g for g in self.orgs() if g['orG_LVL_ID'] == self.GRP_LVL_ID]

    def _get_rec(self, id: int, coll: List[Mapping], idprop: str) -> Mapping:
        hits = [e for e in coll if e[idprop] == id]
        if not hits:
            return None
        return hits[0]

    def _get_org_by(self, prop: str, val) -> Mapping:
        return self._get_rec(val, self.orgs(), prop)

    def _get_person_by(self, prop: str, val) -> Mapping:
        return self._get_rec(val, self.people(), prop)

    def get_org(self, id: int) -> Mapping:
        return self._get_rec(id, self.orgs(), self.ORG_ID_PROP)

    def get_OU(self, id: int) -> Mapping:
        out = self.get_org(id)
        if out['orG_LVL_ID'] != self.OU_LVL_ID:
            return None
        return out
    
    def get_div(self, id: int) -> Mapping:
        out = self.get_org(id)
        if out['orG_LVL_ID'] != self.DIV_LVL_ID:
            return None
        return out

    def get_group(self, id: int) -> Mapping:
        out = self.get_org(id)
        if out['orG_LVL_ID'] != self.GRP_LVL_ID:
            return None
        return out

    def get_person(self, id: int) -> Mapping:
        return self._get_rec(id, self.people(), self.PERSON_ID_PROP)

    class LikeFilter:
        def __init__(self, likes: List[str], props: List[str]):
            self.matcher = re.compile('|'.join(likes), re.IGNORECASE)
            self.props = props
        def matches(self, rec):
            for p in self.props:
                if p in rec and self.matcher.match(rec[p]):
                    return True
            return False

    class ExactFilter:
        def __init__(self, prop: str, wantany: List[str]):
            self.prop = prop
            self.want = wantany
        def matches(self, rec):
            return any(self.prop in rec and rec[self.prop] == v for v in self.want)

    class AnyFilter:
        def __init__(self, filters: List):
            self.filts = list(filters)
        def matches(self, rec):
            for f in self.filts:
                if f.matches(rec):
                    return True
            return False

    class ContainsFilter:
        def __init__(self, likes: List[str], props: List[str]):
            self.matcher = re.compile('|'.join(likes), re.IGNORECASE)
            self.props = props
        def matches(self, rec):
            for p in self.props:
                if p in rec and self.matcher.search(rec[p]):
                    return True
            return False

    def _with_filters(self, filter: Mapping):
        filters = []
        for prop in filter:
            want = filter[prop]
            if not isinstance(want, list):
                want = [want]
            if self.exact or any([not isinstance(f, str) for f in want]):
                filters.append(self.ExactFilter(prop, want))
            else:
                filters.append(self.ContainsFilter(want, [prop]))
        return filters

    def select_people(self, filter: Mapping, like: List[str]=None) -> List[Mapping]:
        if like and not isinstance(like, list):
            like = [like]
        filters = []
        if filter:
            filters = [self.AnyFilter(self._with_filters(filter))]
        if like:
            filters.append(self.LikeFilter(like, "lastName firstName".split()))
        return [p for p in self.people() if all([f.matches(p) for f in filters])]

    def select_orgs(self, filter: Mapping, like: List[str]=None, orgtype: str=None) -> List[Mapping]:
        if like and not isinstance(like, list):
            like = [like]
        filters = []
        if orgtype and orgtype in self.org_lvl:
            filters.append(self.AnyFilter(self._with_filters({"orG_LVL_ID": [self.org_lvl[orgtype]]})))

        if filter:
            filters.append(self.AnyFilter(self._with_filters(filter)))
        if like:
            filters.append(self.LikeFilter(like, "orG_Name orG_ACRNM orG_CD".split()))
        return [g for g in self.orgs() if all([f.matches(g) for f in filters])]

