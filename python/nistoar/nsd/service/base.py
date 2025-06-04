"""
A staff directory look-up service
"""
import json, os
from collections import OrderedDict
from collections.abc import Mapping
from abc import ABC, abstractmethod
from typing import List, Iterator

from .. import NSDException, NSDServerError, NSDClientError, NSDResourceNotFound
from nistoar.base.config import ConfigurationException

class PeopleService(ABC):
    """
    An abstract interface to a staff directory.  
    """
    OU_ORG_TYPE  = "ou"
    DIV_ORG_TYPE = "div"
    GRP_ORG_TYPE = "group"

    # these are overridden by the implementation
    ORG_ID_PROP    = "id"
    PERSON_ID_PROP = "id"
    EMAIL_PROP     = "email"
    ORCID_PROP     = "orcid"
    EID_PROP       = "username"

    @abstractmethod
    def OUs(self) -> List[Mapping]:
        """
        return a list of OU descriptions using the native NSD JSON schema.
        """
        raise NotImplemented()

    @abstractmethod
    def divs(self) -> List[Mapping]:
        """
        return a list of division descriptions using the native NSD JSON schema.
        """
        raise NotImplemented()

    @abstractmethod
    def groups(self) -> List[Mapping]:
        """
        return a list of group descriptions using the native NSD JSON schema.
        """
        raise NotImplemented()

    @abstractmethod
    def select_orgs(self, filter: Mapping, like: List[str]=None, orgtype: str=None) -> List[Mapping]:
        """
        return a list of matching organizations that match given search constraints.
        :param dict filter:  a filter object used to select records.  Schema and 
                             syntax expected is implementation-dependent
        :param [str]  like:  a list of prompt strings; a record will be match this 
                             constraint if any one of a set of key properties 
                             (defined by the implementation) starts with any of the 
                             values.  The prompt string constraints will be OR-ed 
                             together, and that aggregate will be AND-ed with 
                             ``filter``, if provided.
        :param str orgtype:  the type of organization to restrict the query to, one
                             of OU_ORG_TYPE, DIV_ORG_TYPE, GRP_ORG_TYPE
        """
        raise NotImplemented()

    @abstractmethod
    def select_people(self, filter: Mapping, like: List[str]=None) -> List[Mapping]:
        """
        return a list of matching person descriptions that match a given search query.
        :param dict filter:  a filter object used to select records.  Schema and 
                             syntax expected is implementation-dependent
        :param [str]  like:  a list of prompt strings; a record will be match this 
                             constraint if any one of a set of key properties 
                             (defined by the implementation) starts with any of the 
                             values.  The prompt string constraints will be OR-ed 
                             together, and that aggregate will be AND-ed with 
                             ``filter``, if provided.
        """
        raise NotImplemented()

    @abstractmethod
    def get_person(self, id: int) -> Mapping:
        """
        resolve a native person identifier into the description of the person
        """
        raise NotImplemented()

    @abstractmethod
    def get_org(self, id: int) -> Mapping:
        """
        resolve a native group identifier into the description of the group
        """
        raise NotImplemented()

    def _get_person_by(self, prop: str, val) -> Mapping:
        # return a single person description matching a property value.  The implementation
        # assumes that the each person has a unique value for the property.
        hits = self.select_people({ prop: val })
        if not hits:
            return None
        return hits[0]

    def _get_org_by(self, prop: str, val) -> Mapping:
        # return a single organization description matching a property value.  The implementation
        # assumes that the each org has a unique value for the property.
        hits = self.select_orgs({ prop: val })
        if not hits:
            return None
        return hits[0]

    def get_person_by_email(self, email: str) -> Mapping:
        """
        resolve a person's email into a full description of the person, or None if the email
        is not recognized.
        """
        return self._get_person_by(self.EMAIL_PROP, email)

    def get_person_by_orcid(self, orcid: str) -> Mapping:
        """
        resolve a person's ORCID into a full description of the person, or None if the ORCID
        is not recognized.
        """
        return self._get_person_by(self.ORCID_PROP, email)
        
    def get_person_by_eid(self, eid: str) -> Mapping:
        """
        resolve a person's enterprise ID (login name) into a full description of the person, or 
        None if the ID is not recognized.
        """
        return self._get_person_by(self.EID_PROP, eid)
        
    
