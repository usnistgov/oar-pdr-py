"""
A staff directory look-up service
"""
import json, os, logging
from collections import OrderedDict
from collections.abc import Mapping
from abc import ABC, abstractmethod
from typing import List, Iterator

from . import NSDException, NSDServerError, NSDClientError, NSDResourceNotFound
from nistoar.base.config import ConfigurationException

from pymongo import MongoClient
from pymongo.errors import PyMongoError, OperationFailure

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
        
    
class MongoPeopleService(PeopleService):
    """
    An implementation of a PeopleService that uses a Mongo database to store its data
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

    ORGS_COLL = "Orgs"
    PEOPLE_COLL = "People"

    def __init__(self, mongourl, exactmatch=False):
        """
        initialize the service
        """
        self._dburl = mongourl
        self._cli = MongoClient(self._dburl)
        self._db = self._cli.get_default_database()
        self._make_prop_constraint = self._make_prop_constraint_exact if exactmatch else \
                                     self._make_prop_constraint_like

    def OUs(self) -> List[Mapping]:
        return list(self._db['Orgs'].find({"orG_LVL_ID": self.OU_LVL_ID}, {"_id": False}))

    def divs(self) -> List[Mapping]:
        return list(self._db['Orgs'].find({"orG_LVL_ID": self.DIV_LVL_ID}, {"_id": False}))

    def groups(self) -> List[Mapping]:
        return list(self._db['Orgs'].find({"orG_LVL_ID": self.GRP_LVL_ID}, {"_id": False}))

    def _get_rec(self, id: int, coll: str, idprop: str) -> Mapping:
        try:
            hits = list(self._db[coll].find({idprop: id}, {"_id": False}))
        except PyMongoError as ex:
            raise NSDServerError("Org listing failed due to MongoDB failure: "+str(ex)) from ex

        if not hits:
            return None
        return hits[0]

    def _get_org_by(self, prop: str, val) -> Mapping:
        return self._get_rec(val, self.ORGS_COLL, prop)

    def _get_person_by(self, prop: str, val) -> Mapping:
        return self._get_rec(val, self.PEOPLE_COLL, prop)

    def get_org(self, id: int) -> Mapping:
        return self._get_rec(id, self.ORGS_COLL, self.ORG_ID_PROP)

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
        return self._get_rec(id, self.PEOPLE_COLL, self.PERSON_ID_PROP)

    def _to_mongo_filter(self, filter: Mapping) -> Mapping:
        # The input filter is assumed to be an NSD-compatible filter where properties
        # match properties in the record objects and the values are lists of values.
        cnsts = []
        for prop in filter:
            if filter[prop]:
                if not any([isinstance(e, str) for e in filter[prop]]):
                    # if (all) non-string values, ask for exact match
                    cnsts.append(self._make_prop_constraint_exact(prop, filter[prop]))
                else:
                    cnsts.append(self._make_prop_constraint(prop, filter[prop]))

        if len(cnsts) < 1:
            cnsts = {}
        elif len(cnsts) == 1:
            cnsts = cnsts[0]
        else:
            cnsts = {"$or": cnsts}
        return cnsts

    def _like_to_mongo_filter(self, likes: List[str], props: List[str]) -> Mapping:
        if not isinstance(likes, list):
            likes = [likes]
        likes = "|".join(likes)
        if "|" in likes:
            likes = f"({likes})"

        cnsts = []
        for field in props:
            cnsts.append({field: {"$regex": f"^{likes}", "$options": "i"}})
        return {"$or": cnsts}

    def _make_prop_constraint_exact(self, prop, values):
        if len(values) == 1:
            return {prop: values[0]}
        else:
            return {prop: {"$in": values}}

    def _make_prop_constraint_like(self, prop, values):
        return {prop: {"$regex": "|".join(values), "$options": "i"}}

    def select_people(self, filter: Mapping, like: List[str]=None) -> Iterator[Mapping]:
        try:
            mfilt = self._to_mongo_filter(filter) if filter else OrderedDict()
            if like:
                mlike = self._like_to_mongo_filter(like, "lastName firstName".split())
                mfilt = {"$and": [ mfilt, mlike ]} if mfilt else mlike
        except (TypeError, ValueError) as ex:
            raise NSDClientError(collname, 400, "Bad input",
                                 f"Bad input query syntax: {str(filter)} ({str(ex)})")
            
        return self._select_from('People', mfilt)

    def select_orgs(self, filter: Mapping, like: List[str]=None, orgtype: str=None) -> List[Mapping]:
        try:
            mfilt = self._to_mongo_filter(filter) if filter else OrderedDict()
            if orgtype and orgtype in self.org_lvl:
                mfilt["orG_LVL_ID"] = self.org_lvl[orgtype]
            if like:
                mlike = self._like_to_mongo_filter(like, "orG_Name orG_ACRNM orG_CD".split())
                mfilt = {"$and": [ mfilt, mlike ]} if mfilt else mlike
        except (TypeError, ValueError) as ex:
            raise NSDClientError(collname, 400, "Bad input",
                                 f"Bad input query syntax: {str(filter)} ({str(ex)})")
            
        return self._select_from('Orgs', mfilt)

    def _select_from(self, collname: str, filter: Mapping) -> Iterator[Mapping]:
        try:
            return self._db[collname].find(filter, {"_id": False})
        except OperationFailure as ex:
            raise NSDClientError(collname, 400, "Bad input", "Bad input query syntax: "+str(filter))
        except PyMongoError as ex:
            raise NSDServerError(message=collname+" select failed due to MongoDB failure: "+str(ex)) from ex

    def _load_org(self, coll: str, data: List[Mapping]):
        self._db[coll].insert_many(data)

    def load_OUs(self, data: List[Mapping]):
        """
        load the given data into the OU collection
        """
        self._load_org('OUs', data)

    def load_divs(self, data: List[Mapping]):
        """
        load the given data into the Divisions collection
        """
        self._load_org('Divisions', data)

    def load_groups(self, data: List[Mapping]):
        """
        load the given data into the Groups collection
        """
        self._load_org('Groups', data)

    def load_people(self, data: List[Mapping]):
        """
        load the given data into the People collection.  This can be called multiple times
        """
        self._db['People'].insert_many(data)

    def load_orgs(self, data: List[Mapping]):
        """
        load the given data into the Orgs collection.
        """
        self._db['Orgs'].insert_many(data)


    def load(self, config, log=None, clear=True, withtrans=False):
        """
        load all data described in the given configuration object.  This is done safely within a
        database transaction so that it rolls back if there is a failure.  

        The configuration provided to this function is used to control the loading.  In particular,
        the following parameters will be looked for:

        ``dir``
            the directory where data files to be loaded should be found (default: "/data/nsd").
        ``person_file``
            the name of the file containing records describing people
        ``org_file``
            the name of the file containing records describing that organizations the people are 
            assigned to.

        :param dict config:  the configuration to use during loading (see above)
        :param Logger  log:  the Logger to send messages to
        :param bool  clear:  if True, all previously loaded records in the database will be deleted 
                             before loading (default: True)
        :param bool withtrans:  if True, use a database transaction to do the loading.  (Note this 
                             requires that the MongoDB be started with replicaSets; default: False.)

        :raises ConfigurationException:  if any of the configured files or directory does not exist
        """
        datadir = config.get('dir', '.')
        if not os.path.isdir(datadir):
            raise ConfigurationException(f"{datadir}: NSD data directory does not exist as a directory")
        personfile = config.get('person_file', "person.json")
        orgfile = config.get('org_file', "orgs.json")
#        if not os.path.isfile(os.path.join(datadir, personfile)):
#           raise ConfigurationException(f"{personfile}: NSD data does not exist as a file in {datadir}")
#        if not os.path.isfile(os.path.join(datadir, orgfile)):
#           raise ConfigurationException(f"{orgfile}: NSD data does not exist as a file in {datadir}")

        if not withtrans:
            self._load_notrans(datadir, personfile, orgfile, log, clear)
        else:
            self._load_notrans(datadir, personfile, orgfile, log, clear)

    def _load_notrans(self, datadir, personfile, orgfile, log, clear):
        people = self._db.People
        orgs = self._db.Orgs
        
        if clear:
            people.delete_many({})
            orgs.delete_many({})

        self._load_file(people, personfile, datadir, log)
        self._load_file(orgs, orgfile, datadir, log)

    def _load_withtrans(self, datadir, personfile, orgfile, log, clear):

        def _loadit(session):
            people = session.client[self._db.name].People
            orgs   = session.client[self._db.name].Orgs

            if clear:
                people.delete_many({}, session=session)
                orgs.delete_many({}, session=session)

            self._load_file(people, personfile, datadir, log, session)
            self._load_file(orgs, orgfile, datadir, log, session)

        with self._cli.start_session() as session:
            session.with_transaction(_loadit)

    def _load_file(self, mongocoll, file, dir='.', log=None, session=None):
        try:
            datafile = os.path.join(dir, file)
            with open(datafile) as fd:
                data = json.load(fd)
            mongocoll.insert_many(data, session=session)
        except FileNotFoundError as ex:
            if log:
                log.warning("Source data file not found: %s", datafile)
        except ValueError as ex:
            if log:
                log.warning("Source data not parseable as JSON: %s: %s", datafile, str(ex))

    def status(self) -> Mapping:
        """
        return a status message that indicates if the service appears ready
        """
        pc = 0
        oc = 0
        try:
            pc = self._db["People"].count_documents({})
            oc = self._db["Orgs"].count_documents({})
        except Exception as ex:
            return {
                "status": "not ready",
                "message": "Server error: failed to access database",
                "person_count": pc,
                "org_count": oc
            }
        if pc > 0 and oc > 0:
            return {
                "status": "ready",
                "message": f"Ready with {oc} organizations and {pc} people",
                "person_count": pc,
                "org_count": oc
            }
        return {
            "status": "not ready",
            "message": f"Not Ready: {oc} organizations and {pc} people loaded",
            "person_count": pc,
            "org_count": oc
        }
            
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

    elif config.get("factory"):
        raise ConfigurationException("people_service.factory type not supported: "+config["factory"])

    return None

