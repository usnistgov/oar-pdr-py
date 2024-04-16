"""
A semi-mock implementation of a person directory look-up service
"""
import json, os
from collections import OrderedDict
from collections.abc import Mapping
from abc import ABC, abstractmethod
from typing import List

from . import NSDException, NSDServerError, NSDClientError, NSDResourceNotFound

from pymongo import MongoClient
from pymongo.errors import PyMongoError, OperationFailure

class PeopleService(ABC):
    """
    An implementation of the NSD
    """

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
    def select_people(self, filter: Mapping) -> List[Mapping]:
        """
        return a list of matching person descriptions that match a given search query.
        Native schemas are used for the query filter and search results
        """
        raise NotImplemented()

    @abstractmethod
    def get_person(self, id: int) -> Mapping:
        """
        resolve a native person identifier into the description of the person
        """
        raise NotImplemented()
    
class MongoPeopleService(PeopleService):
    """
    An implementation of a PeopleService that uses a Mongo database to store its data
    """

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
        return list(self._db['OUs'].find({}, {"_id": False}))

    def divs(self) -> List[Mapping]:
        return list(self._db['Divisions'].find({}, {"_id": False}))

    def groups(self) -> List[Mapping]:
        return list(self._db['Groups'].find({}, {"_id": False}))

    def _get_rec(self, id: int, coll: str, idprop: str='orG_ID') -> Mapping:
        try:
            hits = list(self._db[coll].find({idprop: id}, {"_id": False}))
        except PyMongoError as ex:
            raise NSDServerError("Org listing failed due to MongoDB failure: "+str(ex)) from ex

        if not hits:
            return None
        return hits[0]

    def get_OU(self, id: int) -> Mapping:
        return self._get_rec(id, "OUs")
    
    def get_div(self, id: int) -> Mapping:
        return self._get_rec(id, "Divisions")

    def get_group(self, id: int) -> Mapping:
        return self._get_rec(id, "Groups")

    def get_person(self, id: int) -> Mapping:
        return self._get_rec(id, "People", "peopleID")

    def _make_mongo_constraints(self, filter: Mapping) -> Mapping:
        cnsts = []
        for prop in filter:
            if filter[prop]:
                cnsts.append(self._make_prop_constraint(prop, filter[prop]))

        if len(cnsts) < 1:
            cnsts = {}
        elif len(cnsts) == 1:
            cnsts = cnsts[0]
        else:
            cnsts = {"$or": cnsts}
        return cnsts

    def _make_prop_constraint_exact(self, prop, values):
        if len(values) == 1:
            return {prop: values[0]}
        else:
            return {prop: {"$in": values}}

    def _make_prop_constraint_like(self, prop, values):
        return {prop: {"$regex": "|".join(values), "$options": "i"}}

    def select_people(self, filter: Mapping) -> List[Mapping]:
        cnsts = self._make_mongo_constraints(filter)
        
        try:
            return list(self._db['People'].find(cnsts, {"_id": False}))
        except OperationFailure as ex:
            raise NSDClientError('People', 400, "Bad input", "Bad input query syntax: "+str(filter))
        except PyMongoError as ex:
            raise NSDServerError(message="people select failed due to MongoDB failure: "+str(ex)) from ex

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

    def load(self, config, log=None, clear=True):
        """
        load all data described in the given configuration object
        """
        if clear:
            self._db['OUs'].delete_many({})
            self._db['Divisions'].delete_many({})
            self._db['Groups'].delete_many({})
            self._db['People'].delete_many({})

        datadir = config.get('dir', '.')
        self._load_file(self.load_OUs, config.get('ou_file', "ou.json"), datadir, log)
        self._load_file(self.load_divs, config.get('div_file', "div.json"), datadir, log)
        self._load_file(self.load_groups, config.get('group_file', "group.json"), datadir, log)
        self._load_file(self.load_people, config.get('person_file', "person.json"), datadir, log)

    def _load_file(self, loader, file, dir='.', log=None):
        try:
            datafile = os.path.join(dir, file)
            with open(datafile) as fd:
                data = json.load(fd)
            loader(data)
        except FileNotFoundError as ex:
            if log:
                log.warning("Source data file not found: %s", datafile)
        except ValueError as ex:
            if log:
                log.warning("Source data not parseable as JSON: %s: %s", datafile, str(ex))


    
