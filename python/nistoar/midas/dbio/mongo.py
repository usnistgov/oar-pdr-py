"""
An implementation of the dbio interface that uses a MongoDB database as it backend store
"""
from collections.abc import Mapping, MutableMapping, Set
import re
from typing import Iterator, List
from . import base

from pymongo import MongoClient

from nistoar.base.config import ConfigurationException

_dburl_re = re.compile(r"^mongodb://(\w+(:\S+)?@)?\w+(\.\w+)*(:\d+)?/\w+$")

class MongoDBClient(base.DBClient):
    """
    an implementation of DBClient using a MongoDB database as the backend store.
    """

    def __init__(self, dburl: str, config: Mapping, projcoll: str, foruser: str = base.ANONYMOUS):
        """
        create the client with its connector to the MongoDB database

        :param str   dburl:  the URL of MongoDB database in the form, 'mongodb://USER:PW@HOST:PORT/DBNAME' 
        :param dict config:  the configuration for the DBClient
        :param str foruser:  the user requesting records from the database; this will be the identity 
                             used to authorize access to its contents
        """
        if not _dburl_re.match(dburl):
            raise ValueError("DBClient: Bad dburl format (need 'mongodb://[USER:PASS@]HOST[:PORT]/DBNAME'): "+
                             dburl)
        self._dburl = dburl
        self._mngocli = None
        super(MongoDBClient, self).__init__(config, projcoll, None, foruser)

    def connect(self):
        """
        establish a connection to the database.  This will set the native property to the pymongo 
        database object.
        """
        self._mngocli = MongoClient(self._dburl)
        # the proper method to use depends on pymongo version
        if not hasattr(self._mngocli, 'get_database'):
            self._mngocli.get_database = self._mngocli.get_default_database

        self._native = self._mngocli.get_database()

    def disconnect(self):
        """
        close the connection to the database.
        """
        if self._mngocli:
            try:
                self._mngocli.close()
            finally:
                self._mngocli = None
                self._native = None

    @property
    def native(self):
        """
        the native pymongo database object that contains the DBIO collections.  Accessing this property
        will implicitly connect this client to the underlying MongoDB database.
        """
        if not self._native:
            self.connect()
        return self._native

    def _upsert(self, collname: str, recdata: Mapping) -> bool:
        try:
            id = recdata['id']
        except KeyError as ex:
            raise DBIOException("_upsert(): record is missing required 'id' property")
        key = {"id": id}

        try:
            db = self.native
            coll = db[collname]

            result = coll.replace_one(key, recdata, upsert=True)
            return result.matched_count == 0

        except DBIOException as ex:
            raise
        except Exception as ex:
            raise DBIOException("Failed to load record with id=%s: %s" % (id, str(ex)))

    def _next_recnum(self, shoulder):
        key = {"slot": shoulder}

        try:
            db = self.native
            coll = db["nextnum"]

            if coll.count_documents(key) == 0:
                coll.insert_one({
                    "slot": shoulder,
                    "next": 1
                })

            result = coll.find_one_and_update(key, {"$inc": {"next": 1}})
            return result["next"]

        except DBIOException as ex:
            raise
        except Exception as ex:
            raise DBIOException("Failed to access named sequence, =%s: %s" % (shoulder, str(ex)))

    def _get_from_coll(self, collname, id) -> MutableMapping:
        key = {"id": id}

        try:
            db = self.native
            coll = db[collname]

            return coll.find_one(key, {'_id': False})

        except Exception as ex:
            raise DBIOException("Failed to access record with id=%s: %s" % (id, str(ex)))

    def _select_from_coll(self, collname, **constraints) -> Iterator[MutableMapping]:
        try:
            db = self.native
            coll = db[collname]

            for rec in coll.find(constraints, {'_id': False}):
                yield rec

        except Exception as ex:
            raise DBIOException("Failed while selecting records: " + str(ex))

    def _select_prop_contains(self, collname, prop, target) -> Iterator[MutableMapping]:
        try:
            db = self.native
            coll = db[collname]

            for rec in coll.find({prop: target}, {'_id': False}):
                yield rec

        except Exception as ex:
            raise DBIOException("Failed while selecting records: " + str(ex))

    def _delete_from(self, collname, id):
        key = {"id": id}
        try:
            db = self.native
            coll = db[collname]

            results = coll.delete_one(key)
            return results.deleted_count > 0

        except Exception as ex:
            raise DBIOException("Failed while deleting record with id=%s: %s" % (id, str(ex)))

    def select_records(self, perm: base.Permissions=base.ACLs.OWN) -> Iterator[base.ProjectRecord]:
        if isinstance(perm, str):
            perm = [perm]
        if isinstance(perm, (list, tuple)):
            perm = set(perm)

        idents = [self.user_id] + list(self.user_groups)
        if len(perm) > 1:
            constraints = {"$or": []}
            for p in perm:
                constraints["$or"].append({"acls."+p: {"$in": idents}})
        else:
            constraints = {"acls."+perm.pop(): {"$in": idents}}
            
        try:
            coll = self.native[self._projcoll]

            for rec in coll.find(constraints, {'_id': False}):
                yield base.ProjectRecord(self._projcoll, rec)

        except Exception as ex:
            raise base.DBIOException("Failed while selecting records: " + str(ex))

class MongoDBClientFactory(base.DBClientFactory):
    """
    a DBClientFactory that creates MongoDBClient instances in which records are stored in a MongoDB 
    database.

    In addition to :py:method:`common configuration parameters <nistoar.midas.dbio.base.DBClient.__init__>`, 
    this implementation also supports:

    ``db_url``
        the URL for the MongoDB connection, of the form, 
        ``mongodb://``*[USER*``:``*PASS*``@``*]HOST[*``:``*PORT]*``/``*DBNAME*
    """

    def __init__(self, config: Mapping, dburl: str = None):
        """
        Create the factory with the given configuration.

        :param dict config:  the configuration parameters used to configure clients
        :param str   dburl:  the URL for the MongoDB connection; it takes the same form as the 
                             ``db_url`` configuration parameter.  If 
                             not provided, the value of the ``db_url`` configuration 
                             parameter will be used.  
        :raise ConfigurationException:  if the database's URL is provided neither as an
                             argument nor a configuration parameter.
        :raise ValueError:  if the specified database URL is of an incorrect form
        """
        super(MongoDBClientFactory, self).__init__(config)
        if not dburl:
            dburl = self._cfg.get("db_url")
            if not dburl:
                raise ConfigurationException("Missing required configuration parameter: db_url")
        if not _dburl_re.match(dburl):
            raise ValueError("MongoDBClientFactory: Bad dburl format (need "+
                             "'mongodb://[USER:PASS@]HOST[:PORT]/DBNAME'): "+
                             dburl)
        self._dburl = dburl

    def create_client(self, servicetype: str, foruser: str = base.ANONYMOUS):
        return MongoDBClient(self._dburl, self._cfg, servicetype, foruser)

