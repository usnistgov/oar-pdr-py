"""
An implementation of the dbio interface that uses a MongoDB database as it backend store
"""
import re
from copy import deepcopy
from collections.abc import Mapping, MutableMapping, Set
from typing import Iterator, List
from . import base

from pymongo import MongoClient, ASCENDING

from nistoar.base.config import ConfigurationException, merge_config

_dburl_re = re.compile(r"^mongodb://(\w+(:\S+)?@)?\w+(\.\w+)*(:\d+)?/\w+(\?\w.*)?$")

class MongoDBClient(base.DBClient):
    """
    an implementation of DBClient using a MongoDB database as the backend store.
    """
    ACTION_LOG_COLL = base.PROV_ACT_LOG
    HISTORY_COLL = 'history'

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
        if self._native is None:
            self.connect()
        return self._native

    def _upsert(self, collname: str, recdata: Mapping) -> bool:
        try:
            id = recdata['id']
        except KeyError as ex:
            raise base.DBIOException("_upsert(): record is missing required 'id' property")
        key = {"id": id}

        try:
            db = self.native
            coll = db[collname]

            result = coll.replace_one(key, recdata, upsert=True)
            return result.matched_count == 0

        except base.DBIOException as ex:
            raise
        except Exception as ex:
            raise base.DBIOException("Failed to load record with id=%s: %s" % (id, str(ex)))

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

        except base.DBIOException as ex:
            raise
        except Exception as ex:
            raise base.DBIOException("Failed to access named sequence, =%s: %s" % (shoulder, str(ex)))

    def _try_push_recnum(self, shoulder, recnum):
        key = {"slot": shoulder}
        try:
            db = self.native
            coll = db["nextnum"]

            with self._mngocli.start_session() as session:
                if coll.count_documents(key) == 0:
                    return
                slot = coll.find_one(key)
                if slot["next"] == recnum+1:
                    coll.update_one(key, {"$inc": {"next": -1}})

        except base.DBIOException as ex:
            raise
        except Exception as ex:
            # ignore database errors
            pass

    def _get_from_coll(self, collname, id) -> MutableMapping:
        key = {"id": id}

        try:
            db = self.native
            coll = db[collname]

            return coll.find_one(key, {'_id': False})

        except Exception as ex:
            raise base.DBIOException("Failed to access record with id=%s: %s" % (id, str(ex)))

    def _select_from_coll(self, collname, incl_deact=False, **constraints) -> Iterator[MutableMapping]:
        try:
            db = self.native
            coll = db[collname]

            if not incl_deact:
                constraints['deactivated'] = None

            for rec in coll.find(constraints, {'_id': False}):
                yield rec

        except Exception as ex:
            raise base.DBIOException("Failed while selecting records: " + str(ex))

    def _select_prop_contains(self, collname, prop, target, incl_deact=False) -> Iterator[MutableMapping]:
        try:
            db = self.native
            coll = db[collname]

            query = { prop: target }
            if not incl_deact:
                query['deactivated'] = None

            for rec in coll.find(query, {'_id': False}):
                yield rec

        except Exception as ex:
            raise base.DBIOException("Failed while selecting records: " + str(ex))

    def _delete_from(self, collname, id):
        key = {"id": id}
        try:
            db = self.native
            coll = db[collname]

            results = coll.delete_one(key)
            return results.deleted_count > 0

        except Exception as ex:
            raise base.DBIOException("Failed while deleting record with id=%s: %s" % (id, str(ex)))
         

    def select_records(self, perm: base.Permissions=base.ACLs.OWN, **cnsts) -> Iterator[base.ProjectRecord]:
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
                yield base.ProjectRecord(self._projcoll, rec, self)

        except Exception as ex:
            raise base.DBIOException("Failed while selecting records: " + str(ex), cause=ex)
        
    
    def adv_select_records(self, filter: dict,
                           perm: base.Permissions=base.ACLs.OWN) -> Iterator[base.ProjectRecord]:
        print(filter)
        if base.DBClient.check_query_structure(filter):
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
                
            filter["$and"].append(constraints)
                
            try:
                
                coll = self.native[self._projcoll]
                for rec in coll.find(filter):
                    yield base.ProjectRecord(self._projcoll, rec, self)

            except Exception as ex:
                raise base.DBIOException(
                    "Failed while selecting records: " + str(ex), cause=ex)
        else:
            raise SyntaxError('Wrong query format')

    def _save_action_data(self, actdata: Mapping):
        try:
            coll = self.native[self.ACTION_LOG_COLL]
            result = coll.insert_one(actdata)
            return True

        except base.DBIOException as ex:
            raise
        except Exception as ex:
            raise base.DBIOException(actdata.get('subject',"id=?")+
                                     ": Failed to save action: "+str(ex)) from ex

    def _select_actions_for(self, id: str) -> List[Mapping]:
        try:
            coll = self.native[self.ACTION_LOG_COLL]
            return [rec for rec in coll.find({'subject': id}, {'_id': False}).sort("timestamp", ASCENDING)]
        except Exception as ex:
            raise base.DBIOException(id+": Failed to select action records: "+str(ex)) from ex

    def _delete_actions_for(self, id):
        try:
            coll = self.native[self.ACTION_LOG_COLL]
            result = coll.delete_many({'subject': id})
            return result.deleted_count > 0
        except Exception as ex:
            raise base.DBIOException(id+": Failed to delete action records: "+str(ex)) from ex

    def _save_history(self, histrec):
        try:
            coll = self.native[self.HISTORY_COLL]
            result = coll.insert_one(histrec)
            return True
        except base.DBIOException as ex:
            raise
        except Exception as ex:
            raise DBIOEception(histrec.get('recid', "id=?")+": Failed to save history entry: "+str(ex)) \
                from ex
    

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

    def create_client(self, servicetype: str, config: Mapping = {}, foruser: str = base.ANONYMOUS):
        cfg = merge_config(config, deepcopy(self._cfg))
        return MongoDBClient(self._dburl, cfg, servicetype, foruser)
