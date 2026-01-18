"""
An implementation of the dbio interface that uses a MongoDB database as it backend store
"""
import re
from copy import deepcopy
from collections.abc import Mapping, MutableMapping, Set
from typing import Iterator, List
from . import base
from .notifier import DBIOClientNotifier

from pymongo import MongoClient, ASCENDING

from nistoar.base.config import ConfigurationException, merge_config
from nistoar.nsd.service import PeopleService, MongoPeopleService, create_people_service

_dburl_re = re.compile(r"^mongodb://(\w+(:\S+)?@)?\w+(\.\w+)*(:\d+)?/\w+(\?\w.*)?$")
SUPPORTED_CONSTRAINTS = set("name id owner status_state".split())

class MongoDBClient(base.DBClient):
    """
    an implementation of DBClient using a MongoDB database as the backend store.
    """
    ACTION_LOG_COLL = base.PROV_ACT_LOG
    HISTORY_COLL = 'history'

    def __init__(self, dburl: str, config: Mapping, projcoll: str, foruser: str = base.ANONYMOUS,
                 peopsvc: PeopleService = None, notifier: DBIOClientNotifier = None):
        """
        create the client with its connector to the MongoDB database

        :param str   dburl:  the URL of MongoDB database in the form, 'mongodb://USER:PW@HOST:PORT/DBNAME' 
        :param dict config:  the configuration for the DBClient
        :param str foruser:  the user requesting records from the database; this will be the identity 
                             used to authorize access to its contents
        :param PeopleService peopsvc:  a PeopleService that the client can use to look up people in the 
                             organization.
        :param DBIOClientNotifier notifier:  a DBIOClientNotifier to use to alert DBIO clients about 
                             updates to the DBIO data.
        """
        if not _dburl_re.match(dburl):
            raise ValueError("DBClient: Bad dburl format (need 'mongodb://[USER:PASS@]HOST[:PORT]/DBNAME'): "+
                             dburl)
        self._dburl = dburl
        self._mngocli = None
        super(MongoDBClient, self).__init__(config, projcoll, None, foruser, peopsvc, notifier)

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

    def free(self):
        """
        free up resources used by this client.  

        This implementation calls :py:meth:`disconnect`.
        """
        self.disconnect()

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
        """
        return an iterator of project records for which the given user has at least one of the given 
        permissions and matches additional optional search constraints

        :param str       user:  the identity of the user that wants access to the records.  
        :param str|[str] perm:  the permissions the user requires for the selected record.  For
                                each record returned the user will have at least one of these
                                permissions.  The value can either be a single permission value
                                (a str) or a list/tuple of permissions
        :param list _cnsts_:    an additional constraint that will match any record with a property
                                refered to by the constraint name if its value matches any of those 
                                given in the constraint's value list.  Supported _constraint_ names 
                                include ``name``, ``id``, ``status.state``, and ``owner``.  Particular 
                                implementations may support additional properties; any unsupported 
                                constraints will be ignored.  Note that multiple constraints are 
                                logically AND-ed together; that is, a matched record must match at least
                                one value from each constraint value list.
        """
        if isinstance(perm, str):
            perm = [perm]
        if isinstance(perm, (list, tuple)):
            perm = set(perm)
        idents = [self.user_id] + list(self.user_groups)

        for prop in cnsts:
            if cnsts.get(prop) and not isinstance(cnsts[prop], (list, tuple)):
                cnsts[prop] = [ cnsts[prop] ]

        # Build permission constraints (same as in select_records method)
        if len(perm) > 1:
            constraints = {"$or": []}
            for p in perm:
                constraints["$or"].append({"acls."+p: {"$in": idents}})
        else:
            constraints = {"acls."+perm.pop(): {"$in": idents}}

        # Combine other filters with permission constraints
        if cnsts:
            for prop in SUPPORTED_CONSTRAINTS:
                vals = cnsts.get(prop)
                if vals:
                    if prop == "status_state":
                        prop = "status.state"
                    constraints[prop] = {"$in": vals}
            
        try:
            coll = self.native[self._projcoll]

            for rec in coll.find(constraints, {'_id': False}):
                yield base.ProjectRecord(self._projcoll, rec, self)

        except Exception as ex:
            raise base.DBIOException("Failed while selecting records: " + str(ex), cause=ex)

    def select_records_by_ids(self, ids: List[str],  perm: base.Permissions=base.ACLs.OWN) -> Iterator[base.ProjectRecord]:
        if isinstance(perm, str):
            perm = [perm]
        if isinstance(perm, (list, tuple)):
            perm = set(perm)
        idents = [self.user_id] + list(self.user_groups)

        # Build permission constraints (same as in select_records method)
        if len(perm) > 1:
            perm_constraints = {"$or": []}
            for p in perm:
                perm_constraints["$or"].append({"acls."+p: {"$in": idents}})
        else:
            perm_constraints = {"acls."+list(perm)[0]: {"$in": idents}}

        # Combine ID filter with permission constraints
        query = {"id": {"$in": ids}, **perm_constraints}

        try:
            coll = self.native[self._projcoll]
            for rec in coll.find(query, {'_id': False}):
                yield base.ProjectRecord(self._projcoll, rec, self)

        except Exception as ex:
            raise base.DBIOException("Failed while selecting records: " + str(ex), cause=ex)

    def adv_select_records(self, filter: dict,
                           perm: base.Permissions=base.ACLs.OWN) -> Iterator[base.ProjectRecord]:
        
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
                for rec in coll.find(filter, {'_id': False}):
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

    def client_for(self, projcoll: str, foruser: str = None):
        """
        create a new DBClient using the same backend as this one but attached to a different collection
        (and possibly user).
        :param str projcol:  the project collection name
        :param str foruser:  the user this should be used on behalf of.  This controls what records the 
                             client has access to.
        """
        if not foruser:
            foruser = self.user_id
        return self.__class__(self._dburl, self._cfg, projcoll, foruser)
                

class MongoDBClientFactory(base.DBClientFactory):
    """
    a DBClientFactory that creates MongoDBClient instances in which records are stored in a MongoDB 
    database.

    In addition to :py:method:`common configuration parameters <nistoar.midas.dbio.base.DBClient.__init__>`, 
    this implementation also supports:

    ``db_url``
        the URL for the MongoDB connection, of the form, 
        ``mongodb://``*[USER*``:``*PASS*``@``*]HOST[*``:``*PORT]*``/``*DBNAME*
    ``people_service``:
        either a string label or a dictionary for configuring a people service client that will be 
        attached to the client.  If it is a dictionary, it will be passed to the :py:meth:`create_people_service`
        method.  A string label will be converted to a static configuration.  Supported labels include
        ``embedded`` which tells the factory that the people database collections are contained within 
        the MIDAS backend database; the combined (``dbio``) configuration provided to the factory and 
        the :py:meth:`create_client` method will be passed to the :py:meth:`create_people_service` method.
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

        peopsvc = self._peopsvc
        if not peopsvc:
            pscfg = cfg.get("people_service", {})
            if pscfg == "embedded":
                # The Mongo database includes people service collections; use the
                # same connection for the people service
                pscfg = cfg
                if "db_url" not in pscfg:
                    pscfg["db_url"] = self._dburl
            peopsvc = self.create_people_service(pscfg)

        notifier = self._notifier
        if not notifier:
            notifier = self._create_notifier_from_config(cfg)

        return MongoDBClient(self._dburl, cfg, servicetype, foruser, peopsvc, notifier)


