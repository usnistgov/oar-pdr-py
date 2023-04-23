"""
An implementation of the dbio interface that persists data to files on disk.
"""
import os, json
from pathlib import Path
from copy import deepcopy
from collections.abc import Mapping, MutableMapping, Set
from typing import Iterator, List
from . import base

from nistoar.pdr.utils import read_json, write_json
from nistoar.base.config import ConfigurationException, merge_config

class FSBasedDBClient(base.DBClient):
    """
    an implementation of DBClient in which the data is persisted to flat files on disk.
    """

    def __init__(self, dbroot: str, config: Mapping, projcoll: str, foruser: str = base.ANONYMOUS):
        self._root = Path(dbroot)
        if not self._root.is_dir():
            raise base.DBIOException("FSBasedDBClient: %s: does not exist as a directory" % dbroot)
        super(FSBasedDBClient, self).__init__(config, projcoll, self._root, foruser)

    def _ensure_collection(self, collname):
        collpath = self._root / collname
        if not collpath.exists():
            os.mkdir(collpath)

    def _read_rec(self, collname, id):
        recpath = self._root / collname / (id+".json")
        if not recpath.is_file():
            return None
        try:
            return read_json(str(recpath))
        except ValueError as ex:
            raise DBIOException(id+": Unable to read DB record as JSON: "+str(ex))
        except IOError as ex:
            raise DBIOException(str(recpath)+": file locking error: "+str(ex))

    def _write_rec(self, collname, id, data):
        self._ensure_collection(collname)
        recpath = self._root / collname / (id+".json")
        exists = recpath.exists()
        try: 
            write_json(data, str(recpath))
        except Exception as ex:
            raise DBIOException(id+": Unable to write DB record: "+str(ex))
        return not exists

    def _next_recnum(self, shoulder):
        num = self._read_rec("nextnum", shoulder)
        if num is None:
            num = 0
        num += 1
        self._write_rec("nextnum", shoulder, num)
        return num

    def _try_push_recnum(self, shoulder, recnum):
        recpath = self._root / "nextnum" / (shoulder+".json")
        if not recpath.exists():
            return
        num = self._read_rec("nextnum", shoulder)
        if num >= 0 and num == recnum:
            num -= 1
            self._write_rec("nextnum", shoulder, num)

    def _get_from_coll(self, collname, id) -> MutableMapping:
        return self._read_rec(collname, id)

    def _select_from_coll(self, collname, incl_deact=False, **constraints) -> Iterator[MutableMapping]:
        collpath = self._root / collname
        if not collpath.is_dir():
            return
        for root, dirs, files in os.walk(collpath):
            for fn in files:
                try:
                    rec = read_json(os.path.join(root, fn))
                except ValueError:
                    # skip over corrupted records
                    continue

                if rec.get('deactivated') and incl_deact:
                    continue
                cancel = False
                for ck, cv in constraints.items():
                    if rec.get(ck) != cv:
                        cancel = True
                        break
                if cancel:
                    continue
                yield rec

    def _select_prop_contains(self, collname, prop, target, incl_deact=False) -> Iterator[MutableMapping]:
        collpath = self._root / collname
        if not collpath.is_dir():
            return
        for root, dirs, files in os.walk(collpath):
            for fn in files:
                try:
                    recf = os.path.join(root, fn)
                    rec = read_json(recf)
                except ValueError:
                    # skip over corrupted records
                    continue
                except IOError as ex:
                    raise DBIOException(recf+": file locking error: "+str(ex))

                if rec.get('deactivated') and not incl_deact:
                    continue
                if prop in rec and isinstance(rec[prop], (list, tuple)) and target in rec[prop]:
                    yield rec

    def _delete_from(self, collname, id):
        recpath = self._root / collname / (id+".json")
        if recpath.is_file():
            recpath.unlink()
            shldr, num = self._parse_id(id)
            if shldr:
                self._try_push_recnum(shldr, num)
            return True
        return False

    def _upsert(self, coll: str, recdata: Mapping) -> bool:
        self._ensure_collection(coll)
        try:
            return self._write_rec(coll, recdata['id'], recdata)
        except KeyError:
            raise base.DBIOException("_upsert(): record is missing 'id' property")

    def select_records(self, perm: base.Permissions=base.ACLs.OWN) -> Iterator[base.ProjectRecord]:
        if isinstance(perm, str):
            perm = [perm]
        if isinstance(perm, (list, tuple)):
            perm = set(perm)

        collpath = self._root / self._projcoll
        if not collpath.is_dir():
            return
        for root, dirs, files in os.walk(collpath):
            for fn in files:
                try:
                    recf = os.path.join(root, fn)
                    rec = base.ProjectRecord(self._projcoll, read_json(recf), self)
                except ValueError:
                    # skip over corrupted records
                    continue
                except IOError as ex:
                    raise base.DBIOException(recf+": file locking error: "+str(ex))
                for p in perm:
                    if rec.authorized(p):
                        yield rec
                        break

    def _save_action_data(self, actdata: Mapping):
        self._ensure_collection(base.PROV_ACT_LOG)
        try:
            recpath = self._root / base.PROV_ACT_LOG / (actdata['subject']+".lis")
            return self._append_json_to_listfile(actdata, recpath)
        except KeyError as ex:
            raise ValueError("_save_action_data(): Action is missing subject id")
        except Exception as ex:
            raise base.DBIOException(actdata['subject']+": Unable to append action: "+str(ex)) from ex

    # the action log list file contains one JSON object per line
    def _append_json_to_listfile(self, data: Mapping, outpath: Path):
        exists = outpath.exists()
        with open(outpath, 'a') as fd:
            fd.write(json.dumps(data))
            fd.write("\n")
        return not exists

    # the action log list file contains one JSON object per line
    def _load_from_listfile(self, inpath: Path):
        if not inpath.exists():
            return []
        with open(inpath) as fd:
            return [json.loads(line.strip()) for line in fd]
                
    def _select_actions_for(self, id: str) -> List[Mapping]:
        self._ensure_collection(base.PROV_ACT_LOG)
        recpath = self._root / base.PROV_ACT_LOG / (id+".lis")
        if not recpath.is_file():
            return []
        try:
            return self._load_from_listfile(recpath)
        except Exception as ex:
            raise base.DBIOException(id+": Unable to read actions: "+str(ex))

    def _delete_actions_for(self, id):
        self._ensure_collection(base.PROV_ACT_LOG)
        recpath = self._root / base.PROV_ACT_LOG / (id+".lis")
        if recpath.is_file():
            recpath.unlink()

    def _save_history(self, histrec):
        if not histrec.get('recid'):
            raise ValueError("_save_history(): History is missing record id")
        self._ensure_collection("history")

        history = []
        recpath = self._root / 'history' / (histrec['recid']+".json")
        if recpath.is_file():
            try:
                history = read_json(str(recpath))
            except Exception as ex:
                raise base.DBIOException(histrec['recid']+": Failed to read old history entries: "+str(ex))
        elif recpath.exists():
            raise base.DBIOException(str(recpath)+": not a file")
        
        history.append(histrec)
        try:
            write_json(history, str(recpath))
        except Exception as ex:
            raise base.DBIOException(histrec['recid']+": Failed to write history entries: "+str(ex))

class FSBasedDBClientFactory(base.DBClientFactory):
    """
    a DBClientFactory that creates FSBasedDBClient instances in which records are stored in JSON
    files on disk under a specified directory.

    In addition to :py:method:`common configuration parameters <nistoar.midas.dbio.base.DBClient.__init__>`, 
    this implementation also supports:

    ``db_root_dir``
         the root directory where the database's record files will be store below.  If not specified,
         this value must be provided to the constructor directly.  
    """

    def __init__(self, config: Mapping, dbroot: str = None):
        """
        Create the factory with the given configuration.

        :param dict config:  the configuration parameters used to configure clients
        :param str  dbroot:  the root directory to use to store database record files below; if 
                             not provided, the value of the ``db_root_dir`` configuration 
                             parameter will be used.  
        :raise ConfigurationException:  if the database's root directory is provided neither as an
                             argument nor a configuration parameter.
        :raise DBIOException:  if the specified root directory does not exist
        """
        super(FSBasedDBClientFactory, self).__init__(config)
        if not dbroot:
            dbroot = self.cfg.get("db_root_dir")
            if not dbroot:
                raise ConfigurationException("Missing required configuration parameter: db_root_dir")
        if not os.path.isdir(dbroot):
            raise base.DBIOException("FSBasedDBClientFactory: %s: does not exist as a directory" % dbroot)
        self._dbroot = dbroot

    def create_client(self, servicetype: str, config: Mapping = {}, foruser: str = base.ANONYMOUS):
        cfg = merge_config(config, deepcopy(self._cfg))
        return FSBasedDBClient(self._dbroot, cfg, servicetype, foruser)

