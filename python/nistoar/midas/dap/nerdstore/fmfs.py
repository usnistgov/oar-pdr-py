""" 
an implementation of the nerstore interface that stores nerdm data on local disk but consults the 
file manager for the files that should be part of the resource.
"""
import os
from copy import deepcopy
from collections import OrderedDict
from collections.abc import Mapping
from json import JSONDecodeError

from .fsbased import *
from .base import (DATAFILE_TYPE, SUBCOLL_TYPE, DOWNLOADABLEFILE_TYPE,
                   RemoteStorageException, NERDStorageException, ObjectNotFound)
from ..fm import FileManager
from nistoar.nerdm.constants import core_schema_base, schema_versions
from nistoar.pdr.preserve.bagit.builder import (NERD_DEF, NERDM_CONTEXT, BagBuilder)

SCAN_CHECKSUM_ALGORITHM = "sha256"

class FMFSResourceStorage(FSBasedResourceStorage):
    """
    a factory for opening records stoared in the JSON files on disk
    """

    @classmethod
    def from_config(cls, config: Mapping, logger: Logger):
        """
        an class method for creatng an FSBasedResourceStorage instance from configuration data.

        Recognized configuration paramters include:

        ``store_dir``
             (str) _required_. The root directory under which all resource data will be stored.
        ``default_shoulder``
             (str) _optional_. The shoulder that new identifiers are minted under.  This is not 
             normally used as direct clients of this class typically choose the shoulder on a 
             per-call basis.  The default is "nrd".
        ``file_manager``
             (dict) _optional_. The configuration for clients of the remote file manager service.

        :param dict config:  the configuraiton for the specific type of storage
        :param Logger logger:  the logger to use to capture messages
        """
        if not config.get('store_dir'):
            raise ConfigurationException("Missing required configuration parameter: store_dir")

        fmcfg = not config.get('file_manager')
        
        return cls(config['store_dir'], config.get("default_shoulder", "nrd"),
                   fmcfg.get('base_url'), fmcfg.get('auth'), logger)

    def __init__(self, storeroot: str, newidprefix: str="nrd",
                 fmappbaseurl: str=None, fmauth: dict=None, logger: Logger=None):
        """
        initialize a factory with with the resource data storage rooted at a given directory
        When used, this implementation is expecting ``username`` and ``password`` properties for
        
        :param str    storeroot:  the root directory under which all JSON files will be stored
        :param str  newidprefix:  a prefix to use when minting new identifiers
        :param str fmappbaseurl:  the base URL for the file-manager.  If ``None``, a file manager 
                                  will not be used
        :param dict   fmappauth:  the data needed for authentication.  If ``None`` (or empty),
                                  authentication credentials will be not be sent in file-manager 
                                  requests.
        :param Logger    logger:  the Logger to use for messages
        """
        super(FMFSResourceStorage, self).__init__(storeroot, newidprefix, logger)
        self._fmbase = fmappbaseurl
        if self._fmbase and self._fmbase.endswith('/'):
            self._fmbase = self._fmbase.rstrip('/')
        self._fmauth = None
        self._fmcli = None

        if self._fmbase:
            if fmappauth:
                if not fmauth.get('username') or not fmappauth.get('password') or \
                   (fmappauth.get('type') and fmappauth.get('type') != 'basic'):
                    raise ConfigurationException("Unsupported file-manager authentication: "+str(fmappauth))
                self._fmauth = fmappauth

            self._fmcli = FileManager({
                'base_url': self._fmbase,
                'authentication_user': self._fmauth.get('username'),
                'authentication_password': self._fmauth.get('password')
            })
        if not self._fmcli:
            self._log.warning("FMFSResourceStorage: no file manager client set; proceding without access")

    def open(self, id: str=None) -> NERDResource:
        if not id:
            id = self._new_id()
        return FMFSResource(id, self._dir, self._fmcli, True, self._log)


class FMFSResource(FSBasedResource):
    """
    a file-based implementation of the NERDResource interface that leverages the file-manager to 
    get the file list.
    """

    def __init__(self, id: str, storeroot: str, fmclient: FileManager = None,
                 create: bool=True, parentlog: Logger=None):
        super(FMFSResource, self).__init__(id, storeroot, create, parentlog)
        self._fmcli = fmclient

    @property
    def files(self):
        if self.deleted:
            raise RecordDeleted(self.id, "get metadata")
        if not self._files:
            dir = self._dir / "files"
            if not dir.exists():
                dir.mkdir()
            self._files = FMFSFileComps(self, dir, self._fmcli)
        return self._files

_NO_FM_STATUS = OrderedDict([
    ("resource_uri", None),
    ("file_count", -1),
    ("folder_count", -1),
    ("syncing", False),
    ("last_modified", "(unknown)")
])

class FMFSFileComps(FSBasedFileComps):
    """
    an file-based implementation of the NERDFileComps interface that leverages a remote file-manager
    that contains the extra files.  It uses the file-manager to determine which files are in the 
    collection and how they are organized.  
    """
    _comp_types = deepcopy(BagBuilder._comp_types)
    
    def __init__(self, resource: NERDResource, filedir: str, fmcli: FileManager=None, iscollf=None):
        super(FMFSFileComps, self).__init__(resource, filedir, iscollf)
        self._last_scan_id = None
        self._fmcli = fmcli
        self.update_hierarchy()

    def update_hierarchy(self) -> Mapping:
        if self._fmcli:
            scan = self._scan_files()
            self._last_scan_id = scan['id']
            return self._update_files_from_scan(scan)
        return _NO_FM_STATUS

    def update_metadata(self) -> Mapping:
        if self._fmcli:
            return self._update_files_from_scan(self._get_filescan())
        return _NO_FM_STATUS

    def _scan_files(self):
        # trigger a remote scan of the files (done at construction time)

        # delete last scan
        if self._last_scan_id:
            try:
                self._fmcli.delete_scan_files(self._res.id, self._last_scan_id)
            except Exception as ex:
                self._res.log.warning("Failed to delete old scan (id=%s)", self._last_scan_id)
            finally:
                self._last_scan_id = None

        try:
            resp = self._fmcli.post_scan_files(self._res.id)
            if not isinstance(resp, Mapping):
                self._res.log.error("Unexpected response from scan request: "+
                                    "not a JSON object (is URL correct?)")
                raise RemoteStorageException("%s: failed to trigger file scan")

            elif 'scan_id' not in resp:
                self._res.log.error("Unexpected response from scan request; no scan_id included")
                if resp.get('message'):
                    self._res.log.error("Message from server: "+message);
                raise RemoteStorageException("%s: failed to get file scan report (no scan id returned)" %
                                             self._res.id)

            self._last_scan_id = resp['scan_id']
            if not self._last_scan_id:
                self._res.log.error("Unexpected response from scan request: empty scan_id")
                raise RemoteStorageException("%s: Failed to get scan report (empty scan id returned)" %
                                             self._res.id)

        except JSONDecodeError as ex:
            self._res.log.error("Unexected response from scan request: failed to parse as JSON (%s)",
                                str(ex))
            self._res.log.warning("(Is the fm base URL correct?)")
            raise RemoteStorageException("%s: Failed to trigger file scan (unexpected response format)") \
                from ex

        except Exception as ex:
            raise RemoteStorageException("%s: failed to trigger file scan: %s", self._res.id, str(ex)) \
                from ex
            
        return self._get_file_scan()

    def _get_file_scan(self):
        # pull the result of a directory scan (without triggering)
        if not self._last_scan_id:
            return self._fmcli.scan_files(self._res.id)

        try:
            resp = self._fmcli.get_scan_files(self._res.id, self._last_scan_id)
            if 'message' in resp:
                resp = resp['message']

        except JSONDecodeError as ex:
            self._res.log.error("Unexected response while retrieving scan: failed to parse as JSON (%s)",
                                str(ex))
            self._res.log.warning("(Is the fm base URL correct?)")
            raise RemoteStorageException("%s: Failed to trigger file scan (unexpected response format)") \
                from ex

        except Exception as ex:
            raise RemoteStorageException("%s: failed to trigger file scan: %s", self._res.id, str(ex)) \
                from ex

        if 'contents' not in resp or not isinstance(resp['contents'], list):
            raise StorageFormatException("Unexpected response in scan data: missing 'contents' property")
        if not resp.get('user_dir'):
            # property needs to at least contain /
            raise StorageFormatException("Unexpected response in scan data: missing 'user_dir' property")

        return resp

    def _update_files_from_scan(self, scanmd):
        # consume the result of a file scanning to cache the organization of files locally
        topchildren = []
        basepath = scanmd.get("user_dir")
        if not basepath.endswith(os.sep):
            basepath += os.sep    # because file paths are absolute (and OS(this) == OS(fm))

        def new_folder_md(id, fpath):
            return OrderedDict([
                ("_schema", NERD_DEF + "Component"),
                ("@context", NERDM_CONTEXT),
                ("@id", id),
                ("@type", deepcopy(self._comp_types["Subcollection"][0])),
                ("_extensionSchemas", deepcopy(self._comp_types["Subcollection"][1])),
                ("filepath", fpath),
                ("__children", {})
            ])

        def new_file_md(id: str, fpath: str, size: int=None, checksum: str=None):
            out = OrderedDict([
                ("_schema", NERD_DEF + "Component"),
                ("@context", NERDM_CONTEXT),
                ("@id", id),
                ("@type", deepcopy(self._comp_types["DataFile"][0])),
                ("_extensionSchemas", deepcopy(self._comp_types["DataFile"][1])),
                ("filepath", fpath),
                ("downloadURL", "pdr:nrd:@id/nrd:filepath")
            ])
            if size is not None:
                out['size'] = size
            if checksum:
                out['checksum'] = OrderedDict([
                    ('hash', checksum),
                    ('algorithm', { '@type': "Thing", 'tag': SCAN_CHECKSUM_ALGORITHM })
                ])
            return out

        # first ensure we describe all folders: pick out the new ones

        # get an inventory of what's in the scan
        failed = 0
        problems = set()
        scfolders = {}      # the folders found in the scan (id -> path)
        scfiles = {}        # the files found in the scan (id -> path)
        reqfolders = set()  # the folders required as implied by the paths
        total_size = 0
        for entry in scanmd.get("contents", []):
            if not entry.get('fileid'):
                failed += 1
                problems.add("missing file id")
                continue
            if not entry.get('path'):
                failed += 1
                problems.add("missing file id")
                continue

            if entry['path'].startswith(basepath):
                entry['path'] = entry['path'][len(basepath):].rstrip(os.sep)
            else:
                failed += 1
                problems.add("disallowed basepath")
                continue

            # determine all the parent folders implied by this file path
            reqfolders.update([str(d) for d in Path(entry['path']).parents if str(d) != '.'])

            id = entry['fileid']    # Note: nextcloud ids are numbers
            if entry.get('resource_type') == "folder":
                scfolders[id] = entry
            else:
                scfiles[id] = entry

        if failed:
            if self._res.log.isEnabledFor(logging.ERROR):
                msg = "%d fatal problem%s found in scan listing" % \
                      (failed, "s" if failed > 1 else "")
                if problems:
                    msg += ", including" + "\n  ".join(problems)
                self._res.log.error(msg)
            raise RemoteStorageException(self._res.id + ": Retrieved scan data is too flawed " +
                                         "to process; file metadata not updated")

        # Fold in implied folders, just in case
        reqfolders = set([p for p in reqfolders
                            if p not in [f['path'] for f in scfolders.values()]])
        if reqfolders:
            # should not happen if the scan is complete
            for fpath in reqfolders:
                try:
                    id = self._find_fmd_id_by_path(fpath)
                except ObjectNotFound:
                    id = self._new_id()
                scfolders[id] = {
                    "fileid": id,
                    "path": fpath,
                    "resource_type": "folder"
                }

        # Remove existing file metadata for files/folders not refered to in the scan
        deleted = []
        for id in self.iter_ids():
            fmd = self._get_file_by_id(id)
            if self.is_collection(fmd):
                if id not in scfolders:
                    deleted.append(id)
            elif id not in scfiles:
                deleted.append(id)
        len(deleted)
        for id in deleted:
            self.delete_file(id)

        missing = []
        
        # Create or update the folders in order of top to bottom
        for entry in sorted(scfolders.values(), key=lambda e: e['path']):
            if not self.exists(entry['fileid']):
                fmd = new_folder_md(entry['fileid'], entry['path'])
            else:
                fmd = self._get_file_by_id(entry['fileid'])
                fmd['filepath'] = entry['path']

            # save to disk
            try:
                self.set_file_at(fmd)
            except ObjectNotFound as ex:
                missing.append(ex.key)

        # Create or update any the files (in alphabetical order)
        for entry in sorted(scfiles.values(), key=lambda e: e['path']):
            if 'size' in entry and isinstance(entry['size'], str):
                try:
                    entry['size'] = int(entry['size'])
                    total_size += entry['size']
                except ValueError:
                    self._res.log.warning("%s: scanned size is not an integer: %s",
                                          fmd['filepath'], entry['size'])

            if not self.exists(entry['fileid']):
                fmd = new_file_md(entry['fileid'], entry['path'], entry.get('size'), entry.get('checksum'))
            else:
                fmd = self._get_file_by_id(entry['fileid'])
                fmd['filepath'] = entry['path']
                if 'size' in entry and entry['size'] is not None:
                    fmd['size'] = int(entry['size'])
                if entry.get('checksum'):
                    fmd['checksum'] = OrderedDict([
                        ('hash', entry['checksum']),
                        ('algorithm', { '@type': "Thing", 'tag': SCAN_CHECKSUM_ALGORITHM })
                    ])

            # save to disk
            try:
                self.set_file_at(fmd)
            except ObjectNotFound as ex:
                missing.append(ex.key)

        if missing:
            # should not happen
            self._res.log.error("update scan: apparently missing %d folder%s (incl. %s)",
                                len(missing), "s" if len(missing) > 1 else "", missing[0])
            self._res.log.warning("File hierarchy may be incomplete")
            raise RuntimeError("Failed add/update files due to missing folders")

        if scanmd.get("is_complete"):
            self._fmcli.delete_scan_files(self._last_scan_id)
            self._last_scan_id = None

        return OrderedDict([
            ("file_count", len(scfiles)),
            ("folder_count", len(scfolders)),
            ("syncing", bool(self._last_scan_id)),
            ("last_scan_started", scanmd.get("scan_datetime", "(unknown)")),
            ("usage", total_size),
            ("last_modified", scanmd.get("last_modified", "(unknown)")),
        ])

