"""
An implementation of MIDAS-specific application layer of the Nextcloud-based file manager.  The 
application layer to Nextcloud provides functionality specific to use by MIDAS, namely:
  *  it creates spaces used organize and manage files associated with a MIDAS Digital Asset 
     Publication (DAP).
  *  it manages access permissions to a space to allow end users to upload files into it
  *  it manages the scanning of upload spaces, collecting metadata to be incorporated into 
     the DAP.

This implementation assumes that it has direct access to storage (for scanning purposes); however,
space manipulation is done through the Nextcloud's generic and WebDAV APIs using an administrative,
functional identity.  
"""
import os, logging, json, re
from logging import Logger
from copy import deepcopy
from pathlib import Path
from collections import OrderedDict
from collections.abc import Mapping
from typing import List
from urllib.parse import urljoin

import requests
from webdav3.exceptions import WebDavException

from .clients import NextcloudApi, FMWebDAVClient
from .exceptions import *
from nistoar.base.config import merge_config, ConfigurationException
from nistoar.pdr.utils import read_json, write_json

class MIDASFileManagerService:
    """
    a service for managing file manager spaces on behalf of an end-user.  This class provides the 
    functionality of the MIDAS-specific application layer of the file manager.  

    This class supports the following configuration parameters:

    ``admin_user``
        (str) _required_.  the Nextcloud user name that is used to manage all the file spaces.
    ``nextcloud_base_url``
        (str) _optional_.  the base URL for the file manager application.  If not provided, the 
        ``webdav`` and ``generic_api`` must each include a ``service_endpoint`` sub-parameter 
        specified, and 
    ``local_storage_root_dir``
        (str) _optional_.  a local file system path that points to the file manager's root 
        directory for project spaces.  When provided, the directory's base name is typically the
        value of ``admin_user``.  Its contents are the root directories for each project, each 
        named after its ID.  If this parameter is not provided, the spaces are assumed to be not 
        accessible via the file system.  
    ``nextcloud_files_url``
        (str) _optional_.  the base URL that end-users should use to access folders via the browser 
        interface.  This parameter is optional as long as ``nextcloud_base_url`` is provided in which 
        case a default will be formed from it.
    ``webdav``
        (dict) _optional_.  the data for configuring the client for the file manager's WebDAV API 
        (see :py:class:`~nistoar.midas.dap.fm.clients.webdav.FMWebDAVClient`).  If not provided, 
        default values will be assembled from the other parameters given here (requiring 
        ``nextcloud_base_url``, and ``authentication`` to be specified).
    ``generic_api``
        (dict) _optional_.  the data for configuring the client for the nextcloud's generic layer 
        API. (see :py:class:`~nistoar.midas.dap.fm.clients.nextcloud.NextcloudApi`).  If not provided, 
        default values will be assembled from the other parameters given here (requiring 
        ``nextcloud_base_url``, and ``authentication`` to be specified).
    ``authentication``
        (dict) _optional_.  common authentication configuration shared by the WebDAV and generic layer
        APIs.  If not provided, the ``webdav`` and ``generic_api`` dictionaries need to provide their 
        own ``authentication`` parameters (see 
        :py:class:`~nistoar.midas.dap.fm.clients.webdav.FMWebDAVClient` and 
        :py:class:`~nistoar.midas.dap.fm.clients.nextcloud.NextcloudApi`).
    ``ca_bundle``
        (str) _optional_.  the path to a X.509 CA certificate bundle used to verify the nextcloud 
        server's site certificate.  This is needed only if the required CA certs are not installed 
        into the OS.  If provided, this parameter will be passed is as a default for the API clients.
    """

    def __init__(self, config: Mapping, log: Logger=None,
                 nccli: NextcloudApi=None, wdcli: FMWebDAVClient=None):
        """
        initialize the service

        :param dict          config:  the service configuration
        :param Logger           log:
        :param NextcloudApi   nccli:  the Nextcloud generic layer API client to use; if not provided,
                                      one will be constructed from ``config``.
        :param FMWebDAVClient wdcli:  the Nextcloud WebDAV client to use; if not provided, 
                                      one will be constructed from ``config``.
        """
        if not log:
            log = logging.getLogger('file-manager')
        self.log = log
        self.cfg = deepcopy(config)

        self._ncbase = self.cfg.get('nextcloud_base_url')
        if self._ncbase and not self._ncbase.endswith('/'):
            self._ncbase += '/'
        self._adminuser = self.cfg.get('admin_user')
        if not self._adminuser:
            raise ConfigurationException("MIDASFileManagerService: Missing config parameter: admin_user")
        self._root_dir = self.cfg.get("local_storage_root_dir")
        if self._root_dir:
            self._root_dir = Path(self._root_dir)
            if not self._root_dir.is_dir():
                raise ConfigurationException("local_storage_root_dir: does not exist as a directory")

        self._ncfilesurl = self.cfg.get('nextcloud_files_url')
        if not self._ncfilesurl:
            if not self._ncbase:
                raise ConfigurationException("MIDASFileManagerService: Missing config parameter: "+
                                             "nextcloud_files_url (or nextcloud_base_url)")
            self._ncfilesurl = "/".join([self.cfg['nextcloud_base_url'], "apps/files/files"])

        if not nccli:
            nccli = self.make_generic_layer_client()
        self.nccli = nccli
        if not wdcli:
            wdcli = self.make_webdav_client(nccli.base_url)
        self.wdcli = wdcli

        self.spaceid_pats = [re.compile(p) for p in self.cfg.get('space_id_patterns', [':'])]

    def make_webdav_client(self, generic_url: str=None, _override=None):
        """
        create an :py:class:`~nistoar.midas.dap.fm.clients.FMWebDAVClient` according to the 
        configuration provided to this class.

        :param str generic_url:  the base url for the Nextcloud generic layer API.  If provided, it 
                                 will be used to form the WebDAV client's default authentication endpoint 
        """
        cfg = deepcopy(self.cfg.get('webdav', {}))
        if _override:
            cfg = merge_config(_override, cfg)

        if not cfg.get('service_endpoint'):
            if not self._ncbase:
                raise ConfigurationException("Missing config parameter: webdav.service_endpoint")
            cfg['service_endpoint'] = urljoin(self._ncbase, f"remote.php/dav/files/{self._adminuser}")

        if not cfg.get('ca_bundle') and self.cfg.get('ca_bundle'):
            cfg['ca_bundle'] = self.cfg['ca_bundle']

        if not cfg.get('authentication'):
            cfg['authentication'] = deepcopy(self.cfg.get('authentication', {}))
        if not cfg['authentication'].get('client_auth_url') and generic_url:
            cfg['authentication']['client_auth_url'] = urljoin(generic_url, "auth")

        return FMWebDAVClient(cfg, self.log.getChild('webdav'))

    def make_generic_layer_client(self, _override=None):
        """
        create an :py:class:`~nistoar.midas.dap.fm.clients.FMWebDAVClient` according to the 
        configuration provided to this class.
        """
        cfg = deepcopy(self.cfg.get('generic_api', {}))
        if _override:
            cfg = merge_config(_override, cfg)

        if not cfg.get('service_endpoint'):
            if not self._ncbase:
                raise ConfigurationException("Missing config parameter: webdav.service_endpoint")
            cfg['service_endpoint'] = urljoin(self._ncbase, f"api/genapi.php/")

        if not cfg.get('ca_bundle') and self.cfg.get('ca_bundle'):
            cfg['ca_bundle'] = self.cfg['ca_bundle']

        if not cfg.get('authentication'):
            cfg['authentication'] = deepcopy(self.cfg.get('authentication', {}))

        return NextcloudApi(cfg, self.log.getChild('generic'))

        

    def create_space_for(self, id: str, foruser: str):
        """
        create and set up the file space for a DAP record with the given ID.  

        :param str      id:  the identifier of the DAP being drafted that needs record space
        :param str foruser:  the identifier for the primary user of the space.  If this user does 
                             not known to nextcloud, it will be created.
        :rtype:  FMSpace
        """
        space = FMSpace(id, self)
        if self.space_exists(id):
            raise FileManagerOpConflict(f"{id}: space already exists")

        # create the user if necessary (may raise exception)
        self.ensure_user(foruser)

        # create the directories (may raise exception)
        self.wdcli.ensure_directory(space.root_davpath)
        self.wdcli.ensure_directory(space.system_davpath)
        self.wdcli.ensure_directory(space.uploads_davpath)
        self.wdcli.ensure_directory(space.exclude_davpath)
        self.wdcli.ensure_directory(space.trash_davpath)

        # share space with user (may raise exception)
        if foruser != self._adminuser:
            self.nccli.set_user_permissions(foruser, PERM_READ, space.root_davpath)
            # space.set_permissions_for(space.system_folder, userid, PERM_READ)
            space.set_permissions_for(space.uploads_folder, foruser, PERM_ALL)

        space._set_creator(foruser)  # side-effect: sets the uploads directory file id
        return space

    def space_exists(self, id: str) -> bool:
        """
        return True if space for the given DAP record ID exists already
        """
        if self._root_dir:
            return (self._root_dir / id).exists()
        return self.wdcli.is_directory(id)

    def space_ids(self) -> List[str]:
        """
        return the list of identifiers for the existing spaces
        """
        if not self._root_dir:
            return []
        return [d for d in os.listdir(self._root_dir) if not d.startswith('.') and 
                                                         not d.startswith("_") and
                                                         any([p.search(d) for p in self.spaceid_pats])]
    def get_space(self, id: str):
        """
        return the space that has been set up for the DAP with the given record ID
        :rtype:  FMSpace
        """
        if not self.space_exists(id):
            raise FileManagerResourceNotFound(id)

        out = FMSpace(id, self)
        #  out.refresh_uploads_info()   # incurs an API call
        return out

    def delete_space(self, id: str):
        """
        remove the file space setup for a DAP record with the given ID
        :param str      id:  the identifier of the DAP being drafted whose space should be deleted
        """
        if not self.space_exists(id):
            raise FileManagerResourceNotFound(id)

        self.wdcli.delete_resource(id)

    def ensure_user(self, userid: str):
        """
        ensure that a user with the given id has been registered as a Nextcloud user, creating 
        the account if necessary.
        """
        if not self.nccli.is_user(userid):
            self.nccli.create_user(userid)

    def test(self):
        """
        test access to the Nextcloud API.
        """
        resp = self.nccli.test()
        if not hasattr(resp, 'status_code'):
            return False
        return resp.status_code == 200

PERM_NONE   = 0
PERM_READ   = 3
PERM_WRITE  = 7
PERM_DELETE = 15
PERM_SHARE  = 29
PERM_ALL    = 31

perm_name = {
    PERM_NONE:   "None",
    PERM_READ:   "Read",
    PERM_WRITE:  "Write",
    PERM_DELETE: "Delete",
    PERM_SHARE:  "Share",
    PERM_ALL:    "All"
}
perm_code = {
    perm_name[PERM_NONE]:   PERM_NONE,
    perm_name[PERM_READ]:   PERM_READ,
    perm_name[PERM_WRITE]:  PERM_WRITE,
    perm_name[PERM_DELETE]: PERM_DELETE,
    perm_name[PERM_SHARE]:  PERM_SHARE,
    perm_name[PERM_ALL]:    PERM_ALL
}

_NO_FM_SUMMARY = OrderedDict([
    ("file_count", -1),
    ("folder_count", -1),
    ("usage", -1),
    ("syncing", "unsynced"),
    ("last_modified", "(unknown)"),
    ("last_scan_id", None)
])

class FMSpace:
    """
    an encapsulation of a file space in the file manager.  
    """
    trash_folder = "TRASH"
    exclude_folder = "EXCLUDE"
    summary_file_name = "space_summary.json"

    PERM_NONE   = PERM_NONE
    PERM_READ   = PERM_READ
    PERM_WROTE  = PERM_WRITE
    PERM_DELETE = PERM_DELETE
    PERM_SHARE  = PERM_SHARE
    PERM_ALL    = PERM_ALL

    def __init__(self, id: str, fmsvc: MIDASFileManagerService, log: Logger=None):
        """
        initialize the view of the file space
        """
        self.svc = fmsvc
        self._id = id
        self._root = self.svc._root_dir / id
        self._uploads_fileid = None
        if not log:
            log = self.svc.log.getChild(id)
        self.log = log

    @property
    def id(self):
        """
        the identifier for the file space.  This usually matches the identifier for the DAP it is 
        associated with.
        """
        return self._id

    @property
    def root_dir(self) -> Path:
        """
        the file path to the user's space's directory on a local filesystem.  This directory contains
        the user's system directory and uploads directory.  
        :rtype: Path
        """
        return self._root

    @property
    def root_davpath(self):
        """
        the resource path to the root folder for the space.  This path is used to access the 
        folder via the WebDAV API.
        """
        return self.id

    @property
    def uploads_davpath(self):
        """
        the resource path to the uploads folder.  This path is used to access the folder via 
        the WebDAV API.
        """
        return "/".join([self.root_davpath, self.uploads_folder])

    @property
    def uploads_folder(self):
        """
        the resource path to the system folder for the space relative to the :py:prop:`root_davpath`.
        """
        return f"{self.id}"

    @property
    def exclude_davpath(self):
        """
        the resource path to the user's uploads exclude folder.  This path is used to access the 
        folder via the WebDAV API.
        """
        return "/".join([self.uploads_davpath, self.exclude_folder])

    @property
    def trash_davpath(self):
        """
        the resource path to the space's uploads trash folder.  This path is used to access the 
        folder via the WebDAV API.
        """
        return "/".join([self.uploads_davpath, self.trash_folder])

    @property
    def system_davpath(self):
        """
        the resource path to the system folder for the space.  This path is used to access the 
        folder via the WebDAV API.
        """
        return "/".join([self.root_davpath, self.system_folder])

    @property
    def system_folder(self):
        """
        the resource path to the system folder for the space relative to the :py:prop:`root_davpath`.
        """
        return f"{self.id}-sys"

    @property
    def uploads_file_id(self):
        if not self._uploads_fileid:
            md = self.get_resource_info(self.uploads_folder)
            self._uploads_fileid = md['fileid']
        return self._uploads_fileid

    def get_resource_info(self, resource: str=''):
        """
        return the Nextcloud metadata describing the specified resource with in the space.

        :param str resource:  the path to the resource relative to the space's root folder
        """
        res = self.root_davpath
        if resource:
            res += '/'+resource
        out = self.svc.wdcli.get_resource_info(res)
        if out.get('fileid'):
            out['gui_url'] = self._make_gui_url(out['fileid'])
        return out

    def _make_gui_url(self, ncresid):
        return f"{self.svc._ncfilesurl}/{ncresid}?dir={self.uploads_davpath}"

    def resource_exists(self, resource: str):
        """
        return True if the named resource exists in the space

        :param str resource:  the path to the resource relative to the space's root folder
        :rtype: bool
        """
        if self.svc._root_dir:
            path = os.sep.join(resource.split('/'))
            return (self.root_dir / path).exists()

    def _load_summary(self):
        sumfile = self.root_dir/self.system_folder/self.summary_file_name
        if sumfile.is_file():
            out = read_json(sumfile)
        else:
            out = deepcopy(_NO_FM_SUMMARY)
            out['uploads_dir_id'] = self.uploads_file_id
            out['id'] = self.id
            self._cache_fm_summary(out)
        return out

    def summarize(self):
        """
        return a metadata object that summarizes the state of the space.  The properties can include:
        
        ``file_count``
            the number of files currently found in the space's uploads folder (as a result of 
            the last scan).  A negative value indicates the count is unknown (because the scan
            has not happened yet.
        ``folder_count``
            the number of sub-folders currently found in the space's uploads folder (as a result of 
            the last scan).  A negative value indicates the count is unknown (because the scan
            has not happened yet.
        ``usage``
            the total number of bytes stored as files under the uploads folder
        ``syncing``
            the state of scanning: "unsynced", "synced", or "syncing"
        ``last_scan_id``
            the ID of the last file scan that was done.  Use this to retrieve the data resulting 
            from the scan via :py:meth:`get_scan`.
        ``last_scan_datetime``
            the ISO-formatted date of the last file scan
        ``uploads_dir_id``
            the nextcloud file identifier for the uploads directory (see :py:prop:`uploads_file_id`
        """
        out = self._load_summary()
        try:
            if out.get('last_scan_id') and out.get('syncing') == "syncing":
                # determine if syncing has finished
                scanmd = self.get_scan(out['last_scan_id'])
                self._update_summary_from_scan(scanmd, out)
        except Exception as ex:
            self.log.error("trouble reading last scan data: "+str(ex))

        return out

    def _cache_fm_summary(self, summary):
        sumfile = self.root_dir/self.system_folder/self.summary_file_name
        try:
            write_json(summary, sumfile)
        except Exception as ex:
            self.log.error("trouble caching space summary data: "+str(ex))

    def _set_creator(self, userid: str):
        """
        cache (as part of the summary) a user as the creator of the space
        """
        summary = self._load_summary()
        summary['created_by'] = userid
        summary.setdefault('users', [])
        if userid not in summary['users']:
            summary['users'].append(userid)
        self._cache_fm_summary(summary)

    @property
    def creator(self):
        summary = self._load_summary()
        return summary.get('created_by', '')
            
    def get_known_users(self):
        """
        return a list of IDs for users known to have permissions on this space
        """
        summary = self._load_summary()
        return summary.get('users', [])

    def get_permissions_for(self, resource: str, userid: str):
        """
        return the permission level on the specified resource assigned to the given user.

        Nextcloud permissions reflect a level of access (as opposed to a set of access rights
        that can be assigned independently), where ``PERM_NONE`` enforces no access included 
        read, and ``PERM_ALL`` allows complete access.  

        :param str resource:  the path to the resource relative to the space's root folder
        :param str   userid:  the id of the user of interest
        :return:  the permission code 
                  :rtype: int
        """
        pdata = self.svc.nccli.get_user_permissions(self.root_davpath+'/'+resource)
        if not pdata.get('ocs'):
            if not self.resource_exists(resource):
                raise FileManagerResourceNotFound(resource)
            raise UnexpectedFileManagerResponse(f"Unexpected permissions query response: missing ocs property"+
                                                "\n  "+str(pdata))
        if not pdata['ocs'].get('data'):
            self.log.warning("Missing permission info for resource, %s", resource)
            return PERM_NONE

        for data in pdata['ocs']['data']:
            if data['share_with'] == userid:
                return data['permissions']

        return PERM_NONE

    def get_permissions(self, resource: str):
        """
        return permissions for all known users set on the specified resource.  This uses 
        :py:meth:`get_known_users` to generate a dictionary of users to permissions.

        :param str resource:  the path to the resource relative to the space's root folder
        :return:  a dictionary whose keys are user IDs and values are the integer-valued 
                  permissions assigned to each.
        """
        out = {}
        for userid in self.get_known_users():
            try:
                out[userid] = self.get_permissions_for(resource, userid)
            except Exception as ex:
                self.log.warning("Trouble accessing permissions for user %s: %s", userid, str(ex))

        if not out and self.creator:
            raise FileManagerException("Failed to generate permissions on " + resource)

        return out
        

    def set_permissions_for(self, resource: str, userid: str, perm: int):
        """
        assign the permission level on the specified resource to the given user

        :param str resource:  the path to the resource relative to the space's root folder
        :param str   userid:  the id of the user of interest
        :param int     perm:  the permission level code to set
        """
        if perm not in perm_name:
            raise ValueError(f"perm: code not recognized/supported: {perm}")

        self.svc.ensure_user(userid)
        self._add_user(userid)
        self.svc.nccli.set_user_permissions(userid, perm, self.root_davpath+'/'+resource)

    def _add_user(self, userid):
        """
        cache (as part of the summary) a user as the creator of the space
        """
        summary = self._load_summary()
        summary.setdefault('users', [])
        if userid not in summary['users']:
            summary['users'].append(userid)
        self._cache_fm_summary(summary)
        

    _scan_report_tmpl = "scan-report-%s.json"
    def scan_report_filename_for(self, scanid):
        """
        return the name of scan report file in the space's system folder corresponding to 
        a given scan identifier.
        """
        return self._scan_report_tmpl % scanid

    def save_scan_metadata(self, scan_id, md):
        """
        write updated scan metadata to disk where it can be retrieved.  This method can be 
        used by the :py:meth:`fast_scan` and  :py:meth:`slow_scan` implementations to update 
        the scan metadata cached to disk.  
        :param str scan_id:  the ID for the scan request
        :param dict     md:  the scan metadata to write out.  This should have the same structure
                             as what is provided to and returned by :py:meth:`fast_scan`.  
        :raises FileManagerException:  if there is an error encountered while interacting with 
                             the file manager system, including if the target output folder is 
                             not found.
        """
        wdcli = self.svc.wdcli

        # Upload initial report
        filename = self.scan_report_filename_for(scan_id)

        try:
            wdcli.authenticate()
            response = wdcli.wdcli.upload_to(json.dumps(md, indent=4).encode('utf-8'),
                                             self.system_davpath+'/'+filename)

            if response.status_code < 200 or response.status_code >= 300:
                msg = "Unexpected response during upload of '%s' to system_davpath: %s (%s)" % \
                      (filename, response.reason, response.status_code)
                raise UnexpectedFileManagerResponse(msg, code=response.status_code)

            self.log.debug('Uploaded scan file: ' + filename)

        except WebDavException as e:
            msg = "Failed to upload '%s' to %s: %s" % (filename, self.system_davpath, str(e))
            raise FileManagerServerError(msg) from e

        except json.JSONDecodeError as e:
            raise FileManagerClientError(f"Failed to JSON-encode scan metadata: {str(e)}") from e

#        except Exception as e:
#            msg = "Unexpected error while uploading '%s' to %s: %s" % \
#                  (filename, self.system_davpath, str(e))
#            raise UnexpectedFileManagerResponse(msg) from e

    def launch_scan(self, type: str = "def"):
        """
        start a scan of the contents of the space

        :param str type:  a label identifying the type of scan to launch.  The default 
                          is "def", indicating the default type.
        :return:  preliminary data resulting from the initial "fast scan" of the space.
                  This will include "scanid" which can be used to retrieve the scan 
                  results later.
                  :rtype: dict
        :raise FileManagerException:  if a failure happens, either while preparing scan or 
                  during the synchronous "fast_scan" phase.
        """
        from . import scan

        if not type or type == 'def':
            type = 'basic'

        scanq = self._get_scan_queue()

        if type == 'basic':
            driver = scan.UserSpaceScanDriver(self, scan.DefaultScannerFactory,
                                              scanq, self.log.getChild("basicscan"))
        else:
            raise scan.FileManagerScanException("unrecognized scan type requested: "+type)

        scanid = driver.launch_scan()

        report = self.root_dir/self.system_folder/self.scan_report_filename_for(scanid)
        try: 
            out = read_json(report)
        except FileNotFoundError as ex:
            raise FileManagerScanException("scanner failed to write initial report (file not found)") from ex
        except Exception as ex:
            raise FileManagerScanException("failure while reading initial report: "+str(ex)) from ex

        try:
            self._update_summary_from_scan(out)
        except Exception as ex:
            self.log.error("Trouble updating summary info from scan: "+str(ex))

        return out

    def _update_summary_from_scan(self, scanmd, summary=None):
        if not summary:
            summary = self._load_summary()
        if 'scan_id' in scanmd:
            summary["last_scan_id"] = scanmd['scan_id']
            summary['last_scan_started'] = scanmd.get('scan_datetime', 'unknown')
        summary['syncing'] = "syncing" if scanmd.get('in_progress') else "synced"
        files = [f for f in scanmd.get('contents',[]) if f.get('resource_type', 'file') == 'file']
        summary['file_count'] = len(files)
        summary['folder_count'] = len([f for f in scanmd.get('contents',[])
                                         if f.get('resource_type', 'file') == 'folder'])
        summary['usage'] = sum(f.get('size', 0) for f in files)

        self._cache_fm_summary(summary)

    def _get_scan_queue(self, jobdir=None):
        from .scan import base as scan   # need base to manipulate slow_scan_queue
        if not scan.slow_scan_queue:
            qcfg = self.svc.cfg.get('scan_queue', {})
            if not jobdir:
                jobdir = qcfg.get("jobdir")
            if not jobdir:
                raise ConfigurationException("Missing required parameter: scan_queue.jobdir")

            if not isinstance(jobdir, Path):
                jobdir = Path(str(jobdir))
            if not jobdir.exists():
                parent = jobdir.parents[0]
                if not parent.is_dir():
                    raise ConfigurationException("Cannot create scan_queue.jobdir directory: "+
                                                 "parent does exist as a directory")
                os.mkdir(jobdir)
            
            scan.set_slow_scan_queue(jobdir, qcfg)

        return scan.slow_scan_queue

    def get_scan(self, scanid: str):
        """
        return the current results from the specified scan

        :param str scanid:  the unique ID assigned to the scan
        :return:  the data that has been collected thus far from the scan operation
                  This will include "is_complete" which will be False if the scan is 
                  still in progress.
                  :rtype: dict
        :raises FileNotFoundError:  if report could not be found in system folder
        :raises FileManagerScanException:  if a failure occurs while reading or parsing the report
        """
        report = self.root_dir/self.system_folder/self.scan_report_filename_for(scanid)
        try: 
            out = read_json(report)
        except FileNotFoundError as ex:
            raise 
        except Exception as ex:
            raise FileManagerScanException("failure while reading initial report: "+str(ex)) from ex

        return out

    def delete_scan(self, scanid: str):
        """
        stop a scan operation (if it is still running) and delete its artifacts from the 
        system folder.
        :param str scanid:  the unique ID assigned to the scan
        """
        report = self.root_dir/self.system_folder/self.scan_report_filename_for(scanid)
        if not report.is_file():
            # don't care
            return

        report = "/".join([self.system_davpath, self.scan_report_filename_for(scanid)])

        try:
            self.svc.wdcli.authenticate()
            resp = self.svc.wdcli.wdcli.clean(report)
        except RemoteResourceNotFound as ex:
            pass
        except Exception as ex:
            raise FileManagerScanException("Apparently failed to delete report from space: "+str(ex))

