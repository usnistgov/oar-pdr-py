"""
provides a base implementation of the scanning capabilties that should be extended to 
provide the specific logic for extracting metadata from files in a user's space.
"""
import datetime
import json
import logging
import os
import re
import tempfile
import threading
import time
import re
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import List, Callable, Union, Mapping
from logging import Logger
from random import randint
from pathlib import Path

from nistoar.jobmgt import JobQueue
from nistoar.base import config as cfgmod
from nistoar.pdr.utils import read_json
from ..clients import helpers, NextcloudApi, FMWebDAVClient
from nistoar.midas.dbio import ObjectNotFound
from ..service import FMSpace
from ..exceptions import (FileManagerException, UnexpectedFileManagerResponse, FileManagerResourceNotFound,
                          FileManagerClientError, FileManagerServerError, FileManagerScanException)

from webdav3.exceptions import WebDavException

# Global dictionary to keep track of scanning job statuses
scans_states = {}

GOOD_SCAN_FILE = "lastgoodscan.json"

class UserSpaceScanner(ABC):
    """
    the API for scanning a user space for application-specific purposes.

    From the perspective of this scanner, the user space is characterized by an identifier
    and two locally mounted filesystem directories: the "user" directory where
    the user has uploaded files, and the "system" directory where this scanner can
    read and write files not visible to the end-user.  When a scan is initiated, an
    implementation of this class is instantiated with these characteristics as properties,
    and then its :py:meth:`fast_scan` and :py:meth:`slow_scan` functions are called in that
    order.  Passed into these functions are the file-manager's metadata for the files (which
    can include subdirectories) that should be scanned.  Note that the slow_scan is called
    asynchronously (i.e. via the ``async`` keyword); however, :py:meth:`fast_scan` is
    guaranteed to be called before :py:meth:`slow_scan` is queued for the same set of files.

    The scanning functions are passed a proto scan report object--a dictionary of metadata 
    that describe the files that should be scanned.  The output of the scanning functions are 
    the same scan report object with additional metadata about the files filled in.  The scan
    report schema includes top-level properties that capture information 
    about the set of files as a whole; the recognized properties in this dictionary are as follows:

    ``space_id``
        str -- the identifier for the space
    ``scan_time``
        float -- the epoch time that the scan the produced this file listing was started.
    ``scan_datetime``
        str -- an ISO-formatted string form of the ``scan_time`` value (for display purposes)
    ``fm_space_path``
        str -- the file path of the user space from the file-manager perspective.  This is the
        path that a user would see as the location within the file-manager (nextcloud)
        application for the user's upload directory.
    ``scan_root``
        str -- the path to the directory that was scanned relative to the value of ``fm_space_path``.
        If the full space path was scanned, this value will be an empty string
    ``contents``
        list -- an array of objects in which each object describes a file or subfolder within
        the user space.  (See file metadata properties below.)
    ``last_modified``
        str -- a formatted string marking the last time any file was modified in this record space.
    ``is_complete``
        bool -- True, if the contents represents a complete listing of all files and folders
        within the space.  If False, it is expected that the scannning functions will be called
        additional times with different sets of contents until the entire contents have been
        examined.

    The metadata may include additional top-level properties.  For example, it may include
    nextcloud properties describing the top level folder that represents the user space.

    Each object in the ``contents`` list is a dictionary that describes a file or folder.  The
    following properties can be expected:

    ``fileid``
        str -- an identifier assigned by nextcloud for the file being described.
    ``path``
        str -- the path to the file or folder being described.  This path will be relative to 
        location indicated by the value of ``fm_space_path`` (defined above).
    ``resource_type``
        str -- the type of this resource.  Allowed values are: "file", "collection"
    ``last_modified``
        str -- the formatted date-time marking the time the file was last modified
    ``size``
        int -- the size of the file in bytes
    ``scan_errors``
        list[str] -- a list of messages describing errors that occurred while scanning this file.

    Additional properties may be included.  For example, the file metadata may include nextcloud
    file properties for the file.

    The scanning functions can update any of this metadata in their returned version which will be
    made accessible to the client.

    See also the :py:class:`UserSpaceScannerBase` which can serve as a partially-implemented
    base class for a full implementation.
    """

    @property
    def space_id(self) -> str:
        """
        the identifier for the user space
        """
        raise NotImplementedError()

    @property
    def user_dir(self) -> str:
        """
        the directory where the end-user has uploaded data.
        """
        raise NotImplementedError()

    @property
    def system_dir(self) -> str:
        """
        the directory where this scanner can read and write files that are not visible
        to the end-user.
        """
        raise NotImplementedError()

    @abstractmethod
    def init_scannable_content(self, folder=None) -> List[Mapping]:
        """
        generate (quickly) a listing of the scannable files under the user's upload directory.  
        The returned list will be conformant with the ``contents`` property of a scan report that 
        can be fed to :py:meth:`fast_scan`.  The returned list represents the files allowable by 
        algorithm implemented by this scanner.  For instance, it may exclude files found in certain 
        folders (e.g. ``TRASH``) or having a certain naming patterns (e.g. beginning with a ".").  

        :param str folder:  the folder (relative to the user's uploads folder) to list the files 
                            under.  This will list all files in the named folder along with all in 
                            its descendent folders.  If None (or an empty string), the uploads 
                            directory will be assumed as the root of the list.  This folder will 
                            _not_ be included in the list.  
        """
        raise NotImplementedError()

    @abstractmethod
    def fast_scan(self, content_md: Mapping) -> Mapping:
        """
        synchronously examine a set of files specified by the given file metadata.

        The implementation should assume that this scanning was initiated via a web request
        that is waiting for this function to finish; thus, this function should return as
        quickly as possible.  Typically, an implementation would use this function to
        _initialize_ some information about the files and store that information under the
        system area.

        Typically, the files described in the input metadata will be the full set of files
        found in the user area.  However, a controller implementation (i.e. the implementation
        that calls this function) may choose to call this function for only a subset of the
        files in the space.  (For example, if the space contains a very large number of files,
        the controller may choose to split the full collection over a series of calls.)

        :param dict content_md:  the file-manager metadata describing the files to be
                                 examined.  See the
                                 :py:class:`class documentation<UserSpaceScanner>`
                                 for the schema of this metadata.
        :return:  the file-manager metadata that was passed in, possibly updated.
                  :rtype: dict
        """
        raise NotImplementedError()

    @abstractmethod
    def slow_scan(self, content_md: Mapping) -> Mapping:
        """
        extract metadata from a deep examination a set of files specified by scan metadata.  This 
        function is expected to run asynchronously.  

        For the set of files described in the input metadata, it is guaranteed that the
        :py:meth:`fast_scan` method has been called and returned its result.  If the
        :py:meth:`fast_scan` method updated the metadata, those updates should be
        included in the input metadata to this function.

        Note that as part of executing this function asynchronously, the instance for this class 
        running this method is not guaranteed to be the same one that ran :py:meth:`fast_scan`; thus,
        implementations should not assume any shared data outside of the configuration (``self.cfg``)
        and the data passed into the functions.  (The two functions, for example, could be run in two 
        completely independent processes.)  What is guaranteed is that the input ``content_md`` parameter
        will be (or otherwise derived from) the output of :py:meth:`fast_scan``.  

        :param dict content_md:  the content metadata returned by the :py:meth:`fast_scan` method that 
                                 describes the files that should be scanned.  See the
                                 :py:class:`class documentation<UserSpaceScanner>` for the schema of 
                                 this metadata.
        :return:  the file-manager metadata that was passed in, possibly updated.
                  :rtype: dict
        """
        raise NotImplementedError()

class UserSpaceScannerBase(UserSpaceScanner, ABC):
    """
    a partial implementation of the :py:meth:`UserSpaceScanner` that can be used as a
    base class for full implementations.  See :py:mod:`~nistoar.midas.dap.fm.scan.basic.BasicScanner`
    for the simplest scanner implementation extending this class.  This implementation is based on the 
    assumption that this scanner has direct access to the file system containing the folder to be 
    scanned.
    """

    def __init__(self, space: FMSpace, scan_id: str, skip_pats=[], log: Logger=None):
        """
        initialize the scanner.

        :param FMSpace  space:  the file manager space object for the space that will be scanned
        :param str    scan_id:  the unique identifier to give to the scan report.
        :param list skip_pats:  a list of regular expressions (pattern strings or objects)
                                that match file names (not including its directory path)
                                that should be ignored.  Files that match these patterns
                                will not be scanned and included in the output scan reports.
        :param Logger     log:  the Logger to send messages to
        """
        self.sp = space
        self.scanid = scan_id
        self.skipre = []
        for pat in skip_pats:
            if not isinstance(pat, re.Pattern):
                pat = re.compile(pat)
            self.skipre.append(pat)
        if not log:
            log = logging.getLogger(__name__)
        self.log = log

    @property
    def space_id(self):
        """
        the identifier for the space.  This, in practice, matches the DAP record the space is 
        attached to.
        """
        return self.sp.id

    @property
    def user_dir(self):
        """
        the path on local disk where the user's uploads directory is located
        """
        return self.sp.root_dir / self.sp.uploads_folder

    @property
    def system_dir(self):
        """
        the path on local disk where the system directory for the space is located.  This is 
        used as the output for the scan metadata file as well as other files the scanner desires 
        to write out.  
        """
        return self.sp.root_dir / self.sp.system_folder

    def _load_last_scan(self):
        lastscan = self.system_dir / GOOD_SCAN_FILE
        if not lastscan.is_file():
            return {}

        scan = read_json(lastscan)

        out = OrderedDict()
        for fmd in scan.get('contents', []):
            if fmd.get('path'):
                out[fmd['path']] = fmd

        return out

    def init_scannable_content(self, folder=None):

        # load up data from last successful scan which has file ids cached
        lastfileprops = self._load_last_scan()

        scanroot = self.user_dir
        if folder:
            scanroot /= folder

        # descend through the file hierachy to output files
        out = []
        base = scanroot
        try:
            for base, dirs, files in os.walk(scanroot):
                upath = base[len(str(self.user_dir))+1:]

                for f in files:
                    # ignore any file matching any of the skip patterns
                    if not any(p.search(f) for p in self.skipre):
                        path = os.path.join(upath, f)
                        if path in lastfileprops and lastfileprops[path].get('resource_type') == "file":
                            out.append(lastfileprops[path])
                        else:
                            out.append({'path': path, 'resource_type': "file"})

                for i in range(len(dirs)):
                    d = dirs.pop(0)
                    # ignore any directory matching any of the skip patterns and prevent further descending
                    if not any(p.search(d) for p in self.skipre):
                        path = os.path.join(upath, d)
                        if path in lastfileprops and lastfileprops[path].get('resource_type')=="collection":
                            out.append(lastfileprops[path])
                        else:
                            out.append({'path': path, 'resource_type': "collection"})
                        dirs.append(d)

        except IOError as ex:
            raise FileManagerScanException(f"Trouble scanning {base} via filesystem: {str(ex)}")
        except Exception as ex:
            raise FileManagerScanException(f"Unexpected error while scanning {base}: {str(ex)}")

        return out

    def _save_report(self, scanid, md):
        try:
            self.sp.save_scan_metadata(self.scanid, md)
        except FileManagerException as ex:
            self.log.error(str(ex))
        except Exception as ex:
            self.log.exception(ex)

    def ensure_registered(self, folder=None):
        """
        Request nextcloud to scan the specified folder to register any new or changed files.
        :param str folder:  the folder relative to the user's space's root (i.e. where id refers 
                            to the uploads directory).
        """
        # Using scan_directory_files() is probably the function we want; however at this time,
        # it is implemented to just return metadata about the directory.  
        # 
        # dirpth = self.sp.root_davpath
        # if folder:
        #    dirpath += '/'+folder
        # self.sp.nccli.scan_directory_files(dirpth)

        # so in the meantime, we'll use this instead:
        self.sp.svc.nccli.scan_user_files(self.sp.svc.cfg.get('adminuser', 'oar_api'))

    @classmethod
    def create_basic_skip_scanner(cls, space, scanid, log=None):
        """
        a factory function for creating the scanner assuming the "basic" rules for skipping 
        certain files (skips files starting with "." or "_").
        """
        if not log:
            log = logging.getLogger(f"scan.{space.id}")
        return cls(space, scanid, basic_skip_patterns, log)

    @classmethod
    def create_excludes_skip_scanner(cls, space, scanid, log=None):
        """
        a factory function for creating the scanner assuming the "excludes" rules for skipping 
        certain files.  This base implementation skips files and folders that start with either 
        "." or "#".  
        """
        # DEPRECATED:
        # In addition to the basic skip rules (skipping files that start with "."
        # or "#"), it skips folders with names "TRASH" or "HIDE".
        
        if not log:
            log = logging.getLogger(f"scan.{space.id}")

        # use this if extra folder patterns are needed
        # return cls(space, scanid, exclude_folders_skip_patterns, log)

        # use this if # covers all excludable things
        return cls(space, scanid, basic_skip_patterns, log)

basic_skip_patterns = [
    re.compile(r"^\."),       # hidden files
    re.compile(r"^#")         # anything starting with an underscore ("#")
]
exclude_folders_skip_patterns = basic_skip_patterns + [
    re.compile(r"^TRASH$"),   # trash folders
    re.compile(r"^HIDE$")     # hide folders
]

# see .basic for BasicScanner, a minimal extension of UserSpaceScannerBase

slow_scan_queue = None

def create_slow_scan_queue(queuedir: Union[Path,str], config: Mapping=None,
                           log: Logger=None, resume: bool=True) -> JobQueue:
    """
    create a :py:class:`nistoar.jobmgt.JobQueue` to use for scanning
    :param str|Path queuedir:  the directory to use to persist the queue state
    :param dict       config:  the configuration to use 
    :param Logger        log:  the Logger to use
    :param bool       resume:  if True, launch any zombied jobs found in the state directory
    """
    from . import jobexec
    return JobQueue("slow_scan", queuedir, jobexec, config, log, resume)

def set_slow_scan_queue(queuedir: Union[Path,str], config: Mapping=None,
                        log: Logger=None, resume: bool=True) -> JobQueue:
    global slow_scan_queue
    cfg = {
        "runner": {
            "capture_logging": True
        }
    }
    if config:
        cfg = cfgmod.merge_config(config, cfg)
    slow_scan_queue = create_slow_scan_queue(queuedir, cfg, log, resume)
    return slow_scan_queue
        
class UserSpaceScanDriver:
    """
    A class that creates, launches, and manages a :py:class:UserSpaceScanner to scan a given file
    space.  It is responsible for calling the scanner's ``fast_scan()`` and ``slow_scan()`` methods.
    """
    def __init__(self, space: FMSpace, scanner_fact: Callable, slowscanq: JobQueue, log: Logger=None):
        """
        Create a driver that will execute a scanner.  
        :param FMSpace         space:  the file-manager space to scan
        :param Callable scanner_fact:  a factory function for instantiating a UserSpaceScanner to 
                                       extract metadata from the files found in ``space``.
                                       This function will be called with three arguments:
                                       the :py:class:`~nistoar.midas.dap.fm.service.FMSpace` instance 
                                       (given by ``space``), an identifier to assign to the scan, and
                                       the Logger to use (given by ``log``).  
        :param JobQueue    slowscanq:  the JobQueue to use to launch slow scans into
        :param Logger            log:  the Logger to send messages to
        """
        self.space = space
        self.scnrfact = scanner_fact
        if not log:
            log = self.space.log.getChild("scan-driver")
        self.scanq = slowscanq
        self.log = log

    def _init_scan_md(self, scan_id, folder=None, launch_time=None):
        iscomplete = True
        scfolder = self.space.uploads_folder
        if folder is None:
            folder = ''
        if folder:
            iscomplete = False
            scfolder += '/' + folder

        if not launch_time:
            launch_time = time.time()

        return {
            'space_id': self.space.id,
            'scan_id': scan_id,
            'scan_time': launch_time,
            'scan_datetime':  datetime.datetime.fromtimestamp(launch_time).isoformat(),
            'fm_space_path': self.space.uploads_davpath,
            'scan_root': folder,
            'uploads_dir': str(self.space.root_dir/self.space.uploads_folder),
            'scan_root_dir': str(self.space.root_dir/scfolder),
            'contents': []
        }

    def _create_scan_id(self):
        return "%04x" % randint(0, 65536)

    def launch_scan(self, folder=None) -> str:
        """
        start the file scanning process of the user's upload space.  This executes the fast scan,
        saves the results, and then launches the asynchronous slow scan.  
        :param str folder:  the folder in the file manager space to scan.  If None, the user's 
                            uploads folder will be scanned
        :raises FileManagerException: if an error occurs while prepping scan or during the fast
                            scan phase.  
        :return: the identifier for the launched scan
                 :rtype: str
        """
        scan_id = self._create_scan_id()
        scan_md = self._init_scan_md(scan_id, folder, time.time())
        self.log.debug("Creating scan for %s with id=%s", self.space.id, scan_id)

        scanner = None
        try:
            scanner = self.scnrfact(self.space, scan_id, self.log)
        except Exception as ex:
            raise FileManagerScanException("Failed to create scanner from factory: "+str(ex)) from ex

        # get list of files to scan
        try:
            scan_md['contents'] = scanner.init_scannable_content(folder)
            
        except FileManagerException:
            raise
        except Exception as ex:
            if not folder:
                folder = "uploads"
            raise FileManagerScanException("Unexpected error while getting scan folder contents%s: %s" %
                                           ((" (%s)" % folder) if folder else "", str(ex),)) from ex

        self.log.debug("Starting scan %s", scan_id)
        try:
            scan_md = scanner.fast_scan(scan_md)

            scanfactname = getattr(self.scnrfact, '__fullname__', None)
            if not scanfactname:
                scanfactname = f"{self.scnrfact.__module__}.{self.scnrfact.__name__}"
            cfg = {
                'service': self.space.svc.cfg,
                'factory': scanfactname,
                'scandir': str(self.scanq.qdir)
            }
            self.scanq.submit(self.space.id, [scan_id], cfg)

            # # Run the slowScan asynchronously using a thread
            # def run_slow_scan():
            #     # Read files from disk for performance
            #     scanner.slow_scan(scan_md)

            # # TODO: track threads, avoid simultaneous scanning of same DAP
            # thread = threading.Thread(target=run_slow_scan)
            # thread.start()
            # self.log.info(f"Scan %s started successfully", scan_id)

        except FileManagerException as ex:
            raise

        except Exception as ex:
            self.log.exception(ex)
            if not folder:
                folder = "uploads folder"
            raise FileManagerScanException(f"Unexpected error while scanning {folder}: {str(ex)}") from ex

        return scan_id


def total_scan_contents_size(contents):
    total_size = 0
    for item in contents:
        if item.get('resource_type') == 'file':
            total_size += int(item['size'])
    return total_size
