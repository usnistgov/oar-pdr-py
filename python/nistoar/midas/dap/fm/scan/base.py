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
from collections.abc import Mapping
from typing import List, Callable
from logging import Logger
from operator import itemgetter
from random import randint

# from flask import current_app, copy_current_request_context
# from flask_jwt_extended import jwt_required
# from flask_restful import Resource

from ..clients import helpers, NextcloudApi, FMWebDAVClient
from nistoar.midas.dbio import ObjectNotFound
from nistoar.pdr.utils import checksum_of
from ..service import FMSpace
from ..exceptions import (FileManagerException, UnexpectedFileManagerResponse, FileManagerResourceNotFound,
                          FileManagerClientError, FileManagerServerError)
from webdav3.exceptions import WebDavException

logging.basicConfig(level=logging.INFO)

# Global dictionary to keep track of scanning job statuses
scans_states = {}


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
        str -- the path to the file or folder being described.  [This path will be the full
        path to the file and will start with the value of ``fm_space_path`` (defined above).]
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
                            under.  This list will all files in the name folder along with all in 
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

class FileManagerScanException(FileManagerException):
    """
    an exception indicating a problem while starting or running a scanning operation
    in a file space.  
    """
    def __init__(self, message: str, space_id: str=None, scan_id: str=None):
        self.space_id = space_id
        self.scan_id = scan_id
        if space_id:
            scid = scan_id if scan_id else ""
            message += f" [{space_id}:{scid}]"
        super(FileManagerScanException, self).__init__(message)


class UserSpaceScannerBase(UserSpaceScanner, ABC):
    """
    a partial implementation of the :py:meth:`UserSpaceScanner` that can be used as a
    base class for full implementations.
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

    def init_scannable_content(self, folder=None):

        scanroot = self.user_dir
        if folder:
            scanroot /= folder

        # descend through the file hierachy to output files
        out = []
        base = scanroot
        try:
            for base, dirs, files in os.walk(scanroot):

                for f in files:
                    # ignore any file matching any of the skip patterns
                    if not any(p.search(f) for p in self.skipre):
                        out.append({'path': os.path.join(base[len(str(self.user_dir))+1:], f),
                                    'resource_type': "file"})

                for i in range(len(dirs)):
                    d = dirs.pop(0)
                    # ignor any directory matching any of the skip patterns and prevent further descending
                    if not any(p.search(d) for p in self.skipre):
                        out.append({'path': os.path.join(base[len(str(self.user_dir))+1:], d),
                                    'resource_type': "collection"})
                        dirs.append(d)

        except IOError as ex:
            raise FileManagerScanException(f"Trouble scanning {base} via filesystem: {str(ex)}")
        except Exception as ex:
            raise FileManagerScanException(f"Unexpected error while scanning {base}: {str(ex)}")

        return out

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
        wdcli = self.sp.svc.wdcli

        # Upload initial report
        filename = self.sp.scan_report_filename_for(scan_id)

        try:
            wdcli.authenticate()
            response = wdcli.wdcli.upload_to(json.dumps(md, indent=4).encode('utf-8'),
                                             self.sp.system_davpath+'/'+filename)

            if response.status_code < 200 or response.status_code >= 300:
                msg = "Unexpected response during upload of '%s' to self.sp.system_davpath: %s (%s)" % \
                      (filename, response.reason, response.status_code)
                raise UnexpectedFileManagerResponse(msg, code=response.status_code)

            self.log.debug('Uploaded scan file: ' + filename)

        except WebDavException as e:
            msg = "Failed to upload '%s' to %s: %s" % (filename, self.sp.system_davpath, str(e))
            raise FileManagerServerError(msg) from e

        except json.JSONDecodeError as e:
            raise FileManagerClientError(f"Failed to JSON-encode scan metadata: {str(e)}") from e

#        except Exception as e:
#            msg = "Unexpected error while uploading '%s' to %s: %s" % \
#                  (filename, self.sp.system_davpath, str(e))
#            raise UnexpectedFileManagerResponse(msg) from e

    def _save_report(self, scanid, md):
        try:
            self.save_scan_metadata(self.scanid, md)
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
    def create_basic_skip_scanner(cls, space, scanid, parentlog=None):
        """
        a factory function for creating the scanner assuming the "basic" rules for skipping 
        certain files (skips files starting with "." or "_").
        """
        if not parentlog:
            parentlog = logging.getLogger(f"scan.{space.id}")
        return cls(space, scanid, basic_skip_patterns, parentlog.getChild("basicskip"))

    @classmethod
    def create_excludes_skip_scanner(cls, space, scanid, parentlog=None):
        """
        a factory function for creating the scanner assuming the "excludes" rules for skipping 
        certain files.  In addition to the basic skip rules (skipping files that start with "."
        or "_"), it skips folders with names "TRASH" or "EXCLUDE".
        """
        if not parentlog:
            parentlog = logging.getLogger(f"scan.{space.id}")
        return cls(space, scanid, exclude_folders_skip_patterns, parentlog.getChild("exclskip"))

basic_skip_patterns = [
    re.compile(r"^\."),       # hidden files
    re.compile(r"^_")         # anything starting with an underscore ("_")
]
exclude_folders_skip_patterns = basic_skip_patterns + [
    re.compile(r"^TRASH$"),   # trash folders
    re.compile(r"^EXCLUDE$")  # exclude folders
]

class BasicScanner(UserSpaceScannerBase):
    """
    a basic scanner that handles the minimum scanner that captures the minimum file metadata.  All 
    captured data is included only in the scan report.  
    """

    def fast_scan(self, content_md: Mapping) -> Mapping:
        """
        synchronously examine a set of files specified by the given file metadata.

        This implementation makes sure that each file currently exists and captures basic filesystem
        metadata for it (size, last modified time, etc.).
        """
        if content_md.get('contents'):
            # sort alphabetically; this puts top files first and folders ahead of their members
            content_md['contents'].sort(key=itemgetter('path'))
            content_md['in_progress'] = True
            totals = {'': 0}

            for i in range(len(content_md['contents'])):
                fmd = content_md['contents'].pop(0)
                if 'scan_errors' not in fmd or not isinstance(fmd['scan_errors'], list):
                    fmd['scan_errors'] = []
                try:
                    stat = (self.user_dir / fmd['path']).stat()
                    fmd['size'] = stat.st_size if fmd['resource_type'] == "file" else 0
                    fmd['ctime'] = stat.st_ctime
                    fmd['mtime'] = stat.st_mtime
                    fmd['last_modified'] = datetime.datetime.fromtimestamp(fmd['mtime']).isoformat()
                except FileNotFoundError as ex:
                    # No longer exists: remove it from the list
                    fmd = None
                except Exception as ex:
                    fmd['scan_errors'].append("Failed to stat file: "+str(ex))

                if fmd:
                    content_md['contents'].append(fmd)

                # total up sizes
                if fmd['resource_type'] == 'file':
                    dir = os.path.dirname(fmd['path'])
                    if dir not in totals:
                        totals[dir] = 0
                    totals[dir] += fmd['size']
                    if dir:
                        totals[''] += fmd['size']

        # record total sizes
        content_md['accumulated_size'] = totals['']
        for fmd in content_md['contents']:
            if fmd['resource_type'] == "collection" and fmd['path'] in totals:
                fmd['accumulated_size'] = totals[fmd['path']]

        # write out the report
        self._save_report(self.scanid, content_md)
        
        return content_md

    def slow_scan(self, content_md: Mapping) -> Mapping:
        """
        extract metadata from a deep examination a set of files specified by scan metadata.

        This implementation will fetch nextcloud metadata and calculate checksums
        """
        # make sure all files are registered with nextcloud
        scanroot = content_md.get('fm_folder_path', self.sp.uploads_davpath)
        self.ensure_registered(scanroot)

        update_files_lim = 10
        update_size_lim = 10000000 # 10 MB
        size_left = update_size_lim
        files_left = update_files_lim
        for fmd in content_md.get('contents'):
            self.slow_scan_file(fmd)

            # update the report (only after processing a certain number of files/bytes)
            size_left -= fmd.get('size', 0)
            files_left -= 1
            if size_left <= 0 or files_left <= 0:
                files_left = update_files_lim
                size_left = update_size_lim
                self._save_report(self.scanid, content_md)

        # write out the final report
        content_md['in_progress'] = False
        self._save_report(self.scanid, content_md)

        return content_md
        
    def slow_scan_file(self, filemd: Mapping):
        """
        examine the specified file and update its metadata accordingly.  This is called by 
        :py:meth:`slow_scan` on each file in the scan report object.  
        """
        # fetch the Nextcloud metadata for the entry (especially need fileid)
        davpath = '/'.join([self.sp.uploads_davpath] + filemd['path'].split('/'))
        try:
            
            ncmd = self.sp.svc.wdcli.get_resource_info(davpath)
            skip = "name type getetag size resource_type".split()
            filemd.update(i for i in ncmd.items() if i[0] not in skip)
            if 'getetag' in ncmd:
                filemd['etag'] = ncmd['getetag']
            if 'size' in ncmd and filemd['resource_type'] == 'file':
                filemd['size'] = ncmd['size']

        except FileManagerResourceNotFound as ex:
            # either this file is not registered yet or it has been deleted
            pass
        except Exception as ex:
            filemd['scan_errors'].append(f"Failed to stat file, {filemd['path']}: {str(ex)}")

        # calculate the checksum
        fpath = self.user_dir/filemd['path']
        if fpath.is_file():
            try:
                filemd['checksum'] = checksum_of(fpath)
                filemd['last_checksum_date'] = time.time()
            except Exception as ex:
                filemd['scan_errors'].append(f"Failed to calculate checksum for {filemd['path']}: {str(ex)}")
        
        
        
class UserSpaceScanDriver:
    """
    A class that creates, launches, and manages a :py:class:UserSpaceScanner to scan a given file
    space.  It is responsible for calling the scanner's ``fast_scan()`` and ``slow_scan()`` methods.
    """
    def __init__(self, space: FMSpace, scanner_fact: Callable, log: Logger=None):
        """
        Create a driver that will execute a scanner.  
        :param FMSpace         space:  the file-manager space to scan
        :param Callable scanner_fact:  a factory function for instantiating a UserSpaceScanner to 
                                       extract metadata from the files found in ``space``.
                                       This function will be called with three arguments:
                                       the :py:class:`~nistoar.midas.dap.fm.service.FMSpace` instance 
                                       (given by ``space``), an identifier to assign to the scan, and
                                       the Logger to use (given by ``log``).  
        :param Logger            log:  the Logger to send messages to
        """
        self.space = space
        self.scnrfact = scanner_fact
        if not log:
            log = self.space.log.getChild("scan-driver")
        self.log = log

    def _init_scan_md(self, scan_id, folder=None, launch_time=None):
        iscomplete = True
        scfolder = self.space.uploads_folder
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
            'root_folder_path': str(self.space.root_dir/scfolder),
            'fm_folder_path': scfolder,
            'contents': []
        }

    def _create_scan_id(self):
        return "%04x" % randint(0, 65536)

    def launch_scan(self, folder=None):
        """
        start the file scanning process of the user's upload space.  This executes the fast scan,
        saves the results, and then launches the asynchronous slow scan.  
        :param str folder:  the folder in the file manager space to scan.  If None, the user's 
                            uploads folder will be scanned
        :raises FileManagerException: if an error occurs while prepping scan or during the fast
                            scan phase.  
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
            raise FileManagerScanException("Unexpected error while getting scan folder contents%s: %s" %
                                           ((" (%s)" % folder) if folder else "", str(ex),)) from ex

        self.log.debug("Starting scan %s", scan_id)
        try:
            scan_md = scanner.fast_scan(scan_md)

            # Run the slowScan asynchronously using a thread
            def run_slow_scan():
                # Read files from disk for performance
                scanner.slow_scan(scan_md)

            # TODO: track threads, avoid simultaneous scanning of same DAP
            thread = threading.Thread(target=run_slow_scan)
            thread.start()
            self.log.info(f"Scan %s started successfully", scan_id)

        except FileManagerException as ex:
            raise

        except Exception as ex:
            raise FileManagerScanException(f"Unexpected error while scanning {folder}: {str(ex)}") from ex

        return scan_id


def total_scan_contents_size(contents):
    total_size = 0
    for item in contents:
        if item.get('resource_type') == 'file':
            total_size += int(item['size'])
    return total_size
