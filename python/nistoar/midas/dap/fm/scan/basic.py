"""
A basic implementation of the :py:class:`~nistoar.midas.dap.fm.scan.base.UserSpaceScanner` interface.  
This scanner captures minimal file metadata--namely, size, checksum, and the Nextcloud-issued file ID.
(See the :py:mod:`fm.scan module documentation<nistoar.midas.dap.fm.scan>` for more information on 
the scanning framework.)
"""
import os, time
from logging import Logger
from collections.abc import Mapping
from operator import itemgetter
from datetime import datetime

from .base import UserSpaceScannerBase, FileManagerScanException
from ..exceptions import *
from ..service import FMSpace
from nistoar.pdr.utils import checksum_of

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
        spaceroot = content_md.get('fm_space_path', self.sp.uploads_davpath)
        scanroot = spaceroot
        if content_md.get("scan_root"):
            scanroot = '/'.join([scanroot, content_md["scan_root"]])

        if self.sp.svc.cfg.get('external_uploads_allowed'):
            # make sure all files are registered with nextcloud
            self.ensure_registered(scanroot)

        # get the fileid property for the files in the target directory as a starter (without them
        # the DAP service can't register them).
        fileprops = self.sp.svc.wdcli.list_folder_info(scanroot)

        totals = {'': 0}
        if content_md.get('contents'):
            # sort alphabetically; this puts top files first and folders ahead of their members
            content_md['contents'].sort(key=itemgetter('path'))
            content_md['in_progress'] = True

            for i in range(len(content_md['contents'])):
                fmd = content_md['contents'].pop(0)
                if 'scan_errors' not in fmd or not isinstance(fmd['scan_errors'], list):
                    fmd['scan_errors'] = []
                try:
                    stat = (self.user_dir / fmd['path']).stat()
                    fmd['size'] = stat.st_size if fmd['resource_type'] == "file" else 0
                    fmd['ctime'] = stat.st_ctime
                    fmd['mtime'] = stat.st_mtime
                    fmd['last_modified'] = datetime.fromtimestamp(fmd['mtime']).isoformat()
                except FileNotFoundError as ex:
                    # No longer exists: remove it from the list
                    fmd = None
                except Exception as ex:
                    fmd['scan_errors'].append("Failed to stat file: "+str(ex))

                davpath = '/'.join([spaceroot, fmd['path']])
                if davpath in fileprops:
                    props = fileprops[davpath]
                    for prop in "fileid etag content_type".split():
                        if props.get(prop):
                            fmd[prop] = props[prop]

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
        for fmd in content_md.get('contents', []):
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
        if not self.sp.svc.cfg.get('external_uploads_allowed'):
            scanroot = content_md.get('fm_space_path', self.sp.uploads_davpath)
            self.ensure_registered(scanroot)

        update_files_lim = 10
        update_size_lim = 10000000 # 10 MB
        size_left = update_size_lim
        files_left = update_files_lim
        for fmd in content_md.get('contents', []):
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
        etag = filemd.get('etag')
        
        # fetch the Nextcloud metadata for the entry (especially need fileid)
        davpath = '/'.join([self.sp.uploads_davpath] + filemd['path'].split('/'))
        try:
            
            ncmd = self.sp.svc.wdcli.get_resource_info(davpath)
            skip = "path urlpath type size resource_type".split()
            filemd.update(i for i in ncmd.items() if i[0] not in skip)
            if 'size' in ncmd and filemd['resource_type'] == 'file':
                filemd['size'] = int(ncmd['size'])

        except FileManagerResourceNotFound as ex:
            # either this file is not registered yet or it has been deleted
            pass
        except Exception as ex:
            filemd['scan_errors'].append(f"Failed to stat file, {filemd['path']}: {str(ex)}")

        # calculate the checksum if needed; leverage etag to see if file has changed
        fpath = filemd['path']
        if fpath.startswith(self.sp.uploads_davpath):  # and it better
            fpath = fpath[len(self.sp.uploads_davpath)+1:]
        fpath = self.user_dir/fpath
        if fpath.is_file() and (not etag or etag != filemd.get("etag") or not filemd.get("checksum")):
            self.log.debug("%s: calculating checksum: %s v. %s, %s", filemd['path'], 
                           str(etag), filemd.get("etag","x"), filemd.get("checksum", "x"))
            try:
                filemd['checksum'] = checksum_of(fpath)
                filemd['last_checksum_date'] = time.time()
            except Exception as ex:
                filemd['scan_errors'].append(f"Failed to calculate checksum for {filemd['path']}: {str(ex)}")

def BasicScannerFactory(space: FMSpace, scanid: str, log: Logger=None):
    return BasicScanner.create_excludes_skip_scanner(space, scanid, log)
BasicScannerFactory.__fullname__ = f"{__name__}.{BasicScannerFactory.__name__}"
                
