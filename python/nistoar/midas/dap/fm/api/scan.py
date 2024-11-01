"""
/scans endpoint manages record space files scanning operations
"""
import asyncio
import datetime
import json
import logging
import os
import re
import tempfile
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Mapping

from flask import current_app, copy_current_request_context
from flask_jwt_extended import jwt_required
from flask_restful import Resource

import helpers
from app.clients.nextcloud.api import NextcloudApi
from app.clients.webdav.api import WebDAVApi
from config import Config

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

    The scanning functions are passed a dictionary of metadata that describe the files
    that should be scanned.  The top-level properties capture information about the set of
    files as a whole; the expected properties in this dictionary are as follows:

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
    nextcloud properties describing the top level folder that is represents the user space.

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
    async def slow_scan(self, content_md: Mapping) -> Mapping:
        """
        asynchronously examine a set of files specified by the given file metadata.

        For the set of files described in the input metadata, it is guaranteed that the
        :py:meth:`fast_scan` method has been called and returned its result.  If the
        :py:meth:`fast_scan` method updated the metadata, those updates should be
        included in the input metadata to this function.

        :param dict content_md:  the content metadata returned by the
                                 :py:meth:`fast_scan` method that describes the
                                 files that should be scanned.  See the
                                 :py:class:`class documentation<UserSpaceScanner>`
                                 for the schema of this metadata.
        :return:  the file-manager metadata that was passed in, possibly updated.
                  :rtype: dict
        """
        raise NotImplementedError()


class UserSpaceScannerBase(UserSpaceScanner, ABC):
    """
    a partial implementation of the :py:meth:`UserSpaceScanner` that can be used as a
    base class for full implementations.
    """

    def __init__(self, space_id: str, user_dir: str, sys_dir: str):
        """
        initialize the scanner.

        :param str  space_id:  the identifier for the user space that should be scanned
        :param str user_dir:  the full path on a local filesystem to the directory where
                               the end-user has uploaded files.
        :param str  sys_dir:  the full path on a local filesystem to a directory where
                               the scanner can read and write files that are not visible
                               to the end user.
        """
        self._id = space_id
        self._userdir = user_dir
        self._sysdir = sys_dir

    @property
    def space_id(self):
        return self._id

    @property
    def user_dir(self):
        return self._userdir

    @property
    def system_dir(self):
        return self._sysdir


class FileManagerDirectoryScanner(UserSpaceScannerBase):
    """
    an implementation of the :py:meth:`UserSpaceScanner` leveraging :py:meth:`UserSpaceScannerBase`
    for the file manager scanning operations.
    """

    def __init__(self, space_id: str, user_dir: str, sys_dir: str):
        super().__init__(space_id, user_dir, sys_dir)

    def fast_scan(self, content_md: Mapping) -> Mapping:
        logging.info("Starting fast scan")
        scan_id = content_md['scan_id']
        fm_system_path = content_md['fm_system_path']

        # Upload initial report
        filename = f"report-{scan_id}.json"
        file_content = json.dumps(content_md, indent=4)
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        try:
            # Write content as bytes
            temp_file.write(file_content.encode('utf-8'))
            # Go back to the beginning of the file
            temp_file.seek(0)
            webdav_client = WebDAVApi(Config)
            webdav_client = WebDAVApi(Config)
            response = webdav_client.upload_file(str(fm_system_path), temp_file.name, filename)

            if response['status'] not in [200, 201, 204]:
                logging.error(f"Failed to upload scan report '{filename}'")
                raise Exception("Failed to upload scan report")

            logging.info('Upload file:' + filename)

        except KeyError as e:
            logging.exception(f"Key error: {e}")
            raise KeyError(f"Key not found: {e}")
        except json.JSONDecodeError as e:
            logging.exception(f"JSON encoding error: {e}")
            raise ValueError("Invalid JSON format")
        except Exception as e:
            logging.exception("An unexpected error occurred")
            raise RuntimeError("An unexpected error occurred: " + str(e))

        finally:
            # Close and delete the file
            temp_file.close()
            os.unlink(temp_file.name)

        logging.info("Fast scan completed successfully")
        return content_md

    async def slow_scan(self, content_md: Mapping) -> Mapping:
        """
        Perform a custom scan by calculating file checksums if the file has been modified after
        the last checksum date (or there has never been a checksum calculated).
        This method is asynchronous and scans each file of the contents.

        Args:
            content_md (Mapping): Metadata describing the files to be scanned.

        Returns:
            Mapping:  The updated metadata with checksums added or updated for each file.
        """
        logging.info("Starting slow scan")
        try:
            current_time = datetime.datetime.now()
            scan_id = content_md['scan_id']
            fm_system_path = content_md['fm_system_path']

            last_scan = self.find_most_recent_scan(scan_id)

            async def process_resource(resource):
                if resource['resource_type'] == 'file':
                    await update_checksum(resource)
                elif 'contents' in resource:
                    for sub_resource in resource['contents']:
                        await process_resource(sub_resource)

            async def update_checksum(resource):
                last_modified = datetime.datetime.strptime(resource['last_modified'], '%a, %d %b %Y %H:%M:%S GMT')
                file_id = resource['fileid']
                checksum = None
                last_checksum_date = datetime.datetime.fromisoformat('1970-01-01T00:00:00').replace(
                    tzinfo=None).isoformat()

                if last_scan:
                    last_scan_content_md = next(
                        (item for item in last_scan['contents'] if item['fileid'] == file_id),
                        None)
                    if last_scan_content_md and 'last_checksum_date' in last_scan_content_md:
                        last_checksum_date = datetime.datetime.fromisoformat(
                            last_scan_content_md['last_checksum_date']).replace(tzinfo=None)
                        if last_modified < last_checksum_date:
                            checksum = last_scan_content_md['checksum']
                            last_checksum_date = last_checksum_date.isoformat()

                if checksum is None:
                    file_path = resource['path']
                    checksum = helpers.calculate_checksum(file_path)
                    last_checksum_date = current_time.isoformat()

                # Update resource
                resource['checksum'] = checksum
                resource['last_checksum_date'] = last_checksum_date

            for resource in content_md['contents']:
                await process_resource(resource)

            # Update the report.json file after each resource has been updated
            update_json_data = json.dumps(content_md, indent=4)
            filename = f"report-{scan_id}.json"
            filepath = os.path.join(fm_system_path, filename)
            disk_filepath = os.path.join(self.system_dir, filename)
            webdav_client = WebDAVApi(Config)
            response = webdav_client.modify_file_content(filepath, update_json_data)

            logging.info("Slow scan completed successfully")
            return content_md

        except Exception as e:
            logging.exception(f"An unexpected error occurred during the slow scan: {e}")
            raise

    def find_most_recent_scan(self, scan_id):
        most_recent_file = None
        most_recent_time = 0

        for filename in os.listdir(self.system_dir):
            file_path = os.path.join(self.system_dir, filename)
            if not os.path.isfile(file_path):
                continue
            modification_time = os.path.getmtime(file_path)
            # Exclude current scan file from the search
            if scan_id not in os.path.basename(filename):
                if modification_time > most_recent_time:
                    most_recent_file = file_path
                    most_recent_time = modification_time

        if most_recent_file is None:
            return None

        # Read the most recent JSON file and convert it to a Python dictionary
        with open(most_recent_file, 'r') as file:
            data = json.load(file)

        return data


class ScanFiles(Resource):
    """Resource to handle file scanning operations."""

    def scan_directory_contents(self, fm_file_path, root_dir_from_disk):
        """
        Recursively scans directories and their contents.

        Args:
            fm_file_path (str): File manager file path relative to the root directory.
            root_dir_from_disk (str): Root directory path from disk.

        Returns:
            list: List of metadata dictionaries for all files and directories.
        """
        nextcloud_client = NextcloudApi(Config)
        webdav_client = WebDAVApi(Config)
        contents = []
        nextcloud_xml = nextcloud_client.scan_directory_files(fm_file_path)
        nextcloud_md = helpers.parse_nextcloud_scan_xml(fm_file_path, nextcloud_xml)

        for resource in nextcloud_md:
            resource_path = re.split(Config.NEXTCLOUD_ADMIN_USER, resource['path'], flags=re.IGNORECASE)[-1].lstrip('/')
            resource_type = 'file' if 'getcontenttype' in resource else 'folder'
            file_type = resource.get('getcontenttype', '')
            size = resource.get('getcontentlength', resource.get('quota-used-bytes', '0'))
            path = os.path.join(root_dir_from_disk, resource_path)

            resource_md = {
                'name': path.rstrip('/').split("/")[-1],
                'fileid': resource['getetag'].strip('"'),
                'path': path,
                'size': size,
                'last_modified': resource['getlastmodified'],
                'resource_type': resource_type,
                'file_type': file_type,
                'scan_errors': [],
                'contents': [],
            }

            if resource_md['resource_type'] == 'folder':
                subdirectory_path = resource_path
                sub_contents = self.scan_directory_contents(subdirectory_path, root_dir_from_disk)
                resource_md['contents'].extend(sub_contents)
                contents.append(resource_md)
            else:
                contents.append(resource_md)

        return contents

    @jwt_required()
    def post(self, record_name):
        """
        Starts a file scanning process for a specific record.
        Initiates a fast scan followed by an asynchronous slow scan.

        Args:
            record_name (str): Name of the record space to scan.

        Returns:
            tuple: Success response with status code 200, or error response with status code 500.
        """
        try:
            webdav_client = WebDAVApi(Config)
            logging.info(f"Starting file scanning process for record: {record_name}")

            # Instantiate dirs
            space_id = record_name
            fm_system_path = os.path.join(space_id, f"{space_id}-sys")
            fm_space_path = os.path.join(space_id, f"{space_id}")
            root_dir_from_disk = Config.NEXTCLOUD_ROOT_DIR_PATH
            user_dir = os.path.join(root_dir_from_disk, fm_space_path)
            sys_dir = os.path.join(root_dir_from_disk, fm_system_path)

            # Create task for this scanning task
            scan_id = str(uuid.uuid4())

            # Find current time
            current_epoch_time = time.time()

            if not webdav_client.is_directory(fm_space_path):
                logging.error(f"Record name '{record_name}' does not exist or missing information")
                return {"error": "Not Found",
                        "message": f"Record name '{record_name}' does not exist or is missing information"}, 404

            # Find user space last modified file date
            dir_info = webdav_client.get_directory_info(fm_space_path)

            last_modified_date = dir_info['last_modified']

            # Convert epoch time to an ISO-formatted string
            current_datetime = datetime.datetime.fromtimestamp(current_epoch_time)
            display_time = current_datetime.isoformat()

            # Scan the initial directory and get content metadata
            contents = self.scan_directory_contents(fm_space_path, root_dir_from_disk)

            content_md = {
                'space_id': space_id,
                'scan_id': scan_id,
                'scan_time': current_epoch_time,
                'scan_datetime': display_time,
                'user_dir': str(user_dir),
                'fm_system_path': str(fm_system_path),
                'last_modified': last_modified_date,
                'size': helpers.calculate_size(contents),
                'is_complete': True,
                'contents': contents
            }

            # Instantiate Scanning Class
            scanner = FileManagerDirectoryScanner(space_id, user_dir, sys_dir)

            # Run fast scanning and update metadata
            content_md = scanner.fast_scan(content_md)

            # Run the slowScan asynchronously using a thread
            @copy_current_request_context
            def run_slow_scan():
                with current_app.app_context():
                    # Read files from disk for performance
                    asyncio.run(scanner.slow_scan(content_md))

            thread = threading.Thread(target=run_slow_scan)
            thread.start()

            success_response = {
                'success': 'POST',
                'message': 'Scanning successfully started!',
                'scan_id': scan_id
            }

            logging.info(f"Scanning started successfully for record: {record_name}")
            return success_response, 200

        except FileNotFoundError:
            logging.error(f"File not found for record: {record_name}")
            return {'error': 'Not Found', 'message': 'Requested record not found'}, 404

        except ValueError as e:
            logging.exception(f"Value error: {str(e)}")
            return {'error': 'Bad Request', 'message': str(e)}, 400

        except Exception as error:
            logging.exception("An unexpected error occurred: " + str(error))
            return {"error": "Internal Server Error", "message": "An unexpected error occurred"}, 500

    @jwt_required()
    def get(self, record_name, scan_id):
        """
        Retrieves the current state and details of a scanning task by its ID.

        Args:
            record_name (str): Unique identifier of the record.
            scan_id (str): Unique identifier of the scanning task.

        Returns:
            tuple: Content metadata object and status code 200, or error message with status code 404.
        """
        try:
            nextcloud_client = NextcloudApi(Config)
            content = None

            # Instantiate dirs
            space_id = record_name
            fm_system_path = os.path.join(space_id, f"{space_id}-sys")
            fm_space_path = os.path.join(space_id, f"{space_id}")
            root_dir_from_disk = Config.NEXTCLOUD_ROOT_DIR_PATH
            user_dir = os.path.join(root_dir_from_disk, fm_space_path)
            full_sys_dir = os.path.join(root_dir_from_disk, fm_system_path)

            # Retrieve nextcloud metadata
            nextcloud_scanned_dir_files = nextcloud_client.scan_directory_files(fm_system_path)
            nextcloud_md = helpers.parse_nextcloud_scan_xml(user_dir, nextcloud_scanned_dir_files)
            for file_md in nextcloud_md:
                if scan_id in file_md['path']:
                    file_path = helpers.get_correct_path(
                        os.path.join(full_sys_dir, re.split(r'/|\\', file_md['path'])[-1]))
                    with open(file_path, 'r') as file:
                        content = json.load(file)
                        logging.info(f"Scan {scan_id} found and returned successfully")
                        return {'success': 'GET', 'message': content}, 200

            if content is None:
                logging.error(f"Scan {scan_id} not found")
                return {'error': 'Scan Not Found', 'message': f'Scan {scan_id} not found'}, 404

        except FileNotFoundError as e:
            logging.exception(f"File not found: {e}")
            return {'error': 'File Not Found', 'message': str(e)}, 404
        except Exception as error:
            logging.exception("An unexpected error occurred: " + str(error))
            return {"error": "Internal Server Error", "message": "An unexpected error occurred"}, 500

    @jwt_required()
    def delete(self, record_name, scan_id):
        """
        Deletes a scanning task report from the file manager using its unique ID.

        Args:
            record_name (str): Unique identifier of the record.
            scan_id (str): Unique identifier of the scanning task.

        Returns:
            tuple: Success response with status code 200, or error response with status code 500.
        """
        try:
            webdav_client = WebDAVApi(Config)
            # Instantiate dirs
            space_id = record_name
            fm_system_path = os.path.join(space_id, f"{space_id}-sys")
            file_path = os.path.join(fm_system_path, f"report-{scan_id}.json")

            # Delete file
            logging.info(f"Attempting to delete file: {file_path}")
            response = webdav_client.delete_file(file_path)

            if response['status'] not in [200, 204]:
                logging.error(f"Failed to delete scan report '{scan_id}'")
                return {'error': 'Internal Server Error', 'message': 'Failed to delete scan report'}, 500

            success_response = {
                'success': 'DELETE',
                'message': f"Scanning '{scan_id}' deleted successfully!",
            }
            logging.info(f"File deleted successfully: {file_path}")
            return success_response, 200

        except Exception as error:
            logging.exception("An unexpected error occurred: " + str(error))
            return {"error": "Internal Server Error", "message": "An unexpected error occurred"}, 500