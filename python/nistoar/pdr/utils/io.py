"""
Utility functions and classes for file reading and writing
"""
from collections import OrderedDict, Mapping
import json, os, threading
try:
    import fcntl
except ImportError:
    fcntl = None

from ..exceptions import (NERDError, PODError, StateException)
from .logging import blab, utilslog
log = utilslog

__all__ = [
    'LockedFile', 'read_nerd', 'read_pod', 'read_json', 'write_json',
    'NERDError', 'PODError', 'StateException'
]

class LockedFile(object):
    """
    An object representing a file in a locked state.  The file is locked against
    simultaneous accesses across both threads and processes.  

    The easiest way to use this class is via the with statement.  For example,
    to read a file with a shared lock (many reads, no writes):
    .. code-block:: python

       with LockedFile(filename) as fd:
           data = json.load(fd)

    And to write a file with an exclusive write (no other simultaneous reads 
    or writes):
    .. code-block:: python

       with LockedFile(filename, 'w') as fd:
           json.dump(data, fd)

    An example of its use without the with statement might be:
    .. code-block:: python

       lkdfile = LockedFile(filename)
       fd = lkdfile.open()
       data = json.load(fd)
       lkdfile.close()    #  do not call fd.close() !!!

       lkdfile.mode = 'w'
       with lkdfile as fd:
          json.dump(data, fd)

    """
    _thread_locks = {}
    _class_lock = threading.RLock()

    class _ThreadLock(object):
        _reader_count = 0
        def __init__(self):
            self.ex_lock = threading.Lock()
            self.sh_lock = threading.Lock()
        def acquire_shared(self):
            with self.ex_lock:
                if not self._reader_count:
                    self.sh_lock.acquire()
                self._reader_count += 1
        def release_shared(self):
            with self.ex_lock:
                if self._reader_count > 0:
                    self._reader_count -= 1
                if self._reader_count <= 0:
                    self.sh_lock.release()
        def acquire_exclusive(self):
            with self.sh_lock:
                self.ex_lock.acquire()
        def release_exclusive(self):
            self.ex_lock.release()
            
    @classmethod
    def _get_thread_lock_for(cls, filepath):
        filepath = os.path.abspath(filepath)
        with cls._class_lock:
            if filepath not in cls._thread_locks:
                cls._thread_locks[filepath] = cls._ThreadLock()
            return cls._thread_locks[filepath]

    def __init__(self, filename, mode='r'):
        self.mode = mode
        self._fo = None
        self._fname = filename
        self._thread_lock = self._get_thread_lock_for(filename)
        self._writing = None

    @property
    def fo(self):
        """
        the open file object or None if the file is not currently open
        """
        return self._fo

    def _acquire_thread_lock(self):
        if self._writing:
            self._thread_lock.acquire_exclusive()
        else:
            self._thread_lock.acquire_shared()
    def _release_thread_lock(self):
        if self._writing:
            self._thread_lock.release_exclusive()
        else:
            self._thread_lock.release_shared()

    def open(self, mode=None):
        """
        Open the file so that it is appropriate locked.  If mode is not 
        provided, the mode will be the value set when this object was 
        created.  
        """
        if self._fo:
            raise StateException(str(self._fname)+": file is already open")
        if mode:
            self.mode = mode
            
        self._writing = 'a' in self.mode or 'w' in self.mode or '+' in self.mode
        self._acquire_thread_lock()
        try:
            self._fo = open(self._fname, self.mode)
        except:
            self._release_thread_lock()
            if self._fo:
                try:
                    self._fo.close()
                except:
                    pass
            self._fo = None
            self._writing = None
            raise

        if fcntl:
            lock_type = (self._writing and fcntl.LOCK_EX) or fcntl.LOCK_SH
            fcntl.lockf(self.fo, lock_type)
        return self.fo

    def close(self):
        if not self._fo:
            return
        try:
            self._fo.close()
        finally:
            self._fo = None
            self._release_thread_lock()
            self._writing = None

    def __enter__(self):
        return self.open()

    def __exit__(self, e1, e2, e3):
        self.close()
        return False

    def __del__(self):
        if self._fo:
            self.close()

def read_nerd(nerdfile):
    """
    read the JSON-formatted NERDm metadata in the given file

    :return OrderedDict:  the dictionary containing the data
    """
    try:
        return read_json(nerdfile)
    except ValueError as ex:
        raise NERDError("Unable to parse NERD file, " + str(nerdfile) + ": "+str(ex),
                       cause=ex, src=nerdfile)
    except IOError as ex:
        raise NERDError("Unable to read NERD file, " + str(nerdfile) + ": "+str(ex),
                        cause=ex, src=nerdfile)

def read_pod(podfile):
    """
    read the JSON-formatted POD metadata in the given file

    :return OrderedDict:  the dictionary containing the data
    """
    try:
        return read_json(podfile)
    except ValueError as ex:
        raise PODError("Unable to parse POD file, " + str(podfile) + ": "+str(ex),
                       cause=ex, src=podfile)
    except IOError as ex:
        raise PODError("Unable to read POD file, " + str(podfile) + ": "+str(ex),
                       cause=ex, src=podfile)

def read_json(jsonfile, nolock=False):
    """
    read the JSON data from the specified file

    :param str   jsonfile:  the path to the JSON file to read.  
    :param bool  nolock:    if False (default), a shared lock will be aquired
                            before reading the file.  A True value reads the 
                            file without a lock
    :raise IOError:  if there is an error while acquiring the lock or reading 
                     the file contents
    :raise ValueError:  if JSON format errors are detected.
    """
    with LockedFile(jsonfile) as fd:
        blab(log, "Acquired shared lock for reading: "+str(jsonfile))
        out = json.load(fd, object_pairs_hook=OrderedDict)
    blab(log, "released SH")
    return out

def write_json(jsdata, destfile, indent=4, nolock=False):
    """
    write out the given JSON data into a file with pretty print formatting

    :param dict jsdata:    the JSON data to write 
    :param str  destfile:  the path to the file to write the data to
    :param int  indent:    the number of characters to use for indentation
                           (default: 4).
    :param bool  nolock:   if False (default), an exclusive lock will be acquired
                           before writing to the file.  A True value writes the 
                           data without a lock
    """
    try:
        with LockedFile(destfile, 'a') as fd:
            blab(log, "Acquired exclusive lock for writing: "+str(destfile))
            fd.truncate(0)
            json.dump(jsdata, fd, indent=indent, separators=(',', ': '))
        blab(log, "released EX")
    except Exception as ex:
        raise StateException("{0}: Failed to write JSON data to file: {1}"
                             .format(destfile, str(ex)), cause=ex)

