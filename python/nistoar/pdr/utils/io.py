"""
Utility functions and classes for file reading and writing
"""
from collections import OrderedDict
from collections.abc import Mapping
from pathlib import Path
from abc import ABC, abstractmethod
import json, os, threading, shutil, hashlib
try:
    import fcntl
except ImportError:
    fcntl = None

from ..exceptions import (NERDError, PODError, StateException)
from .logging import blab, utilslog
log = utilslog

__all__ = [
    'LockedFile', 'AtomicAccessFile', 'read_nerd', 'read_pod', 'read_json', 'write_json',
    'NERDError', 'PODError', 'StateException'
]

class AtomicAccessFile(ABC):
    """
    a base class for creating specialized data files that will be shared across processes.  

    This class is subclassed to access data files that are read and written as a whole in an 
    atomic way.  This mode is common for files that represent persistent state for objects in 
    memory.  This base class handles the file locking; the subclass provides the implementation 
    for parsing and formatting the file's content.  

    This class provides a convenient way to atomically read and write the data; for example, if 
    ``MyStateFile`` is your subclass, then you can do your IO like this:
    .. code-block:: python

       data = MyStateFile.read(filepath)
       # manipulate data
       MyStateFile.write(filepath, data)

    During the read operation, a read lock is in place, allowing multiple processes to read the 
    file simultaneously, but waits until any write is finished.  During write, the write will wait 
    until all other accesses are completed before commiting its data.  

    If it is important that no other writing occurs between a read and a write, one can use the 
    following:
    .. code-block:: python

       with MyStateFile(filepath, MyStateFile.LOCK_WRITE) as sfile:
           data = sfile.read_data()
           # manipulate data
           sfile.write_data(data)

    This implementation locks on a proxy file based on the absolute filepath.  This allows the lock 
    to work regardless of whether the target file exists or not; however, unexpected results can 
    occur if different processes access the same file via hard-link paths.
    """
    LOCK_WRITE = fcntl.LOCK_EX
    LOCK_READ  = fcntl.LOCK_SH
    lock_dir = Path(os.environ.get("TMPDIR", "/tmp")) / "_OARlocks"
    open_params = {}
    
    def __init__(self, filepath, locktype=None):
        """
        create the file wrapper
        :param filepath  str: the path to the file
        :param locktype:      the type of lock to acquire.  The value should 
                              be either LOCK_READ or LOCK_WRITE.
                              If None, no lock is acquired.  
        """
        if isinstance(filepath, str):
            filepath = Path(filepath)
        self._file = filepath
        self._type = None
        self._lock = None
        self._lockfile = self._lockfile_for(self._file)
        self._ensure_lockfile(self._lockfile)

        if locktype is not None:
            self.acquire(locktype)

    def _lockfile_for(self, file: Path):
        return self.lock_dir / hashlib.sha1(str(file.resolve()).encode('utf8')).hexdigest()

    def _ensure_lockfile(self, lockfile: Path):
        if not self.lock_dir.exists():
            self.lock_dir.mkdir(parents=True, exist_ok=True)
        if not lockfile.exists():
            with open(lockfile, 'w') as fd:
                pass

    def __del__(self):
        self.release()

    @property
    def lock_type(self):
        """
        the current type of lock held, or None if no lock is held.
        """
        return self._type

    def acquire(self, locktype):
        """
        set a lock on the file
        """
        if self._lock:
            if self._type == locktype or locktype == self.LOCK_READ:
                return False
            elif locktype == self.LOCK_WRITE:
                raise RuntimeError("Release the read lock before "+
                                   "requesting write lock")

        else:
            self._ensure_lockfile(self._lockfile)
            if locktype == self.LOCK_READ:
                self._lock = open(self._lockfile)
            elif locktype == self.LOCK_WRITE:
                self._lock = open(self._lockfile, 'w')
            else:
                raise ValueError("Not a recognized lock type: "+ str(locktype))
            fcntl.flock(self._lock, locktype)
            self._type = locktype

        return True

    def release(self):
        if self._lock:
            fcntl.flock(self._lock, fcntl.LOCK_UN)
            self._lock.close()
            self._lock = None
            self._type = None

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_val, ex_tb):
        self.release()

    def read_data(self):
        """
        read data from the configured file.  If a lock is not 
        currently set, one is acquired and immediately released.  
        """
        release = self.acquire(self.LOCK_READ)
        try:
            with open(self._file, **self.open_params) as fd:
                fcntl.flock(fd, self.LOCK_READ)
                out = self._parse_data(fd)
        finally:
            if release:
                self.release()
        return out
    
    @abstractmethod
    def _parse_data(self, fd):
        """
        read and parse the data from the given file descriptor.

        Subclasses should implement this method for the particular type of data in the file.
        Implementations should assume that the given file descriptor is set at the beginning 
        of the file.  
        """
        raise NotImplemented()

    def write_data(self, data):
        """
        write data to the configured file.  If a lock is not 
        currently set, one is acquired and immediately released.  
        """
        release = self.acquire(self.LOCK_WRITE)
        try:
            with open(self._file, 'w', **self.open_params) as fd:
                fcntl.flock(fd, self.LOCK_WRITE)
                out = self._format_data(data, fd)
        finally:
            if release:
                self.release()
        
    @abstractmethod
    def _format_data(self, data, fd):
        """
        format and write out the data to the given file descriptor.

        Subclasses should implement this method for the particular type of data in the file.
        Implementations should assume that the given file descriptor is set at the beginning 
        of the file.  
        """
        raise NotImplemented()

    @classmethod
    def _clean_lock_dir(cls):
        """
        Delete the lock file directory.  This should only be called when it is known that all 
        processes accessing files through this class have exited; otherwise, locking could fail 
        to prevent simultaneous access.
        """
        if cls.lock_dir.is_dir():
            shutil.rmtree(cls.lock_dir)

    @classmethod
    def read(cls, filepath):
        """
        read, parse, and return the contents of the data file atomically.  

        Simultaneous writes to the file are prevented while reading the data

        :param str|Path filepath:  the path to the data file
        :return:  the contents of the file as an implementation-specific data type
        """
        return cls(filepath).read_data()

    @classmethod
    def write(cls, filepath, data):
        """
        write the given data to a data file atomically.

        All other accesses to the file are blocked while writing the data

        :param str|Path filepath:  the path to the data file
        :param data:  the data to write; the proper type of the data is implementation-specific
        """
        cls(filepath).write_data(data)


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

class _PathTolerantJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Path):
            return str(o)
        return json.JSONEncoder.default(self, o)

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
            json.dump(jsdata, fd, indent=indent, separators=(',', ': '), cls=_PathTolerantJSONEncoder)
        blab(log, "released EX")
    except Exception as ex:
        raise StateException("{0}: Failed to write JSON data to file: {1}"
                             .format(destfile, str(ex)), cause=ex)

