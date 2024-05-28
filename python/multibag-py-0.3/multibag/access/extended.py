"""
a module that extends the interface provided by the bagit.Bag class through
mixin interfaces.  One of the key goals of the mixins is to add methods that 
can work against the base bagit.Bag class directly or against the ReadOnlyBag
class which can read serialized versions of bags.  
"""
from __future__ import absolute_import
import os, sys, re, shutil, codecs, inspect
from collections import OrderedDict, namedtuple
from abc import ABCMeta, abstractmethod
from datetime import datetime, tzinfo

from .bagit import Bag, ReadOnlyBag
from bagit import _parse_tags

from fs.copy import copy_file
from fs.errors import ResourceNotFound
from fs import open_fs

if sys.version_info[0] > 2:
    _unicode = str
    from inspect import Signature, Parameter
else:
    _unicode = unicode
    from funcsigs import Signature, Parameter

_bagsepre = re.compile(r'/')
_ossepre = re.compile(os.sep)

FileTimes = namedtuple('FileTimes', "ctime mtime atime".split())
def _d2e(dt):
    if not isinstance(dt, datetime):
        return dt
    return (dt - datetime(1970, 1, 1, tzinfo=dt.tzinfo)).total_seconds()

class ExtendedReadMixin(object):
    """
    an interface providing additional read-only access to the contents of a bag
    """
    __metaclass__ = ABCMeta

    _is_readonly = True

    def __init__(self, *args, **kw):
        super(ExtendedReadMixin, self).__init__()

    @property
    def is_readonly(self):
        """
        returns True if this bag is set to be read-only.  
        """
        return self._is_readonly

    @property
    def name(self):
        """
        the name of the root directory of the bag (without any parent path
        included).
        """
        return self._get_bag_name()

    @abstractmethod
    def _get_bag_name(self):
        raise NotImplementedError()

    @abstractmethod
    def exists(self, path):
        """
        return True if the given path exists in this bag.  
        :param path str:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        raise NotImplementedError()

    @abstractmethod
    def isdir(self, path):
        """
        return True if the given path exists as a subdirectory in the bag
        :param path str:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        raise NotImplementedError()

    @abstractmethod
    def isfile(self, path):
        """
        return True if the given path exists as a file in the bag
        :param path str:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        raise NotImplementedError()

    @abstractmethod
    def sizeof(self, path):
        """
        return the size of the file in bytes located at the give path
        :param path str:  the path to desired file, given relative to the bag's 
                          base directory.  '/' must be used as the path 
                          delimiter.
        :rtype int:  the size of the file in bytes
        """
        raise NotImplementedError()

    @abstractmethod
    def timesfor(self, path):
        """
        return the timestamps associated with a file or directory with the given path.
        :rtype: FileTimes named tuple
        """
        raise NotImplementedError()

    @abstractmethod
    def walk(self, start=None):
        """
        Walk the source bag contents returning the triplets returned by
        os.path.  The difference is that the first element in the triplet,
        the base directory, will be relative to the bag's base directory;
        for files and directories directly below the base, the field will 
        be an empty string.

        :param str start:  the path to a directory, relative to the root,
                           where the walk should start.  The path must
                           be delimited by forward slashes ('/'), regardless
                           of the platform.  
        """
        raise NotImplementedError()

    @abstractmethod
    def replicate(self, path, destdir):
        """
        copy a file from the source bag to the same location in an output bag.

        :param str path:  the path, relative to the source bag's root directory,
                          to the file to be replicated.
        :param str destdir:  the destination directory.  This is usually the 
                          root directory of another bag.  
        """
        raise NotImplementedError()

    @abstractmethod
    def open_text_file(self, path, encoding='utf-8-sig'):
        """
        return a open, read-only file object on the file from the source bag 
        with the given path.

        :param str path:     the path to the file to open, relative to the source
                             bag's root directory.
        :param str encoding  a label indicating the encoding to expect in the 
                             file.  (Default: "utf-8-sig").
        """
        raise NotImplementedError()

    def nonstandard(self):
        """
        iterate through the non-standard files in this bag.  This will 
        exclude the special files defined by the BagIt standard (bagit.txt,
        bag-info.txt, fetch.txt and the manifest files).  It will include
        any directory in the bag that is empty (to allow it to be replicated
        in output multibags).
        """
        special = [re.compile(r) for r in
                       r"^bagit.txt$ ^bag-info.txt$ ^fetch.txt$".split() +
                       r"^(tag)?manifest-(\w+).txt$".split()]

        for dir, subdirs, files in self.walk():
            if len(subdirs) == 0 and len(files) == 0:
                # spit out a directory if it is empty
                yield dir
            for file in files:
                # don't spit out a file is it's one of the special ones
                if not any([bool(spf.match(file)) for spf in special]):
                    yield os.path.join(dir, file)

    def _load_bag_info(self):
        # this (re-)reads the bag-info data, loading it into an OrderedDict

        tags = OrderedDict()
        with self.open_text_file(self.tag_file_name) as fd:
            for name, value in _parse_tags(fd):
                if name not in tags:
                    tags[name] = value
                    continue

                if not isinstance(tags[name], list):
                    tags[name] = [tags[name], value]
                else:
                    tags[name].append(value)

        self.info = tags

    def is_head_multibag(self):
        """
        return True if this bag is a designated as the head bag of a multibag
        aggregation.  This implementation returns True if the 
        'Multibag-Head-Version' is set.  
        """
        return 'Multibag-Head-Version' in self.info

               
      
class _ExtendedReadWritableMixin(ExtendedReadMixin):
    """
    An implementation of the ExtendedReadMixin for a Bag that is writable
    via the Bag interface.  
    """
    _is_readonly = False

    def __init__(self, *args, **kw):
        """
        Initialize the extended interface.  

        Note that when calling this constructor via super(), no arguments are
        expected.  The following arguments are expected when attaching this 
        mixin to an existing object and __init__() is called explicitly to 
        initialize it.

        :param str bagdir:  the path to the bag's root directory
        """

        # Mixins with non-cooperating classes are not well (properly) served 
        # by super(); that's that reason for the generic signature.
        
        if args or kw:
            # we're attaching this mixin interface to an existing object;
            # launch the mixin's chain of __init__()s
            kw = self._ctor_bind_args(args, kw)
            
            self._bagdir = kw['bagdir'].rstrip(os.sep)
            self._bagname = os.path.basename(self._bagdir)
            self._replwhl = True
            super(_ExtendedReadWritableMixin, self).__init__()

    def _ctor_bind_args(self, args, kw):
        sig = Signature([Parameter("bagdir", Parameter.POSITIONAL_OR_KEYWORD)])
        return sig.bind(*args, **kw).arguments

    @property
    def replicate_with_hardlink(self):
        """
        a flag indicating, if True, that requests to replicate a file will be 
        by attempted by creating a hard link at the destination to the original
        file.  Doing so will save space on disk because bytes are not replicated.
        This is not supported on Windows.  On UNIX-like systems, hard links are 
        only possible when the source and destinations are on the same filesystem.
        When this is not the case, replicate falls back to a true copy.  
        """
        return self._replwhl

    @replicate_with_hardlink.setter
    def replicate_with_hardlink(self, yes):
        """
        set the flag with a True or False
        """
        self._replwhl = bool(yes)

    def _get_bag_name(self):
        return self._bagname

    def _canon_path(self, path):
        if os.sep != '/':
            path = _bagsepre.sub(os.sep, path)
        path = os.path.normpath(os.path.join(self._bagdir, path))
        if not path.startswith(os.path.normpath(self._bagdir)):
            return None
        return path

    def exists(self, path):
        """
        return True if the given path exists in this bag.  
        :param str path:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        path = self._canon_path(path)
        if path is None:
            return False
        return os.path.exists(path)

    def isdir(self, path):
        """
        return True if the given path exists as a subdirectory in the bag
        :param path str:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        path = self._canon_path(path)
        if path is None:
            return False
        return os.path.isdir(path)

    def isfile(self, path):
        """
        return True if the given path exists as a file in the bag
        :param str path:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        path = self._canon_path(path)
        if path is None:
            return False
        return os.path.isfile(path)

    def sizeof(self, path):
        """
        return the size of the file in bytes located at the give path
        :param path str:  the path to desired file, given relative to the bag's 
                          base directory.  '/' must be used as the path 
                          delimiter.
        :rtype int:  the size of the file in bytes
        """
        path = self._canon_path(path)
        return os.stat(path).st_size

    def timesfor(self, path):
        """
        return the timestamps associated with a file or directory with the given path.
        :rtype: FileTimes named tuple
        """
        path = self._canon_path(path)
        st = os.stat(path)
        return FileTimes(ctime=st.st_ctime, mtime=st.st_mtime, atime=st.st_atime)

    def replicate(self, path, destdir, logger=None):
        """
        copy a file from the source bag to the same location in an output bag.

        This implementation will replicate the file as a hard link if possible 
        when self.replicate_with_hardlink is True.  Otherwise, a normal file 
        copy is done.  

        :param str path:  the path, relative to the source bag's root directory,
                          to the file to be replicated.
        :param str destdir:  the destination directory.  This is usually the 
                          root directory of another bag.  
        :param Logger logger: a logger instance to send messages to.
        :raises ValueError:  if the given file path doesn't exist in the source
                          bag.
        """
        if not self.exists(path):
            raise ValueError("replicate: file/dir does not exist in this bag: " +
                             path)
        
        destpath = os.path.join(destdir, path)
        if self.isdir(path):
            if not os.path.isdir(destpath):
                if logger:
                    logger.info("Creating matching directory in output bag: "
                                     +path)
                os.makedirs(destpath)
            return
        
        parent = os.path.dirname(destpath)
        hardlink = self.replicate_with_hardlink
        source = os.path.join(self._bagdir, path)
        
        if not os.path.exists(parent):
            if logger:
                logger.debug("Creating output file's parent directory: " +
                                  parent)
            os.makedirs(parent)

        if hardlink:
            try:
                os.link(source, destpath)
            except OSError as ex:
                hardlink = False
                if logger:
                    msg = "Unable to create hard link for data file (" + path + \
                          "): " + str(ex) + "; swithching to copy mode."
                    self.logger.warning(msg)

        if not hardlink:
            try:
                shutil.copy(source, destpath)
            except Exception as ex:
                if logger:
                    msg = "Unable to copy data file (" + path + \
                          ") into bag (" + destdir + "): " + str(ex)
                    self.log.exception(msg, exc_info=True)
        
    def open_text_file(self, path, encoding='utf-8-sig'):
        """
        return a open, read-only file object on the file from the source bag 
        with the given path.

        :param str path:     the path to the file to open, relative to the source
                             bag's root directory.
        :param str encoding  a label indicating the encoding to expect in the 
                             file.  (Default: "utf-8-sig").
        """
        path = self._canon_path(path)
        return codecs.open(path, encoding=encoding);
        
    def walk(self, start=None):
        """
        Walk the source bag contents returning the triplets returned by
        os.path with two differences.  

        The first difference is that the first element in the triplet,
        the base directory, will be relative to the bag's base directory;
        for files and directories directly below the base, the field will 
        be an empty string.

        The second difference is that the path separator will always be a 
        forward slash ('/'), consistent with the BagIt standard.

        :param str start:  the path to a directory, relative to the root,
                           where the walk should start.  The path must
                           be delimited by forward slashes ('/'), regardless
                           of the platform.  
        """
        if not start:
            start = ''
        start = os.path.join(*(re.split(r'/+', start)))
        if os.path.isabs(start):
            raise ValueError("walk(): start must not be absolute: "+start)
        root = os.path.join(self._bagdir, start).rstrip(os.sep)
        if not os.path.isdir(root):
            raise ValueError("walk(): start is not a subdirectory: "+start)

        for dir, subdirs, files in os.walk(root):
            # make dir relative to bag's root directory
            if dir.startswith(self._bagdir):
                dir = dir[len(self._bagdir):].lstrip(os.sep)
            dir = _ossepre.sub('/', dir)
            
            yield dir, subdirs, files

    def calc_oxum(self):
        """
        calculate and return the Bagit-defined Payload-Oxum as a 2-tuple for 
        the bag in its current state
        :return:  a 2-tuple where the first element is the total number of 
                  file bytes and the second element is the total number of 
                  files.  
        """
        nf = 0
        sz = 0
        for root, dirs, files in self.walk("data"):
            for f in files:
                nf += 1
                sz += self.sizeof("/".join([root, f]))
        return (sz, nf)

    def update_oxum(self):
        """
        calculate and save the current Payload_Oxum to the in-memory tag metadata (self.info).
        The save() method should be called to commit this value into the bag.
        """
        oxum = self.calc_oxum()
        self.info['Payload-Oxum'] = "%s.%s" % oxum
        return oxum

    def calc_bag_size(self):
        """
        estimate the current size of the bag in bytes.  The save() function should be called 
        before this method for a more accurate estimate.
        """
        sz = 0
        for root, dirs, files in self.walk():
            for f in files + dirs:
                sz += self.sizeof(os.path.join(root, f))

        # fine adjustments
        if 'Bag-Size' in self.info:
            sz -= len(self.info['Bag-Size'])
            sz += len(self._format_bytes(sz))

        return sz

    def update_bag_size(self):
        """
        estimate and save the current size of the bag to the in-memory tag metadata (self.info)
        The save() method should be called before this method to get a more accurate estimate, 
        and it should be called after this to commit this value into the bag.
        """
        sz = self.calc_bag_size()
        self.info['Bag-Size'] = self._format_bytes(sz)
        return sz

    def _format_bytes(self, nbytes):
        prefs = ["", "k", "M", "G", "T"]
        ordr = 0
        while nbytes >= 1000.0 and ordr < 4:
            nbytes /= 1000.0
            ordr += 1
        pref = prefs[ordr]
        ordr = 0
        while nbytes >= 10.0:
            nbytes /= 10.0
            ordr += 1
        nbytes = "{0:5f}".format(round(nbytes, 3) * 10**ordr)
        if '.' in nbytes:
            nbytes = re.sub(r"0+$", "", nbytes)
        if nbytes.endswith('.'):
            nbytes = nbytes[:-1]    
        return "{0} {1}B".format(nbytes, pref)

        

class ExtendedReadWritableBag(Bag, _ExtendedReadWritableMixin):
    """
    A Bag with an extended interface.
    """
    def __init__(self, bagpath):
        super(ExtendedReadWritableBag, self).__init__(bagpath)
        self._extend(bagpath)

    def _extend(self, bagdir):
        _ExtendedReadWritableMixin.__init__(self, bagdir)



class _ExtendedReadOnlyMixin(ExtendedReadMixin):
    """
    An implementation of the ExtendedReadMixin for a Bag that is accessible 
    only via the ReadOnlyBag interface.  
    """
    _is_readonly = True

    def __init__(self, *args, **kw):
        """
        Initialize the extended interface.  

        Note that when calling this constructor via super(), no arguments are
        expected.  The following arguments are expected when attaching this 
        mixin to an existing object and __init__() is called explicitly to 
        initialize it.

        :param bool doinit:  True if the mixin interface should be initialized
        """

        # Mixins with non-cooperating classes are not well (properly) served 
        # by super(); that's that reason for the generic signature.

        # This mixin does not require any arguments to initialize; however,
        # to follow our mixin pattern, we've defined the doinit argument as
        # a signal to initialize the mixin interface.
        # see also _ExtendedReadWritableMixin above for a clearer example
        
        if args or kw:
            # we're attaching this mixin interface to an existing object;
            # launch the mixin's chain of __init__()s
            kw = self._ctor_bind_args(args, kw)
            if kw['doinit']:
                super(_ExtendedReadOnlyMixin, self).__init__()

    def _ctor_bind_args(self, args, kw):
        sig = Signature([Parameter("doinit", Parameter.POSITIONAL_OR_KEYWORD)])
        return sig.bind(*args, **kw).arguments

    def _get_bag_name(self):
        return self._name

    def exists(self, path):
        """
        return True if the given path exists in this bag.  
        :param str path:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        if path is None:
            return False
        return self._root.relpath(path).exists()

    def isdir(self, path):
        """
        return True if the given path exists as a subdirectory in the bag
        :param path str:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        if path is None:
            return False
        return self._root.relpath(path).isdir()

    def isfile(self, path):
        """
        return True if the given path exists as a file in the bag
        :param str path:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        if path is None:
            return False
        return self._root.relpath(path).isfile()

    def sizeof(self, path):
        """
        return the size of the file in bytes located at the give path
        :param path str:  the path to desired file, given relative to the bag's 
                          base directory.  '/' must be used as the path 
                          delimiter.
        :rtype int:  the size of the file in bytes
        """
        try:
            info = self._root.fs.getinfo(path, namespaces=['details'])
        except ResourceNotFound as ex:
            raise OSError(2, "File not found: "+path)
        return info.size

    def timesfor(self, path):
        """
        return the timestamps associated with a file or directory with the given path.
        :rtype: FileTimes named tuple
        """
        try:
            info = self._root.fs.getinfo(path, namespaces=['details'])
        except ResourceNotFound as ex:
            raise OSError(2, "File not found: "+path)
        return FileTimes(ctime=_d2e(info.created), mtime=_d2e(info.modified), atime=_d2e(info.accessed))

    def replicate(self, path, destdir, logger=None):
        """
        copy a file from the source bag to the same location in an output bag.

        :param str path:  the path, relative to the source bag's root directory,
                          to the file to be replicated.
        :param str destdir:  the destination directory.  This is usually the 
                          root directory of another bag.  
        :param Logger logger: a logger instance to send messages to.
        :raises ValueError:  if the given file path doesn't exist in the source
                          bag.
        """
        path = _unicode(path)
        if not self.exists(path):
            raise ValueError("replicate: file/dir does not exist in this bag: " +
                             path)

        destfs = open_fs(destdir)
        if self.isdir(path):
    
            if not destfs.isdir(path):
                if logger:
                    logger.info("Creating matching directory in output bag: "
                                     +path)
                destfs.makedirs(path, recreate=True)
            return

        parent = os.path.dirname(path)
        if parent and not destfs.exists(parent):
            if logger:
                logger.debug("Creating output file's parent directory: " +
                             parent)
            destfs.makedirs(parent)

        copy_file(self._root.fs, path, destfs, path)

    def open_text_file(self, path, encoding='utf-8-sig'):
        """
        return a open, read-only file object on the file from the source bag 
        with the given path.

        :param str path:     the path to the file to open, relative to the source
                             bag's root directory.
        :param str encoding  a label indicating the encoding to expect in the 
                             file.  (Default: "utf-8-sig").
        """
        return self._root.fs.open(path, encoding=encoding)

    def walk(self, start=None):
        """
        Walk the source bag contents returning the triplets returned by
        os.path with two differences.  

        The first difference is that the first element in the triplet,
        the base directory, will be relative to the bag's base directory;
        for files and directories directly below the base, the field will 
        be an empty string.

        The second difference is that the path separator will always be a 
        forward slash ('/'), consistent with the BagIt standard.

        :param str start:  the path to a directory, relative to the root,
                           where the walk should start.  The path must
                           be delimited by forward slashes ('/'), regardless
                           of the platform.  
        """
        if not start:
            start = ''
        if start.startswith('/'):
            raise ValueError("walk(): start must not be absolute: "+start)
        if not start:
            root = self._root
        else:
            root = self._root.subfspath(start)

        witer = root.fs.walk.walk()
        while True:
            try: 
                base, dirs, files = next(witer)
                base = '/'.join([start, base.lstrip('/')]).strip('/')
                yield base, [d.name for d in dirs], [f.name for f in files]
            except StopIteration:  # see PEP0479 for supporting 3.7+
                return

class ExtendedReadOnlyBag(ReadOnlyBag, _ExtendedReadOnlyMixin):
    """
    A ReadOnlyBag (which may be serialized) with an extended interface.
    """
    def __init__(self, bagpath, name=None):
        """
        open the bag with the given location
        :param bagpath:  either a Path instance or a filepath to the bag's 
                         root directory.  A Path instance must be used if the 
                         bag is in a serialized form.  
        :type bagpath:   str or Path
        """
        super(ExtendedReadOnlyBag, self).__init__(bagpath, name)
        self._extend()

    def _extend(self):
        _ExtendedReadOnlyMixin.__init__(self)


def as_extended(bag):
    """
    extend the interface of the given bag instance with methods from the 
    ExtendedReadMixin interface.  The input bag instance 
    is returned with its class updated.
    """
    if not isinstance(bag, Bag):
        raise ValueError("as_extended(): input not of type Bag: " + str(bag))

    # already extended; just return it
    if isinstance(bag, ExtendedReadMixin):
        return bag

    # input is a ReadOnlyBag, access via an fs instance (e.g. zip file, tar ball,
    # ...).  Convert to ExtendedReadOnlyBag
    if isinstance(bag, ReadOnlyBag):
        bag.__class__ = ExtendedReadOnlyBag
        bag._extend()
        return bag

    # input is a normal bag
    if not os.path.exists(os.path.join(bag.path, "bagit.txt")):
        raise ValueError("Unsupported Bag implementation: "+str(type(bag)))

    bag.__class__ = ExtendedReadWritableBag
    bag._extend(bag.path)
    return bag
    
