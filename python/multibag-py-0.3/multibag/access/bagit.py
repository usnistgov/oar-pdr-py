"""
This is a proxy module around the LOC bagit module that allows it to be 
adapted to bags not stored in a vanilla directory (e.g. zip files, S3 storage,
etc.).  
"""
from __future__ import absolute_import
import os, sys
from collections import OrderedDict
import fs.osfs, fs.zipfs, fs.tarfs

import bagit as _bagit
from bagit import *    # import everything!
from bagit import _, _load_tag_file, _decode_filename
from bagit import _make_tagmanifest_file   # needed by testing

if sys.version_info[0] > 2:
    _unicode = str
else:
    _unicode = unicode

class Path(object):
    """
    A container class for pointing to a path within a specific FS instance
    """
    def __init__(self, filesys, path, prefix=None):
        """
        wrap a path within a filesystem, given as an FS object
        :param filesys FS:  the filesystem, usually as an FS instance, 
                            where the path is located
        :param path str:    the path to the location within the filesystem
        :param prefix str:  a prefix to use to represent the filesystem in 
                            the string representation of the full path.  It
                            will be prepended to the path value, so it should 
                            include any desired delimiters
        """
        self.fs = filesys
        self.path = _unicode(path)
        if prefix is None:
            prefix = repr(filesys) + ":"
        self._pfx = prefix

    def relpath(self, relpath):
        """
        return a Path instance that represents another path relative to
        this one.  This assumes that the current Path instance points to
        a directory; the relpath string then refers to a file or directory
        relative to it.  Note that relpath need not point to an existing 
        object within the filesystem.
        """
        if not relpath:
            return Path(self.fs, self.path, self._pfx)

        path = ""
        if self.path:
            path += self.path+'/'
        path += relpath.lstrip('/')
        
        return Path(self.fs, path, self._pfx)

    def subfspath(self, reldir=None):
        """
        return a new path with a filesystem that wraps a subdirectory 
        of the current path.  If reldir is None (default) or empty,
        a new Path instance with a new fs attribute wrapping the current 
        path (rather than a subdirectory)

        :raises fs.errors.DirectoryExpected: if reldir is not a subdirectory
        """
        if reldir:
            path = fs.path.join(self.path, reldir)
            return Path(self.fs.opendir(path), "", self._pfx+path+'/')
        elif not self.path:
            return Path(self.fs, self.path, self._pfx)
        else:
            return Path(self.fs.opendir(_unicode(self.path)), "",
                        self._pfx+self.path.lstrip('/')+'/')

    def exists(self):
        """
        return true if the file or directory pointed to exists in the filesystem
        """
        return self.fs.exists(self.path)

    def isfile(self):
        """
        return true if the path points to a file that exists in the filesystem
        """
        return self.fs.isfile(self.path)

    def isdir(self):
        """
        return true if the path points to a directory that exists in the filesystem
        """
        return self.fs.isdir(self.path)

    def __str__(self):
        return "{0}{1}".format(self._pfx, self.path)

    def __repr__(self):
        return "{0}:{1}".format(repr(self.fs), self.path)

_open_text_file_strpath = _bagit.open_text_file

def _open_text_file_Path(path, mode='r', encoding='utf-8', errors='strict', buffering=-1):
    """
    return a file-like object for the text file located at the given path
    relative to the bag's root directory.
    """
    if isinstance(path, Path):
        return path.fs.open(path.path, mode, buffering, encoding, errors)
    else:
        # backward compatibility to the LOC bagit module
        return _open_text_file_strpath(path, mode=mode, buffering=buffering,
                                       encoding=encoding, errors=errors)

open_text_file = _open_text_file_Path
_bagit.open_text_file = _open_text_file_Path

def open_bin_file(path, mode='r', buffering=-1):
    """
    return a file-like object for the binary file located at the given path
    relative to the bag's root directory.
    """
    if isinstance(path, Path):
        return path.fs.openbin(path.path, mode, buffering)
    else:
        return open(path, mode, buffering=buffering)

# This is based on bagit.Bag v1.6.3
# 
class ReadOnlyBag(_bagit.Bag):
    """
    A representation of a (possibly serialized) bag.   

    This implementation extends the LOC bagit.Bag class to access a bag via
    the fs module.  This allows the underlying bag to be in a serialized form.
    To open a serialized bag, the factory function open_bag() is recommended
    instead of instantiating this class directly.
    """

    def __init__(self, bagpath, name=None, location=None):
        """
        open the bag with the given location
        :param bagpath:  either a Path instance or a filepath to the bag's 
                         root directory.  A Path instance must be used if the 
                         bag is in a serialized form.  
        :type bagpath:   str or Path
        :param str name:  the name of bag (i.e. its nominal base directory); if None
                          the name will be the basename for the given bagpath
        :param str location:  the location of the bag; this can be provided when bagpath
                          is a Path instance to specify the source location of the Path's 
                          filesystem.
        """
        if not bagpath:
            raise BagError(_("path to bag root directory not provided"))
        if not isinstance(bagpath, Path):
            bagpath = bagpath.rstrip("/")
            parent = os.path.dirname(bagpath) or "."
            bagname = os.path.basename(bagpath)
            if not location:
                location = bagpath
            bagpath = Path(fs.osfs.OSFS(parent), _unicode(bagname), parent+"/")

        if not name:
            name = os.path.basename(bagpath.path)
        self._name = name
        self._root = bagpath.subfspath()

        path = location
        if not path:
            path = _unicode("/"+self._name)
            if path == "/":
                path = "//" # super __init__ will strip trailing /
        super(ReadOnlyBag, self).__init__(path)

    def __str__(self):
        return str(self._root)

    def _open(self):
        # Open the bagit.txt file, and load any tags from it, including
        # the required version and encoding.
        #
        # This overrides the one inherited from bagit.Bag
        bagit_file = _unicode("bagit.txt")
        bagit_file_path = self._root.relpath(bagit_file)

        if not self._root.fs.isfile(bagit_file):
            raise BagError(_("Expected bagit.txt does not exist: %s") % bagit_file_path)

        self.tags = tags = _load_tag_file(bagit_file_path)

        required_tags = ('BagIt-Version', 'Tag-File-Character-Encoding')
        missing_tags = [i for i in required_tags if i not in tags]
        if missing_tags:
            raise BagError(_("Missing required tag in bagit.txt: %s") % ', '.join(missing_tags))

        # To avoid breaking existing code we'll leave self.version as the string
        # and parse it into a numeric version_info tuple. In version 2.0 we can
        # break that.
        self._version = tags['BagIt-Version']

        try:
            self.version_info = tuple(int(i) for i in self._version.split('.', 1))
        except ValueError:
            raise BagError(_('Bag version numbers must be MAJOR.MINOR numbers, not %s') % self._version)

        if (0, 93) <= self.version_info <= (0, 95):
            self.tag_file_name = "package-info.txt"
        elif (0, 96) <= self.version_info < (2, ):
            self.tag_file_name = "bag-info.txt"
        else:
            raise BagError(_("Unsupported bag version: %s") % self._version)

        self.encoding = tags['Tag-File-Character-Encoding']
        try:
            codecs.lookup(self.encoding)
        except LookupError:
            raise BagValidationError(_("Unsupported encoding: %s") % self.encoding)

        info_file_path = self._root.relpath(self.tag_file_name)
        if info_file_path.exists():
            self.info = _load_tag_file(info_file_path, encoding=self.encoding)

        self._load_manifests()

    def is_head_multibag(self):
        """
        return True if this bag is a designated as the head bag of a multibag
        aggregation.  This implementation returns True if the 
        'Multibag-Head-Version' is set.  
        """
        return 'Multibag-Head-Version' in self.info

    def exists(self, path):
        """
        return True if the given path exists within the bag relative to the 
        bag's root directory.  

        :param str path:  a relative path to a directory or file within the bag
        """
        return self._root.fs.exists(path)

    def isfile(self, path):
        """
        return True if the given path exists as a file below the 
        bag's root directory.  

        :param str path:  a path to a file relative to the bag's root directory
        """
        return self._root.fs.isfile(path)

    def isdir(self, path):
        """
        return True if the given path exists as a directory below the 
        bag's root directory.  

        :param str path:  a path to a directory relative to the bag's root 
                          directory
        """
        return self._root.fs.isdir(path)

    def open_text_file(self, path, encoding='utf-8', errors='strict',
                       buffering=-1):
        """
        open the file with the given path for reading and return a file 
        object for it.  This cannot be used to open for writing. 
        """
        return open_text_file(self._root.relpath(path), 'r',
                              encoding, errors, buffering)

    def manifest_files(self):
        """
        iterate through the names of the manifest files.
        """
        for filename in [_unicode("manifest-%s.txt" % a) for a in CHECKSUM_ALGOS]:
            if self._root.fs.isfile(filename):
                yield filename

    def tagmanifest_files(self):
        """
        iterate through the names of the tag-manifest files.
        """
        for filename in [_unicode("tagmanifest-%s.txt" % a) for a in CHECKSUM_ALGOS]:
            if self._root.fs.isfile(filename):
                yield filename

    def payload_files(self):
        """Returns a list of filenames which are present on the local filesystem"""
        payload_dir = self._root.subfspath("data")

        for f in payload_dir.fs.walk.files():
            yield "data/"+f.lstrip('/')

    def save(self, processes=1, manifests=False):
        """
        save will persist any changes that have been made to the bag
        metadata (self.info).

        This implementation will always raise a BagError exception 
        complaining that the bag was opened read-only
        """
        raise BagError(_("Unable to save as the bag was opened read-only"))

    def missing_optional_tagfiles(self):
        """
        From v0.97 we need to validate any tagfiles listed
        in the optional tagmanifest(s). As there is no mandatory
        directory structure for additional tagfiles we can
        only check for entries with missing files (not missing
        entries for existing files).
        """
        for tagfilepath in self.tagfile_entries().keys():
            if not self._root.fs.isfile(tagfilepath):
                yield tagfilepath

    def fetch_entries(self):
        """Load fetch.txt if present and iterate over its contents

        yields (url, size, filename) tuples

        raises BagError for errors such as an unsafe filename referencing
        data outside of the bag directory
        """
        fetch_file_path = self._root.relpath("fetch.txt")
    
        if fetch_file_path.isfile():
            with open_text_file(fetch_file_path, 'r', encoding=self.encoding) as fetch_file:
                for line in fetch_file:
                    url, file_size, filename = line.strip().split(None, 2)

                    if self._path_is_dangerous(filename):
                        raise BagError(_('Path "%(payload_file)s" in "%(source_file)s" is unsafe') % {
                            'payload_file': filename,
                            'source_file': str(fetch_file_path),
                        })

                    yield url, file_size, filename

    def _load_manifests(self):
        self.entries = OrderedDict()
        manifests = list(self.manifest_files())

        if self.version_info >= (0, 97):
            # v0.97+ requires that optional tagfiles are verified.
            manifests += list(self.tagmanifest_files())

        for manifest_filename in manifests:
            if manifest_filename.find("tagmanifest-") != -1:
                search = "tagmanifest-"
            else:
                search = "manifest-"
            alg = os.path.basename(manifest_filename).replace(search, "").replace(".txt", "")
            self.algorithms.append(alg)

            manifest_filename = self._root.relpath(manifest_filename)

            manifest_file = None
            try:
                manifest_file = open_text_file(manifest_filename, 'r', encoding=self.encoding)

                if manifest_file.encoding.startswith('UTF'):
                    # We'll check the first character to see if it's a BOM:
                    if manifest_file.read(1) == UNICODE_BYTE_ORDER_MARK:
                        # We'll skip it either way by letting line decoding
                        # happen at the new offset but we will issue a warning
                        # for UTF-8 since the presence of a BOM  is contrary to
                        # the BagIt specification:
                        if manifest_file.encoding == 'UTF-8':
                            LOGGER.warning(_('%s is encoded using UTF-8 but contains an unnecessary'
                                             ' byte-order mark, which is not in compliance with the'
                                             ' BagIt RFC'),
                                           manifest_file.name)
                    else:
                        # Pretend the first read never happened
                        # manifest_file.seek(0)  
                        # seek() may not be available, so instead close and reopen
                        manifest_file.close()
                        manifest_file = open_text_file(manifest_filename, 'r', encoding=self.encoding)
                        
                for line in manifest_file:
                    line = line.strip()

                    # Ignore blank lines and comments.
                    if line == "" or line.startswith("#"):
                        continue

                    entry = line.split(None, 1)

                    # Format is FILENAME *CHECKSUM
                    if len(entry) != 2:
                        LOGGER.error(_("%(bag)s: Invalid %(algorithm)s manifest entry: %(line)s"),
                                     {'bag': self, 'algorithm': alg, 'line': line})
                        continue

                    entry_hash = entry[0]
                    entry_path = os.path.normpath(entry[1].lstrip("*"))
                    entry_path = _decode_filename(entry_path)

                    if self._path_is_dangerous(entry_path):
                        raise BagError(
                            _('Path "%(payload_file)s" in manifest "%(manifest_file)s" is unsafe') % {
                                'payload_file': entry_path,
                                'manifest_file': manifest_file.name,
                            }
                        )

                    entry_hashes = self.entries.setdefault(entry_path, OrderedDict())

                    if alg in entry_hashes:
                        warning_ctx = {'bag': self, 'algorithm': alg, 'filename': entry_path}
                        if entry_hashes[alg] == entry_hash:
                            msg = _('%(bag)s: %(algorithm)s manifest lists %(filename)s'
                                    ' multiple times with the same value')
                            if self.version_info >= (1, ):
                                raise BagError(msg % warning_ctx)
                            else:
                                LOGGER.warning(msg, warning_ctx)
                        else:
                            raise BagError(_('%(bag)s: %(algorithm)s manifest lists %(filename)s'
                                             ' multiple times with conflicting values') % warning_ctx)

                    entry_hashes[alg] = entry_hash

            finally:
                if manifest_file:
                    manifest_file.close()
                    manifest_file = None

        self.normalized_manifest_names.update(
            (normalize_unicode(i), i) for i in self.entries.keys()
        )

    def _validate_structure_payload_directory(self):
        if not self._root.fs.isdir("data"):
            raise BagValidationError(_('Expected data directory does not exist in %s') % str(self._root))

    def _validate_structure_tag_files(self):
        # Note: we deviate somewhat from v0.96 of the spec in that it allows
        # other files and directories to be present in the base directory

        if not list(self.manifest_files()):
            raise BagValidationError(_('No manifest files found'))
        if not self._root.relpath("bagit.txt").exists():
            raise BagValidationError(_('Expected %s to contain "bagit.txt"') % self._root)

    def _validate_oxum(self):
        oxum = self.info.get('Payload-Oxum')

        if oxum is None:
            return

        # If multiple Payload-Oxum tags (bad idea)
        # use the first listed in bag-info.txt
        if isinstance(oxum, list):
            LOGGER.warning(_('bag-info.txt defines multiple Payload-Oxum values!'))
            oxum = oxum[0]

        oxum_byte_count, oxum_file_count = oxum.split('.', 1)

        if not oxum_byte_count.isdigit() or not oxum_file_count.isdigit():
            raise BagError(_("Malformed Payload-Oxum value: %s") % oxum)

        oxum_byte_count = int(oxum_byte_count)
        oxum_file_count = int(oxum_file_count)
        total_bytes = 0
        total_files = 0

        for payload_file in self.payload_files():
            info = self._root.fs.getinfo(payload_file, namespaces=['details'])
            total_bytes += info.size
            total_files += 1

        if oxum_file_count != total_files or oxum_byte_count != total_bytes:
            raise BagValidationError(
                _('Payload-Oxum validation failed.'
                  ' Expected %(oxum_file_count)d files and %(oxum_byte_count)d bytes'
                  ' but found %(found_file_count)d files and %(found_byte_count)d bytes') % {
                        'found_file_count': total_files,
                        'found_byte_count': total_bytes,
                        'oxum_file_count': oxum_file_count,
                        'oxum_byte_count': oxum_byte_count,
                    }
            )

    def _validate_entries(self, processes):
        """
        Verify that the actual file contents match the recorded hashes stored in the manifest files
        """
        errors = list()

        if os.name == 'posix':
            worker_init = posix_multiprocessing_worker_initializer
        else:
            worker_init = None

        args = ((self._root,
                 self.normalized_filesystem_names.get(rel_path, rel_path),
                 hashes,
                 self.algorithms) for rel_path, hashes in self.entries.items())

        try:
            if processes == 1:
                hash_results = [_calc_hashes(i) for i in args]
            else:
                try:
                    pool = multiprocessing.Pool(processes if processes else None, initializer=worker_init)
                    hash_results = pool.map(_calc_hashes, args)
                finally:
                    pool.terminate()

        # Any unhandled exceptions are probably fatal
        except:
            LOGGER.exception(_("Unable to calculate file hashes for %s"), self)
            raise

        for rel_path, f_hashes, hashes in hash_results:
            for alg, computed_hash in f_hashes.items():
                stored_hash = hashes[alg]
                if stored_hash.lower() != computed_hash:
                    e = ChecksumMismatch(rel_path, alg, stored_hash.lower(), computed_hash)
                    LOGGER.warning(_unicode(e))
                    errors.append(e)

        if errors:
            raise BagValidationError(_("Bag validation failed"), errors)

    def _validate_bagittxt(self):
        """
        Verify that bagit.txt conforms to specification
        """
        bagit_file_path = self._root.relpath("bagit.txt")

        # Note that we are intentionally opening this file in binary mode so we can confirm
        # that it does not start with the UTF-8 byte-order-mark
        with open_bin_file(bagit_file_path) as bagit_file:
            first_line = bagit_file.read(4)
            if first_line.startswith(codecs.BOM_UTF8):
                raise BagValidationError(_("bagit.txt must not contain a byte-order mark"))

    def _path_is_dangerous(self, path):
        """
        Return true if path looks dangerous, i.e. potentially operates
        outside the bagging directory structure, e.g. ~/.bashrc, ../../../secrets.json,
            \\?\c:\, D:\sys32\cmd.exe
        """
        if fs.path.isabs(path):
            return True
        if os.path.expanduser(path) != path:
            return True
        if os.path.expandvars(path) != path:
            return True

        # possible problem: we're not resolving links.  (Shouldn't happen)
        real_path = os.path.normpath(os.path.join(self.path, path))
        bag_path = os.path.normpath(self.path)
        common = os.path.commonprefix((bag_path, real_path))
        return not (common == bag_path)

def _calc_hashes(args):
    # auto unpacking of sequences illegal in Python3
    (base_path, rel_path, hashes, algorithms) = args
    if not isinstance(base_path, Path):
        return _bagit._calc_hashes(args)
    
    full_path = base_path.relpath(rel_path)

    # Create a clone of the default empty hash objects:
    f_hashers = dict(
        (alg, hashlib.new(alg)) for alg in hashes if alg in algorithms
    )

    try:
        f_hashes = _calculate_file_hashes(full_path, f_hashers)
    except BagValidationError as e:
        f_hashes = dict(
            (alg, _unicode(e)) for alg in f_hashers.keys()
        )

    return rel_path, f_hashes, hashes

def _calculate_file_hashes(full_path, f_hashers):
    """
    Returns a dictionary of (algorithm, hexdigest) values for the provided
    filename
    """
    LOGGER.info(_("Verifying checksum for file %s"), full_path)

    try:
        with open_bin_file(full_path) as f:
            while True:
                block = f.read(HASH_BLOCK_SIZE)
                if not block:
                    break
                for i in f_hashers.values():
                    i.update(block)
    except (OSError, IOError) as e:
        raise BagValidationError(_("Could not read %(filename)s: %(error)s") % {
            'filename': full_path,
            'error': _unicode(e),
        })

    return dict(
        (alg, h.hexdigest()) for alg, h in f_hashers.items()
    )

_ext_fs_lookup = {
    ".zip":      fs.zipfs.ZipFS,
    ".tar":      fs.tarfs.TarFS,
    ".tar.gz":   fs.tarfs.TarFS,
    ".tar.bz2":  fs.tarfs.TarFS,
    ".tgz":      fs.tarfs.TarFS
}

def open_bag(location):
    """
    A factory function for opening a bag; it returns
    a ReadOnlyBag instance opened for a given bag object location.  
    The location string is examined to determine the form of the bag 
    (a directory or some serialized version on a storage device).  
    """
    if not location:
        raise ValueError("open_bag: empty location string")
    location = _unicode(location)

    fspath = None
    name = None
    if '://' in location:
        # FIX: This option is potentially problematic: is it pointing to a (zip) file or a directory?
        # this is an FS URI location string
        name = location.split("/")[-1]
        fspath = Path(fs.open_fs(location), "", location+':')

    elif not os.path.exists(location):
        raise OSError(2, "File not found: "+location)

    elif os.path.isdir(location):
        # it's a unserialized bag on local disk
        name = os.path.basename(location)
        parent = os.path.dirname(location)
        if not parent:
            parent = "."
        fspath = Path(fs.osfs.OSFS(parent), name, "bag:")

    elif os.path.isfile(location):
        # a serialized bag on local disk
        for ext in _ext_fs_lookup.keys():
            if location.endswith(ext):
                label = os.path.basename(location)+':'
                bfs = _ext_fs_lookup[ext](location)
                name = None
                for d in bfs.walk.dirs():
                    if bfs.isfile("/".join([d, "bagit.txt"])):
                        name = d
                        break
                if not name:
                    raise BagError("File does not appear to contain a serialized Bag: "+location)
                fspath = Path(bfs, name, label+name+'/')
                name = name.split("/")[-1]
                break
        if not fspath:
            raise ValueError("open_bag: bag serialization not recognized for "+location)

    if not fspath:
        raise ValueError("open_bag: unsupported bag type/location: "+location)

    return ReadOnlyBag(fspath, name, location)

