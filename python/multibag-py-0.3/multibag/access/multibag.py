"""
This module provides access to the informational content of a multibag
aggregation.  In particular, it provides the read-only HeadBag class.
"""
from __future__ import absolute_import
import re, os, sys
from collections import OrderedDict

from .bagit import Bag, ReadOnlyBag, open_bag
from .exceptions import MultibagError, MissingMultibagFileError
from .extended import (ExtendedReadMixin, ExtendedReadOnlyBag,
                       ExtendedReadWritableBag, as_extended)
from ..constants import CURRENT_VERSION, CURRENT_REFERENCE, Version

if sys.version_info[0] > 2:
    _unicode = str
else:
    _unicode = unicode
_spre = re.compile(r' +')

_about_mbag_morsel = "complies with the Multibag BagIt profile"
ABOUT_MBAG = "This bag {0}.  For more information, refer to the URL given by Multibag-Reference tag." \
             .format(_about_mbag_morsel)

class MemberInfo(object):
    """
    a description of a member bag of a multibag aggregation as given by a 
    line in a member-bags.tsv file.
    """

    def __init__(self, name, uri=None, comment=None, *info):
        """
        initialize the information about a member bag
        :param str name:   the name of the member bag
        :param str uri:    a URI for the member bag, providing a global identity
        :param str comment:  a comment about the bag entry
        :param list info:  extra profile-specific metadata about the 
        
        """
        if info is None:
            info = []
        self.name = name
        self.uri = uri
        self.comment = comment
        self.info = list(info)

    def format(self):
        """
        format the contents of this object into a string that can be written
        to the member-bags.tsv file.  This is the reverse of parse_line_*().  
        The output will contain a trailing newline character.
        """
        out = self.name
        if self.uri:
            out += "\t"+self.uri
        if self.info:
            out += "\t"+"\t".join(info)
        if self.comment:
            out += "\t# " + self.comment
        out += "\n"
        return out

    @classmethod
    def parse_line_03(self, line):
        """
        parse a line from a member-bags.tsv file according to the multibag
        profile version 0.4, returning a MemberInfo object
        """
        fields = line.strip().split('\t')
        if len(fields) < 1 or not fields[0]:
            raise MultibagError("Multibag syntax error in member-bags.tsv: "+
                                "missing fields: " + line.strip())
        name = fields.pop(0)
        comm = None
        uri = None
        if len(fields) > 0:
            if fields[-1].startswith('#'):
                comm = fields.pop(-1).lstrip('#').strip()
        if len(fields) > 0:
            uri = fields.pop(0)
        
        return MemberInfo(name, uri, comm, *fields)

    @classmethod
    def parse_line_02(self, line):
        """
        parse a line from a member-bags.tsv file according to the multibag
        profile version 0.4, returning a MemberInfo object
        """
        fields = _spre.split(line.strip())
        if len(fields) < 1 or not fields[0]:
            raise MultibagError("Multibag syntax error in group-members.txt: "+
                                "missing fields: " + line.strip())
                                
        name = fields.pop(0)
        uri = None
        if len(fields) > 0:
            uri = fields.pop(0)
        
        return MemberInfo(name, uri)

def parse_file_lookup_line_03(line):
    """
    parse a line from the file_lookup.tsv file according to version 0.3 of 
    the multibag profile specification

    :rtype:  a 2-tuple containing the bag file path and the name of the 
             bag containing that file.  
    """
    out = line.strip().split('\t')
    if len(out) < 2:
        raise MultibagError("Multibag syntax error in file-lookup.tsv: "+
                            "missing bagname field: " + line.strip())
    return (out[0], out[1])
        
def parse_group_directory_line(line):
    """
    parse a line from the file_lookup.tsv file according to version 0.3 of 
    the multibag profile specification

    :rtype:  a 2-tuple containing the bag file path and the name of the 
             bag containing that file.  
    """
    out = _spre.split(line.strip())
    if len(out) < 2:
        raise MultibagError("Multibag syntax error in file-lookup.tsv: "+
                            "missing bagname field: " + line.strip())
    return (out[0], out[1])

def parse_deleted_line_04(line):
    """
    parse a line from the deleted.txt file according to version 0.4 of the
    multibag profile specification

    :rtype: str giving the path to a file that should be considered deleted 
            from the aggregation
    """
    return line.strip()
    

class HeadBagReadMixin(ExtendedReadMixin):
    """
    an interface to the informational content in the head bag of a multibag 
    aggregation.
    """

    def __init__(self):
        """
        Initialize the extended interface.  
        """
        # subclasses are expected to initialize the ExtendedReadMixin separately;
        # if they do not, NotImplementedError exceptions will be raised when
        # calling certain methods.
        self._memberbags = None
        self._filelu = None
        self._deleted = None

    @property
    def head_version(self):
        """
        the version of the aggregation described by this head bag (i.e. the 
        value of the Multibag-Head-Version info tag).
        """
        return self._get_required_info_item('Multibag-Head-Version')

    @property
    def profile_version(self):
        """
        the version of the multbag profile specification that this head bag 
        claims to adhere to.  If the bag does not yet specify its profile version,
        the latest supported version will be assumed and returned.  
        """
        try:
            return self._get_required_info_item('Multibag-Version')
        except MultibagError as ex:
            return CURRENT_VERSION

    def _get_required_info_item(self, name):
        try:
            out = self.info[name]
            if isinstance(out, list):
                out = out[-1]
            return out
        except KeyError:
            raise MultibagError("Missing required '" + name + "' info tag")
        except IndexError:
            raise MultibagError("Empty value for '" + name + "' info tag")

    @property
    def multibag_tag_dir(self):
        defval = 'multibag'
        out = self.info.get('Multibag-Tag-Directory', defval)
        if isinstance(out, list):
            if len(out) == 0:
                out = defval
            else:
                out = out[-1]
        return out

    def iter_member_bags(self):
        """
        iterate through the contents of the multibag/member_bags.tsv file.  This
        opens the file anew and returns the contents via an iterator that returns
        MemberInfo instances.
        """
        mbdir = self.info.get('Multibag-Tag-Directory', 'multibag')
        vers = Version(self.profile_version)
        membagfile = (vers < "0.3" and "group-members.txt") \
                      or "member-bags.tsv"
        membagpath = "/".join([mbdir, membagfile])
        
        if not self.isfile(membagpath):
            raise MissingMultibagFileError(membagfile)

        parseline = (vers < "0.3" and MemberInfo.parse_line_02) \
                    or MemberInfo.parse_line_03

        with self.open_text_file(membagpath) as fd:
            for line in fd:
                if line.strip():
                    yield parseline(line)

    def member_bags(self, reread=False):
        """
        return the ordered list of bags that make up this multibag aggregation
        as a list of MemberInfo objects

        :param bool reread:  if True, re-read the contents of the member-bags.tsv
                             file; otherwise, return the cached values.
        """
        if reread or self._memberbags is None:
            self._memberbags = [m for m in self.iter_member_bags()]
        return self._memberbags

    @property
    def member_bag_names(self):
        """
        the ordered list of bag names that make up this multibag aggregation.
        """
        return [m.name for m in self.member_bags()]

    def iter_file_lookup(self):
        """
        iterate through the contents of the multibag/file-lookup.tsv
        """
        mbdir = self.info.get('Multibag-Tag-Directory', 'multibag')
        vers = Version(self.profile_version)
        membagfile = (vers < "0.3" and "group-directory.txt") \
                      or "file-lookup.tsv"
        membagpath = "/".join([mbdir, membagfile])
        
        if not self.isfile(membagpath):
            raise MissingMultibagFileError(membagfile)

        parseline = (vers < "0.3" and parse_group_directory_line) \
                    or parse_file_lookup_line_03

        with self.open_text_file(membagpath) as fd:
            for line in fd:
                if line.strip():
                    yield parseline(line)

    def _cache_file_lookup(self):
        self._filelu = OrderedDict(self.iter_file_lookup())

    def lookup_file(self, filepath, reread=False):
        """
        return the name of the bag containing the given file 

        :param str  filepath:  the bag-root-relative path to the desired file
        :param bool reread:    If True, re-cache the lookup table from the 
                               contents of the multibag/file-lookup.tsv file.
        """
        if reread or self._filelu is None:
            self._cache_file_lookup()
        return self._filelu.get(filepath)

    def files_in_member(self, bagname):
        """
        return a list of files in the file lookup that are registered as being
        in the given named member bag.  An empty list if the bag is not a member
        or otherwise has no files registered with it.  
        """
        out = []
        if self._filelu is None:
            try:
                self._cache_file_lookup()
            except MissingMultibagFileError:
                return out

        for file, member in self._filelu.items():
            if member == bagname:
                out.append(file)
        return out

    def iter_deleted(self):
        """
        iterate through the file paths listed in the multibag/deleted.txt file.
        """
        parseline = parse_deleted_line_04
        delfile = "/".join([self.multibag_tag_dir, 'deleted.txt'])
        if not self.exists(delfile):
            return
        
        with self.open_text_file(delfile) as fd:
            for line in fd:
                if line.strip():
                    yield parseline(line)

    def deleted_paths(self, reread=False):
        """
        return the file paths that should be deleted when the multibag 
        aggregation is recombined into a single bag.
        """
        if reread or self._deleted is None:
            self._deleted = set(self.iter_deleted())
        return list(self._deleted)

class HeadBagUpdateMixin(HeadBagReadMixin):
    """
    an interface for updating the informational content in a head bag of a 
    multibag aggregation.
    """

    def __init__(self):
        """
        Initialize the extended interface.  
        """
        # subclasses are expected to initialize the ExtendedReadMixin separately;
        # if they do not, NotImplementedError exceptions will be raised when
        # calling the methods.
        super(HeadBagUpdateMixin, self).__init__()

    def set_multibag_tag_dir(self, dirname, migrate=False):
        """
        set or change the name of the multibag tag directory.  While setting 
        the name is currently allowed by the multibag specification, changing
        it from its default, "multibag", is not recommended.

        :param str dirname:  the path to the directory, relative to the bag's 
                             root directory, that contains the multibag tag 
                             files.  If this directory is not just below the 
                             root directory, the path should be delimited with 
                             a forward slash, '/'.
        :param bool migrate: If True and there exists a multibag tag directory 
                             already, rename that directory accordingly.
        """
        dirname = os.path.join(*(dirname.split('/')))
        if dirname == self.multibag_tag_dir:
            return

        if migrate:
            tagpath = os.path.join(self._bagdir, self.multibag_tag_dir)
            dest = os.path.join(self._bagdir, dirname)
            if os.path.exists(dest):
                raise RuntimeError("Unable to migrate (tag dir not updated): " +
                                   dirname + " already exists")
            if os.path.exists(tagpath):
                os.renames(tagpath, dest)

        self.info['Multibag-Tag-Directory'] = dirname

    def member_bags(self, reread=False):
        """
        return the ordered list of bags that make up this multibag aggregation
        as a list of MemberInfo objects.  

        Note that if reread=True, any unsaved updates added via add_member_bag() 
        or set_member_bags() will be lost.

        :param bool reread:  if True, re-read the contents of the member-bags.tsv
                             file; otherwise, return the values saved in memory.
        """
        try:
            return super(HeadBagUpdateMixin, self).member_bags(reread)
        except MissingMultibagFileError:
            return []

    def add_member_bag(self, name, uri=None, comment=None, info=None,
                       override=True):
        """
        add a new member bag to the aggregation

        :param str name:    the name of the member bag
        :param str uri:     an optional PID for the member bag
        :param str comment: an optional text comment to associate with the bag
        :param info:        an list of other string data to associate with the bag
        :type info:  list of str
        :param bool override:  if the member bag is already listed as a member,
                            remove it and its associated information from the 
                            member bag list before re-adding it.  Default: True
        """
        if info is None:
            info = []
        if self._memberbags is None:
            self.member_bags()
        if self._memberbags is None:
            self._memberbags = []

        if override:
            self.remove_member_bag(name)

        self._memberbags.append(MemberInfo(name, uri, comment, *info))

    def set_member_bags(self, meminfos):
        """
        completely replace the list of member bags with the given list.
        :param meminfos:   the list of member bags as MemberInfo objects.
        :type  meminfos:   list of MemberInfo objects
        """
        mis = list(meminfos)
        if not all([isinstance(m, MemberInfo) for m in mis]):
            raise TypeError("set_member_bags(): Arg not a list of MemberInfo "+
                            "objects")
        self._memberbags = mis

    def ensure_tagdir(self):
        """
        make sure the tag directory for the special multibag files exists
        """
        mbdir = os.path.join(self._bagdir, self.multibag_tag_dir)
        if not os.path.exists(mbdir):
            os.mkdir(mbdir)

    def save_member_bags(self):
        """
        if it appears that the member bag information has been updated, save 
        those changes to the tag file.  
        """
        if self._memberbags is None:
            return

        self.ensure_tagdir()
        tagfile = os.path.join(self._bagdir, self.multibag_tag_dir,
                               'member-bags.tsv')
        with open(tagfile, 'w') as fd:
            for mi in self._memberbags:
                fd.write(mi.format())

    def lookup_file(self, filepath, reread=False):
        """
        return the name of the bag containing the given file 

        :param str  filepath:  the bag-root-relative path to the desired file
        :param bool reread:    If True, re-cache the lookup table from the 
                               contents of the multibag/file-lookup.tsv file.
        """
        try:
            return super(HeadBagUpdateMixin, self).lookup_file(filepath, reread)
        except MissingMultibagFileError:
            return None

    def add_file_lookup(self, filepath, bagname):
        """
        add a mapping for locating a file in a member bag
        :param str filepath:  the path to the file relative the bag's root 
                              directory
        :param str bagname:   the name of the bag where the file can be found.
        """
        if not filepath or not bagname:
            raise ValueError("input arguments cannot be empty/None: " +
                             str( (filepath, bagname,) ))
        if not isinstance(filepath, (str, _unicode)) or \
           not isinstance(bagname, (str, _unicode)):
            raise TypeError("input arguments must be strings: " +
                            str( (filepath, bagname,) ))
        if self._filelu is None:
            try:
                self._cache_file_lookup()
            except MissingMultibagFileError:
                self._filelu = OrderedDict()
        self._filelu[filepath] = bagname

    def remove_file_lookup(self, filepath):
        """
        remove the given file path from the file lookup.  No exception is 
        raised if the file path is not currently registered.
        """
        if not filepath:
            raise ValueError("remove_file_lookup(): empty filepath argument")
        if self._filelu is None:
            try:
                self._cache_file_lookup()
            except MissingMultibagFileError:
                return
        if filepath in self._filelu:
            del self._filelu[filepath]

    def remove_member_bag(self, bagname):
        """
        remove the bag with the given name as a member of the aggregation.
        This bot removes the name from the member bag list and removes all
        entries pointing to it in the file lookup map.  Any files removed
        from the lookup map are also removed from the deleted list.

        Note that for this removal to be cached, all three save methods--
        save_member_bags(), save_file_lookup(), and save_deleted()--must be
        subsequently called; they are not called internally by this method.
        """
        if self._memberbags is None:
            self.member_bags

        if self._memberbags:
            idxs = [i for i in range(len(self._memberbags))
                      if self._memberbags[i].name == bagname]
            for i in reversed(idxs):
                del self._memberbags[i]

        files = self.files_in_member(bagname)
        for f in files:
            self.unset_deleted(f)
            self.remove_file_lookup(f)
        

    def update_for_member(self, bag, include=None, exclude=None, 
                          make_member=True, member_name=None):
        """
        make the given bag a member of the multibag aggregation.  

        By default, this method will add the given bag to the list of member 
        bags (unless make_member=False).  The file lookup information for this 
        head bag will be updated according to the include and exclude parameters;
        a file must exist in the bag for it to be added.

        Note that when make_member=True (default), the order in which this 
        method is called is significant: it affects the order in which member 
        bags are combined to create the aggregated bag.  

        :param Bag bag:       the bag to be added (or, if make_member=False, 
                              have files from added to the file lookup 
                              information)
        :param list include:  a list of directories or files that should be 
                              included.  If not provided, a default value of 
                              ['data'] will be assumed.  Each element must be 
                              a path relative to the bag's root directory.  If 
                              an entry is a directory, all files under that 
                              directory (recursively) will be included except
                              any whose path matches an entry in the exclude
                              list parameter.  
        :param list exclude:  a list of files or directories to exclude.  The
                              include list will over-ride those listed in this
                              list, unless the corresponding include entry is a
                              directory.
        :param bool make_member:  If False, the bag will _not_ be added to 
                              the member-bags list; only the file lookup 
                              information will be updated.  Set this to False,
                              when calling add_member_bag separately (e.g. to 
                              provide additional member info). 
        :param str member_name:  The name to use for the name of the member 
                              bag.  When provided, this value over-rides the 
                              apparent name of the bag.  
        """
        as_extended(bag)
        if not member_name:
            member_name = bag.name

        if make_member:
            self.add_member_bag(member_name)

        # find the files to add to the lookup map
        if include is None:
            include = ['data']
        elif isinstance(include, (str,_unicode)):
            include = [include]
        elif not isinstance(include, list):
            raise TypeError("update_for_member(): include not a list: "+
                            str(include))
        if exclude is None:
            exclude = []
        elif isinstance(exclude, (str,_unicode)):
            include = [exclude]
        elif not isinstance(exclude, list):
            raise TypeError("update_for_member(): exclude not a list: "+
                            str(exclude))

        for incl in include:
            # skip this include item if it explicitly excluded
            if incl in exclude:
                continue
            
            if bag.isfile(incl):
                self.add_file_lookup(incl, member_name)

            elif bag.isdir(incl):
                for d, subdirs, files in bag.walk(incl):
                    if d and d in exclude:
                        for i in range(len(subdirs)):
                            subdirs.pop(0)
                        continue
                    for f in files:
                        f = "/".join([d, f])
                        if f in exclude:
                            continue
                        self.add_file_lookup(f, member_name)

            # else does not exist in bag; skip it

    def save_file_lookup(self):
        """
        if it appears that the file lookup information has been updated, save 
        those changes to the tag file.  
        """
        if self._filelu is None:
            return

        self.ensure_tagdir()
        tagfile = os.path.join(self._bagdir, self.multibag_tag_dir,
                               'file-lookup.tsv')
        with open(tagfile, 'w') as fd:
            for item in self._filelu.items():
                fd.write("{0}\t{1}\n".format(item[0], item[1]))

    def clear_file_lookup(self):
        """
        empty the file lookup tag file and clear all entries from memory
        """
        tagfile = os.path.join(self._bagdir, self.multibag_tag_dir,
                               'file-lookup.tsv')
        if os.path.exists(tagfile):
            os.remove(tagfile)
        self._filelu = None
        

    def set_deleted(self, filepath):
        """
        register the given filepath as deleted from the multibag aggregation.

        :param str filepath:  the path to the file, relative to the bag's root
                              directory, that should be considered deleted.  The
                              path should be delimited by a forward slash ('/'),
                              regardless of the platform.
        """
        if self._deleted is None:
            self.deleted_paths
        if self._deleted is None:
            self._deleted = set()
        self._deleted.add(filepath)

    def unset_deleted(self, filepath):
        """
        unregister the given filepath as being marked as deleted from the 
        multibag aggregation.

        :param str filepath:  the path to the file, relative to the bag's root
                              directory, that should be considered deleted.  The
                              path should be delimited by a forward slash ('/'),
                              regardless of the platform.
        """
        if self._deleted is None:
            self.deleted_paths
        if self._deleted is None:
            self._deleted = set()
        if filepath in self._deleted:
            self._deleted.remove(filepath)
        

    def save_deleted(self):
        """
        if any files have been registered as deleted, save the list to its
        tag file.
        """
        if self._deleted is None:
            return
        
        tagfile = os.path.join(self._bagdir, self.multibag_tag_dir,
                               'deleted.txt')
        if not self._deleted and os.path.exists(tagfile):
            os.remove(tagfile)

        self.ensure_tagdir()
        with open(tagfile, 'w') as fd:
            for path in self._deleted:
                fd.write(path + "\n")

    def update_info(self, version=None, profver=CURRENT_VERSION):
        """
        update the bag info tag data to include head bag information.

        :param str version:  a version string to set as the version of the 
                             aggregation.  If not given, the currently set
                             value of 'Multibag-Head-Version' (or "1", if 
                             not set) will be saved.
        """
        if version:
            self.info['Multibag-Head-Version'] = version

        self.info['Multibag-Version'] = profver
        self.info.setdefault('Multibag-Tag-Directory', 'multibag')
        self.info.setdefault('Multibag-Head-Version', '1')
        self.info['Multibag-Reference'] = CURRENT_REFERENCE

        if 'Internal-Sender-Description' in self.info:
            if not isinstance(self.info['Internal-Sender-Description'], list):
                self.info['Internal-Sender-Description'] = \
                                [ self.info['Internal-Sender-Description'] ]
            if not any([(_about_mbag_morsel in p) for p in self.info['Internal-Sender-Description']]):
                self.info['Internal-Sender-Description'].append( ABOUT_MBAG )
        else:
            self.info['Internal-Sender-Description'] = ABOUT_MBAG

        if 'Bag-Count' in self.info:
            del self.info['Bag-Count']
        if 'Bag-Size' in self.info:
            del self.info['Bag-Size']
        Bag.save(self)
        self.info['Bag-Size'] = self._bag_size()
        Bag.save(self)

    def _bag_size(self):
        size = 0
        for root, subdirs, files in os.walk(self._bagdir):
            for f in files:
                size += os.stat(os.path.join(root,f)).st_size

        out = self._format_bytes(size)
        size += len("Bag-Size: {0}".format(size))
        return self._format_bytes(size)

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
        nbytes = "{0:.3f}".format(round(nbytes, 3) * 10**ordr)
        if '.' in nbytes:
            nbytes = re.sub(r"0+$", "", nbytes)
        if nbytes.endswith('.'):
            nbytes = nbytes[:-1]    
        return "{0} {1}B".format(nbytes, pref)


class ReadOnlyHeadBag(ExtendedReadOnlyBag, HeadBagReadMixin):
    """
    A representation of head bag for a multibag aggregation, opened in 
    a read-only state.  

    The underlying bag may be in a serialized form.  To open a serialized bag, 
    the factory function open_headbag() is recommended instead of instantiating 
    this class directly.
    """

    def __init__(self, bagpath, name=None):
        """
        open the bag with the given location
        :param bagpath:  either a Path instance or a filepath to the bag's 
                         root directory.  A Path instance must be used if the 
                         bag is in a serialized form.  
        :type bagpath:   str or Path
        """
        super(ReadOnlyHeadBag, self).__init__(bagpath, name)
        HeadBagReadMixin.__init__(self)

class _NoUpdateHeadBag(ExtendedReadWritableBag, HeadBagReadMixin):
    """
    A representation of head bag for a multibag aggregation, opened in 
    a un-updateable state.  

    This class is usually not directly instantiated by the user.  It is 
    provided to add only the head bag read interface to a bag that is 
    otherwise updateable.
    """

    def __init__(self, bagpath):
        """
        open the bag with the given root directory

        :param str bagpath:  a filepath to the bag's root directory.  
        """
        super(HeadBag, self).__init__(bagpath)
        HeadBagReadMixin.__init__(self)

    
class HeadBag(ExtendedReadWritableBag, HeadBagUpdateMixin):
    """
    A representation of head bag for a multibag aggregation, opened in 
    a updateable state.  

    The underlying bag can not be in a serialized form as such is supported 
    only in a read-only state.  To open a serialized bag, use the 
    the factory function open_headbag().
    """

    def __init__(self, bagpath):
        """
        open the bag with the given root directory

        :param str bagpath:  a filepath to the bag's root directory.  
        """
        if not os.path.isdir(bagpath):
            raise ValueError("HeadBag(): bagpath must point to a directory (for write access)")
        super(HeadBag, self).__init__(bagpath)
        HeadBagReadMixin.__init__(self)
        HeadBagUpdateMixin.__init__(self)

def is_headbag(bagpath):
    """
    return True if this bag is a designated as the head bag of a multibag
    aggregation.  This implementation returns True if the 
    'Multibag-Head-Version' is set.  

    :param str bagpath:  the path to the bag, either as a serialized file or 
                         as the root directory.
    """
    return 'Multibag-Head-Version' in open_bag(bagpath).info

def as_headbag(bag, readonly=False):
    """
    extend the interface of the given bag instance with methods for managing
    head bags.  The input bag instance is returned with its class updated.  

    Note that it is not an error to apply this method to a bag (serialized 
    or otherwise) that is not designated as such according to the Multibag 
    profile spec.  In such a case, head bag read methods may raise a 
    MultibagError.

    :param Bag bag:        the Bag instance to extend
    :param bool readonly:  do not include methods for updating the head bag
                           data.  Note that if the input bag is of type 
                           ReadOnlyBag (e.g. it is in a serialized form), 
                           the update methods will not be included.
    """
    if not isinstance(bag, Bag):
        raise ValueError("as_headbag(): input not of type Bag: " + str(bag))

    if not isinstance(bag, ExtendedReadMixin):
        as_extended(bag)

    if not isinstance(bag, HeadBagReadMixin):
        if isinstance(bag, ExtendedReadOnlyBag):
            bag.__class__ = ReadOnlyHeadBag
        else:
            # isinstance(bag, ExtendedReadWritableBag) == True
            bag.__class__ = _NoUpdateHeadBag
        HeadBagReadMixin.__init__(bag)

    if not isinstance(bag, HeadBagUpdateMixin) and not readonly and \
       not isinstance(bag, ExtendedReadOnlyBag):
        bag.__class__ = HeadBag
        HeadBagUpdateMixin.__init__(bag)

    return bag

def open_headbag(location, readonly=False):
    """
    open the head bag of a multibag aggregation.  

    Note that it is not an error to apply this method to a bag (serialized 
    or otherwise) that is not designated as such according to the Multibag 
    profile spec.  In such a case, head bag read methods may raise a 
    MultibagError.  If the bag is not opened readonly, the update methods 
    can be used to bring it into compliance as a head bag.

    :param str location:   the path to the head bag, either as a serialized 
                           file or as its root directory
    :param bool readonly:  do not include methods for updating the head bag
                           data.  Note that if the input bag is serialized
                           or referenced via a URL, the update methods will
                           not be included.
    """
    if readonly or "://" in location or os.path.isfile(location):
        out = open_bag(location)

    else:
        out = Bag(location)

    return as_headbag(out)
