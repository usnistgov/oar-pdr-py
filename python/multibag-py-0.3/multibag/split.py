"""
Tools for splitting a single bag (a ProgenitorBag) into a set of 
Multibag-compliant bags.
"""
import os, sys, re, shutil, io
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from copy import deepcopy
from functools import cmp_to_key

from .constants import CURRENT_VERSION as MBAG_VERSION
from .access.bagit import Bag, ReadOnlyBag
from .access.multibag import as_headbag, MissingMultibagFileError
from .access.extended import as_extended, ExtendedReadMixin as ProgenitorMixin
from bagit import _parse_tags

from fs.copy import copy_file
from fs import open_fs

_bagsepre = re.compile(r'/')
_ossepre = re.compile(os.sep)

MBAG_INTERNAL_SENDER_DESC = \
"""This bag is part of a Multibag aggregation. (See Multibag-Reference for a 
   description of the Multibag profile.)  The aggregation was formed by splitting
   the source bag (whose ID is set as the Bag-Group-Identifier) into multibag 
   member bags."""

PMAN = "manifest"
TMAN = "tagmanifest"

if sys.version_info[0] > 2:
    _unicode = str
else:
    _unicode = unicode

def asProgenitor(bag):
    """
    extend the interface on an open Bag instance so that it can be used as 
    a progenitor bag that can be split.  

    :param Bag bag:  an instance of a Bag (including ReadOnlyBag) that is 
                     to be the source of data for a split operation.
    """
    return as_extended(bag)
    
class SplitPlan(object):
    """
    a description of how to distribute the payload and metadata files 
    from a progenitor bag across multiple output multibags.  

    The plan is specific to a given source bag, and when it's complete, it 
    can be applied to the source bag to create the output multibags via 
    the `apply_iter()` function.  
    """

    def __init__(self, source):
        """
        create an empty plan for splitting a given source bag

        :param source:  if a str, it is the path to the source bag (serialized
                        or not); otherwise, a Bag instance representing the 
                        source bag.
        :type str or Bag:
        """
        if isinstance(source, (str, _unicode)):
            if os.path.isfile(source):
                source = ReadOnlyBag(source)
            else:
                # so that we can replicate with hard links when possible
                source = Bag(source)
        if not isinstance(source, Bag):
            raise ValueError("SplitPlan(): source is not a Bag instance")
        if not isinstance(source, ProgenitorMixin):
            source = asProgenitor(source)
        self.progenitor = source
        self._manifests = []
        self.head_version = "1"
        self._deprecates = []

    def manifests(self):
        """
        return an iterator for iterating over the manifest descriptions that 
        prescribe the contents of each output multibag.
        """
        if len(self._manifests) == 0:
            return
        
        i = 0
        while i < len(self._manifests):
            out = deepcopy(self._manifests[i])
            out['ishead'] = (i == len(self._manifests)-1)
            yield out
            i += 1

    def is_complete(self):
        """
        return True if the plan is set to produce output multibags that 
        will include all files from the source bag.  
        """
        try:
            next(self.missing())
            return False
        except StopIteration:
            return True

    def missing(self):
        """
        return an iterator to the paths (relative to the base directory) of the 
        files and directories in the source bag that are not yet included in the 
        plan (i.e. any of the output multibags).  A directory is included in 
        the list only if it appears in the source bag as an empty directory.  
        """
        for file in self.required():
            outbag = self.find_destination(file)
            if not outbag:
                yield file

    def required(self):
        """
        return an iterator to the paths (relative to the base directory) of the
        files and directories in the source bag that should be part of a split 
        plan.  A directory is included in the list only if it appears in the 
        source bag as an empty directory.  
        """
        return self.progenitor.nonstandard()

    def _path_in(self, path, manifest):
        # return true if the path can be found in the given plan manifest
        return path in manifest['contents']

    def find_destination(self, path):
        """
        return the dictionary describing the output multibag that will 
        contain the file (or empty directory) with the given path.  If 
        this plan has not assigned this path to an output file, then 
        None is returned.  If the path is listed for more than one output
        bag, the last one it appears in will be returned.  
        """
        for m in reversed(self._manifests):
            if self._path_in(path, m):
                return m
        return None

    def name_output_bags(self, naming_iter, reverse=False):
        """
        set the names of the output bags using the given naming iterator.  
        A naming iterator is an iterator that emits a sequence of bag names
        that should be given to the output multibag names.  Thse names will
        be attached to the manifests.  
        :param iter naming_iter:   an iterator whose next() method emits a 
                                   sequence of bag names.
        :param bool reverse:       if True, the names should be assigned to 
                                   the manifests in reverse order (i.e. with
                                   the head bag getting the first name in 
                                   the sequence).  
        """
        use = self._manifests
        if reverse:
            use = reversed(use)
        try:
            for m in use:
                name = next(naming_iter)
                self._set_manifest_name(m, name)
        except StopIteration as ex:
            after = (name and (" (after %s)" % name)) or ""
            raise RuntimeError("Naming iterator exhausted prematurely"+after)

    def _set_manifest_name(self, manifest, name):
        manifest['name'] = name
        return manifest

    def complete_plan(self):
        """
        if there are any files missing from the current set of manifests (as 
        determined by missing()), add one or more additional manifests to the 
        plan to capture the missing files.  If no files are missing, this 
        method returns without changing the current plan; thus, there is no 
        harm in calling it.

        This implementation will just create a single additional output manifest 
        if there are any missing files.  Subclasses may split the missing files 
        over multiple additional output bags.  
        """
        man = {
            "name": "{0}_{1}.mbag".format(self.progenitor.name,
                                          len(self._manifests)),
            "contents": list(self.missing())
        }
        size = 0
        for f in man['contents']:
            size += self.progenitor.sizeof(f)
        man['totalsize'] = size
        if man['contents']:
            self._manifests.append(man)

    def apply_iter(self, outdir, naming_iter=None, info_nopass=None,logger=None):
        """
        iteratively apply the plan by writing split bags to a given directory.
        This method returns an iterator that iterates through the plan 
        manifests; with each call to its next() method, it writes out the next
        multibag member bag to the output directory and returns a full path to 
        the member.  Each member bag is given a name set either by default, 
        or as set by a previous call to name_output_bags(), or via a naming 
        iterator provided as an argument to this method.  

        The caller is allowed to modify the newly written bag between calls to
        the iterator--e.g. to serialize it or move it to other storage.  

        :param str outdir: the directory to write the split bags to.  
        :param iter naming_iter:  a naming iterator to use to name the bags.  If
                           None, the output bags will retain their currently set
                           names (which may have been set by default or via 
                           name_output_bags()).  See name_output_bags() for 
                           the requirements of a naming iterator.  
        :param info_nopass:  a list of bag-info metadata names from the source
                           bag that should not be passed to the split bags.
                           If None, all names will be transfered to the output
                           multibags; values for some standard names will be 
                           transformed appropriately.
        :type  info_nopass:  list of str
        :param Logger logger: a logger instance to send messages to.
        :rtype: an iterator whose next() method will write the next
                member bag and returns its output path.  StopIteration
                is raised when there are no more output bags to write.
        """
        if not self.manifests:
            if logger:
                logger.warn("Requested plan execution, but no manifests are set")
            raise RuntimeError("No manifests set for output bags")

        if hasattr(self.progenitor, 'replicate_with_hardlink'):
            self.progenitor.replicate_with_hardlink = True

        if not info_nopass:
            info_nopass = []

        def _write_item(fd, key, vals):
            if not isinstance(vals, list):
                vals = [vals]
            for v in vals:
                fd.write(u"%s: %s\n" % (key,v))
            

        # multibag tag info; initialize from the progenitor bag if the
        # progenitor is a head bag itself
        filedest = OrderedDict()
        memberbags = []
        if self.progenitor.is_head_multibag():
            as_headbag(self.progenitor)
            try:
                memberbags = self.progenitor.member_bag_names
            except MissingMultibagFileError:
                pass
            try:
                filedest = OrderedDict(self.progenitor.iter_file_lookup())
            except MissingMultibagFileError:
                pass
        
        for m in self.manifests():
            bagname = m.get('name')
            if naming_iter:
                try:
                    bagname = naming_iter.next()
                except StopIteration as ex:
                    if logger and not m.get('ishead'):
                        logger.warn("naming iterator ran out of names before "+
                                    "output bags")
                    naming_iter = None
            if bagname in memberbags:
                memberbags.remove(bagname)
            memberbags.append(bagname)

            # create the bag directory and its data subdirectory
            bagdir = os.path.join(outdir, bagname)
            os.mkdir(bagdir)
            os.mkdir(os.path.join(bagdir,"data"))

            # create empty manifest files
            for mf in self.progenitor.manifest_files():
                with open(os.path.join(bagdir, os.path.basename(mf)), 'w') as fd:
                    pass

            self.progenitor.replicate("bagit.txt", bagdir)

            payload_count = 0
            payload_size = 0
            for file in m['contents']:

                if not self.progenitor.exists(file):
                    if logger:
                        logger.warn("Plan for %s calls for non-existent file, %s",
                                    bagname, file)
                    continue

                opath = os.path.join(bagdir, file)
                if self.progenitor.isdir(file):
                    if not os.path.exists(opath):
                        if logger:
                            logger.info("Creating (empty) directory, %s", file)
                        os.makedirs(os.path.join(bagdir, file))

                else:
                    if logger:
                        logger.info("Replicating file, %s", file)

                    # copy the file to the output bag
                    parent = os.path.dirname(opath)
                    if not os.path.exists(parent):
                        os.makedirs(parent)
                    self.progenitor.replicate(file, bagdir)
                    if not os.path.isfile(os.path.join(bagdir,file)):
                        raise RuntimeError("Failed to replicate file in output "+
                                           "bag, "+bagname+": "+file)

                    # copy its hash, if it has one recorded
                    if file.startswith("data"+os.sep):
                        payload_count += 1
                        payload_size += \
                                os.stat(os.path.join(bagdir,file)).st_size
                        hd = self.progenitor.payload_entries()
                        mantype = PMAN
                    else:
                        hd = self.progenitor.tagfile_entries()
                        mantype = TMAN
                    if file in hd:
                        hashes = hd[file]
                        for alg in hashes:
                            self._record_hash(bagdir, alg, hashes[alg],
                                              file, mantype)
                        
                    filedest[file] = bagname
                    
            # create the bag-info file
            outinfo = os.path.join(bagdir, "bag-info.txt")
            with io.open(outinfo, 'w', encoding='utf-8') as fd:
                _write_item(fd, 'Multibag-Version', MBAG_VERSION)
                for name, vals in self.progenitor.info.items():
                    if name.startswith('Multibag-'):
                        continue
                    if name in info_nopass:
                        continue
                    
                    if not isinstance(vals, list):
                        vals = [vals]
                    if name == 'Internal-Sender-Identifier':
                        _write_item(fd, name, bagname)
                        name = 'Multibag-Source-'+name
                    elif name == 'Internal-Sender-Description':
                        _write_item(fd, name, m.get(name,
                                                    MBAG_INTERNAL_SENDER_DESC))
                        name = 'Multibag-Source-'+name
                    elif name == 'External-Identifier':
                        _write_item(fd, 'Multibag-Source-'+name, vals)
                        _write_item(fd, name, vals[0]+'/mbag:'+bagname)
                        name = 'Bag-Group-Identifier'
                    elif name == 'Bag-Size':
                        name = 'Multibag-Source-'+name
                    elif name == 'Payload-Oxum':
                        vals = ["%s.%s" % (payload_size, payload_count)]

                    _write_item(fd, name, vals)

                _write_item(fd, "Multibag-Tag-Directory", "multibag")
                if m.get('ishead'):
                    # this is the head bag!
                    _write_item(fd, "Multibag-Head-Version", self.head_version)
                    if self._deprecates:
                        name = "Multibag-Head-Deprecates"
                        for p in self._deprecates:
                            v = p[0]
                            if len(p) > 1:
                                v += ",%s" % p[1]
                            _write_item(fd, name, v)

            # create the multibag files
            if m.get('ishead'):
                mbagdir = os.path.join(bagdir, "multibag")
                if not os.path.isdir(mbagdir):
                    os.mkdir(mbagdir)

                with io.open(os.path.join(mbagdir, "member-bags.tsv"),
                             'w', encoding='utf-8') as fd:
                    for bag in memberbags:
                        fd.write(u"%s" % bag)
                        fd.write(u'\n')
                with io.open(os.path.join(mbagdir, "file-lookup.tsv"),
                             'w', encoding='utf-8') as fd:
                    for f in filedest:
                        fd.write(u"%s\t%s\n" % (f, filedest[f]))
                with io.open(os.path.join(mbagdir, "aggregation-info.txt"),
                             'w', encoding='utf-8') as fd:
                    with self.progenitor.open_text_file("bag-info.txt") as ifd:
                        for f in ifd:
                            fd.write(f)
                

            yield bagdir


    def _record_hash(self, outdir, alg, hash, path, mantype):
        if mantype not in "manifest tagmanifest".split():
            raise ValueError("Unknown manifest type: "+mantype)

        manfile = "{0}-{1}.txt".format(mantype, alg)
        manpath = os.path.join(outdir, manfile)
        if not os.path.exists(manpath):
            open(manpath, 'w').close()
        with io.open(manpath, 'a', encoding='utf-8') as fd:
            fd.write(hash)
            fd.write(u' ')
            fd.write(path)
            fd.write(u'\n')

class Splitter(object):
    """
    an abstract class for algorithms that can create a SplitPlan given a 
    source bag.  

    A Splitter subclass captures a particular strategy for distributing the 
    files found in a source bag into a set of output multibags.  To create a
    Splitter implementation, one would subclass this abstract base class and 
    over-ride the abstract `_create_plan()` function.  This function captures
    the essential logic of the strategy and uses it to create SplitPlan tailored
    to a given source bag.  
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        """
        initialize the common internal planner data
        """
        pass

    @abstractmethod
    def _create_plan(self, bagpath):
        """
        Create a SplitPlan for the given source bag and apply this splitter's 
        strategy for spliting it into multibags.  Not intended for calling 
        publicly, this function is called by plan() and is intended to contain
        the logic behind the split strategy.  

        :param str bagpath:  the path to the the source bag's root directory
        :rtype:   a SplitPlan instance that describes how the given bag should 
                  be split into multibags.  
        """
        raise NotImplementedError()

    def plan(self, bagpath, namebasis=None):
        """
        Apply this splitter's strategy for spliting a source bag into multibags
        and return the result as a SplitPlan instance.  

        :param str bagpath:  the path to the the source bag's root directory
        :param str|iter namebasis:  a guide for naming the output files.  If 
                             a str, a default naming scheme is applied using 
                             namebasis as a base name.  Otherwise, it is 
                             a naming iterator; see SplitPlan.name_output_bags()
                             for its requirements.
        :rtype:   a SplitPlan that describes how the given bag should 
                  be split into multibags.  
        """
        if not namebasis:
            namebasis = os.path.splitext(os.path.basename(bagpath))[0]

        out = self._create_plan(bagpath)

        # update the output names
        ni = namebasis
        if isinstance(namebasis, (str, _unicode)):
            ni = SimpleNamer(namebasis)
        out.name_output_bags(ni)

        return out

    def split(self, bagpath, outdir, namebasis=None, info_nopass=None,
              logger=None):
        """
        Split the given bag into multibags according to the strategy of this 
        splitter.  

        :param str bagpath:  the path to the the source bag's root directory
        :param str outdir:   the path to a directory where the output multibags
                             should be written
        :param str|iter namebasis:  a guide for naming the output files.  If 
                             a str, a default naming scheme is applied using 
                             namebasis as a base name.  Otherwise, it is 
                             a naming iterator; see SplitPlan.name_output_bags()
                             for its requirements.
        :param info_nopass:  a list of bag-info metadata names from the source
                             bag that should not be passed to the split bags.
                             If None, all names will be transfered to the output
                             multibags; values for some standard names will be 
                             transformed appropriately.
        :type info_nopass:   list of str
        :param Logger logger: a logger instance to send messages to.
        :rtype:  a list of str, the names of the output bags.
        """
        plan = self.plan(bagpath, namebasis)

        out = []
        for mb in plan.apply_iter(outdir, info_nopass=info_nopass,logger=logger):
            ## offer option to serialize

            out.append(mb)

        return out
        

class WellPackedSplitter(Splitter):
    """
    a Splitter that sets a maximum size limit, tries to minimize the total 
    number of bags (each under the size limit).
    """

    def __init__(self, maxsize=60000, targetsize=None, forhead=None):
        """
        Create the splitter based on the "well-packed" algorithm

        :param int maxsize:  the maximum size of an output bag.  No bag will be 
                             bigger than this limit except when a single file
                             exceeds this limit (in this case,  this file will 
                             be placed in its on multibag by itself).
        :param int targetsize:  the preferred size of an output bag.  Bags will 
                             be packed until they just exceed this fize by one
                             file.  The total size will still be kept less than 
                             maxsize. 
        :param list[str] forhead:  a list of file paths that should be reserved 
                             for the head bag.  
        """
        self.maxsz = maxsize
        if not targetsize:
            targetsize = self.maxsz
        self.tsz = targetsize
        if forhead is None:
            forhead = []
        self.forhead = list(forhead)
        for i in range(len(forhead)):
            if not forhead[i].startswith('/'):
                forhead[i] = '/'+forhead[i]
                

    def _create_plan(self, bagpath):
        bag = ReadOnlyBag(bagpath)

        # these files are sorted by size, biggest one first
        finfos = self._sorted_files(bag)
        
        out = SplitPlan(bag)
        self._apply_algorithm(finfos, out)
        out.complete_plan()

        return out

    def _apply_algorithm(self, finfos, plan):
        manf = self._new_manifest()

        i = 0
        while len(finfos) > 0:
            if i >= len(finfos):
                # no more files can be found that will fit in this bag
                plan._manifests.append(manf)
                manf = self._new_manifest()
                i = 0

            newsz = manf['totalsize'] + finfos[i]['size']
            if newsz > self.maxsz:
                if manf['totalsize'] == 0:
                    # this file by itself exceeds our maxsize; put it in
                    # bag by itself.
                    self._add_to_manifest(finfos, i, manf)
                    i = 0
                    plan._manifests.append(manf)
                    manf = self._new_manifest()
                else:
                    # find a smaller one
                    i += 1
            else:
                self._add_to_manifest(finfos, i, manf)
                if newsz > self.tsz:
                    # exceeded our target size; start a new one
                    i = 0
                    plan._manifests.append(manf)
                    manf = self._new_manifest()

        if len(manf['contents']) > 0:
            plan._manifests.append(manf)
        
        return plan

    def _add_to_manifest(self, files, idx, manifest):
        fi = files.pop(idx)
        manifest['contents'].append(fi['path'][1:])
        manifest['totalsize'] += fi['size']

    def _new_manifest(self):
        # return an empty manifest
        return {
            'contents': [],
            'totalsize': 0
        }
        
    _special = [re.compile(r) for r in
                       r"^/bagit.txt$ ^/bag-info.txt$ ^/fetch.txt$".split() +
                       r"^/(tag)?manifest-(\w+).txt$".split()]
    def _is_special(self, filename):
        for r in self._special:
            if r.match(filename):
                return True
        return False

    @staticmethod
    def _cmp_by_size(infoa, infob):
        # we want sort to descend in size
        out = infob['size'] - infoa['size']
        if out != 0:
            return out
        return ((infoa['path'] < infob['path']) and -1) or \
               ((infoa['path'] > infob['path']) and +1) or 0

    def _sorted_files(self, bag):
        finfos = [{"path": p, "size": f.size, "name": p.split('/')[-1]}
                   for p,f in bag._root.fs.walk.info(namespaces=['details'])
                       if not f.is_dir and not self._is_special(p)
                                       and p not in self.forhead]
                          
        finfos.sort(key=cmp_to_key(self._cmp_by_size))
        return finfos

class NeighborlySplitter(WellPackedSplitter):
    """
    a Splitter that sets a maximum size limit, tries to minimize the total 
    number of bags (each under the size limit), and tries to keep files close to
    each other in the hierarchy in the same output bag.  
    """

    def _apply_algorithm(self, finfos, plan):
        manf = self._new_manifest()

        i = 0
        while len(finfos) > 0:
            for dirpath in self._dirpaths_for(finfos, i):

                # fill the open manifest with files in the dirpath directory
                i = self._select_from_dir(finfos, i, manf, dirpath)
            
                if i < 0:
                    # the manifest is full; open a new one
                    break
            
                if i >= len(finfos):
                    # no more files in the current dirpath directory will fit 
                    # in the manifest; start looking for files in the next nearby
                    # directory
                    i = 0
                    continue
                
            # the manifest is full or there are no more files that will fit in
            # this manifest.  
            plan._manifests.append(manf)
            manf = self._new_manifest()
            i = 0

        return plan

    def _dirpath(self, path):
        if path == '/':
            return None
        return path.rsplit('/', 1)[0] + '/'

    def _dirpaths_for(self, finfos, ref):
        refdir = self._dirpath(finfos[ref]['path'])

        # sort directories
        dirs = [self._dirpath(f['path']) for f in finfos]
        dirs.sort()

        # uniquify
        for i in range(len(dirs)-1):
            d = dirs.pop(0)
            if d != dirs[0]:
                dirs.append(d)

        # put descendent directories before anscestors
        i = dirs.index(refdir)
        return dirs[i:] + dirs[:i]

    def _select_from_dir(self, finfos, i, manifest, dirpath):
        if not dirpath.endswith('/'):
            dirpath += '/'

        while i < len(finfos):
            if dirpath == self._dirpath(finfos[i]['path']):
                
                newsz = manifest['totalsize'] + finfos[i]['size']
                if newsz > self.maxsz:
                    if manifest['totalsize'] == 0:
                        # this file by itself exceeds our maxsize; put it in
                        # a bag by itself.
                        self._add_to_manifest(finfos, i, manifest)
                        return -1
                    else:
                        # find a smaller file to put in manifest
                        i += 1

                else:
                    self._add_to_manifest(finfos, i, manifest)
                    if newsz > self.tsz:
                        # exceeded our target size; start a new one
                        return -1
            else:
                i += 1
                    
        return i


class SimpleNamer(object):
    """
    A simple naming iterator that creates names for output multibags made up 
    of a given base name and a sequence number.
    """
    def __init__(self, base):
        self.base = base
        self.sn = 0

    def __iter__(self):
        return self

    def __next__(self):
        self.sn += 1
        return "{0}_{1}.mbag".format(self.base, self.sn)

    def next(self):
        return self.__next__()

