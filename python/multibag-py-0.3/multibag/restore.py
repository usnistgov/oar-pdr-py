"""
functions for restoring a bag from its multibag components.
"""
import os, sys, re, shutil, io, time, errno
from collections import OrderedDict

from .constants import CURRENT_VERSION as MBAG_VERSION
from .access.bagit import Bag, ReadOnlyBag
from .access.multibag import (is_headbag, as_headbag, open_headbag, MissingMultibagFileError,
                              HeadBag, ReadOnlyHeadBag, MultibagError, ExtendedReadWritableBag)
from .access.extended import as_extended, ExtendedReadMixin as ProgenitorMixin
from .access.bagit import _ext_fs_lookup, open_bag, Bag

class _FileNotFoundError(OSError):
    def __init__(self, message):
        super(_FileNotFoundError, self).__init__(message)
        self.errno = errno.ENOENT

if sys.version_info[0] > 2:
    _unicode = str
else:
    _unicode = unicode
    FileNotFoundError = _FileNotFoundError 

serialized_extensions = set(_ext_fs_lookup.keys())

class BagRestorer(object):
    """
    A class for managing the restoration of a complete bag from its multibag components.  
    The :py:meth:`restore` method can be used to restore the full bag in one shot, 
    following the multibag metadata from a head bag; however, the other method allow more 
    fine-control over the restoration.

    This class does not implement the bag aggregation specification exactly but rather an
    equivalent alternative algorithm.  The specification outlines a method in which each 
    member bag is copied into the destination bag in the order that they appear in the 
    `member-bags.tsv` tag file.  This implementation copies the member bags in _reverse_ 
    order, copying only those files that are not already in the destination bag.  The 
    latter algorithm will by faster in generla as it avoids superfluous copying of files 
    that will ultimately get overwritten by bags later in the list.  
    """

    def __init__(self, headbag, destbag=None, compdir=None, fetcher=None):
        """
        create the restorer.
        :param headbag:     the head bag for the bag to restore.  If the input is a 
                            ReadOnlyHeadBag, the destbag parameter must be specified.
                            :type headbag: str, HeadBag, or ReadOnlyHeadBag
        :param destbag:     the destination for the restore bag.  Normally, this would 
                            not exist yet at construction time, but providing an existing 
                            bag will cause that bag to be filled out to create the complete 
                            bag.  Not providing this will cause the headbag to be updated
                            in place.  
                            :type destbag: str or Bag
        :param compdir:     a directory where component multibags can be found (or cached to
                            when fetcher is set)
                            :type compdir: str
        :param fetcher:     a function that can fetch a remote multibag and which must two 
                            arguments.  The first argument is the name of the desired 
                            component multibag, as given in the member-bags.tsv file.  The 
                            second is the destination directory where the bag should be 
                            cached; this will be set to the value of compdir, if set.  The 
                            function must return the path to the cached bag.  
        """
        if isinstance(headbag, (str, _unicode)):
            if not os.path.exists(headbag):
                raise FileNotFoundError("Missing head bag: "+headbag)
            if not is_headbag(headbag):
                raise MultibagError("Not a head bag")
            headbag = open_headbag(headbag)

        self._head = headbag
        self._inplace = False

        if not destbag:
            destbag = self._head
            self._inplace = True

        if isinstance(destbag, Bag):
            destbag = destbag.path
        destbag = os.path.abspath(destbag)
        parent = os.path.dirname(destbag)
        if not os.path.isdir(parent):
            raise FileNotFoundError("Parent of destination bag directory does not exist as a directory")
        self._destdir = destbag

        if not self._inplace:
            self._inplace = destbag == os.path.abspath(self._head.path)

        self._fetcher = fetcher
        if not compdir:
            if self._fetcher:
                # set a default cached directory inside the target destination bag
                compdir = os.path.join(self._destdir, "multibag", "_membercache")
            else:
                compdir = os.path.dirname(os.path.abspath(self._head.path))
        self._cachedir = compdir

    @property
    def destination_bagdir(self):
        return self._destdir

    @property
    def head_bag(self):
        return self._head

    @property
    def cache_dir(self):
        return self._cachedir

    def _create_dest_bag(self):
        if not os.path.exists(self._destdir):
            os.mkdir(self._destdir)
        if not os.path.exists(self._cachedir) and self._cachedir.startswith(self._destdir):
            # if the cache dir is located inside the destination bag, create that dir, too
            os.makedirs(self._cachedir)

    def find_member_bag(self, bagname):
        """
        Look for the member bag in the cache directory and return its path.  The member bag 
        may be in a serialized form.  None is returned if no usable form of the bag can be found
        """
        # does the bag exist in the cache as a directory?
        bagdir = os.path.join(self._cachedir, bagname)
        if os.path.isdir(bagdir):
            return bagdir

        # look for a supported serialized version of the bag in the cache
        sers = [f for f in [bagdir+e for e in serialized_extensions] if os.path.isfile(f)]
        if sers:
            return sers[0]
        return None

    def _fetch_member_bag(self, bagname):
        if not self._fetcher:
            return None
        if not os.path.exists(self._cachedir) and self._cachedir.startswith(self._destdir):
            self._create_dest_bag()
        return self._fetcher(bagname, self._cachedir)

    def get_member_bag(self, bagname):
        """
        return the path to a readable bag matching the given bagname.  
        """
        out = self.find_member_bag(bagname)
        if not out:
            out = self._fetch_member_bag(bagname)
        return out

    def restore_member(self, bagname, skip=None, fetch=None, overwrite=False):
        """
        copy files from the specified member bag into the destination bag according to the rules 
        for multibag restoration.  In particular, only files from the member bag not present in the
        destination bag will be copied (unless overwrite=True).  
        :param str bagname:  the name of the bag to restore into the destination bag.  This name is 
                             expected to be of a form as given in the member-bags.tsv file.
        :param skip:         a list of paths in that bag to not restore; this is intended to hold 
                             a list of files that have been marked as deleted from an aggregation.
        """
        src = self.get_member_bag(bagname)
        if not src:
            raise FileNotFoundError("Member bag not found: "+bagname)
        self._restore_from(src, skip, overwrite)

        if fetch is not None:
            membag = open_bag(src)
            if membag.isfile("fetch.txt"):
                with membag.open_text_file("fetch.txt") as fd:
                    for line in fd:
                        filen = line.strip().rsplit(None, 1)[0]
                        if filen not in fetch:
                            fetch[filen] = line

    def _restore_from(self, srcbag, skip=None, overwrite=False):
        if os.path.abspath(srcbag) == self._destdir:
            # srcbag is destination bag
            return
        if not os.path.exists(self._destdir):
            self._create_dest_bag()
        if skip is None:
            skip = []
        updated = []

        srcbag = as_extended(open_bag(srcbag))
        for root, dirs, files in srcbag.walk():
            if root in skip or len([f for f in skip if root.startswith(f+'/')]) > 0:
                # if this root directory has been marked as deleted, skip it
                continue

            rmtime = os.stat(os.path.join(self._destdir, root)).st_mtime

            for f in files:
                if os.path.join(root, f) in skip:
                    continue
                destf = os.path.join(self._destdir, root, f)
                if overwrite and os.path.exists(destf):
                    os.remove(destf)
                if not os.path.exists(destf):
                    srcpath = "/".join([root, f])
                    srcbag.replicate(srcpath, self._destdir)
                    updated.append(srcpath)

                    # try to copy the modification time
                    times = srcbag.timesfor(srcpath)
                    if times:
                        os.utime(destf, (time.time(), times.mtime))

            for f in dirs:
                if os.path.join(root, f) in skip:
                    continue
                destf = os.path.join(self._destdir, root, f)
                if not os.path.exists(destf):
                    os.makedirs(destf)

                    # try to copy the modification time
                    srcpath = "/".join([root, f])
                    times = srcbag.timesfor(srcpath)
                    if times:
                        os.utime(destf, (time.time(), times.mtime))

            if rmtime:
                os.utime(os.path.join(self._destdir, root), (time.time(), rmtime))

        if updated:
            self._update_manifests(srcbag, updated)

    def _update_manifests(self, srcbag, updated):
        destbag = open_bag(self._destdir)

        updated_by_alg = OrderedDict()
        for path in updated:
            if path in srcbag.entries:
                for alg in srcbag.entries[path]:
                    if alg not in updated_by_alg:
                        updated_by_alg[alg] = OrderedDict()
                    updated_by_alg[alg][path] = srcbag.entries[path][alg]
                
        for path in destbag.entries:
            for alg in updated_by_alg:
                if path not in updated_by_alg[alg]:
                    updated_by_alg[alg][path] = destbag.entries[path][alg]

        for alg in updated_by_alg:
            files = [f for f in updated_by_alg[alg].keys() if f.startswith("data/")]
            if len(files) > 0:
                with open(os.path.join(self._destdir, "manifest-%s.txt" % alg), 'w') as fd:
                    for f in files:
                        fd.write("%s %s\n" % (updated_by_alg[alg][f], f))

            files = [f for f in updated_by_alg[alg].keys() if not f.startswith("data/")]
            if len(files) > 0:
                with open(os.path.join(self._destdir, "tagmanifest-%s.txt" % alg), 'w') as fd:
                    for f in files:
                        fd.write("%s %s\n" % (updated_by_alg[alg][f], f))

        
    def restore_fetch(self, members=None):
        """
        restore the fetch file to the output bag.  
        """
        if not os.path.exists(self._destdir):
            self._create_dest_bag()

        if members is None:
            members = list(self._head.member_bags())
        fetch = OrderedDict()
        for member in members:
            src = self.get_member_bag(member.name)
            bag = open_bag(src)
            if bag.isfile("fetch.txt"):
                with bag.open_text_file("fetch.txt") as fd:
                    for line in fd:
                        parts = line.strip().rsplit(None, 1)
                        if len(parts) > 1 and parts[1]:
                            fetch[parts[1]] = line
        if fetch:
            with open(os.path.join(self._destdir, "fetch.txt"), 'w') as fd:
                for f in fetch:
                    fd.write(fetch[f])

    def restore(self, update_sizes=True, remove_multibag_tags=True):
        """
        properly recombine all of member bags in the multibag aggregation specified by the head bag
        into the destination bag. 
        """
        skip = self._head.deleted_paths()
        members = list(reversed(self._head.member_bags()))
        if not os.path.exists(self._destdir):
            self._create_dest_bag()

        if self._head.isfile("multibag/aggregation-info.txt"):
            with self._head.open_text_file("multibag/aggregation-info.txt") as fd:
                content = fd.read()
            with open(os.path.join(self._destdir, "bag-info.txt"), 'w') as fd:
                fd.write(content)
                    
        if not self._inplace:
            self._restore_from(self._head.path, skip)
        if members and members[0].name == self._head.name:
            members.pop(0)

        for member in members:
            self.restore_member(member.name, skip)

        # now build the fetch.txt file if any found
        if members:  # skipping head bag
            self.restore_fetch(members)

        restoredbag = as_extended(Bag(self._destdir))
        if remove_multibag_tags:
            shutil.rmtree(os.path.join(self._destdir, "multibag"))
            mbkeys = "Multibag-Version Multibag-Reference Multibag-Tag-Directory".split()
            mbkeys += "Multibag-Head-Version Multibag-Head-Deprecates".split()
            mbkeys = [k for k in mbkeys if k in restoredbag.info]
            for key in mbkeys:
                del restoredbag.info[key]
            restoredbag.save()
            
        if update_sizes:
            restoredbag.update_oxum()
            restoredbag.save()
            restoredbag.update_bag_size()
            restoredbag.save()
            

def restore_bag(headbag, destbag=None, compdir=None, fetcher=None):
    """
    restore a multibag aggregation to a single bag
    :param headbag:     the head bag for the bag to restore.  If the input is a 
                        ReadOnlyHeadBag, the destbag parameter must be specified.
                        :type headbag: str, HeadBag, or ReadOnlyHeadBag
    :param destbag:     the destination for the restore bag.  Normally, this would 
                        not exist yet at construction time, but providing an existing 
                        bag will cause that bag to be filled out to create the complete 
                        bag.  Not providing this will cause the headbag to be updated
                        in place.  
                        :type destbag: str or Bag
    :param compdir:     a directory where component multibags can be found (or cached to
                        when fetcher is set)
                        :type compdir: str
    :param fetcher:     a function that can fetch a remote multibag and which must two 
                        arguments.  The first argument is the name of the desired 
                        component multibag, as given in the member-bags.tsv file.  The 
                        second is the destination directory where the bag should be 
                        cached; this will be set to the value of compdir, if set.  The 
                        function must return the path to the cached bag.  
    :return:  the restored bag
    """
    r = BagRestorer(headbag, destbag, compdir, fetcher)
    r.restore()
    return ExtendedReadWritableBag(r.destination_bagdir)


