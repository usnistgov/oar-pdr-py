"""
This module will create tests datasets that can be turned into bags and then 
into multibags.
"""
import os, math, random
from copy import deepcopy
from abc import ABCMeta, abstractmethod

def mkdataset(destdir, totalsize, filecount=10, plan=None):
    """
    create a fake dataset--a collection of files with dummy contents--subject
    to contstraints.

    :param str destdir:   path to a root directory to contain the created files
    :param int totalsize: the total size--the total sum of bytes across all 
                          files--of the dataset to be created.
    :param int filecount: the total number of files to create
    :param dict plan:     hints on how to distribute bytes across different 
                          files.
    """
    if not plan:
        plan = {
            'files': [{
                'totalsize': totalsize,
                'totalfiles': filecount,
                'type': 'uniform'
            }]
        }
    plan.update({
        'totalsize': totalsize,
        'totalfiles': filecount
    })
    DatasetMaker(destdir, plan).fill()

class DatasetMaker(object):
    """
    a class for making a dataset
    """

    def __init__(self, destdir, plan):
        self.root = destdir
        parent = os.path.dirname(destdir)
        if parent and not os.path.isdir(parent):
            raise OSError(2, "Parent directory does not exist: "+parent)

        self.plan = deepcopy(plan)
        
        if 'totalsize' not in self.plan:
            raise ValueError("Plan requires 'totalsize' limit")
        if 'totalfiles' not in self.plan:
            self.plan['totalfiles'] = 10
        self._totfiles = self.plan['totalfiles']
        
        self._iters = {
            "inventory": InventorySizeIterator,
            "uniform": UniformSizeIterator
        }


    def fill(self):
        """
        fill out the dataset according to plan
        """
        return self._fill_dir('',
                              self.plan['totalsize'], 
                              self.plan['totalfiles'],
                              self.plan.get('files', []),
                              self.plan.get('dirs', []))

    def ensure_root(self):
        """
        Ensure that the destination's root directory exists
        """
        if not os.path.exists(self.root):
            os.mkdir(self.root)

    def _create_dir(self, under=''):
        self.ensure_root()

        out = self._mkfilename(lambda id: id + "_d", under)
        os.makedirs(os.path.join(self.root, out))
        return out

    def _create_file(self, size, under=''):
        self.ensure_root()

        out = self._mkfilename(lambda id: id + "_{0}".format(size), under)
        if under:
            up = os.path.join(self.root, under)
            if not os.path.isdir(up):
                os.makedirs(up)
        sz = create_file(os.path.join(self.root, out), size)
        return out

    def _mkfilename(self, namefunc, parent=''):
        out = None
        fp = None
        i = 10
        ord = 10
        while not fp or os.path.exists(fp):
            if i < 1:
                ord *= 10
            out = os.path.join(parent, namefunc(self._mkfid(ord)))
            fp = os.path.join(self.root, out)

        return out

    def _mkfid(self, ord=100):
        return "%03x" % random.randrange(self._totfiles*ord)

    def _fill_dir(self, targetdir, totalsize=0, totalfiles=0, files=[], dirs=[]):
        # :param list files:  array of dicts that describe the creation of
        #                     sets of files
        # :param list dirs:   array of dicts where each dict describes how to
        #                     to fill a subdirectory of the target directory;
        #                     the number of dicts in the array indicate the
        #                     the number of subdirectories to create.
        if totalsize <= 0 or totalfiles <= 0:
            return (0, 0)

        if isinstance(dirs, int):
            dirs = [{} for i in range(dirs)]

        self._distribute(totalsize, totalfiles, files, dirs)

        # fill the directory with files
        nb = 0
        nf = 0
        for fs in files:
            if nb > totalsize or nf > totalfiles:
                break
            if 'iter' in fs:
                for i in range(fs.get('reps', 1)):
                    n = self._fill_with_files(targetdir, fs['totalsize'],
                                              fs['totalfiles'],
                                              fs['iter'].iterate())
                    nb += n[0]
                    nf += n[1]

        # fill the subdirectories (recursively)
        for fs in dirs:
            if nb > totalsize or nf > totalfiles:
                break
            for i in range(fs.get('reps', 1)):
                if nb > totalsize or nf > totalfiles:
                    break
                dir = self._create_dir(targetdir)
                n = self._fill_dir(dir, fs['totalsize'], fs['totalfiles'],
                                   fs.get('files', []), fs.get('dirs', []))
                nb += n[0]
                nf += n[1]

        return (nb, nf)

    def _iter_for(self, type, cfg):
        try:
            return self._iters[type](**cfg)
        except KeyError as ex:
            raise ValueError("Unknown SizeIterator type: " + type)

    def _distribute(self, totalsize, totalfiles, files, dirs):
        # convert the file and directory directives to a canonical form
        # that distributes the given totalsize across the various files
        # and directories

        # resolve the iterators
        for fs in files:
            if 'iter' not in fs:
                tp = fs.setdefault('type', 'uniform')
                fs['iter'] = self._iter_for(tp, fs)

        # tally up the total size and number of files expected based
        # the sub-totals specified.  Some directives may not have
        # totals specified
        # 
        nb = totalsize
        nf = totalfiles
        ndz = []
        ndf = []
        for fs in files + dirs:
            reps = fs.get('reps', 1)
            if 'iter' in fs:
                try:
                    fs['totalsize'] = fs['iter'].totalsize
                    nb -= fs['totalsize'] * reps
                except RuntimeError:
                    ndz += [fs]
                try:
                    fs['totalfiles'] = fs['iter'].totalfiles
                    nf -= fs['totalfiles'] * reps
                except RuntimeError:
                    ndf += [fs]
            else:
                if 'totalsize' in fs:
                    nb -= fs['totalsize'] * reps
                else:
                    ndz += [fs]
                if 'totalfiles' in fs:
                    nf -= fs['totalfiles'] * reps
                else:
                    ndf += [fs]

        # update the files and dirs descriptions to ensure they each
        # have 'totalsize' and 'totalfiles' fields.  Distribute the unclaimed
        # size/file no. among those directives without totals specified
        if len(ndz) > 0:
            # distribute the remaining requested bytes across the directives
            # that don't have a totalsize specified/determined
            iiter = UniformSizeIterator(nb, sum([fs.get('reps',1)
                                                 for fs in ndz])).iterate()
            for fs in ndz:
                fs['totalsize'] = next(iiter, 0)
                if 'iter' in fs:
                    fs['iter'].target_totalsize = fs['totalsize']
                nb -= fs['totalsize'] * fs.get('reps', 1)
                
        if len(ndf) > 0:
            # distribute the remaining requested files across the directives
            # that don't have a totalfiles specified/determined
            iiter = UniformSizeIterator(nf, sum([fs.get('reps',1)
                                                 for fs in ndf])).iterate()
            for fs in ndf:
                fs['totalfiles'] = next(iiter, 1)
                if 'iter' in fs:
                    fs['iter'].target_totalfiles = fs['totalfiles']
                nf -= fs['totalfiles']

        # if all the directives have specified their totals, then we may need to
        # add some extra files to reach the full total.
        if nb > 0:
            if nf <= 0:
                nf = 1
            files.append({
                'type': 'uniform',
                'totalsize': nb,
                'totalfiles': nf
            })
            files[-1]['iter'] = UniformSizeIterator(**(files[-1]))
        

    def _fill_with_files(self, targetdir, maxsize=0, maxfiles=0, sizes=None):
        # :param dict sizes:  iterator of file sizes to create
        if sizes is None:
            sizes = []

        nb = 0
        nf = 0

        for sz in sizes:
            if nb+sz > maxsize or nf >= maxfiles:
                break
            f = self._create_file(sz, targetdir)
            nb += os.stat(os.path.join(self.root, f)).st_size
            nf += 1

        return (nb, nf)

class SizeIterator(object):
    """
    an iterator class that spits out desired sizes for files to be created.
    The iterator() method returns the actual iterator instance.  Some iterators
    require a target total size and/or number of files to create.  If this is
    the case one is not set, then iterator(), totalsize, and totalfiles will 
    raise a Runtime exception; the target values can either be set at 
    construction (via the totalsize and totalfiles parameters) or via the 
    properties target_totalsize and target_totalfiles.  
    """
    __metaclass__ = ABCMeta

    def __init__(self, **kw):
        self._cfg = kw
        self._type = kw.get('type', 'unkwn')

    @property
    def type(self):
        return self._type

    def __repr__(self):
        return "SizeIterator({0})".format(self.type)

    @property
    def target_totalsize(self):
        """
        the total of the sizes desired from this iterator.  The actual total
        may be different.
        """
        return self._cfg.get('totalsize', None)
        
    @target_totalsize.setter
    def target_totalsize(self, size):
        if size is not None and not isinstance(size, int):
            raise ValueError("Not an integer: "+str(size))
        self._cfg['totalsize'] = size
        
    @property
    def target_totalfiles(self):
        """
        the total number of files desired from this iterator.  The actual total
        may be different (usually less than or equal).  
        """
        return self._cfg.get('totalfiles', None)

    @target_totalfiles.setter
    def target_totalfiles(self, n):
        if n is not None and not isinstance(n, int):
            raise ValueError("Not an integer: "+str(n))
        self._cfg['totalfiles'] = n

    @abstractmethod
    def iterate(self):
        """
        return an iterator that returns sizes, ending when limits (set by 
        target_totalsize, target_totalfiles, and perhaps other constraints) 
        are reached.
        :raise RuntimeError  if the iterator requires a target total size or 
                             number of files to work.
        """
        raise NotImplementedError()

    @property
    def totalsize(self):
        """
        return the actual total of the sizes that this iterator returns
        """
        return sum([sz for sz in self.iterate()])
        
    @property
    def totalfiles(self):
        """
        return the actual total of the sizes that this iterator returns
        """
        return len([sz for sz in self.iterate()])

class InventorySizeIterator(SizeIterator):
    """
    an iterator that returns sizes according to a inventory dictionary.
    """

    def __init__(self, sizes, **kw):
        """
        :param dict sizes:  dict mapping file sizes to number of desired
                            files with that size directly in the target
                            directory
        """
        kw['sizes'] = sizes
        super(InventorySizeIterator, self).__init__(**kw)

    def iterate(self):
        sizes = self._cfg.get('sizes', {})
        for sz in reversed(sorted(sizes.keys())):
            for i in range(sizes[sz]):
                yield sz
        
class UniformSizeIterator(SizeIterator):
    """
    an iterator that evenly splits the total desired size among the total 
    desired number of files.
    """

    def __init__(self, totalsize=None, totalfiles=None, **kw):
        """
        :param dict sizes:  dict mapping file sizes to number of desired
                            files with that size directly in the target
                            directory
        """
        if totalsize is not None:
            kw['totalsize'] = totalsize
        if totalfiles is not None:
            kw['totalfiles'] = totalfiles
        super(UniformSizeIterator, self).__init__(**kw)

    def iterate(self):
        tz = self.target_totalsize
        if tz is None or tz < 0:
            raise RuntimeError("Need to set target_totalsize")
        nf = self.target_totalfiles
        if nf is None or nf < 0:
            raise RuntimeError("Need to set target_totalfiles")
        
        sz = int(math.floor(1.0 * tz / nf + 0.5))
        extra = tz - sz*nf
        while nf > 0:
            if nf == math.fabs(extra):
                sz += int(math.copysign(1, extra))
            nf -= 1
            yield sz


def create_file(destfile, size):
    """
    create a file of a given size.  The file created will be filled with ascii
    text, formatted into lines of 100 bytes or less.
    :param str destfile:  the path to output file to create.  Its parent 
                          directory must exist; if the file exists, it will
                          be overwritten.
    :param int size:      the number of bytes to fill it with.
    :return int:     the actual size of the file in bytes that was created (as
                     measured by os.stat()).  
    :raise OSError:  if the parent directory does not exist or permissions
                     prevent writing the file.
    """
    fulllines = int(math.floor(size / 100.0))
    nwd = int(math.floor(math.log(fulllines+1))) + 1
    fmt = "%{0}d ".format(nwd)

    with open(destfile, 'w') as fd:
        for i in range(fulllines):
            fd.write(fmt % i)
            fd.write('x' * (98-nwd))
            fd.write('\n')

        left = size - (fulllines * 100)
        if left > nwd:
            fd.write(fmt % (fulllines))
            left -= nwd + 1
        if left > 1:
            fd.write('x' * (left-1))
            left = 1
        if left > 0:
            fd.write('\n')

    return os.stat(destfile).st_size

