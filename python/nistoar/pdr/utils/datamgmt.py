"""
Utility functions managing data and files
"""
from collections import Mapping
import hashlib, os, shutil, time, re, math

__all__ = [
    'update_mimetypes_from_file', 'build_mime_type_map', 'checksum_of', 'measure_dir_size',
    'rmtree_sys', 'rmtree_retry', 'rmtree'
]

def_ext2mime = {
    "html": "text/html",
    "txt":  "text/plain",
    "xml":  "text/xml",
    "json": "application/json"
}

def update_mimetypes_from_file(map, filepath):
    """
    load the MIME-type mappings from the given file into the given dictionary 
    mapping extensions to MIME-type values.  The file can have either an nginx
    configuration format or the common format (i.e. used by Apache).  
    """
    if map is None:
        map = {}
    if not isinstance(map, Mapping):
        raise ValueError("map argument is not dictionary-like: "+ str(type(map)))

    commline = re.compile(r'^\s*#')
    nginx_fmt_start = re.compile(r'^\s*types\s+{')
    nginx_fmt_end = re.compile(r'^\s*}')
    with open(filepath) as fd:
        line = '#'
        while line and (line.strip() == '' or commline.search(line)):
            line = fd.readline()

        if line:
            line = line.strip()
            if nginx_fmt_start.search(line):
                # nginx format
                line = fd.readline()
                while line:
                    if nginx_fmt_end.search(line):
                        break
                    line = line.strip()
                    if line and not commline.search(line):
                        words = line.rstrip(';').split()
                        if len(words) > 1:
                            for ext in words[1:]:
                                map[ext] = words[0]
                    line = fd.readline()

            else:
                # common server format
                while line:
                    if commline.search(line):
                        continue
                    words = line.strip().split()
                    if len(words) > 1:
                        for ext in words[1:]:
                            map[ext] = words[0]
                    line = fd.readline()

    return map

def build_mime_type_map(filelist):
    """
    return a dictionary mapping filename extensions to MIME-types, given an 
    ordered list of files defining mappings.  Entries in files appearing later 
    in the list can override those in the earlier ones.  Files can be in either 
    the nginx configuration format or the common format (i.e. used by Apache).  

    :param filelist array:  a list of filepaths defining the MIME-types to
                            extensions mappings.
    """
    out = def_ext2mime.copy()
    for file in filelist:
        update_mimetypes_from_file(out, file)
    return out

def checksum_of(filepath, bufsize: int=10240000):
    """
    return the checksum for the given file

    :param str|Path filepath:  the path of the file to calculate the checksum for
    :param int       bufsize:  the memory buffer size to use when reading the file.
                               The default is 10 MB; multithreaded applications should 
                               consider a smaller value.
    """
    if not isinstance(bufsize, int):
        raise TypeError("checksum_of(): bufsize arg must be an integer")
    if bufsize < 1:
        raise ValueError("checksum_of(): bufsize arg must be a positive integer")
    sum = hashlib.sha256()
    with open(filepath, mode='rb') as fd:
        while True:
            buf = fd.read(bufsize)
            if not buf: break
            sum.update(buf)
    return sum.hexdigest()

def measure_dir_size(dirpath):
    """
    return a pair of numbers representing, in order, the totaled size (in bytes)
    of all files below the directory and the total number of files.  

    Note that the byte count does not include the capacity taken up by directory
    entries and thus is not an accurate measure of the space the directory takes
    up on disk.

    :param str dirpath:  the path to the directory of interest
    :rtype:  list containing 2 ints
    """
    size = 0
    count = 0
    for root, subdirs, files in os.walk(dirpath):
        count += len(files)
        for f in files:
            size += os.stat(os.path.join(root,f)).st_size
    return [size, count]

def formatBytes(nb, numAfterDecimal=-1):
    """
    format a byte count for display using metric byte units.  
    :param int nb:  the number of bytes to format
    :param int numAfterDecimal:  the number of digits to appear after the decimal if the value is 
                                 greater than 1000; if less than zero (default), the number will be 
                                 1 or 2.
    :rtype: str
    """
    if not isinstance(numAfterDecimal, int):
        numAfterDecimal = -1
    if not isinstance(nb, int):
        return ''
    if nb == 0:
        return "0 Bytes"
    if nb == 1:
        return "1 Byte"
    base = 1000
    e = ['Bytes', 'kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    f = math.floor(math.log10(nb) / math.log10(base))
    v = nb / math.pow(base, f)
    d = numAfterDecimal
    if d < 0:
        if f == 0:   # less than 1 kilobyte
            d = 0
        elif v < 10.0:
            d = 2
        else:
            d = 1
        v = round(v, d)
    return "%s %s" % ( (("%%.%df" % d) % v), e[f] )

def rmtree_sys(rootdir):
    """
    an implementation of rmtree that is intended to work on NSF-mounted 
    directories where shutil.rmtree can often fail.
    """
    if '*' in rootdir or '?' in rootdir:
        raise ValueError("No wildcards allowed in rootdir")
    if not os.path.exists(rootdir):
        return
    cmd = "rm -r ".split() + [rootdir]
    subprocess.check_call(cmd)

def rmtree_retry(rootdir, retries=1):
    """
    an implementation of rmtree that is intended to work on NSF-mounted 
    directories where shutil.rmtree can often fail.
    """
    if not os.path.exists(rootdir):
        return
    if not os.path.isdir(rootdir):
        os.remove(rootdir)
        return
    
    for root,subdirs,files in os.walk(rootdir, topdown=False):
        try:
            shutil.rmtree(root)
        except OSError as ex:
            if retries <= 0:
                raise
            # wait a little for NFS to catch up
            time.sleep(0.25)
            rmtree(root, retries=retries-1)
    
rmtree = rmtree_retry
