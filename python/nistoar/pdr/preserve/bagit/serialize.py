"""
Tools for serializing and running checksums on bags
"""
import subprocess as sp
from io import StringIO
import logging, os, zipfile, re

from .exceptions import BagSerializationError
from ...exceptions import StateException
from .. import sys as _sys

def _exec(cmd, dir, log):
    log.info("serializing bag: %s", ' '.join(cmd))
    log.debug("expecting bag in dir: %s", dir)

    out = None
    try:
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, cwd=dir)
        out, err = [s.strip() for s in proc.communicate()]
    except OSError as ex:
        log.exception("serialize command failed to exec: "+str(ex))
        if ex.errno == 2:
            log.error("Is the serializer command, %s, installed?", cmd[0])
        raise 
    except Exception as ex:
        log.exception("serialize command failed to exec: "+str(ex))
        raise 

    if out:
        log.debug("%s:\n%s", cmd[0], out)

    if proc.returncode > 0:
        log.error("%s exited with error (%d): %s", cmd[0], proc.returncode, err)
        raise sp.CalledProcessError(proc.returncode, cmd, err)
    else:
        log.debug(err)

zip_error = {
    '12': "zip has nothing to do",
    '13': "missing or empty zip file",
    '14': "error writing to a file",
    '15': "zip was unable to create a file to write to",
    '18': "zip could not open a specified file to read",
    '6': "component file too large"
}

def zip_serialize(bagdir, destdir, log, destfile=None):
    """
    serialize a bag with zip

    :param bagdir   str:  path to the bag root directory to be serialized
    :param destdir  str:  path to the output directory to write serialized 
                             file to.  
    :param log   Logger:  a logger to write messages to
    :param destfile str:  the name to give to the serialized file.  If not 
                             provided, one will be constructed from the 
                             bag directory name (and an appropriate extension)
    """
    parent, name = os.path.split(bagdir)
    if not destfile:
        destfile = name+'.zip'
    destfile = os.path.join(destdir, destfile)

    if not os.path.exists(bagdir):
        raise StateException("Can't serialize missing bag directory: "+bagdir)
    if not os.path.exists(destdir):
        raise StateException("Can't serialize to missing destination directory: "
                             +destdir)
    
    cmd = "zip -qr".split() + [ os.path.abspath(destfile), name ]
    try:
        _exec(cmd, parent, log)
    except sp.CalledProcessError as ex:
        if os.path.exists(destfile):
            try:
                os.remove(destfile)
            except Exception:
                pass
        message = zip_error.get(str(ex.returncode))
        if not message:
            message = "Bag serialization failure using zip (consult log)"
        raise BagSerializationError(message, name, ex, sys=_sys)

    return destfile

def zip_deserialize(bagfile, destdir, log):
    """
    unpack a zip-serialized bag into a specified directory
    :param str bagfile:  the name of the serialized bag file
    :param str destdir:  the output directory to write the the unpacked bag into
    :param log:   the Logger object to send comments to 
    """
    if not os.path.isfile(bagfile):
        raise StateException("Can't unpack a missing bag file: "+bagfile)
    if not os.path.isdir(destdir):
        raise StateException("Can't unpack a serialized bag into missing destination diretory: "
                             +destdir)
    outbag = os.path.join(destdir, zip_determine_bagname(bagfile))
    if os.path.exists(outbag):
        raise StateException("Destination bag already exists: "+outbag)

    bagfile = os.path.join(os.getcwd(), bagfile)

    cmd = "unzip -q %s" % bagfile
    try:
        _exec(cmd.split(), destdir, log)
    except sp.CalledProcessError as ex:
        if os.path.exists(outbag):
            try:
                shutil.rmtree(outbag)
            except Exception:
                pass
        message = zip_error.get(str(ex.returncode))
        if not message:
            message = "Bag deserialzation failure using zip (consult log)"
        raise BagSerializationError(message, os.path.basename(bagfile), ex, sys=_sys)

    return outbag

def zip_determine_bagname(bagfile):
    """
    peek into the zip-serialized bag and determine its name.  It will look for the bag-info.txt file
    and determine the name of its parent directory.  A BagSerializationError is raised if the file 
    does not appear to be a readable zipfile containing a legal bag.  
    """
    baginfore = re.compile(r"^[^/]+/bag-info.txt$")
    try:
        infos = []
        with zipfile.ZipFile(bagfile) as zf:
            infos = [f for f in zf.namelist() if baginfore.match(f)]
        if len(infos) == 0:
            raise BagSerializationError("%s: not a bag file (missing bag-info.txt file)" % bagfile, bagfile)
        if len(infos) > 1:
            raise BagSerializationError("%s: not a bag file (too many bag-info.txt files)" % bagfile, bagfile)

        return infos[0].split('/')[0]

    except zipfile.BadZipFile as ex:
        raise BagSerializationError("%s: not a legal zip file (too many bag-info.txt files)" % bagfile,
                                    bagfile)

def zip7_serialize(bagdir, destdir, log, destfile=None):
    """
    serialize a bag with 7zip

    :param bagdir   str:  path to the bag root directory to be serialized
    :param destdir  str:  path to the output directory to write serialized 
                             file to.  
    :param log   Logger:  a logger to write messages to
    :param destfile str:  the name to give to the serialized file.  If not 
                             provided, one will be constructed from the 
                             bag directory name (and an appropriate extension)
    """
    parent, name = os.path.split(bagdir)
    if not destfile:
        destfile = name+'.7z'
    destfile = os.path.join(destdir, destfile)
    
    cmd = "7z a -t7z -bsp0".split() + [ destfile, name ]
    try:
        _exec(cmd, parent, log)
    except sp.CalledProcessError as ex:
        if os.path.exists(destfile):
            try:
                os.remove(destfile)
            except Exception:
                pass
        if ex.returncode == 1:
            msg = "7z could not read one or more files"
        else:
            msg = "Bag serialization failure using 7z (consult log)"
        raise BagSerializationError(msg, name, ex, sys=_sys)

    return destfile

def zip7_deserialize(bagfile, destdir, log):
    """
    unpack a zip-serialized bag into a specified directory
    :param str bagfile:  the name of the serialized bag file
    :param str destdir:  the output directory to write the the unpacked bag into
    :param log:   the Logger object to send comments to 
    """
    if not os.path.isfile(bagfile):
        raise StateException("Can't unpack a missing bag file: "+bagfile)
    if not os.path.isdir(destdir):
        raise StateException("Can't unpack a serialized bag into missing destination diretory: "
                             +destdir)
    outbag = os.path.join(destdir, os.path.splitext(os.path.basename(bagfile))[0])
    if os.path.exists(outbag):
        raise StateException("Destination bag already exists: "+outbag)

    bagfile = os.path.join(os.getcwd(), bagfile)

    cmd = "7z x %s -bso0 -bsp0" % bagfile
    try:
        _exec(cmd.split(), destdir, log)
    except sp.CalledProcessError as ex:
        if os.path.exists(outbag):
            try:
                shutil.rmtree(outbag)
            except Exception:
                pass
        message = zip_error.get(str(ex.returncode))
        if not message:
            message = "Bag deserialzation failure using zip (consult log)"
        raise BagSerializationError(message, os.path.basename(bagfile), ex, sys=_sys)

    return outbag

class Serializer(object):
    """
    a class that serialize a bag using the archiving technique identified 
    by a given name.  
    """

    def __init__(self, typefunc=None, log=None):
        """
        """
        self._map = {}
        if typefunc:
            self._map.update(typefunc)
        self.log = log

    def setLog(self, log):
        self.log = log

    @property
    def formats(self):
        """
        a list of the names of formats supported by this serializer
        """
        return list(self._map.keys())

    def register(self, format, serfunc):
        """
        register a serialization function to make available via this serializer.
        The provided function must take 3 arguments:
          bagdir -- the root directory of the bag to serialize
          destination -- the path to the desired output bagfile.  
          log -- a logger object to send messages to.

        :param format str:   the name users can use to select the serialization
                             format.
        :param serfunc func:  the serializaiton function to associate with this
                           name.  
        """
        if not isinstance(serfunc, func):
            raise TypeError("Serializer.register(): serfunc is not a function: "+
                            str(func))
        self._map[format] = serfunc

    def serialize(self, bagdir, destdir, format, log=None):
        """
        serialize a bag using the named serialization format
        """
        if format not in self._map:
            raise BagSerializationError("Serialization format not supported: "+
                                        str(format))
        if not log:
            if self.log:
                log = self.log
            else:
                log = logging.getLogger(_sys.system_abbrev).getChild(_sys.subsystem_abbrev)

        return self._map[format][0](bagdir, destdir, log)

    def deserialize(self, bagfile, destdir, format=None, log=None):
        """
        unpack a bag file based on the serialization format indicated by its name
        :param str bagfile:  the name of the serialized bag file
        :param str destdir:  the output directory to write the the unpacked bag into
        :param str format:   the serialization format to assume; if None, the format will 
                               discerned from the bag file's name
        :param log:   the Logger object to send comments to 
        """
        if not format:
            format = os.path.splitext(bagfile)[1][1:]
            if not format or len(format) > 5:
                raise BagSerializationError("Unable to determine serialization format for "+bagfile)
        if format not in self._map:
            raise BagSerializationError("Serialization format not supported: "+
                                        str(format))
        if not log:
            if self.log:
                log = self.log
            else:
                log = logging.getLogger(_sys.system_abbrev).getChild(_sys.subsystem_abbrev)

        return self._map[format][1](bagfile, destdir, log)

class DefaultSerializer(Serializer):
    """
    a Serializer configured for some default serialization formats: zip, 7z.
    """

    def __init__(self, log=None):
        super(DefaultSerializer, self).__init__({
            "zip": (zip_serialize, zip_deserialize),
            "7z": (zip7_serialize, zip7_deserialize)
        }, log)
