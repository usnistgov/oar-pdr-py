"""
CLI command that will retrieve NERDm records by ID.  This command can either pull from the PDR's public 
metadata API (exactly as the ``describe`` command does) or from the public archival information packages
(as the ``md recover`` command does).
"""
import logging, argparse, sys, os, shutil, time, tempfile, re, json
from urllib.parse import urlparse, urljoin

from nistoar.pdr.preserve.bagit import NISTBag
from nistoar.pdr.utils import write_json
from nistoar.pdr.exceptions import PDRException, ConfigurationException, StateException
from nistoar.pdr.cli import PDRCommandFailure
from nistoar.pdr.describe import MetadataClient, RMMServerError, IDNotFound
from nistoar.pdr.distrib import (RESTServiceClient, BagDistribClient,
                                 DistribServerError, DistribResourceNotFound)
from nistoar.pdr.preserve.bagit.serialize import DefaultSerializer
from nistoar.pdr.constants import RELHIST_EXTENSION, VERSION_EXTENSION_PAT, to_version_ext, ARK_PFX_PAT
VERSION_EXTENSION_RE = re.compile(VERSION_EXTENSION_PAT)
ARK_PFX_RE = re.compile(ARK_PFX_PAT)

default_name = "get"
help = "retrieve NERDm records from the PDR (via its public APIs)"
description = """
  Retrieve a NERDm record from the PDR given its ID.  By default, the record is pulled from the PDR's 
  metadata (or "describe") service; however it can be extracted from the archive information packages
  (AIPs), pulled from the PDR's public distribution service (see also the ``md recover`` command).  

  Records with the same ID retrieved from the two sources should be the same; however, they are not 
  guaranteed to be, as a difference may reflect modifications as part of the ingest process.  Further,
  not all records available from the metadata service are guaranteed to be available from the AIPs: while
  versioned and ReleaseCollection identifiers are not supported for retrieval from AIPs, some records 
  available from the metadata service may not have originated from the PDR preservation process.  
  (See "md trans rmm" for converting an AIP-sourced record to a versioned or ReleaseCollection one.)

  By default, the requested record is written to standard out, but with the -o option, it can be written
  to a specific file.  
"""

def load_into(subparser):
    """
    load this command into a CLI by defining the command's arguments and options
    :param argparser.ArgumentParser subparser:  the argument parser instance to define this command's 
                                                interface into it 
    :rtype: None
    """
    p = subparser
    p.description = description
    p.add_argument("id", metavar="ID", type=str, 
                   help="an identifier to retrieve metadata for.  Normally this is a PDR ARK identifier, "+
                        "but an (old-style) EDI-ID is also recognized.")
    p.add_argument("-o", "--output-file", metavar="FILE", type=str, dest="outfile",
                   help="write the output to the named file instead of standard out")
    p.add_argument("-V", "--get-version", metavar="VER", type=str, dest="version",
                   help="return the VER version of the record. (Note that versions are not available for "+
                        "all ID classes)")
    p.add_argument("-A", "--from-aip", action="store_const", dest="src", const="aip",
                   help="extract the requested metadata from the appropriate AIP")
    p.add_argument("-U", "--services-base-url", metavar="BASEURL", type=str, dest='srvrbase',
                   help="use this base URL as the basis for the PDR's RMM and distribution services.  "+
                        "The default is https://data.nist.gov/.")
    p.add_argument("-D", "--dist-base-url", metavar="BASEURL", type=str, dest='distbase',
                   help="use this base URL for the distribution service from which AIPs should be "+
                        "donwloaded.  The default is is BASE/od/ds")
    p.add_argument("-R", "--rmm-base-url", metavar="BASEURL", type=str, dest='rmmbase',
                   help="use this base URL for the RMM service that should be used when ALL is requested as "+
                        "an AIPID.  The default is is BASE/rmm")

def execute(args, config=None, log=None):
    """
    execute this command: create a base metadata bag to update a dataset based on its last 
    published version
    """
    cmd = default_name
    if not log:
        log = logging.getLogger(cmd)
    if not config:
        config = {}

    if isinstance(args, list):
        # cmd-line arguments not parsed yet
        p = argparse.ArgumentParser()
        load_command(p)
        args = p.parse_args(args)

    _process_args(args, config, cmd, log)

    # implementation
    rec = None
    try: 
        if args.src == "aip":
            rec = extract_from_AIP(args.id, args.distbase, args.version, args.rmmbase, None, config, log)
        else:
            rec = describe(args.id, args.rmmbase, args.version, config)

    except (IDNotFound, DistribResourceNotFound) as ex:
        raise PDRCommandFailure(cmd, "ID not found: "+args.id, 2)
    except (RMMServerError, DistribServerError) as ex:
        raise PDRCommandFailure(cmd, "Unexpected service failure: "+str(ex), 4)
    except Exception as ex:
        raise PDRCommandFailure(cmd, "Unexpected failure retrieving metadata: "+str(ex), 4)

    # write the output
    fp = None   # file object for file output
    op = None   # file object for output (may be equal to fp)
    try:
        if args.outfile and args.outfile != '-':
            fp = open(args.outfile, 'w')
            op = fp
        else:
            op = sys.stdout

        json.dump(rec, op, indent=4, separators=(',', ': '))

    except OSError as ex:
        raise PDRCommandFailure(cmd, "Failed to write data to %s: %s" %
                                ((fp and args.outfile) or "standard out", str(ex)), 3)
    finally:
        if fp: fp.close()

def describe(id, mdsvc, version=None, config=None):
    """
    retrieve the metadata describing the resource with a given identifier from a metadata service at 
    the given endpoint.  This uses the ``nistoar.pdr.describe.MetadataClient`` to retrieve the record.
    :param str id:  the identifier of the resource of interest.  This is usually a PDR ARK identifier,
                    but an old-style EDI-ID identifier will also be recognized.  
    :param mdsvc:   either the metadata service's endpoint URL or a MetadataClient instance to use
                    to query the metadata service.  
    :param str version:   the particular version of the resource to retrieve metadata for; if None,
                          the latest one is returned.  
    :param dict config:   configuration parameters for controlling the retrieval behavior (currently 
                          ignored by this implementation).  
    :raises IDNotFound:   if the given id is not found in the metadata database
    :raises RMMServerError:  if a server error occurs from the metadata service
    :raises RMMClientError:  may be raised if the service endpoint URL is not correct
    """
    if version:
        versioned = id + to_version_ext(version)
        if id.endswith(RELHIST_EXTENSION) or VERSION_EXTENSION_RE.search(id):
            raise IDNotFound(versioned, "Versions of requested ID=%s are not available" % id)
        id = versioned

    if isinstance(mdsvc, str):
        mdsvc = MetadataClient(mdsvc)
    return mdsvc.describe(id)

def extract_from_AIP(id, distribsvc, version=None, mdsvc=None, tmpdir=None, config=None, log=None):
    """
    fetch the head bag for the resource with the given identifier and extract its NERDm record.
    Metadata records for ReleaseCollection or versioned identifiers are not available via this 
    method (see also ``nistoar.nerdm.convert.rmm``).  

    It is recommended that the metadata service endpoint be provided: if available, it will be 
    consulted to check for the records AIP identifier; otherwise, it will be surmised from the 
    given id, which will not always be correct.  

    :param str id:  the identifier of the resource of interest.  An EDI-ID identifier will most
                    reliably return a result (unless mdsvc is provided), but a PDR ARK ID can be
                    provided as well.
    :param distribsvc:   either the distribution service's endpoint URL or a RESTServiceClient instance
                         connected to the distribution service to use to access the needed AIP
    :param str version:   the particular version of the resource to retrieve metadata for; if None,
                          the latest one is returned.  
    :param mdsvc:   either the metadata service's endpoint URL or a MetadataClient instance to use
                    to query the metadata service.  If not provided, a metadata service will not be
                    consulted.
    :param dict config:   configuration parameters for controlling the retrieval behavior (currently 
                          ignored by this implementation).  
    :param str tmpdir:    a directory where temporary archive bag files can be placed and unpacked 
                          under.  A temporary subdirectory under this will be created, and all caching 
                          will take place under there.  If not provided, the configuration value for 
                          "temp_dir" will be used; barring that, the OS shell-recommended TMP_DIR 
                          (defaulting to /tmp) will be used.  
    """
    if not id:
        raise ValueError("extract_from_AIP(): missing id argument")
    if id.endswith(RELHIST_EXTENSION) or VERSION_EXTENSION_RE.search(id):
        raise DistribResourceNotFound(id, message=id + ": AIP access not available for this identifer class")
    if not config:
        config = {}

    aipid = None
    if mdsvc:
        if isinstance(mdsvc, str):
            mdsvc = MetadataClient(mdsvc)

        # look up the aipid (or ediid as backup)
        try:
            nerdm = describe(id, mdsvc, version, config)
            aipid = nerdm.get("pdr:aipid")
            if not aipid:
                aipid = nerdm.get("ediid")

        except IDNotFound as ex:
            pass
        except Exception as ex:
            if log:
                log.warning("Failure while consulting metadata service: "+str(ex))
                log.info("Skipping metadata service consultation")

    if not aipid:
        # discern the aipid to look from the given ID
        aipid = id

    aipid = ARK_PFX_RE.sub('', aipid)
    if '/' in aipid:
        raise DistribResourceNotFound(id, message=id + ": AIP access not available for this identifer class")

    # set up the temporary directory to cache bags into
    if not tmpdir:
        tmpdir = config.get("temp_dir", os.environ.get("TMPDIR", "/tmp"))
    if not os.path.isdir(tmpdir):
        raise ConfigurationException(tmpdir + ": temp dir does not exist as a directory")
    mytmproot = "pdr_md_get"
    if config.get("hide_temp_data"):
        mytmproot = '.' + mytmproot

    uselog = log
    if uselog:
        uselog = uselog.getChild("serializer")
    ser = DefaultSerializer(uselog)
    if isinstance(distribsvc, str):
        distribsvc = RESTServiceClient(distribsvc)

    with tempfile.TemporaryDirectory(prefix=mytmproot, dir=tmpdir) as tmpuse:
        try:
            dist = BagDistribClient(aipid, distribsvc)
            headbag = dist.head_for_version(version)
        except DistribResourceNotFound as ex:
            if log:
                log.warning("AIP head bag not found for %s", id)
            raise
        except Exception as ex:
            if log:
                log.error("Unable query distribution service: %s", str(ex))
            raise

        try:
            dist.save_bag(headbag, tmpuse)
            serbag = os.path.join(tmpuse, headbag)
            outbag = ser.deserialize(serbag, tmpuse)
            bag = NISTBag(outbag)
            return bag.nerdm_record()

        except Exception as ex:
            if log:
                log.error("Failed to extract metadata from %s %s: %s" % (aipid, ver, str(ex)))
            raise

def _process_args(args, config, cmd, log=None):

    # ensure the args.srvrbase, the PDR's services base URL
    srvrbase = args.srvrbase
    if not srvrbase:
        srvrbase = config.get("nist_pdr_base", "https://data.nist.gov/")

    try:
        _check_url(srvrbase)
    except ValueError as ex:
        if args.srvrbase:
            raise PDRCommandFailure(cmd, "Bad PDR URL provided: %s: %s" % (args.srvrbase, str(ex)))
        else:
            raise ConfigurationException(cmd, "Config parameter, nist_pdr_base: bad value: %s: %s" %
                                         (args.srvrbase, str(ex)), 3)
    args.srvrbase = srvrbase
    if args.srvrbase and not args.srvrbase.endswith('/'):
        args.srvrbase += '/'

    if args.src == "aip":
        if not args.distbase:
            args.distbase = config.get("pdr_dist_base", urljoin(args.srvrbase, "od/ds/"))
        else:
            args.distbase = urljoin(args.srvrbase, args.distbase)
        try:
            _check_url(args.distbase)
        except ValueError as ex:
            raise PDRCommandFailure(cmd, "Bad distrib service URL: %s: %s" % (args.distbase, str(ex)))

    else:    # defaults to "desc"
        if not args.rmmbase:
            args.rmmbase = config.get("pdr_rmm_base", urljoin(args.srvrbase, "rmm/"))
        else:
            args.rmmbase = urljoin(args.srvrbase, args.rmmbase)
        try:
            _check_url(args.rmmbase)
        except ValueError as ex:
            raise PDRCommandFailure(cmd, "Bad RMM service URL: %s: %s" % (args.rmmbase, str(ex)))

def _check_url(url):
    purl = urlparse(url)
    if not purl.netloc or not purl.scheme:
        raise ValueError("absolute URL required")
    if purl.scheme != "http" and purl.scheme != "https":
        raise ValueError("unsupported URL scheme: "+purl.scheme)

