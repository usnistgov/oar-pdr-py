"""
CLI command that will extract NERDm records from the public archival information packages (e.g. so that they 
can be reloaded into the metadata database).
"""
import logging, argparse, sys, os, shutil, time, tempfile, re
from urllib.parse import urlparse, urljoin

from nistoar.pdr.preserve.bagit import NISTBag
from nistoar.pdr.utils import write_json
from nistoar.pdr.exceptions import PDRException, ConfigurationException, StateException
from nistoar.pdr.cli import PDRCommandFailure
from nistoar.pdr.describe import MetadataClient, RMMServerError
from nistoar.pdr.distrib import (RESTServiceClient, BagDistribClient,
                                 DistribServerError, DistribResourceNotFound)
from nistoar.pdr.publish.bagger.utils import Version
from nistoar.pdr.preserve.bagit.serialize import DefaultSerializer
from ._args import process_svcep_args, define_comm_md_opts

arkre = re.compile(r"^ark:/\d+/")

default_name = "recover"
help = "extract NERDm records from the public archival information packages"
description = """
  Extract NERDm records from the public archival information packages.

  This command will (for specified IDs) retrieve "head bag" archive information packages (AIPs) from the PDR's 
  public distribution service and extract the NERDm records describing the resources referred to by the 
  AIP-IDs.  The extracted records could then be re-ingested into the PDR's Resource Metadata Manager (RMM).  

  By default, records are written as files in JSON format into the current directory; however, use of the 
  -d option is recommended to set the output directory when recovering many records.  Normally (without the 
  -l option), one record file is written for each version of an AIP ID resolved.  Each file will be given a
  a name of the form, AIPID-vV_V_V.json. 
"""

def load_into(subparser, current_dests, as_cmd=None):
    """
    load this command into a CLI by defining the command's arguments and options.
    :param argparser.ArgumentParser subparser:  the argument parser instance to define this command's 
                                                interface into it 
    :param set current_dests:  the current set of destination names that have been defined so far; this
                               can indicate if a parent command has defined required options already
    :param str as_cmd:  the command name that this command is being loaded as (ignored)
    :rtype: None
    """
    p = subparser
    p.description = description
    p.add_argument("aipids", metavar="AIPID", type=str, nargs="*",
                   help="an AIP identifier to recover; the special value 'ALL' will attempt "+
                        "recover all AIPs via a list generated from a query to the RMM.")
    p.add_argument("-I", "--ids-from-file", metavar="FILE", type=str, dest='idfile', 
                   help="read desired IDs from the FILE file, listed space-delimited or one-per-line")
    p.add_argument("-d", "--output-directory", metavar="DIR", type=str, dest='outdir',
                   help="write output records to the DIR, rather than the current one.  (This option may "+
                        "be required when specific AIPIDs are not specified).")
    p.add_argument("-g", "--generate-aip-list", metavar="FILE", nargs='?', dest='outlist', const="aipids.lis",
                   help="do not recover records; rather generate a list of AIP IDs via an RMM query and "+
                        "save it to FILE, which (if given as a relative path) will be interpreted relative "+
                        "to the output directory.  The default is 'aipids.lis'.  Not intended for use with "+
                        "AIPID or -I")
#    p.add_argument("-r", "--rmm-format", action="store_true", dest='rmmfmt',
#                   help="save the records in parts as they would be stored in the RMM")
    p.add_argument("-l", "--latest-version", action="store_true", dest="latestonly",
                   help="recover only the latest version of each AIP")
    p.add_argument("-w", "--overwrite", action="store_true", dest="overwrite",
                   help="if a record's data is already found written in the output directory (based on the "+
                        "file name), overwrite it with the freshly recovered data.  Without this option, "+
                        "the record will be skipped.")
    p.add_argument("-B", "--from-bags-only", action="store_true", dest="bagsonly",
                   help="recover only records extracted from bags.  Without this option, if bags do not "+
                        "exist for an ID, an attempt to create a v1.0.0 record will be created from the "+
                        "latest record in the RMM")
    p.add_argument("-V", "--select-version", metavar="VERSION", action="append", dest="inclvers",
                   help="write out records only for specified versions; include multiple times for multiple "+
                        "versions")
    if 'rmmbase' not in current_dests:
        define_comm_md_opts(p)

    return None

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

    rmm = MetadataClient(args.rmmbase)
    if 'ALL' in args.aipids:
        # get a list of all known AIP IDs
        log.info("Retrieving all known AIP IDs")
        args.aipids.remove('ALL')
        args.aipids = get_all_aip_ids(rmm, cmd) + args.aipids

    if args.outlist:
        # just write the IDs out to a file and exit
        try:
            write_aipid_list(args.aipids, args.outlist, config, log)
        except Exception as ex:
            msg = "Failure while writing out IDs: "+str(ex)
            log.exception(msg)
            raise PDRCommandFailure(cmd, msg, 4)
        return


    # raise PDRCommandFailure(cmd, "Halting for testing", 9)

    ser = DefaultSerializer(log.getChild("serializer"))
    distsvc = RESTServiceClient(args.distbase)
    dotre = re.compile(r"\.")
    errs = []

    with tempfile.TemporaryDirectory(prefix=".pdr_recover_", dir=args.outdir) as tmpdir:
        for aipid in args.aipids:
            aipid = arkre.sub('', aipid)

            try: 
                dist = BagDistribClient(aipid, distsvc)
                versions = dist.list_versions()
            except DistribResourceNotFound as ex:
                log.warn("%s: no AIPs found for this ID" % aipid)
                if not args.bagsonly:
                    try:
                        create_from_describe(rmm, aipid, args.outdir, args.overwrite, cmd, log)
                    except RMMServerError as ex:
                        log.error("Failure contacting the RMM: %s", str(ex))
                        errs.append(ex)
                    except Exception as ex:
                        log.exception("Unexpected error writing record from RMM: %s", str(ex))
                        errs.append(ex)
                    if len(errs) > 5:
                        raise PDRCommandFailure(cmd, "Too many errors; aborting", 8)
                continue
            except DistribServerError as ex:
                msg = "Distrib service error querying AIP %s: %s" % (aipid, str(ex))
                log.error(msg)
                errs.append(ex)
                if len(errs) > 5:
                    raise PDRCommandFailure(cmd, "Too many errors; aborting", 8)
                continue
            except Exception as ex:
                msg = "Unexpected error while querying for AIPID %s: %s" % (aipid, str(ex))
                log.exception(msg)
                raise PDRCommandFailure(cmd, msg, 8)

            if args.latestonly:
                versions.sort(key=Version, reverse=True)
                versions = [versions[0]]

            for ver in versions:
                if args.inclvers and ver not in args.inclvers:
                    if args.verbose:
                        log.info("%s: Skipping version %s on request", aipid, ver)
                    continue

                outrec = os.path.join(args.outdir, "%s-v%s.json" % (aipid, dotre.sub('_', ver)))
                if not args.overwrite and os.path.exists(outrec):
                    # we've already got this one
                    if args.verbose:
                        log.info("Skipping %s; already exists", os.path.basename(outrec))
                    continue
                
                bagname = dist.head_for_version(ver)
                if args.verbose:
                    log.info("Writing %s...", os.path.basename(outrec))

                serbag = None
                outbag = None
                try: 
                    dist.save_bag(bagname, tmpdir)
                    serbag = os.path.join(tmpdir, bagname)
                    outbag = ser.deserialize(serbag, tmpdir)
                    bag = NISTBag(outbag)
                    write_json(bag.nerdm_record(), outrec)

                except Exception as ex:
                    msg = "Failed to extract metadata from %s %s: %s" % (aipid, ver, str(ex))
                    if len(errs) == 0:
                        log.exception(msg)
                    else:
                        log.error(msg)
                    errs.append(ex)
                    if len(errs) > 5:
                        raise PDRCommandFailure(cmd, "Too many errors; aborting", 8)
                    continue

                finally:
                    if serbag and os.path.exists(serbag):
                        os.remove(serbag)
                    if outbag and os.path.exists(outbag):
                        shutil.rmtree(outbag)
                

def get_all_aip_ids(rmm, cmd=None):
    """
    return a list of AIP IDs via a constraint-free search to the RMM.  
    :param MetadataClient|str rmm:  either an instance of MetadataClient connected to the RMM API service
                                    or (as a str value) the base URL for that service.
    :param str cmd:  the name that this CLI is running under (default: recover)
    """
    if not cmd:
        cmd = default_name
    if isinstance(rmm, str):
        # rmm is the base URL for the RMM API
        rmm = MetadataClient(rmm)

    try:
        return [r.get('aipid') or arkre.sub('', r.get('ediid'))
                for r in rmm.search()]
    except RMMServerError as ex:
        raise PDRCommandFailure(cmd, "Trouble accessing metadata service: "+str(ex), 5)


def _process_args(args, config, cmd, log=None):

    aipids = []
    if args.idfile:
        try:
            with open(args.idfile) as fd:
               aipids.extend(read_ids_from_file(fd))
        except Exception as ex:
            raise PDRCommandFailure(cmd, "Failed to read AIP IDs from file: %s: %s" % (args.idfile, str(ex)),4)
    if args.aipids:
        aipids.extend(args.aipids)
    args.aipids = aipids
    if len(args.aipids) == 0:
        raise PDRCommandFailure(cmd, "No AIP IDs provided", 2)

    if not args.outdir:
        if not args.outlist and (len(args.aipids) > 10 or 'ALL' in args.aipids):
            print("%s: many output files are expected; -d is recommended.")
            print("    Hit Ctrl-C within 10 seconds to cancel...")
            time.sleep(10)
            print("    ...proceeding")
        args.outdir = "."
    elif not os.path.exists(args.outdir):
        parent = os.path.dirname(args.outdir)
        if parent and not os.path.exists(parent):
            raise PDRCommandFailure(cmd, "Unable to create output directory, %s: parent not found"
                                    % args.outdir, 4)
        if log and args.verbose:
            log.info("Creating output directory, %s", args.outdir)
        try:
            os.mkdir(args.outdir)
        except OSError as ex:
            raise PDRCommandFailure(cmd,
                                    "Unable to create output directory, %s: %s" % (args.outdir, str(ex)), 4)

    if args.inclvers:
        args.inclvers = [v.replace('_', '.') for v in args.inclvers]

    # ensure the PDR's services base URLs
    process_svcep_args(args, config, cmd, log)

def _check_url(url):
    purl = urlparse(url)
    if not purl.netloc or not purl.scheme:
        raise ValueError("absolute URL required")
    if purl.scheme != "http" and purl.scheme != "https":
        raise ValueError("unsupported URL scheme: "+purl.scheme)

def read_ids_from_file(fd):
    """
    return a list of the AIP IDs found in the given open file
    :param file-like fd:   a file-like instance representing an opened file containing AIP IDs.  The 
                           caller is responsible for closing this file. 
    :raises OSError: if a failure occurs while reading
    """
    out = []
    for line in fd:
        out.extend(line.strip().split())
    return out

def write_aipid_list(aipids, tofile, config=None, log=None):
    """
    write the given list of AIP IDs to the named file
    :param list aipids:  the list of strings to write out
    :param str  tofile:  the path to the file to write to
    :param dict config:  the configuration this command is running under
    :param Logger  log:  the Logger instance to send messages to; if None, this function will be silent
    :raises StateException:  if the output file exists and the configuration does not allow it to be 
                             overwritten
    :raises OSError:     if failures occur while writing (including the file cannot be opened due to, 
                         say, permission or existance issues)
    """
    if not config:
        config = {}
    if config.get("allow_idfile_overwrite", False) and os.path.exists(tofile):
        raise StateException(tofile+": file exists; won't overwrite.")
    if log:
        log.info("Writing AIP IDs to %s", tofile)
    with open(tofile, 'w') as fd:
        for aid in aipids:
            fd.write(aid)
            fd.write("\n")

def create_from_describe(rmm, aipid, destdir, overwrite=False, cmd=None, log=None):
    """
    write out a version 1.0.0 record for the given AIP ID based from its record in the RMM
    :param MetadataClient|str rmm:  either an instance of MetadataClient connected to the RMM API service
                                    or (as a str value) the base URL for that service.
    :param str destdir:  the directory to write the file to
    :param bool overwrite:  if True and the output file already exists, overwrite it (default: False)
    :param str cmd:  the name that this CLI is running under (default: recover)
    :param Logger log:  the Logger instance to send messages to; if None, this function will be silent
    :rtype str:
    :return:  the path to the output file containing the record
    :raises RMMServerError:  if there was problem accessing the RMM
    :raises StateException:  if there was an error while writing out the JSON record
    """
    if not cmd:
        cmd = default_name
    if isinstance(rmm, str):
        # rmm is the base URL for the RMM API
        rmm = MetadataClient(rmm)

    outf = os.path.join(destdir, "%s-v1.0.0.json" % aipid)
    if not overwrite and os.path.exists(outf):
        if log:
            log.info("Skipping %s; already exists", os.path.basename(outf))
        return outf

    nerdm = rmm.describe(aipid)  # may raise exception
    if log:
        log.info("Writing %s (from RMM record)...", os.path.basename(outf))
    write_json(nerdm, outf)
    
