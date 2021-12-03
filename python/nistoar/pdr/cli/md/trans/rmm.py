"""
CLI NERDm transformation commanad that converts a record to a form
used by the PDR.  (See also ``nistoar.nerdm.convert.rmm``.)
"""
import logging, argparse, sys, os, re, json

from nistoar.nerdm.convert import rmm
from nistoar.nerdm import utils as nerdmutils
from nistoar.pdr.distrib import DistribServerError, DistribResourceNotFound
from nistoar.pdr.constants import ARK_PFX_PAT
from nistoar.pdr import config as cfgmod
from ._comm import define_comm_trans_opts, process_svcep_args, define_comm_md_opts
from ._comm import _get_record_for_cmd, _write_record_for_cmd, PDRCommandFailure

_ark_pfx_re = re.compile(ARK_PFX_PAT)

default_name = "rmm"
help = "convert a NERDm record to a three-part form useful for loading into the RMM"
description = """
  This comand will read in NERDm record (from a file or standard input) or retrieve it from the PDR
  and convert it to one of two forms useful for loading into the RMM.  If the output is specified 
  (via -o) as an existing directory, the output will be written to three files in three subdirectories
  of that directory (see also -b); otherwise, it will get written to one file containing a JSON record 
  with three parts in the 'record', 'version', and 'releaseSet' properties. 
"""

def load_into(subparser, current_dests, as_cmd=None):
    """
    load this command into a CLI by defining the command's arguments and options
    :param argparser.ArgumentParser subparser:  the argument parser instance to define this command's 
                                                interface into it 
    :param set current_dests:  the current set of destination names that have been defined so far; this
                               can indicate if a parent command has defined required options already
    :param str as_cmd:  the command name that this command is being loaded as (ignored)
    :rtype: None
    """
    p = subparser
    p.description = description

    if 'src' not in current_dests:
        define_comm_trans_opts(p)
    if 'rmmbase' not in current_dests:
        define_comm_md_opts(p)
    p.add_argument("-b", "--out-base-name", action="store", metavar="NAME", type=str, dest="outbase",
                   help="write the output to a file with the given base name, if not provided and "+
                        "--output-file specifies a directory (which causes the output to be split"+
                        "into 3 files), a base name is used that is based on the input identifiers.")
    p.add_argument("--keep-pdr-eps", action="store_true", dest="pdreps",
                   help="When it is necessary to augment the output record with URLs pointing, use the "+
                        "standard NIST PDR enpoints; with out this option, endpoints based on the "+
                        "-U/--services-base-url will be used")

    return None

def execute(args, config=None, log=None):
    """
    execute this command: convert the input record to the latest NERDm schemas
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

    # may raise PDRCommandFailure
    rec = _get_record_for_cmd(args, cmd, config, log)

    defver = "1.0.0"
    if args.version:
        defver = args.version
    pubeps = {}
    if not args.pdreps:
        pubeps["portalBase"] = args.srvrbase

    cvtr = rmm.NERDmForRMM(log, pubeps=pubeps)
    rec = cvtr.to_rmm(rec, defver)

    if args.outfile and os.path.isdir(args.outfile):
        # write out as three files under a root directory
        _write_split_files(rec, args.outfile, args, cmd, config, log)
        
        # raise PDRCommandFailure(cmd, "3-file output not yet implemented", 2)
    else:
        # write out as a single file
        if args.outbase and not args.outfile:
            args.outfile = args.outbase
            if rec['record'].get('version'):
                ver = rec['record']['version'].replace('.','_')
                if not args.outfile.endswith(ver) and not args.outfile.endswith(rec['version']['version']):
                    args.outfile += "-v" + ver
            args.outfile += ".json"
            if os.path.exists(args.outfile):
                raise PDRCommandFailure(cmd, "%s: already exists (won't overwrite)" % args.outfile)
        _write_record_for_cmd(rec, args, cmd, config, log)

def _process_args(args, config, cmd, log):
    if (args.filesrc or not (args.aipsrc or args.mdsrc)) and args.version:
        log.warning("-V/--version argument ignored when reading from file or stdin")
    process_svcep_args(args, config, cmd, log)

def _write_split_files(rec, rootdir, args, cmd, config, log):
    if not os.path.isdir(rootdir):
        raise PDRCommandFailure(cmd, "Failed to write data to %s: does not exist as a directory" %
                                rootdir)

    basen = args.outbase
    if not basen:
        # set base name based on the record's AIP ID
        basen = rec['record'].get('pdr:aipid', rec['record'].get('ediid', rec['record'].get('@id')))
        basen = _ark_pfx_re.sub('', basen)  # strip off ark prefix

    for part in "record version releaseSet".split():
        odir = os.path.join(rootdir, part + "s")
        if not os.path.exists(odir):
            try:
                os.mkdir(odir)
            except OSError as ex:
                raise PDRCommandFailure(cmd, "%s: unable to create as dir: %s" % (odir, str(ex)))

        ofile = basen
        if part == "version" and rec['version'].get('version'):
            ver = rec['version']['version'].replace('.','_')
            if not ofile.endswith(ver) and not ofile.endswith(rec['version']['version']):
                ofile += "-v" + ver
        ofile = os.path.join(odir, ofile+".json")

        # In this mode, a pre-existing part file will only get overwritten if its version is older 
        # than in the new one.
        if os.path.isfile(ofile):
            try:
                with open(ofile) as fd:
                    oldrec = json.load(fd)
            except Exception:
                raise PDRCommandFailure(
                    cmd, "%s: file already exists with unexpected content (won't overwrite)")
            if 'releaseSet' in oldrec or 'record'in oldrec or 'title' not in oldrec:
                raise PDRCommandFailure(
                    cmd, "%s: file already exists with unexpected content (won't overwrite)")
            if part != "version" and 'version' in oldrec and \
               nerdmutils.cmp_versions(oldrec['version'], rec[part].get('version', '1.0.0')) > 0:
                log.log(cfgmod.NORMAL,
                        "%s: existing %s record has a newer/equal version value: won't overwrite",
                        basen, part)
                continue

        # write the file
        try:
            with open(ofile, 'w') as fd:
                json.dump(rec[part], fd, indent=4, separators=(',', ': '))
        except OSError as ex:
            raise PDRCommandFailure(cmd, "Failed to write data to %s: %s" % (ofile, str(ex)))

