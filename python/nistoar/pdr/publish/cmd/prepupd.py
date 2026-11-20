"""
CLI command that will prepare an SIP bag for publishing an update to a previously published dataset.
"""
import logging, argparse, os, shutil, tempfile, re
from copy import deepcopy
from typing import Mapping
from logging import Logger
from pathlib import Path

from nistoar.base.config import ConfigurationException
from nistoar.pdr.utils.cli import CommandFailure, explain
from nistoar.pdr.exceptions import IDNotFound
from nistoar.pdr.publish.bagger.prepupd import UpdatePrepService
from nistoar.pdr.constants import ARK_NAAN, ARK_PFX_PAT
from nistoar.pdr.describe import rmm
arkpfxre = re.compile(ARK_PFX_PAT)

default_name = "prepupd"
help = "create an SIP bag as the basis for an update"
description = \
"""creates an SIP bag based on the last published version of a specified dataset to be used as 
the basis for an update.
"""
def load_into(subparser: argparse.ArgumentParser, current_dests: list=None, as_cmd: str=None):
    """
    load this command into a CLI by defining the command's arguments and options.
    :param argparser.ArgumentParser subparser:  the argument parser instance to define this command's 
                                                interface into it 
    :param list current_dests:  a list of destination names for parameters that have already been 
                                defined
    :param str as_cmd:  the subcommand name assigned to the action provided by this module
    :rtype: None
    """
    p = subparser
    p.description = description
    p.cmd = as_cmd

    p.add_argument("id", metavar="ID", type=str, 
                   help="the identifier for the dataset to create an SIP bag for.  The intended type "
                        "of identifier can be set explicitly via the -A or -P options; without them, "
                        "an ID that starts with 'ark:/' is assumed to be a PDR ID; otherwise, it is "
                        "interpreted as its AIP ID")
    p.add_argument("-A", "--aipid", action='store_true', dest='is_aipid',
                   help="the AIP identifier of the published dataset to create the bag for")
    p.add_argument("-P", "--pdrid", action='store_true', dest='is_pdrid',
                   help="the PDR identifier of the published dataset.  It must begin with "+
                        "'ark:/'")
    p.add_argument("-d", "--output-dir", "-b", "--bag-parent-dir", metavar='DIR', type=str, dest='outdir',
                   help="the directory to write the bag into; if not provided, defaults to the "+
                        "configured SIP directory for the current convention (given by -C) or the "+
                        "current working directory if an SIP directory is not configured.")
    p.add_argument("--cache-dir", metavar='DIR', type=str, dest='cachedir',
                   help="a local directory to cache retrieved head bags into")
    p.add_argument("-M", "--metadata-svc-url", metavar='URL', type=str, dest='mdsvcurl',
                   help="the base URL for the Resource Metadata Manager service used to determine the "+
                        "most recent version of a dataset that has been released.  This will override "+
                        "the service endpoint provided in the configuration")
    p.add_argument("-D", "--distrib-svc-url",  metavar='URL', type=str, dest='distsvcurl',
                   help="the base URL for the Distribution Service used to pull the last published "+
                        "head bag from which to create the SIP bag for the next version.  This will "+
                        "override the service endpoint provided in the configuration")

    return None

def process_options(args, config):
    if args.is_aipid and args.is_pdrid:
        raise CommandFailure(args.cmd, "-A/--aip-id and -P/--pdr-id are mutually exclusive", 2)

    if not args.workdir:
        args.workdir = config.get('working_dir', '.')
    args.workdir = Path(args.workdir).resolve()
    if not args.outdir:
        args.outdir = config.get('sip_bags_dir')
        if args.outdir:
            args.outdir = Path(args.outdir)
            if not args.outdir.is_absolute():
                args.outdir = args.workdir/args.outdir
        else:
            args.outdir = args.workdir

    rcfg = config.setdefault('repo_access', {})
    if args.mdsvcurl:
        rcfg.setdefault("metadata_service", {})['service_endpoint'] = args.mdsvcurl
    if args.distsvcurl:
        rcfg.setdefault("distrib_service", {})['service_endpoint'] = args.distsvcurl
    if args.cachedir:
        rcfg['headbag_cache'] = args.cachedir

def execute(args, config: Mapping=None, log: Logger=None):
    """
    execute this command: create the update SIP
    """
    if not log:
        log = logging.getLogger(default_name)
    if not config:
        config = {}

    if isinstance(args, list):
        # cmd-line arguments not parsed yet
        p = argparse.ArgumentParser()
        load_command(p)
        args = p.parse_args(args)

    process_options(args, config)
    repocfg = config.get('repo_access')

    aipid = pdrid = None
    if args.is_aipid:
        aipid = args.id
    elif args.is_pdrid or args.id.startswith('ark:/'):
        pdrid = args.id
    else:
        aipid = args.id

    pdrid, aipid, version = lookup_latest(pdrid, aipid, args, repocfg)
    if version is None:
        raise CommandFailure(args.cmd, f"{args.id} has not yet been published")

    usenm = aipid 
    if len(usenm) > 15:
        usenm = usenm[:4]+"..."+usenm[-4:]
    log = log.getChild(usenm)

    outbag = args.outdir/aipid
    if outbag.exists():
        raise CommandFailure(args.cmd, "Output bag already exists: "+outbag, 2)

    if 'headbag_cache' not in repocfg:
        cdir = tempfile.TemporaryDirectory(args.workdir, prefix=f"_headbags.{args.aipid}.")
        repocfg['headbag_cache'] = cdir

    try:
        svc = UpdatePrepService(repocfg)
        if not svc.prepper_for(pdrid, version, log, aipid).create_new_update(outbag):
            # should not happen?  lookup latest should have detected unpublished ID
            raise CommandFailure(args.cmd, "Unexpected failure to create SIP bag: not yet published?", 1)
    except CommandFailure:
        raise
    except ConfigurationException as ex:
        raise CommandFailure(args.cmd, str(ex), 8)
    except Exception as ex:
        raise CommandFailure(args.cmd, "Failed to create bag: "+str(ex), 1)

    
def lookup_latest(pdrid, aipid, args, repocfg):
    baseurl = args.mdsvcurl
    if not baseurl and repocfg and \
       repocfg.get('metadata_service',{}).get('service_endpoint'):
        baseurl = repocfg['metadata_service']['service_endpoint']
    if not baseurl:
        raise CommandFailure(args.cmd, "No metadata service configured; "
                             "unable to determine latest publicaiton (use -M)", 8)
    
    cli = rmm.MetadataClient(baseurl)

    if aipid and len(aipid) < 15:
        pdrid = f"ark:/{ARK_NAAN}/{aipid}"

    try:
        if not pdrid:
            # if aipid has the long random identifier form, it is equal to the EDI-ID
            nerd = cli._describe_ediid(aipid)
        else:
            nerd = cli.describe(pdrid)
    except IDNotFound as ex:
        return (pdrid, aipid, None)  # not published yet
    except rmm.RMMServerError as ex:
        raise CommandFailure(args.cmd, str(ex), 5)
    except Exception as ex:
        raise CommandFailure(args.cmd, "Unable to determine lastest published version: "+str(ex), 1)

    version = None   # if this remains None, then the ID has not been published yet
    if nerd:
        version = nerd.get('version', '1.0.0')
        if not pdrid:
            pdrid = nerd.get("@id")
        if not aipid:
            aipid = arkpfxre.sub('', nerd.get('ediid'))
        
    return (pdrid, aipid, version)

    
    
            
