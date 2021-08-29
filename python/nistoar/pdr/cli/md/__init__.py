"""
CLI command suite for accessing metadata from the PDR public metadata service.  See 
:py:module:`nistoar.pdr.cli` and the :program:`pdr` script for info on the general CLI infrastructure.

This module defines a set of subcommands to a commmand called (by default) "md".  These subcommands
include
  - ``recover``:  extract NERDm records from the public archival information packages (e.g. so that they 
    can be reloaded into the metadata database).

See also :py:module:`nistoar.pdr.describe.cmd` for the ``describe`` command.
"""
from urllib.parse import urlparse, urljoin

from nistoar.pdr import cli
from . import recover

default_name = "md"
help = "provide specialized access to PDR metadata"
description = \
"""This provides a suite of subcommands that provided specialized access to PDR metadata via its 
public APIs (apart from identifier resolution; see the describe command)."""

def load_into(subparser, current_dests=None):
    """
    load this command into a CLI by defining the command's arguments and options.
    :param argparser.ArgumentParser subparser:  the argument parser instance to define this command's 
                                                interface into it 
    """
    p = subparser
    p.description = description
    define_comm_md_opts(p)

    if not as_cmd:
        as_cmd = default_name
    out = cli.CommandSuite(as_cmd, p, current_dests)
    out.load_subcommand(recover)
    return out

def define_comm_md_opts(subparser):
    """
    define some arguments that apply to all md subcommands.  These are:
     - --services-base-url - specifies the PDR's base URL for all APIs
     - --dist-base-url     - specifies the distribution service endpoint URL
     - --rmm-base-url      - specifies the RMM service endpoint URL
    """
    p = subparser
    p.add_argument("-U", "--services-base-url", metavar="BASEURL", type=str, dest='srvrbase',
                   help="use this base URL as the basis for the PDR's RMM and distribution services.  "+
                        "The default is https://data.nist.gov/.")
    p.add_argument("-D", "--dist-base-url", metavar="BASEURL", type=str, dest='distbase',
                   help="use this base URL for the distribution service from which AIPs should be "+
                        "donwloaded.  The default is is BASE/od/ds")
    p.add_argument("-R", "--rmm-base-url", metavar="BASEURL", type=str, dest='rmmbase',
                   help="use this base URL for the RMM service that should be used when ALL is requested as "+
                        "an AIPID.  The default is is BASE/rmm")

    return None

def process_args(args, config, cmd, log=None):
    """
    process the common md options.  Currently, this only normalizes the service endpoing URLs (via 
    ``process_svcep_args()``)
    """
    process_svcep_args(args, config, cmd, log)

def process_svcep_args(args, config, cmd, log=None):
    """
    normalize the service endpoint URLs set in the given set of parsed arguments.  
    """
    # ensure the args.srvrbase, the PDR's services base URL
    srvrbase = args.srvrbase
    if not srvrbase:
        srvrbase = config.get("nist_pdr_base", "https://data.nist.gov/")

    try:
        check_url(srvrbase)
    except ValueError as ex:
        if args.srvrbase:
            raise PDRCommandFailure(cmd, "Bad PDR URL provided: %s: %s" % (args.srvrbase, str(ex)))
        else:
            raise ConfigurationException(cmd, "Config parameter, nist_pdr_base: bad value: %s: %s" %
                                         (args.srvrbase, str(ex)), 3)
    args.srvrbase = srvrbase
    if args.srvrbase and not args.srvrbase.endswith('/'):
        args.srvrbase += '/'

    if not args.distbase:
        args.distbase = config.get("pdr_dist_base", "od/ds/")
    args.distbase = urljoin(args.srvrbase, args.distbase)
    try:
        check_url(args.distbase)
    except ValueError as ex:
        raise PDRCommandFailure(cmd, "Bad distrib service URL: %s: %s" % (args.distbase, str(ex)))

    if not args.rmmbase:
        args.rmmbase = config.get("pdr_rmm_base", "od/ds/")
    args.rmmbase = urljoin(args.srvrbase, args.rmmbase)
    try:
        check_url(args.rmmbase)
    except ValueError as ex:
        raise PDRCommandFailure(cmd, "Bad RMM service URL: %s: %s" % (args.rmmbase, str(ex)))

def check_url(url):
    """
    Do some sanity checking on the service URL endpoint.  ValueError is raise if problems are detected.
    """
    purl = urlparse(url)
    if not purl.netloc or not purl.scheme:
        raise ValueError("absolute URL required")
    if purl.scheme != "http" and purl.scheme != "https":
        raise ValueError("unsupported URL scheme: "+purl.scheme)

