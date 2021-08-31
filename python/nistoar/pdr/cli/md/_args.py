"""
This module defines some reusable functions shared by the md command package and its subcommand modules.
(This avoids circular imports!)
"""
from urllib.parse import urlparse, urljoin

from nistoar.pdr.cli import PDRCommandFailure

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
            raise PDRCommandFailure(cmd, "Bad PDR URL provided: %s: %s" % (args.srvrbase, str(ex)), 7)
        else:
            raise ConfigurationException("Config parameter, nist_pdr_base: bad value: %s: %s" %
                                         (args.srvrbase, str(ex)))
    args.srvrbase = srvrbase
    if args.srvrbase and not args.srvrbase.endswith('/'):
        args.srvrbase += '/'

    if not args.distbase:
        args.distbase = config.get("pdr_dist_base", "od/ds/")
    args.distbase = urljoin(args.srvrbase, args.distbase)
    try:
        check_url(args.distbase)
    except ValueError as ex:
        raise PDRCommandFailure(cmd, "Bad distrib service URL: %s: %s" % (args.distbase, str(ex)), 7)

    if not args.rmmbase:
        args.rmmbase = config.get("pdr_rmm_base", "rmm/")
    args.rmmbase = urljoin(args.srvrbase, args.rmmbase)
    try:
        check_url(args.rmmbase)
    except ValueError as ex:
        raise PDRCommandFailure(cmd, "Bad RMM service URL: %s: %s" % (args.rmmbase, str(ex)), 7)

def check_url(url):
    """
    Do some sanity checking on the service URL endpoint.  ValueError is raise if problems are detected.
    """
    purl = urlparse(url)
    if not purl.netloc or not purl.scheme:
        raise ValueError("absolute URL required")
    if purl.scheme != "http" and purl.scheme != "https":
        raise ValueError("unsupported URL scheme: "+purl.scheme)

