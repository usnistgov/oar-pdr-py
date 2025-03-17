"""
a command-line interface to health checks.  The :py:func:`main` function provides the implementation
"""
import argparse, sys, os, re, logging, textwrap, traceback as tb
from argparse import ArgumentParser

import yaml, requests

from nistoar.base import config
from nistoar.base.config import ConfigurationException
# from ...notify.service import TargetManager, NotificationService
# from ...notify.cli import StdoutMailer, StdoutArchiver, Failure
from nistoar.pdr.notify.cli import Failure
from .syncer import NSDSyncer
from .. import NSDException

prog = re.sub(r'\.py$', '', os.path.basename(sys.argv[0]))

def define_options(progname):
    """
    return an ArgumentParser instance that is configured with options
    for the command-line interface.
    """
    description = "Retrieve organization and people data from the NIST Staff Directory, " \
                  "cache it to local disk, and trigger a reload of the DBIO's mirror database"
    epilog = None
    
    parser = ArgumentParser(progname, None, description, epilog)

    parser.add_argument('-d', '--output-dir', type=str, dest='datadir', metavar='DIR', default=None,
                        help="the directory where the org and people data will be cached")
    parser.add_argument('-c', '--config-file', type=str, dest='cfgfile', metavar='FILE', 
                        help="a file containing the configuration to use.  The schema can be "+
                             "either for NSD data, NSD web app, or the DBIO app configuration")
    parser.add_argument('-C', '--config-url', type=str, dest='cfgurl', metavar='URL',
                        help="a URL that the configuration should be retrieved from.  If not "+
                             "provided, the OAR_CONFIG_URL environment variable will be "+
                             "consulted.  This option is ignored if -c/--config-file is provided.")
    parser.add_argument('-t', '--trigger-url', type=str, dest='trigrurl', metavar='URL',
                        help="the endpoint URL for the DBIO's staff directory service; this overrides "+
                             "the 'trigger' config property")
    parser.add_argument('-T', '--no-trigger', action='store_false', dest='trigrurl',
                        help="After fetch the NSD data, do not trigger a reload of the DBIO database "+
                             "(over-rides -t).")
    parser.add_argument('-l', '--logfile', action='store', dest='logfile', type=str, metavar='FILE',
                        help="write messages that normally go to standard error to FILE as well.  "+
                             "If -q is also specified, the messages will only go to the logfile")
    parser.add_argument('-v', '--verbose', action='store_true', dest='verbose',
                        help="print more (debug) messages to standard error and/or the log file")
    parser.add_argument('-q', '--quiet', action='store_true', dest='quiet',
                        help="suppress all error and warning messages to standard error")
    parser.add_argument('ous', metavar='OU', type=str, nargs='*', default=[],
                        help="OU abbreviation(s) to restrict the people data to.  If not provided, "
                             "all OUs will be included")
    return parser

def request_reload(dbionsdep, authtoken=None):
    """
    trigger a reload
    """
    hdrs = {}
    if authtoken:
        hdrs['Authorization'] = f"Bearer {authtoken}"
    try: 
        resp = requests.request("LOAD", dbionsdep, headers=hdrs)
        if resp.status_code == 401:
            raise Failure("Valid trigger auth token not provided")
        if resp.status_code == 405:
            raise Failure("Trigger API appears not to be supported (check trigger URL)")
        if resp.status_code >= 300 or resp.status_code < 200:
            raise Failure(f"Unexpected DBIO server response: {resp.reason} ({resp.status_code})", 2)
    except Exception as ex:
        raise Failure(f"Failed to trigger reload on mirror: {str(ex)}", 2)

def main(progname, args):
    """
    fetch the staff data and trigger an upload to our mirror
    """
    parser = define_options(progname)
    opts = parser.parse_args(args)

    rootlog = logging.getLogger()
    level = (opts.verbose and logging.DEBUG) or logging.INFO
    if opts.logfile:
        # write messages to a log file
        fmt = "%(asctime)s " + (opts.origin or prog) + ".%(name)s %(levelname)s: %(message)s"
        hdlr = logging.FileHandler(opts.logfile)
        hdlr.setFormatter(logging.Formatter(fmt))
        hdlr.setLevel(logging.DEBUG)
        rootlog.addHandler(hdlr)
        rootlog.setLevel(level)

    # configure a default log handler
    if not opts.quiet:
        fmt = prog + ": %(levelname)s: %(message)s"
        hdlr = logging.StreamHandler(sys.stderr)
        hdlr.setFormatter(logging.Formatter(fmt))
        hdlr.setLevel(logging.DEBUG)
        rootlog.addHandler(hdlr)
        rootlog.setLevel(level)
    elif not rootlog.handlers:
        rootlog.addHandler(logging.NullHandler())

    # look for a provided configuration file
    cfg = {}
    if opts.cfgfile:
        try:
            cfg = read_config(opts.cfgfile)
        except EnvironmentError as ex:
            raise Failure("problem reading config file, {0}: {1}"
                          .format(opts.cfgfile, ex.strerror)) from ex
    elif config.service:
        cfg = config.service.get("midas-dbio")
    else:
        raise Failure("Unable to locate configuration; set OAR_CONFIG_SERVICE or use -c")

    if cfg.get('services', {}).get('nsd'):
        cfg = cfg['services']['nsd']
    if cfg.get('data'):
        cfg = cfg['data']

    try:
        syncer = NSDSyncer(cfg)
        syncer.cache_data(opts.datadir)

        if opts.trigrurl is None and cfg.get("trigger", {}).get("service_endpoint"):
            opts.trigrurl = cfg.get("trigger", {}).get("service_endpoint")
        if opts.trigrurl:
            request_reload(opts.trigrurl, cfg.get("trigger", {}).get("auth_token"))

    except ConfigurationException as ex:
        raise Failure(str(ex)) from ex
    except NSDException as ex:
        raise Failure("Failed to update NSD data: "+str(ex), 2) from ex

def read_config(filepath):
    """
    read the configuration from a file having the given filepath

    :except Failure:  if the contents contains syntax or format errors
    :except IOError:  if a failure occurs while opening or reading the file
    """
    try:
        return config.load_from_file(filepath)
    except (ValueError, yaml.reader.ReaderError, yaml.parser.ParserError) as ex:
        raise Failure("Config parsing error: "+str(ex), 3, ex)
