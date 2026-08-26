"""
Support for monitoring publishing requests so that MIDAS DAP records can be provided with 
status updates.

This modules includes support for running the monitor as stand-alone process launch from the 
command-line.  
"""
import os, sys, logging, argparse
from logging import Logger
from typing import Mapping

from .. import dbio
from ..dbio import status as mstatus
from nistoar.pdr.publish.service import status as pstatus
from nistoar.pdr.publish.service.monitor import LocalPublishingMonitor
from nistoar.base import config
from nistoar.pdr.utils.cli import CommandFailure
from nistoar.pdr.utils.prov import Agent

class MIDASPublishingMonitor(LocalPublishingMonitor):
    """
    A specialization of the :py:class:`nistoar.pdr.publish.service.monitor.LocalPublishingMonitor`
    that will update an SIP's corresponding MIDAS DAP record.  
    """
    def __init__(self, dbclient_factory: dbio.DBClientFactory, statusdir: str, qfile: str,
                 dapconfig: Mapping={}, who: Agent=None, log: Logger=None):
        """
        initialize the monitor with an internal :py:class:`~nistoar.midas.dbio.project.ProjectService`
        that will be used to update the status of MIDAS records going through the publishing service
        (i.e. "in press").  

        Implementation Note: this internally creates a generic ``ProjectService`` rather than a 
        :py:class:`~nistoar.midas.dap.service.mds3.DAPService`.  Since this monitor only updates 
        the status, it does not need to be attached to a particular convention.  

        :param DBClientFactory dbclient_factory:  the factory needed to the proper DBIO client instance 
                                    to back the ``ProjectService``.
        :param str|Path statusdir:  the directory where publishing status files are recorded
        :param str|Path     qfile:  the monitor queue file to process
        :param int      cyclesecs:  the number of seconds to allow to pass before starting a new
                                    before updating the status of the SIPs in the queue
                                    (Default: 10 minutes).
        :param dict     dapconfig:  the DAP configuration to configure the ``ProjectService``.  
        :param who          Agent:  the publisher user agent that is authorized to update DAP records 
                                    that are currently being published.  If not set, the generic admin
                                    identity will be used.
        :param Logger         log:  the logger to use for log messages
        """
        if not log:
            log = Logger.getLogger("MIDASPublishingMonitor")
        self.log = log

        if not who:
            who = Agent("MIDASPublishingMonitor", Agent.AUTO, dbio.AUTOADMIN, Agent.ADMIN)

        self.projsvc = ProjectService(dbio.DAP_PROJECTS, dbclient_factory, dapconfig, who,
                                      self.log.getChild(dbio.DAP_PROJECTS))

        super(MIDASPublishingMonitor, self).init(statusdir, queue_file, self.update_status,
                                                 cyclesecs, log)

    def update_status(self, sipid: str, state:str , message: str):
        """
        process a detected change in the publishing state and update the MIDAS record status accordingly. 
        """
        acts_on = [pstatus.SUBMITTED, pstatus.PROCESSING, pstatus.PUBLISHED, pstatus.FAILED, pstatus.ONHOLD]
        if state not in acts_on:
            return False

        try:
            prec = self.projsvc.get_record(sipid)
        except ObjectNotFound as ex:
            self.log.warning("Failed to find record %s currently under watch", sipid)
        except NotAuthorized as ex:
            self.log.error("Not authorized to read record %s as %s; has it really been submitted yet?",
                           sipid, str(self.projsvc.user))

        if state == pstatus.SUBMITTED:
            prec.status.set_state(mstatus.ACCEPTED)
            prec.status.message = message
        elif state == pstatus.PROCESSING:
            prec.status.set_state(mstatus.INPRESS)
            prec.status.message = "Publication is being preserved and published"
        elif state == pstatus.PUBLISHED:
            prec.status.set_state(mstatus.PUBLISHED)
            prec.status.message = message
        elif state == pstatus.FAILED:
            prec.status.set_state(mstatus.UNWELL)
            prec.status.message = "A publication failure was encountered; admins have been alerted"
        elif state == pstatus.ONHOLD:
            prec.status.set_state(mstatus.INPRESS)
            prec.status.message = "Publication has been paused; admins have been alerted"
        try:
            prec.save()
        except dbio.NotAuthorized as ex:
            self.log.error("%s not authorized to update status of %s; has it really be submitted yet?",
                           str(self.projsvc.user), sipid)

description = """\
Continuously monitor the publishing progress of SIPs on behalf of the MIDAS
service 

This script will repeatedly loop through a configured queue of SIPs submitted to
the publishing (PDP) service, checking their progress, and updating the
corresponding MIDAS records accordingly.  The looping will, by default, occur in
the process foreground (running as a daemon is not yet supported).

This command is intended for use when the MIDAS and publishing services share a 
filesystem where the latter stores its status.  This monitor will consult the 
status files directly (rather than, say, going through the publishing service's 
web API) to determine the latest publishing status of each SIP in the queue.
"""
epilog="""\
CONFIGURATION

The monitor process, by default, pulls its configuration from a live
configuration service based on the usual environment variables,
OAR_CONFIG_SERVICE and OAR_CONFIG_ENV.  This can be over-ridden either by
providing the configuration via --config or by via other command line options;
if the service is not available, the options must used to provide the
configuration parameters.

The supported configuration parameters are:

  queue_file -- same as --queue-file

  status_dir -- same as --statusdir

  cycle_time -- same as --cycle-time

  dap_service -- a dictionary configuring details about the DAP service; its
      parameters are those supported by the ProjectService class
      (nistoar.midas.dbio.project.ProjectService).  If not provided, an attempt 
      to determine this from the "midas-dbio" configuration pulled from the 
      configuration service.  If provided, it should at least include a 'dbio'
      configuration.

ENVIRONMENT

Several environmnent variables can be set to affect the monitor's behavior, and 
some can be used to override the corresponding information in the configuration.  
These include:

  OAR_CONFIG_SERVICE -- the endpoint for the configuration service, not 
      including application name.  If this is not set, the --config option must
      be used.  

  OAR_CONFIG_ENV -- the platform environment label to use when pulling
      configuration data

  OAR_CONFIG_TIMEOUT -- the maximum time, in seconds, to allow for the
      configuration service to come up at start-up before giving up waiting

  OAR_MONGODB_URL -- same as --dbio-url; overrides configuration (but not the
      command-line option).

  OAR_MONGODB_HOST, OAR_MONGODB_PORT, OAR_MONGODB_USER, OAR_MONGODB_PASS --
      parameters for constructing the OAR_MONGODB_URL is not set.  For these 
      to be used,  the configuration paraemeter dap_service.dbio.type must be
      set to "mongo".
      
"""

def define_options(progname):
    parser = argparse.ArgumentParser(progname, description=description, epilog=epilog,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("-C", "--cycle-time", type=int, dest='cycletime', metavar='SECS', 
                        help="the number of seconds to allow to pass between publishing status checks "+
                             "(over-riding the configuration; default: 10 minutes)")
    parser.add_argument("-S", "--statusdir", type=str, dest='statusdir', metavar='DIR',
                        help="the location of the publishing service's status directory (over-riding "+
                             "the configuration)")
    parser.add_argument("-Q", "--queue-file", type=str, dest='qfile', metavar='FILE',
                        help="the location of the queue containing submitted SIPs that should be monitored "+
                             "(over-riding the configuration).")
    parser.add_argument("-c", "--config", type=str, dest='conf', metavar='FILE',
                        help="read configuration from FILE (see CONFIGURATION section below for details)")
    parser.add_argument("--until-empty", action='store_true', dest='tillempty',
                        help="stop monitoring and exit when the queue becomes empty (over-rides --stop-after)")
    parser.add_argument("--stop-after", type=int, dest='stopafter', metavar='COUNT',
                        help="stop monitoring and exit after COUNT loops through the queue.  If COUNT is "+
                             "negative, monitoring will stop when the queue is empty (see also "+
                             "--until-empty).  If COUNT is 0 (default), the monitor will run forever.")
    parser.add_argument("--dbio-url", type=str, dest='dburl', metavar='DBURL', default='inmem',
                        help="the URL for accessing the DBIO database.  If the URL starts with 'mongodb://', "+
                             "it will be taken as a MongoDB URL (it should include the database name and "+
                             "user credentials).  If it starts with 'file:', a filesystem-based backend will "+
                             "be assumed with the rest of the URL providing the root path to the DBIO "+
                             "directory.  If it equals 'inmem' (the default when no configuration is "+
                             "provided), an in-memory implementation will be assumed; this can be used for "+
                             "testing.")
    parser.add_argument("-l", "--logfile", type=str, dest='logfile', metavar='FILE', 
                        help="log messages to FILE, over-riding the configured logfile.  Use of this option "+
                             "also over-rides 'logserver' and 'logdir' in the configuration.")
    parser.add_argument("-D", "--debug", action="store_true", dest='debug',
                        help="send DEBUG level messages to the log file")
    parser.add_argument("-v", "--verbose", action="store_true", dest='verbose',
                        help="print all messages to the terminal as well as the configured logfile")
    parser.add_argument("-A", "--actor-id", type=str, dest="actor", metavar='USERID',
                        help="The identity to use when updating the status of MIDAS records.  This identity "+
                             "must be authorized to update MIDAS records submitted to the publishing "+
                             "service.  If not provided, the generic admin identity will be used.")
    parser.add_argument("--monitor-config-name", type=str, dest="moncfgname", default="midas-pubmon",
                        metavar="NAME",
                        help="If --config is not specified, pull the configuration from the live configuration "+
                             "service using NAME (default: midas-pubmon)")
    parser.add_argument("--midas-config-name", type=str, dest="midascfgname", default="midas-dbio",
                        metavar="NAME",
                        help="If the monitor configuration (whether retrieved from the configuration service "+
                             "or provided via --config) does not have a 'dap_service' property (which "+
                             "configures the DBIO project service used to update MIDAS record statuses), "+
                             "the 'dap_service' configuration will by contructed from parameters retrieved "+
                             "from the configuration service using NAME (default: midas-dbio)")

    return parser

def main(progname, args):
    """
    Launch the SIP publishing monitor on behalf of MIDAS.
    """
    parser = define_options(progname)
    opts = parser.parse_args(args)

    if opts.conf:
        cfg = config.resolve_configuration(opts.conf)
        
    elif config.service:
        if not opts.moncfgname:
            raise cli.CommandFailure("--monitor-config-name: empty value provided", 2)
            
        config.service.wait_until_up(int(os.environ.get('OAR_CONFIG_TIMEOUT', 10)),
                                     True, sys.stderr)
        cfg = config.service.get(opts.moncfgname, {})
    else:
        cfg = {}  # there better be some CL options provided!

    if opts.cycletime is None:
        opts.cycletype = cfg.get('cycle_time', 600)  # default: ten minutes
    if opts.stopafter is None:
        opts.stopafter = cfg.get('stop_after', 0)    # default: run forever
    if opts.tillempty:
        opts.stopafter = -1

    if not opts.statusdir:
        opts.statusdir = cfg.get('status_dir')
    if not opts.statusdir:
        raise CommandFailure("No queue_file specified/configured (try --status-dir)", 2)
    if not os.path.isdir(opts.statusdir):
        raise CommandFailure(f"{opts.statusdir}: status dir does not exists", 6)
        
    if not opts.qfile:
        opts.qfile = cfg.get('queue_file')
    if not opts.qfile:
        raise CommandFailure("No queue_file specified/configured (try --queue-file)", 2)

    configure_log(opts, cfg, progname)

    who = None
    if opts.agentid:
        who = Agent("MIDASPublishingMonitor", Agent.AUTO, opts.agentid, Agent.ADMIN)

    dapcfg = cfg.get('dap_service')
    if dapcfg is None:
        if config.service:
            if not opts.moncfgname:
                raise cli.CommandFailure("--midas-config-name: empty value provided", 2)
            dapcfg = config.service.get(opts.moncfgname, {})
        else:
            # it's actually likely that no DAP-specific configuration is needed
            dapcfg = {}
        if 'services' in dapcfg:
            # We have a midas-dbio configuration; extract the dap service configuration
            extracted = ServiceAppFactory(dapcfg, {}).config_for_convention("dap", "def")
            dapcfg = config.merge_config(extracted, dapcfg.get('dbio', {}))

    dbfact = create_dbfactory(dapcfg, opts.dburl)
    log = logging.getLogger(progname)
    monitor = MIDASPublishingMonitor(dbfact, opts.statusdir, opts.qfile, dapcfg, who, log)

    # TODO: enable launching as daemon
    monitor.monitor(opts.stopafter)
    return opts

def create_dbfactory(cfg: Mapping, dburl: str=None):
    if not dburl:
        dburl = os.environ.get('OAR_MONGODB_URL')

    dbcfg = cfg.get('dbio', {})
    if dburl:
        if dburl == "inmem":
            dbcfg['factory'] = "inmem"
        elif dburl.startswith("file:"):
            dbcfg['factory'] = "fsbased"
            dbcfg['db_root_dir'] = dburl.split(':', 1)[-1]
        elif dburl.startswith("mongodb:"):
            dbcfg['factory'] = "mongo"
            dbcfg['db_url'] = dburl
        else:
            raise CommandFailure("Unrecognized DBIO URL: "+dburl, 2)

    dbtype = dbcfg.get('factory', 'inmem') 
    if dbtype == "mongo":
        dburl = dbcfg.get('db_url')
        if not dburl:
            port = ":%s" % os.environ.get("OAR_MONGODB_PORT", dbcfg.get("port", "27017"))
            user = os.environ.get("OAR_MONGODB_USER", dbcfg.get("user"))
            cred = ""
            if user:
                pasw = os.environ.get("OAR_MONGODB_PASS", dbcfg.get("pw", os.environ.get("OAR_MONGODB_USER")))
                cred = "%s:%s@" % (user, pasw)
            host = os.environ.get("OAR_MONGODB_HOST", dbcfg.get("host", "localhost"))
            dburl = "mongodb://%s%s%s/midas" % (cred, host, port)

        fact = dbio.MongoDBClientFactory(dbcfg, dburl)
        fact.wait_until_ready()
        return fact

    elif dbtype == "fsbased":
        return dbio.FSBasedDBClientFactory(dbcfg)

    elif dbtype == "inmem":
        return dbio.InMemoryDBClientFactory(dbcfg)

    raise CommandFailure("No DBIO service configured (dap_service.dbio)", 6)

def configure_log(args, cfg: Mapping, progname="pubmonitor"):
    """
    set-up logging according to the command-line arguments and the given configuration.
    """
    if args.debug:
        cfg['loglevel'] = logging.DEBUG
    if cfg.get('loglevel') is None:
        cfg['loglevel'] = config.NORMAL
        
    if args.logfile:
        cfg['logfile'] = opts.logfile
        # use of --logfile overrides logserver and logdir in configuration
        if cfg.get('logserver'):
            del cfg['logserver']
        if cfg.get('logdir'):
            del cfg['logdir']

    # config.configure_logging(cfg)
    config.configure_log(config=cfg)   # deprecated method

    if args.verbose:
        level = (args.debug and logging.DEBUG) or logging.INFO
        format = f"{progname} %(levelname)s: %(message)s"
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter(format))
        logging.getLogger().addHandler(handler)



if __name__ == "__main__":
    try:
        progname = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        opts = main(progname, sys.argv[1:])
        sys.exit(0)
    except CommandFailure as ex:
        logging.getLogger(progname).critical(str(ex))
        if not opts.verbose:
            print(f"{progname}: {str(ex)}", file=sys.stderr)
        sys.exit(ex.stat)
    except config.ConfigurationException as ex:
        logging.getLogger(progname).critical("Config error: "+str(ex))
        if not opts.verbose:
            print(f"{progname}: Configuration error: {str(ex)}", file=sys.stderr)
        sys.exit(6)
    except Exception as ex:
        logging.getLogger(progname).exception(ex)
        print(f"{progname}: Unexpected failure: {str(ex)}", file=sys.stderr)
        sys.exit(200)


