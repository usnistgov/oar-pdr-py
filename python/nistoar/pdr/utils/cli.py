"""
a module providing utility code for creating command-line interfaces to OAR systems.
"""
import os, sys, logging
from copy import deepcopy
from argparse import ArgumentParser, HelpFormatter

from nistoar.pdr.exceptions import StateException
from nistoar.base.config import ConfigurationException
from nistoar.base import config as cfgmod

EXPLAIN=cfgmod.NORMAL

def explain(log, message, *params):
    """
    log a message that is quieter than INFO but louder than DEBUG.  This is intended for messages 
    that should go into the log, but not necessarily to the terminal (unless --verbose was specified).
    :param Logger log:    the Logger to send message to
    :param str message:   the message (or message template) to record
    :param list params:   data to insert into ``message``; if provided, ``message`` is treated as a template.
    """
    log.log(EXPLAIN, message, *params)

class _MyHelpFormatter(HelpFormatter):
    def _fill_text(self, text, width, indent):
        paras = []
        for para in text.split("\n\n"):
            paras.append(super(_MyHelpFormatter, self)._fill_text(para, width, indent))
        return "\n\n".join(paras)

def define_prog_opts(progname, description=None, epilog=None, parser=None):
    """
    define the top level arguments for a script that will provide a suite of subcommands

    :param str progname:    the name show as the name of the top-level program
    :param str description: the summary of the command that precedes the argument descriptions (optional)
    :param str epilog:      description to appear after the argument descriptions (optional)
    :param ArgumentParser parser:  a pre-instantiated parser to configure and resturn; if not provided,
                            one will be created fresh and returned configured.
    """
    if not parser:
        parser = ArgumentParser(progname, None, description, epilog, formatter_class=_MyHelpFormatter)

    morehelp = "Run '%(prog)s CMD -h' for help specifically on CMD."
    if parser.epilog:
        parser.epilog = morehelp+"\n\n"+parser.epilog
    else:
        parser.epilog = morehelp

    parser.add_argument("-w", "--workdir", type=str, dest='workdir', metavar='DIR', default="", 
                        help="target input and output files with DIR by default (including log); default='.'")
    parser.add_argument("-c", "--config", type=str, dest='conf', metavar='FILE',
                        help="read configuration from FILE (over-rides --in-live-sys)")
    parser.add_argument("-S", "--in-live-system", action="store_true", dest='livesys',
                        help="operate within the live OAR system environment; this is " +
                             "accomplished by loading a configuration from the configuration service")
    parser.add_argument("-l", "--logfile", type=str, dest='logfile', metavar='FILE', 
                        help="log messages to FILE, over-riding the configured logfile")
    parser.add_argument("-q", "--quiet", action="store_true", dest='quiet',
                        help="do not print error messages to standard error")
    parser.add_argument("-D", "--debug", action="store_true", dest='debug',
                        help="send DEBUG level messages to the log file")
    parser.add_argument("-v", "--verbose", action="store_true", dest='verbose',
                        help="print INFO and (with -d) DEBUG messages to the terminal")
    parser.add_argument("-A", "--actor-id", type=str, dest="actor", metavar='USERID',
                        help="An identifer representing the real user executing this command, used to "+
                             "record the actor in provenance logs")

    return parser

class CommandFailure(Exception):
    """
    An exception that indicates that a failure occured while executing a command.  The CLI is 
    expected to exit with a non-zero exit code

    The following conventions for exit codes are recommended:
      * 0:  normal successful completion
      * 1:  general, unspecific processing failures due to the current state of the system
      * 2:  error due to missing or otherwise misused command-line options
      * 3:  syntax or other read error while reading provided input data
      * 4:  error occured while writing output data
      * 5:  an unexpected remote system error occured
      * 6:  if a configuration error was detected
      * 7:  a bad service URL was provided
      * 8:  invalid or missing configuration data encountered
      * 9:  insufficient permission
      * 10: unrecognized subcommand requested

    Other exit codes greater than 10 may be used for more specified errors.  
    """
    
    def __init__(self, cmdname, message, exstat=1, cause=None):
        """
        Create the exception
        :param str cmdname:   the name of the command that failed to execute
        :param str message:   an explanation of what went wrong
        :param int exstat:    the recommended (relative) status to exit with.  As the parent command 
                                may offset form this actual value (by a factor of 10), it is recommended 
                                that it is a value less than 10.  
        """
        if not message:
            if cause:
                message = str(cause)
            else:
                message = "Unknown command failure"

        super(CommandFailure, self).__init__(message)
        self.stat = exstat
        self.cmd = cmdname
        self.cause = cause

class CommandSuite(object):
    """
    an interface for running the sub-commands of a parent command
    """
    def __init__(self, suitename, parent_parser, current_dests=None):
        """
        create a command interface
        :param str suitename:  the command name used to access this suites' subcommands
        :param argparse.ArgumentParser parent_parser:  the ArgumentParser for the command that this 
                               suite will be added into.
        """
        self.suitename = suitename
        self._subparser_src = None
        if parent_parser:
            self._subparser_src = parent_parser.add_subparsers(title="subcommands", dest=suitename+"_subcmd")
        self._cmds = {}
        self._dests = set()
        if current_dests:
            self._dests.update(current_dests)
        if parent_parser:
            self._register_parser_dests(parent_parser)

    def _register_parser_dests(self, parser):
        self._dests.update([a.dest for a in parser._actions])

    def load_subcommand(self, cmdmod, cmdname=None):
        """
        load a subcommand into this suite of subcommands.  

        The ``cmdmod`` arguemnt is a module or object that must specify a load_into() function, a help 
        string property, and default_name string property.  The load_into() should accept three arguments: 

         *  an ``ArgumentParser`` instance,
         *  a list of destination names for parameters that have already been defined (optional), and 
         *  the subcommand name assigned to the action provided by the module (optional).

        The intent of the second argument is to allow a subcommand to determine whether an option has 
        already been defined by its parent command (if it hasn't, it may choose to define it itself).  
        The function's implementation should load its command-line option and argument defintions into 
        ArgumentParser.  It should return either None or CommandSuite instance.  If None, then the given 
        cmd module/object must also include an execute() function (that has the same signature as the 
        execute function in this class).  

        :param module|object cmdmod: the subcommand to load.  
        :param str cmdname:     the name to assign the sub-command, used on the command-line to invoke it;
                                if None, the default name provided in the module will be used.
        """
        if not cmdname:
            cmdname = cmdmod.default_name
        subparser = self._subparser_src.add_parser(cmdname, description=cmdmod.description,
                                                   help=cmdmod.help, formatter_class=_MyHelpFormatter)
        subcmd = cmdmod.load_into(subparser, self._dests, cmdname)
        
        if not subcmd:
            subcmd = cmdmod
        self._cmds[cmdname] = subcmd

        if subparser._subparsers is not None:
            morehelp = "Run '%(prog)s CMD -h' for help specifically on CMD"
            if subparser.epilog:
                subparser.epilog = morehelp + "\n\n" + subparser.epilog
            else:
                subparser.epilog = morehelp

    def extract_config_for_cmd(self, config, cmdname, cmd=None):
        """
        merge command-specific configuration with the top-level configuration.  The input config
        can contain a property ``cmd`` that holds configuration data that is specific to particular 
        subcommands.  The properties of the ``cmd`` object are names of the commands (either the commands' 
        default name or names as configured).  If a matching config property is found, it's contents 
        are extracted and merged into top-level metadata (after deleting the ``cmd`` object).  The resulting 
        dictionary is returned as the configuration to use.

        :param dict config:  the configuration to extract the specific command configuration from
        :param str cmdname:  the name of the command to look for
        :param module  cmd:  the module where command's implementation is defined.  If provided and 
                             ``cmdname`` is not found within the ``cmd`` property, the value of 
                             ``default_name`` from the module will be looked for instead.  
        """
        if 'cmd' not in config:
            return config

        out = deepcopy(config)
        del out['cmd']
        if cmdname not in config['cmd'] and cmd and hasattr(cmd, 'default_name'):
            cmdname = getattr(cmd, 'default_name')
        if cmdname in config['cmd']:
            out = cfgmod.merge_config(config['cmd'][cmdname], out)

        return out

    def execute(self, args, config=None, log=None):
        """
        execute a subcommand from this command suite
        :param argparse.Namespace args:  the parsed arguments
        :param dict             config:  the configuration to use
        :param Logger              log:  the log to send messages to 
        """
        if not log:
            log = logging.getLogger(self.suitename)

        subcmd = getattr(args, self.suitename+"_subcmd")
        cmd = self._cmds.get(subcmd)
        if cmd is None:
            raise CommandFailure(args.cmd, "Unrecognized subcommand of "+cmdname+": "+subcmd, 2)

        config = self.extract_config_for_cmd(config, subcmd, cmd)

        log = log.getChild(subcmd)
        try:
            return cmd.execute(args, config, log)
        except CommandFailure as ex:
            if ' ' in ex.cmd:
                ex.cmd = subcmd + ' ' + ex.cmd
            else:
                ex.cmd = subcmd
            raise ex

class CLISuite(CommandSuite):
    """
    a class that manages the execution of command-line program via a suite of subcommands.

    This class should be subclassed to create a specific command-line script (e.g. ``pdr``, 
    ``midasadm``, etc.)
    """
    config_svc_app_name = None

    def __init__(self, progname, defconffile=None, parser=None):
        super(CLISuite, self).__init__(progname, None)
        self._set_parser(parser)
        self._defconffile = defconffile

    def _set_parser(self, parser=None):
        if not parser:
            parser = define_prog_opts(self.suitename)
        self.parser = parser
        self._register_parser_dests(self.parser)
        self._subparser_src = self.parser.add_subparsers(title="commands", dest="cmd")
        self._cmds = {}
    
    def parse_args(self, args):
        """
        parse the given list of arguments according to the current argument configuration
        :param list args:  the command line arguments where the first item is the first argument
        """
        return self.parser.parse_args(args)

    def load_subcommand(self, cmdmod, cmdname=None, exit_offset=None):
        if not hasattr(cmdmod, "load_into"):
            raise StateException("command module/object has no load_into() function: " + repr(cmdmod))
        if not cmdname:
            cmdname = cmdmod.default_name

        subparser = self._subparser_src.add_parser(cmdname, help=cmdmod.help)
        cmd = cmdmod.load_into(subparser, self._dests, cmdname)
        self._dests.update([a.dest for a in subparser._actions])

        if not cmd:
            cmd = cmdmod
        self._cmds[cmdname] = cmd

        if subparser._subparsers is not None:
            morehelp = "Run '%(prog)s CMD -h' for help specifically on CMD"
            if subparser.epilog:
                subparser.epilog = morehelp + "\n\n" + subparser.epilog
            else:
                subparser.epilog = morehelp


    def configure_log(self, args, config):
        """
        set-up logging according to the command-line arguments and the given configuration.
        """
        loglevel = (args.debug and logging.DEBUG) or cfgmod.NORMAL

        if not args.logfile and 'logfile' not in config:
            config['logfile'] = self.suitename + ".log"
        if 'logdir' not in config:
            config['logdir'] = config.get('working_dir', os.getcwd())
        
        if args.logfile:
            # if logfile given on cmd-line, it will always go into the working dir
            config['logfile'] = os.path.join(config.get('working_dir', os.getcwd()), args.logfile)
        cfgmod.configure_log(level=loglevel, config=config)

        if not args.quiet:
            level = logging.INFO
            format = self.suitename + " %(levelname)s: %(message)s"
            if args.verbose:
                level = (args.debug and logging.DEBUG) or cfgmod.NORMAL
                format = "%(name)s %(levelname)s: %(message)s"
            handler = logging.StreamHandler(sys.stderr)
            handler.setLevel(level)
            handler.setFormatter(logging.Formatter(format))
            logging.getLogger().addHandler(handler)

        log = logging.getLogger("cli."+self.suitename)
        log.setLevel(cfgmod.NORMAL)
        if args.verbose:
            log.info("FYI: Writing log messages to %s", cfgmod.global_logfile)

        return log

    def load_config(self, args):
        """
        load the configuration according to the specified arguments.  A specific config file can be 
        specified via --config, and --in-live-sys will pull the configuration from an available 
        configuration service (the former overrides the latter).  A configuration service is detected 
        when the OAR_CONFIG_SERVICE environment variable is set to the service URL.  If neither are set,
        the default configuration file, set at construction, will be loaded

        :param argparse.Namespace args:  the parsed command line arguments
        :rtype:  dict
        :return:  the configuration data
        """
        if args.conf:
            config = cfgmod.load_from_file(args.conf)
        elif args.livesys:
            if not cfgmod.service:
                raise CommandFailure(args.cmd,
                                        "Live system not detected; config service not availalbe", 5)
            config = cfgmod.service.get(self.config_svc_app_name)
        elif self._defconffile and os.path.isfile(self._defconffile):
            config = cfgmod.load_from_file(self._defconffile)
        else:
            config = {}
        return config
                
    def execute(self, args, config=None):
        """
        execute the command given in the arguments
        :param list|object args:   the program arguments (including the command name).  Typically, 
                                     this is a string list; if it isn't, it's assumed to be an 
                                     already parsed version of the arguments--i.e., an 
                                     argparse.Namespace instance.  
        """
        origargs = None
        if isinstance(args, list):
            origargs = args
            args = self.parse_args(args)
        cmd = self._cmds.get(args.cmd)
        if cmd is None:
            raise CommandFailure(args.cmd, "Unrecognized command: "+args.cmd, 2)

        if config is None:
            config = self.load_config(args)
        config = self.extract_config_for_cmd(config, args.cmd, cmd)

        if args.workdir:
            args.workdir = os.path.abspath(args.workdir)
            if not os.path.isdir(args.workdir):
                raise CommandFailure(args.cmd, "Working dir is not an existing directory: "+args.workdir, 2)
            config['working_dir'] = args.workdir
        elif 'working_dir' in config:
            config['working_dir'] = os.path.abspath(config['working_dir'])
        else:
            config['working_dir'] = os.getcwd()

        proglog = self.configure_log(args, config)
        if origargs:
            explain(proglog, "Executing: %s %s", self.suitename, " ".join(origargs))

        try:
            cmd.execute(args, config, proglog.getChild(args.cmd))
        except CommandFailure as ex:
            if ex.cmd:
                ex.cmd = args.cmd + " " + ex.cmd
            else:
                ex.cmd = args.cmd
            raise ex
        except ConfigurationException as ex:
            raise CommandFailure(args.cmd, "Configuration error: "+str(ex), 6, ex)

