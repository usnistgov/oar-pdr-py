"""
an executable envelope for launching a job in a new python process
"""
import sys, os, logging, importlib, time, signal
from argparse import ArgumentParser
from typing import Callable
from pathlib import Path
import traceback as tb

from nistoar.jobmgt import Job, FatalError, job_state_file

def define_options(progname):
    """
    return an ArgumentParser instance that is configured with options for generically launching 
    a job
    """
    description = "process an OAR job task.  The configuration will be read as JSON from standard input."
    epilog = None

    parser = ArgumentParser(progname, None, description, epilog)

    parser.add_argument('-I', '--data-id', type=str, metavar="ID", dest='id', 
                        help="an identifier for the dataset being operated on")
    parser.add_argument('-Q', '--queue-name', type=str, metavar="NAME", dest='queue', default="jobexec",
                        help="the name of the queue that has launched this job process")
#    parser.add_argument('-M', '--module-name', type=str, metavar="DOTNAME", dest='mod', 
#                        help="the module name that contains the process function to call to process the data")
    parser.add_argument('-L', '--log-out', action='store_true', dest='logout',
                        help="Send log messages to standard out so that they can be captured by the job "+
                             "manager")
    parser.add_argument('-d', '--job-dir', type=str, metavar="DIR", dest='jobdir',
                        default=os.environ.get('OAR_JOB_DIR'), 
                        help="the directory where the job state files are stored.  If provided, this "+
                             "execution envelope will upgrade the state file on exit")
    parser.add_argument('-l', '--log-file', type=str, metavar="FILE", dest='logfile', default=None,
                        help="Send log messages to the specified FILE; can be used with -L")
#    parser.add_argument('args', action='append', dest='args',
#                        help="Extra arguments to pass into the processor function")

    return parser

def main(args):
    """
    execute the requested processing.  
    """
    parser = define_options("jobexec")
    try:
        opts = parser.parse_args(args)
    except SystemExit as ex:
        raise FatalError(f"Failed to parse jobexec arguments ({' '.join(args)}); "+
                         "SystemExit triggered.", 13)

    if not opts.id:
        raise FatalError(f"Missing required data ID option (-I): {' '.join(args)}", 27)
    if not opts.jobdir:
        raise FatalError(f"{opts.queue}/{opts.id}: Missing required Job data dir (-d): {' '.join(args)}", 26)
    statedir = Path(opts.jobdir)
    if not statedir.is_dir():
        raise FatalError(f"{opts.queue}/{opts.id}: Job data dir does not exist: {str(statedir)}", 25)

    statefile = job_state_file(statedir, opts.id)
    try:
        job = Job.from_state_file(statefile)
    except Exception as ex:
        raise FatalError(f"Failed to read job file, {statefile}: {str(ex)}", 24)
    try:
        job.mark_running(os.getpid())
        job.save_to(statefile)
    except Exception as ex:
        raise FatalError(f"Failed to update job status into {statefile}: {str(ex)}", 13)
        

    cfg = job.info.get("config", {})

    errors = []
    exitcode = 0
    log = None
    killed = False
    start = time.time()
    try:
        # send log messages to a file?
        if opts.logfile:
            cfg['logfile'] = opts.logfile
        if cfg.get('logfile'):
            config.configure_log(config=cfg)

        # send logging messages to stdout?
        if opts.logout:
            h = logging.StreamHandler(sys.stdout)
            fmt = '{"name":"%(name)s","created":"%(created)s","level":%(levelno)s,"msg":"%(message)s",' + \
                   '"lineno":"%(lineno)d","pathname":"%(pathname)s"}'
            fmtr = logging.Formatter(fmt)
            h.setFormatter(fmtr)
            h.setLevel(logging.DEBUG)
            logging.getLogger().addHandler(h)
            lev = cfg.get('loglevel', logging.DEBUG)
            if not isinstance(lev, int):
                lev = config._log_levels_byname.get(str(lev), lev)
            logging.getLogger().setLevel(lev)

        if not job.info.get('execmodule'):
            msg = "Execution Module missing from job file"
            logging.getLogger("jobexec").error(msg)
            raise FatalError(msg, 23)
        modname = job.info['execmodule']

        try:
            mod = importlib.import_module(modname)
        except ImportError as ex:
            raise FatalError("Unable to import job module: "+str(ex), 22)

        name = opts.queue
        if hasattr(mod, 'LOGNAME'):
            name += f".{mod.LOGNAME}"
        name += f".{job.data_id}"
        if opts.logout or cfg.get('logfile'):
            log = logging.getLogger(name)

        if not hasattr(mod, 'process'):
            raise FatalError(f"{modname}: Missing process() function", 21)
        if not callable(mod.process):
            raise FatalError(f"{modname}: process symbol is not callable", 2)

        start = time.time()
        def sighandle(sig, stack):
            end = time.time()
            job.mark_killed(end, end-start, errors=[f"Caught signal={sig} requesting interruption"])
            job.save_to(statefile)
            
        signal.signal(signal.SIGHUP, sighandle)
        signal.signal(signal.SIGTERM, sighandle)

        mod.process(opts.id, cfg, job.info.get('args',[]), log)

    except KeyboardInterrupt as ex:
        if log:
            log.error("job killed via keyboard interrupt")
        errors.append("keyboard interrupt")
        killed = True
    except SystemExit as ex:
        exitcode = ex.code
    except FatalError as ex:
        errors.append(str(ex))
        exitcode = ex.exitcode
        if log:
            log.critical(str(ex))
        raise
    except Exception as ex:
        exitcode = 11
        errors.append(str(ex))
        if log:
            log.exception(ex)
        raise FatalError("Failure occurred during processing: "+str(ex), 11) from ex
    finally:
        ended = time.time()
        runt = ended - start
        if killed:
            job.mark_killed(ended, runt, errors)
        else:
            job.mark_complete(exitcode, ended, runt, errors)
        job.save_to(statefile)

        
if __name__ == '__main__':
    try:
        main(sys.argv[1:])
        sys.exit(0)
    except FatalError as ex:
        print(str(ex), file=sys.stderr)
        sys.exit(ex.exitcode)
    except Exception as ex:
        print(str(ex), file=sys.stderr)
        tb.print_exception(ex)
        sys.exit(30)

