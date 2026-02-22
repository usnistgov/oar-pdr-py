#! /usr/bin/env python3
"""
Fetch NSD data and load it into our OAR mirror.  

Execute this script with the -h option to display the list of options.
"""
# nsdsync [-h] [-d odir] [-c CONFFILE] [-D DBIONSDURL]  [OU ...]
import sys, os, logging, traceback as tb
from nistoar.nsd.sync import cli

prog = os.path.basename(sys.argv[0])
if prog.endswith('.py'):
    prog = prog[:-(len('.py'))]

def err(msg):
    rootlog = logging.getLogger()
    if rootlog.handlers:
        rootlog.error(msg)
    else:
        if prog:
            sys.stderr.write(prog)
            sys.stderr.write(": ")
        sys.stderr.write(msg)
        sys.stderr.write("\n")

try:
    
    cli.main(prog, sys.argv[1:])
    
except cli.Failure as ex:
    err(str(ex))
    sys.exit(ex.exitcode)

except Exception as ex:
    # unexpected failure
    tb.print_exc()
    err(str(ex))
    sys.exit(1)
    
