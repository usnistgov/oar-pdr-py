"""
This module defines some reusable functions shared by the trans command package and its subcommand modules.
(This avoids circular imports!)
"""
import sys, json
from collections import OrderedDict

from nistoar.pdr.cli import PDRCommandFailure
from .._args import process_svcep_args, define_comm_md_opts

def define_comm_trans_opts(subparser):
    """
    define some arguments that apply to all trans subcommands.  These are:
     - --from-aip    - pulls the input record from an AIP
     - --from-mdserv - pulls the input record from the metadata service
     - ID|FILE       - the identifier for the source of the input record

    This is not called trans command level; rather it is intended to be called by the `define_opts()` 
    functions of the individual transformer commands to load the common options.  
    """
    p = subparser
    p.add_argument("id", metavar="ID|FILE", type=str, nargs="?",
                   help="the name of the file containing the input record (or the record's identifier"
                        "when --from-aip or --from-mdserv is used) to translate.  "+
                        "If not provided, the standard input is read for the data")
    p.add_argument("-A", "--from-aip", action="store_const", dest="src", const="aip",
                   help="extract the requested metadata from the appropriate AIP")
    p.add_argument("-M", "--from-mdserv", action="store_const", dest="src", const="md",
                   help="pull the requested metadata from the public PDR metadata service")
    p.add_argument("-o", "--output-file", metavar="FILE", type=str, dest="outfile",
                   help="write the output to the named file instead of standard out")
    p.add_argument("-V", "--get-version", metavar="VER", type=str, dest="version",
                   help="return the VER version of the record. (Note that versions are not available for "+
                        "all ID classes)")

def fetch_input_rec(id, version, rmmbase, from_aip=False, distbase=None, config=None, log=None):
    """
    retrieve the NERDm record with the given id through one of the PDR's public APIs.
    :param str      id:  the id of interest
    :param str version:  version of the resource to retrieve
    :param     rmmbase:  either the RMM's API endpoint URL or an instance of the MetadataClient
    :param bool from_aip:  True if the the record should be returned from archive information package
                         (using the PDR's distribution service); otherwise, the PDR metadata service will 
                         be used.  
    :param    distbase:  either the distribution service's endpoint URL or an instance of RESTServiceClient
                         connected to the PDR's distribution service. 
    :param dict config:  configuration parameters that modifies the behavior of the service
    :param Logger  log:  the logger to send messages to.
    """
    if from_aip:
        return describe(id, rmmbase, version, config)
    else:
        if not distbase:
            raise ValueError("get_input_rec(): when from_aip=True, distbase must be provided")
        return extract_from_AIP(id, distbase, version, rmmbase, None, config, log)

def _get_record_for_cmd(args, cmd, config=None, log=None):
    """
    read in from whatever source stipulated by the command arguments and return the record to transform, 
    assuming a command context.  This means that all errors or failues will result in raising a 
    PDRCommandFailure exception.  The args parameter contains the parsed command arguments (assuming the
    set defined by define_comm_trans_opts() and define_comm_md_opts()) which have been fully processed 
    and normalized.  This function is intended only to be called from the ``trans`` subcommand modules.
    :param args:  the parsed arguments 
    :param dict config:  configuration parameters that modifies the behavior of the service
    :param Logger  log:  the logger to send messages to.
    """
    if args.src:
        try:

            return get_input_rec(args.id, args.version, args.rmmbase,
                                 args.src=='aip', args.distbase, config, log)

        except (IDNotFound, DistribResourceNotFound) as ex:
            raise PDRCommandFailure(cmd, "ID not found: "+args.id, 1)
        except (RMMServerError, DistribServerError) as ex:
            raise PDRCommandFailure(cmd, "Unexpected service failure: "+str(ex), 5)
        except Exception as ex:
            raise PDRCommandFailure(cmd, "Unexpected failure retrieving metadata: "+str(ex), 8)
            
    else:
        fd = None
        id = None
        try:
            if args.id:
                fd = open(args.id)
                id = fd
            else:
                id = sys.stdin
            return json.load(fd, object_pairs_hook=OrderedDict)
        except Exception as ex:
            raise PDRCommandFailure(cmd, "Failed to read input record from " +
                                    ((fd is None and "stdin") or args.id) + ": " + str(ex), 3)
        finally:
            if fd:
                fd.close()

def _write_record_for_cmd(rec, args, cmd, config=None, log=None):
    """
    write out the given JSON record (to an output file or standard out) according to the given 
    command arguments, assuming a command context.  This means that all errors or failues will result 
    in raising a PDRCommandFailure exception.  The args parameter contains the parsed command arguments 
    (assuming the set defined by define_comm_trans_opts() and define_comm_md_opts()) which have been 
    fully processed and normalized.  This function is intended only to be called from the ``trans`` 
    subcommand modules.
    """
    fp = None   # file object for file output
    op = None   # file object for output (may be equal to fp)
    try:
        if args.outfile and args.outfile != '-':
            fp = open(args.outfile, 'w')
            op = fp
        else:
            op = sys.stdout

        json.dump(rec, op, indent=4, separators=(',', ': '))

    except OSError as ex:
        raise PDRCommandFailure(cmd, "Failed to write data to %s: %s" %
                                ((fp and args.outfile) or "standard out", str(ex)), 4)
    finally:
        if fp: fp.close()

    
