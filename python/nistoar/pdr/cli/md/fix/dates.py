"""
CLI NERDm fix commanad that updates (or adds, if necessary) specific dates at the resource level of  
resource record.  (See also ``nistoar.nerdm.convert.latest``.)
"""
import logging, argparse, sys, os, re, json, time, math
from datetime import datetime

from nistoar.nerdm.convert import latest
from nistoar.pdr.cli import PDRCommandFailure
from ..trans._comm import define_comm_trans_opts, process_svcep_args, define_comm_md_opts
from ..trans._comm import _get_record_for_cmd, _write_record_for_cmd

# the names of the date properties
FIRST_ISSUED = "firstIssued"
ISSUED = "issued"
MODIFIED = "modified"
REVISED = "revised"
ANNOTATED = "annotated"

default_name = "dates"
help = "fix specific datest in a NERDm record"
description = """
  This comand will read in NERDm record (from a file or standard input) or retrieve it from the PDR
  and update it with given date information.  This mode of this command is to provide a singe date and 
  indicate, via options, which date properties should be updated.  Different dates can be applied to 
  different properties by piping the output of one ``fix date`` command into the input of another.  In
  each ``fix date`` command at least one of -f, -i, -m, -r, or -a must be specified.

  The date may either be specified in one of two forms.  It can be in ISO 8601 format (at any resolution);
  a space or the standard 'T' can be used as the date-time delimiter.  In no time zone information is given, 
  the date is assumed to be UTC.  Alternatively, the date can be given as a single integer representing 
  UTC Epoch seconds.  
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
        define_comm_trans_opts(p)    # fix supports the same common options as trans

    p.add_argument("date", metavar="DATE_PART", nargs="+", type=str,
                   help="the date to set.")
    p.add_argument("-f", "--first-issued", action="append_const", const=FIRST_ISSUED, dest="dprops", 
                   default=[], help="add or update the firstIssued property to the given date")
    p.add_argument("-i", "--issued", action="append_const", const=ISSUED, dest="dprops", 
                   help="add or update the issued property to the given date")
    p.add_argument("-m", "--modified", action="append_const", const=MODIFIED, dest="dprops",
                   help="add or update the revised property (the time of the addition or update to the data) "
                        "to the given date")
    p.add_argument("-r", "--revised", action="append_const", const=REVISED, dest="dprops",
                   help="add or update the revised property (the time of the addition or update to the data) "
                        "to the given date")
    p.add_argument("-a", "--annotated", action="append_const", const=ANNOTATED, dest="dprops",
                   help="add or update the annotated property (the time of the last update to metadata) "
                        "to the given date")

    if 'rmmbase' not in current_dests:
        define_comm_md_opts(p)

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
    nerdm = _get_record_for_cmd(args, cmd, config, log)

    for prop in args.dprops:
        nerdm[prop] = args.date
        
    _write_record_for_cmd(nerdm, args, cmd, config, log)

def _process_args(args, config, cmd, log):
    args.date = _parse_date(args.date, config, cmd, log)
    if not args.dprops:
        raise PDRCommandFailure(cmd, "One of -f, -i, -m, -r, or -a must be specified", 2)
    process_svcep_args(args, config, cmd, log)

def _parse_date(date_parts, config, cmd, log):
    if len(date_parts) == 0:
        raise PDRCommandFailure(cmd, "No date value provided", 2)
    try:
        if len(date_parts) > 3:
            raise ValueError("too many date arguments provided")
        if len(date_parts) == 1:
            # see if it's an integer
            try:
                t = int(date_parts[0])
            except ValueError as ex:
                pass
            else:
                next_year = datetime.now().year + 1
                epoch1980 = datetime(1980,1,1).timestamp()
                if t < 1900 or (t > next_year and t < epoch1980):
                    raise ValueError("%s is a suspicious year; please provide a more precise value" %
                                     date_parts[0])
                if t > 1900 and t < next_year:
                    # interpret this as a year
                    return date_parts[0]

            # doesn't look like a year
            try:
                t= float(date_parts[0])
            except ValueError as ex:
                pass
            else:
                out = datetime.utcfromtimestamp(t).isoformat()
                next_year = datetime.utcnow() + timedelta(365)
                if out > next_year:
                    raise ValueError("Epoch time is too far in the future; please provide ISO format: " +out)
                return out

        elif re.match(r'\d{4}\-((0\d)|(1[012]))', date_parts[0]):
            # just a year and a month
            return date_parts[0]

    except ValueError as ex:
        raise PDRCommandFailure(cmd, "Illegal date-time syntax: "+" ".join(date_parts), 2)

    date = date_parts[0]
    if len(date_parts) > 1:
        date += "T" + date_parts[1]
    if len(date_parts) > 2:
        if date_parts[2][0] not in "+-":
            date += "+"
        date += date_parts[2]

    try:
        return normalize_date_str(date) # may raise ValueError
    except ValueError as ex:
        raise PDRCommandFailure(cmd, " ".join(date_parts) + ": " +str(ex), 2)

def normalize_date_str(dt):
    if re.match(r'\d{4}(\-((0\d)|(1[012])))?$', dt):
        return dt

    m = re.match(r'(\d{4})\-(\d\d)-(\d\d)(T(\d\d)(:(\d\d)(:(\d\d)(\.(\d{1,6}))?)?)?)?', dt)
    if not m:
        raise ValueError("Unable to interpret as a date-time")
    dp = [p and int(p) or 0 for p in
          [m.group(1), m.group(2), m.group(3), m.group(5), m.group(7), m.group(9), m.group(11)]]
    if dp[6]:
        dp[6] *= int(math.pow(10, 5 - int(math.floor(math.log10(dp[6])))))

    # it's okay if this does not raise an exception
    datetime(*dp)

    if re.search(r'T\d\d$', dt):
        dt += ":00"
    return dt


