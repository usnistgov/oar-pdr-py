#! /usr/bin/python3
#
import json, sys, os, re
import traceback as tb
from argparse import ArgumentParser, HelpFormatter
from collections.abc import Mapping
from collections import OrderedDict

usage = None
description="""
Select and print data from input JSON data.

This script is useful for extracting values from JSON data within a shell script.  It reads JSON-encoded 
data either from standard input or a named file (via -i).  The command line argumens must include one or 
more selectors that indicate which parts of the input data to extract.  For each SELECTOR, the script will
print on a single line (in the order of the given selectors) the matched value.  If the selector corresponds 
to an object or array, the value will be printed in JSON format.  If data corresponding to a given selector
cannot be found, a blank line is printed (see also --not-found-value). 

A SELECTOR represents a hierarchical pointer to some piece of data within the input JSON and is composed of
a sequence of dot (.)-delimited fields.  (See --select-delimiter to use a different delimter.)  The first 
field is matched against the input data; if a match is found in the data, the input data is replaced with 
the matched value, and then the next field is matched.  When no match is found, processing stops and the 
not-found value is printed.  If the data is an object, then a matching field must be the name of a property 
of that object; if the data is an array, a matching field must be an integer that is within the range of 
the length of the array.  (A negative number selects values relative to the end of the array.)  A field 
will never match the input data if the data is a string, number, or null.  The matched data that is left 
after processing all of the fields in a selector will be printed.  

If any of the given selectors match the input data, the script will exit with 0.  If none of the selectors
match, the exit code will be 1.  If the input data is not parseable as JSON, the exit code will be 3.  
"""
epilog=""

ARRAY_INPUT="a"
OBJECT_INPUT="o"
AS_SH_VARS="b"
AS_CSH_VARS="c"

def define_opts(progname: str, parser: ArgumentParser=None):
    if not parser:
        parser = ArgumentParser(progname, usage, description, epilog, formatter_class=_MyHelpFormatter)

    parser.add_argument("select", type=str, nargs="+", metavar='SELECTOR',
                        help="a hierarchical selector string for extracting data from the input JSON.  "+
                             "A selector is a .-delimited sequence of one or more fields, where each "+
                             "field is either a property name or integer index into an array.  (See also "+
                             "-D)")
    parser.add_argument("-i", "--input-file", type=str, dest='infile', metavar="FILE", 
                        help="read input JSON data from the given FILE")
    parser.add_argument("-b", "--as-bash-vars", action="store_const", const=AS_SH_VARS, dest='fmt',
                        help="write out values using BASH variable syntax: each selection will be "+
                             "a BASH-interpretable line that sets a variable whose name is based on the "+
                             "selection string and the value is the selected value.")
    parser.add_argument("-c", "--as-csh-vars", action="store_const", const=AS_CSH_VARS, dest='fmt',
                        help="write out values using CSH variable syntax: each selection will be "+
                             "a CSH-interpretable line that sets a variable whose name is based on the "+
                             "selection string and the value is the selected value.")
    parser.add_argument("-N", "--not-found-value", type=str, dest='nf', default="", metavar='STR',
                        help="print STR for the value when a datum matching a selector cannot be found")
    parser.add_argument("-D", "--select-delimiter", type=str, dest='seldelim', default=".", metavar='STR',
                        help="print STR for the value when a datum matching a selector cannot be found")
    parser.add_argument("-q", "--quiet", action="store_true", default=False,
                        help="do not print any error messages.")
    parser.add_argument("-s", "--silent", action="store_true", default=False,
                        help="suppress all printing, including matched data; a non-zero status indicates "+
                             "that data was found.")
    return parser

class _MyHelpFormatter(HelpFormatter):
    def _fill_text(self, text, width, indent):
        paras = []
        for para in text.split("\n\n"):
            paras.append(super(_MyHelpFormatter, self)._fill_text(para, width, indent))
        return "\n\n".join(paras)

class FatalError(Exception):
    def __init__(self, msg, exitcode: int=1):
        super(FatalError, self).__init__(msg)
        self.exitcode = exitcode

def complain(msg, progname=None):
    if progname:
        sys.stderr.write(progname)
        sys.stderr.write(": ")
    sys.stderr.write(msg)
    sys.stderr.write("\n")
    sys.stderr.flush()

def pop_selection(select: str, data, nf='', delim: str='.'):
    """
    pop off the top field from the given selector, select out the subdata from the given data, and 
    return the remaining selector string and the selected data.  If the subdata does not exist, the 
    not-found value will be returned as the selected data. 
    @param str select:   a selector string made up of one or more delim-delimated fields
    @param data:         the data to select from
                         @type data: Mapping, list, tuple, str, int, float, or None
    @param nf:           the value to return if subdata requested by the selector does not exist
    @param str delim:    the delimiter used to separate fields in the selector
    @return:  2-tuple containing the remaining selector and the selected data.  If the input selector 
              (select) only contained one field, an empty string is returned as the remaining selector. 
              If the requested subdata is not found in the given data, the nf value is returned for the 
              selected data.  
    """
    parts = select.split(delim, 1)
    if len(parts) < 2:
        parts.append('')

    if isinstance(data, Mapping):
        if parts[0] in data:
            return (parts[1], data[parts[0]])
        return (parts[1], nf)

    if isinstance(data, (list, tuple)):
        try:
            idx = int(parts[0])
            if idx >= -1*len(data) and idx < len(data):
                return (parts[1], data[idx])
        except ValueError:
            pass
        return (parts[1], nf)

    return (parts[1], nf)

def select(select: str, data, nf='', delim: str='.'):
    """
    return the subdata from the given data corresponding to the given JSON data selector
    @param str select:  the hierarchical JSON data selector.  This selector is made up of fields
                        that are either property names or integers, delimited by the delim field.  
                        a property name field selects a property from a JSON object, and an integer
                        field selects an element from a list.  If select is an empty string, the given
                        data is returned.
    @param data:        the JSON data to select from
    @param nf           the value to return when the selector cannot be matched to the given data
    @param delim        the string value used as the field delimiter in select (default: '.')
    """
    while select and data != nf:
        select, data = pop_selection(select, data, nf, delim)

    return data

def load_data(file=None):
    fstrm = None
    istrm = sys.stdin
    if file:
        fstrm = open(file)
        istrm = fstrm

    try:
        return json.load(istrm, object_pairs_hook=OrderedDict)
    finally:
        if fstrm:
            fstrm.close()

def format_data(data, select=None, fmttype=None):
    if isinstance(data, (Mapping, list, tuple)):
        data = json.dumps(data)

    if select:
        select = re.sub(r'[\-\W]+', '_', select)
    else:
        fmttype = None

    if fmttype == AS_SH_VARS:
        data = "{0}='{1}'".format(select, data)
    elif fmttype == AS_CSH_VARS:
        data = "set {0}='{1}'".format(select, data)

    return data

def main(progname, args):
    parser = define_opts(progname)
    opts = parser.parse_args(args)
    if opts.silent:
        opts.quiet = True

    try:
        data = load_data(opts.infile)
    except FileNotFoundError as ex:
        if not opts.quiet:
            complain(opts.infile + ": file not found", progname)
        raise FatalError(str(ex), 2)
    except OSError as ex:
        if not opts.quiet:
            complain("Problem reading input JSON data: "+str(ex), progname)
        raise FatalError(str(ex), 2)
    except ValueError as ex:
        if not opts.quiet:
            complain("Failed to parse input JSON data: "+str(ex), progname)
        raise FatalError(str(ex), 3)

    found = False
    for sel in opts.select:
        seldata = select(sel, data, opts.nf, opts.seldelim)
        if seldata:
            found = True
        if not opts.silent:
            print(format_data(seldata, sel, opts.fmt))

    return (not found and 1) or 0

if __name__ == '__main__':
    try:
        progname = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        sys.exit(main(progname, sys.argv[1:]))
    except FatalError as ex:
        sys.exit(ex.exitcode)
    except Exception as ex:
        tb.print_exc()
        complain("Unexpected error: "+str(ex), progname)
        sys.exit(3)


        
