#! /usr/bin/env python3
#
import os, sys, json, argparse, traceback, re
from pathlib import Path
from collections import OrderedDict
from collections.abc import Mapping

description="""create copies of NERDm schemas with loosened requirements appropriate for the 
metadata drafting process (i.e. within MIDAS)"""
epilog=""
def_progname = "loosen_nerdm"

def define_options(progname, parser=None):
    """
    define command-line arguments
    """
    if not parser:
        parser = argparse.ArgumentParser(progname, None, description, epilog)

    parser.add_argument("srcdir", metavar="SRCDIR", type=str, 
                        help="the directory containing the NERDm schemas")
    parser.add_argument("destdir", metavar="DESTDIR", type=str, 
                        help="the directory write loosened schemas to")
    parser.add_argument("-D", "--no-dedocument", dest="dedoc", action="store_false", default=True,
                        help="do not remove documentation from source schemas")
    parser.add_argument("-J", "--assume-post2020", dest="post2020", action="store_true", default=False,
                        help="assume schemas are compliant with a post-2020 JSON Schema specification "+
                             "(and uses $defs)")
    parser.add_argument("-m", "--make-dest-dir", dest="mkdest", action="store_true", default=False,
                        help="create the destination directory if it does not exist")

    return parser

def set_options(progname, args):
    """
    define and parse the command-line options
    """
    return define_options(progname).parse_args(args)

directives_by_file = {
    "nerdm-schema.json": {
        "derequire": [ "Resource", "Organization" ]
    },
    "nerdm-pub-schema.json": {
        "derequire": [ "PublicDataResource", "Person" ]
    }
}

try:
    import nistoar.nerdm.utils as utils
except ImportError:
    sys.path.insert(0, find_nistoar_code())
    import nistoar.nerdm.utils as utils

def find_nistoar_code():
    execdir = Path(__file__).resolve().parents[0]
    basedir = execdir.parents[0]
    mdpydir = basedir / "metadata" / "python"
    return mdpydir

def loosen_schema(schema: Mapping, directives: Mapping, opts=None):
    """
    apply the given loosening directive to the given JSON Schema.  The directives is a 
    dictionary describes what to do with the following properties (the directives) supported:

    ``derequire``
         a list of type definitions within the schema from which the required property 
         should be removed (via :py:func:`~nistoar.nerdm.utils.unrequire_props_in`).  Each
         type name listed will be assumed to be an item under the "definitions" node in the 
         schema this directive is applied to.
    ``dedocument``
         a boolean indicating whether the documentation annotations should be removed from 
         the schema.  If not set, the default is determined by opts.dedoc if opts is given or
         True, otherwise.  

    :param dict schema:      the schema document as a JSON Schema schema dictionary
    :param dict directives:  the dictionary of directives to apply
    :param opt:              an options object (containing scripts command-line options)
    """
    dedoc = directives.get("dedocument", True)
    if opts and not opts.dedoc:
        dedoc = False
    if dedoc:
        utils.declutter_schema(schema)

    p2020 = False
    if opts:
        p2020 = opts.post2020
    deftag = "$defs" if p2020 else "definitions"

    dereqtps = [ deftag+'.'+t for t in directives.get("derequire", []) ]
    utils.unrequire_props_in(schema, dereqtps, p2020)

def process_nerdm_schemas(srcdir, destdir, opts=None):
    """
    process all NERDm schemas (core and extensions) found in the source directory
    and write the modified schemas to the output directory
    """
    if not os.path.isdir(srcdir):
        raise RuntimeException(f"{srcdir}: schema source directory does not exist as directory")
    
    if not os.path.exists(destdir):
        if opts and opts.mkdest:
            os.makedirs(destdir)
        else:
            raise FileNotFoundError(destdir)
    if not os.path.isdir(srcdir):
        raise RuntimeException(f"{destdir}: schema destination is not a directory")

    nerdfilere = re.compile(r"^nerdm-([a-zA-Z][^\-]*\-)?schema.json$")
    schfiles = [f for f in os.listdir(srcdir) if nerdfilere.match(f)]

    failed={}
    for f in schfiles:
        try:
            with open(os.path.join(srcdir, f)) as fd:
                schema = json.load(fd, object_pairs_hook=OrderedDict)
        except IOError as ex:
            failed[f] = f"Trouble reading schema file: {str(ex)}"
            continue

        directives = directives_by_file.get(f, {})
        try:
            loosen_schema(schema, directives, opts)
        except Exception as ex:
            failed[f] = f"Trouble processing schema file: {str(ex)}"
            continue

        with open(os.path.join(destdir, f), 'w') as fd:
            json.dump(schema, fd, indent=2)
            fd.write("\n")

    return failed

def main(progname=None, args=[]):
    global def_progname;
    if not progname:
        progname = def_progname
    else:
        progname = os.path.basename(progname)
        if progname.endswith(".py"):
            progname = progname[:-1*len(".py")]

    opts = set_options(progname, args)

    failed = process_nerdm_schemas(opts.srcdir, opts.destdir, opts)  # may raise exceptions
    if failed:
        print(f"{progname}: WARNING: failed to process the following schemas:", file=sys.stderr)
        for f, err in failed:
            print(f"  {f}: {err}", file=sys.stderr)

        return 3

    return 0

if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[0], sys.argv[1:]))
    except RuntimeError as ex:
        print(f"{progname}: {str(ex)}", file=sys.stderr)
        sys.exit(1)
    except Exception as ex:
        print("Unexpected error: "+str(ex), file=sys.stderr)
        traceback.print_tb(sys.exc_info()[2])
        sys.exit(4)


        
        
    
            
            
