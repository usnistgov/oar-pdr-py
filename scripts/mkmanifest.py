#! /usr/bin/env python3
#
# type "mkmanifest.py -h" to see help
# 
description = "scan a directory and create a manifest of its contents as a CSV table file"
epilog = ""

recnum = "[XXXX]"
duri_fmt = "https://doi.org/10.18434/mds2-%s"
dlbase_fmt = "https://nist-midas-large.s3.amazonaws.com/%s/"
def_title = "[title]"

import os, sys, re, hashlib, argparse

prfx = re.compile(r'^.*/2417/')
mtypes = {
    "":    "application/octet-stream",
    "bin": "application/octet-stream",
    "stl": "application/octet-stream",
    "obj": "application/octet-stream",
    "txt": "text/plain",
    "log": "text/plain",
    "log~": "text/plain",
    "csv": "text/csv",
    "sh":  "application/x-sh",
    "mat": "application/x-matlab-data",
    "json": "application/json",
    "png": "image/png",
    "mp4": "video/mp4",
    "avi": "video/x-msvideo",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "swp": "application/octet-stream",
    "series": "application/octet-stream"
}

mtdesc = {
    "": "data",
    "avi": "video",
    "mp4": "video",
    "png": "image",
    "xlsx": "spreadsheet",
    "csv":  "data table",
    "txt": "plain text",
    "log": "log file",
    "ink": "OpenFOAM file",
    "obj": "OpenFOAM object file",
    "stl": "Stereolithography file",
    "mat": "Matlab data",
    "json": "JSON data",
    "sh": "shell script",
    "py": "python script",
    "vtk": "Paraview/VTK file",
    "vtu": "Paraview/VTK data file",
    "vtm": "Paraview/VTK data file",
    "vtp": "Paraview/VTK data file",
    "series": "Paraview/VTK data file",
    "swp": "VIM backup file"
}

txtpats = [
    r'/mesh/system/',
    r'/nb\d[^/]+/case/system/',
    r'/nb\d[^/]+/case/constant/[^/]+$',
    r'/nb\d[^/]+/case/0/alpha.ink.orig$',
    r'/nb\d[^/]+/case/0/(U|p_rgh)$'
]
txtres = [re.compile(r) for r in txtpats]

def checksum_of(filepath):
    """
    return the SHA-256 checksum hash for the given file
    """
    bfsz = 10240000   # 10 MB buffer
    sum = hashlib.sha256()
    with open(filepath, 'rb') as fd:
        while True:
            buf = fd.read(bfsz)
            if not buf: break
            sum.update(buf)
    return sum.hexdigest()

def media_type_for(filename):
#    if any([r.search(filename) for r in txtres]):
#        return "text/plain"

    base, ext = os.path.splitext(filename)
    base = os.path.basename(base)

    if not ext:
#        if base.startswith("log_"):
#            return "text/plain"
#        if base.startswith("All") or base == "Continue":
#            return "application/x-sh"
        return "text/plain"
    if ext.startswith("."):
        ext = ext[1:]
    if ext in mtypes:
        return mtypes[ext]
    return mtypes['bin']

def type_desc_for(filename):
    filename = os.path.basename(filename)
    base, ext = os.path.splitext(filename)
    base = os.path.basename(base)
    if not ext:
#        if base.startswith("log_"):
#            return "log file"
#        if base.startswith("All") or base == "Continue":
#            return "shell script"
        return mtdesc['txt']
    if ext and ext.startswith("."):
        ext = ext[1:]
    if ext and ext in mtdesc:
        return mtdesc[ext]
    return mtdesc['']

def summarize_file(filepath, dlbase, prefix=None):
    """
    create a manifest record for the given file.  This will probe for the file's size and calculate 
    its checksum.
    """
    if not prefix:
        prefix = ''
    sz = os.stat(filepath).st_size
    mt = media_type_for(filepath)
    desc = type_desc_for(filepath)

    hash = checksum_of(filepath)

    filepath = os.path.join(prefix, filepath)
    dl = dlbase + filepath
    
    return [filepath, str(sz), desc, mt, hash, dl]

def main(args):
    prog = os.path.splitext(args.pop(0))[0]
    parser = define_options(prog)
    opts = parser.parse_args(args)
    
    root = opts.rootdir.rstrip('/')
    if not os.path.isdir(root):
        raise IllegalArgumentException("%s: not an existing directory");

    recid = opts.recid or recnum
    duri = opts.duri or (duri_fmt % recid)
    dlbase = opts.dlbase or (dlbase_fmt % recid)
    title = opts.title or def_title

    mode = 'w'
    try:
        if opts.outfile:
            if os.path.exists(opts.outfile):
                mode = 'a'
            outfd = open(opts.outfile, mode)
        else:
            outfd = sys.stdout

        if root != '.':
            os.chdir(root)
            root = '.'

        if mode == 'w':
            write_header(outfd, duri, title)
        examine_dir(root, outfd, dlbase, opts.prefix)

    finally:
        if opts.outfile:
            outfd.close()

def examine_dir(root, outfd, dlbase, prefix=None):
    for indir, dirs, files in os.walk(root):
        if indir == root:
            indir = ''
        elif indir.startswith(root+'/'):
            indir = indir[len(root)+1:]

        for f in files:
            fpath = os.path.join(indir, f)
            print(",".join(summarize_file(fpath, dlbase, prefix)), file=outfd)
        top = ''

def write_header(outfd, dsid="[dataset_URI]", title="[title]"):
    hdr = """# Data file listing for NIST data publication,
# %s
# (%s)
#
# file path, file_size(bytes), file type, MIME type, SHA-256 hash, download URL
"""
    outfd.write(hdr % (title, dsid))

def define_options(progname, parser=None):
    """
    define command-line arguments
    """
    if not parser:
        parser = argparse.ArgumentParser(progname, None, description, epilog)

    parser.add_argument("rootdir", metavar="DIR", type=str, nargs='?', default='.',
                        help="the directory to scan (default: current directory)")
    parser.add_argument("-o", "--outfile", metavar="FILE", type=str, dest='outfile',
                        help="write the output table to FILE; if it exists, it will be "+
                             "appended to without a header")
    parser.add_argument("-p", "--prefix-path", metavar="PATH", type=str, dest="prefix",
                        help="prepend this path to each filename before recording its record")
    parser.add_argument("-r", "--record-id", metavar="ID", type=str, dest="recid", 
                        help="assume this record ID (usually a 4-digit number) when determining "+
                             "the dataset URI and base download URL (will be ignored if "+
                             'both -u and -i are set; default: "[XXXX]")')
    parser.add_argument("-u", "--base-download-url", metavar="URL", type=str, dest="dlbase",
                        help="use URL as the base for a file's download URL")
    parser.add_argument("-i", "--dataset-id", metavar="URI", type=str, dest="duri",
                        help="record this URI in the header to identify the dataset; the default "+
                             "will be the standard DOI based on the record number (see -r)")
    parser.add_argument("-t", "--title", metavar="TITLE", type=str, dest="title",
                        help='record TITLE in the header to identify the dataset (default: "[title]")')

    return parser

if __name__ == '__main__':
    try: 
        main(sys.argv)

    except OSError as ex:
        print("IO problem: "+str(ex), file=sys.stderr)
        sys.exit(1)

    except Exception as ex:
        print("Unexpected error: "+str(ex), file=sys.stderr)
        sys.exit(2)

