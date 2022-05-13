#! /usr/bin/env python3
#
# type "python3 pdrdownload.py -h" for help on command-line options
#
def_pdrid = "mds2-2512"
def_filelist_url = None
description="""download data from the NIST PDR dataset, %s""" % def_pdrid
epilog="Note that no data will actually get downloaded unless --download (or -D) is provided."

def_filelist_url_pat = "https://data.nist.gov/od/ds/%s/filelisting.csv"
def_metadata_url_pat = "https://data.nist.gov/rmm/records?@id=%s"
def_progname = "pdrdownload"

import os, sys, shutil, csv, hashlib, argparse, json, re, traceback, math
from collections.abc import Mapping
from urllib.request import urlopen, URLError, HTTPError
from urllib.parse import urlparse

SILENT = -1
QUIET  =  0
NORMAL =  1
VERBOSE=  2

COL_FILE = 0
COL_SIZE = 1
COL_HASH = 4
COL_URL  = 5
COL_MSG  = 6

def define_options(progname, parser=None):
    """
    define command-line arguments
    """
    if not parser:
        parser = argparse.ArgumentParser(progname, None, description, epilog)

    parser.add_argument("select", metavar="FILE-OR-FOLDER", type=str, nargs='*', 
                        help="the dataset filepaths to download; if not given, all available files will "+
                             "be selected for download (see also -D)")
    parser.add_argument("-D", "--download", action="store_true", dest="dodownload",
                        help="actually download the selected files; if not provided, just a summary of "+
                             "the data that's available and has been download will be printed.")
    parser.add_argument("-d", "--dest-dir", metavar="DIR", type=str, dest='destdir', 
                        help="write the downloaded files to paths under DIR; the default is a "+
                             "directory under the current one named after the dataset's identifier.  "+
                             "If it doesn't already exist, it will be created.")
    parser.add_argument("-f", "--file-list-table", metavar="URLorFILE", dest="filelist",
                        help="read the list of available files from URLorFILE which can be either a "+
                             "URL or a local file; if not specified, a hard-coded URL will be used.")
    parser.add_argument("-e", "--error-list-table", metavar="FILE", dest="errfile", default="_failed.csv",
                        help="write or append a list of the requested files that failed to download "+
                             "correctly (including due to failed checksum check).  This file will not "+
                             "be written if no errors occur.  Default: _failed.csv")
    parser.add_argument("-E", "--no-error-list-table", action="store_const", dest="errfile", const=None,
                        help="do not write a file listing the failed downloads (a la -e)")
    parser.add_argument("-I", "--pdrid", metavar="ID", dest="pdrid",
                        help="assume ID as the identifier for the dataset; ignored if -F is specified. "+
                             "This can either be the PDR ARK identifier or its local part.")
    parser.add_argument("-w", "--overwrite", action="store_true", dest="force",
                        help="download each requested file regardless of whether it already exists in "+
                             "the destination directory; without this option, files already downloaded "+
                             "will not be downloaded again.")
    parser.add_argument("-c", "--check-remove", action="store_true", dest="clean",
                        help="after each download, run a check-sum check to ensure that file downloaded "+
                             "without error.  If the calculated value is not correct (i.e. different from "+
                             "what is in the file list table), the file will be removed from the destination "+
                             "directory (see also -C).")
    parser.add_argument("-C", "--check", action="store_true", dest="docheck",
                        help="after each download, run a check-sum check to ensure that file downloaded "+
                             "without error.  If the calculated value is not correct (i.e. different from "+
                             "what is in the file list table), then an error message will be printed but "+
                             "file is NOT deleted (see also -c).")
    parser.add_argument("-v", "--verbose", action="store_const", dest="verbosity", const=VERBOSE, default=NORMAL, 
                        help="if true, print extra messages about what's going on")
    parser.add_argument("-q", "--quiet", action="store_const", dest="verbosity", const=QUIET, default=NORMAL, 
                        help="if true, do not print any summary information")
    parser.add_argument("-s", "--silent", action="store_const", dest="verbosity", const=SILENT, default=NORMAL, 
                        help="if true, do not print any messages to the terminal (or as few messages as "+
                             "possible).")
    return parser

def set_options(progname, args, parser=None):
    """
    parse the given command-line arguments and save them as global options (stored as "opts" in 
    this script/module).
    :param progname:   the name to give to this script (in error messages)
    :param args:       the command line arguments to the script as an array of strings
    :param parser:     the ArgumentParser instance to use to parse args 
    """
    global opts
    global clparser
    global prog

    prog = progname
    if not parser:
        parser = clparser
    parser.prog = prog
        
    opts = parser.parse_args(args)

    if not opts.pdrid:
        opts.pdrid = def_pdrid
    if opts.pdrid.startswith("ark:"):
        opts.pdrid = ARK_PFX_RE.sub('', opts.pdrid)

#    if not opts.filelist:
#        opts.filelist = def_filelist_url_pat % opts.pdrid

    if not opts.destdir:
        opts.destdir = os.path.join(os.getcwd(), opts.pdrid)

    if opts.clean:
        opts.docheck = True

    return opts

prog = def_progname
clparser = define_options(def_progname)
opts = set_options(def_progname, [])

class MortalError(Exception):
    """
    an exception representing an error that should cause this script should exit with an 
    error status.
    """
    def __init__(self, msg, exitwith=3):
        super(MortalError, self).__init__(msg)
        self.excode = exitwith

    def die(self):
        complain(str(self), self.excode)

def complain(message, exitwith=-1, ostrm=sys.stderr):
    """
    conditionaly emit an error message, depending on a given verbosity level
    :param message:   the message to print to standard error
    :param exitwith:  if non-negative, exit the script with this exit code 
    :param ostrm:     the output stream to write the message to
    """
    global prog
    if ostrm and opts.verbosity > SILENT:
        if prog:
            ostrm.write("%s: " % prog)
        print(message, file=ostrm)

    if exitwith >= 0:
        sys.exit(exitwith)

def run():
    """
    carry out a download request according to the parsed command-line options
    """
    global opts
    if not os.path.exists(opts.destdir):
        parent = os.path.dirname(opts.destdir)
        if not os.path.isdir(parent):
            raise MortalError("Output parent dir is not an existing directory: "+parent, 2)
        os.mkdir(opts.destdir)

    if opts.filelist:
        filelist = ensure_filelist(opts.filelist, opts.destdir)
    else:
        filelist = get_default_filelist(opts.pdrid, opts.destdir)

    todo = 0
    dlcount = 0
    failed = 0
    found = 0
    if not opts.select:
        opts.select = True
    try:
        if opts.verbosity > QUIET:
            print("")
            (found, todo) = summarize_todo(filelist, opts.destdir, opts.select,
                                           opts.dodownload or not opts.docheck)
            print("")
    except OSError as ex:
        fname = os.path.join(os.path.basename(os.path.dirname(filelist)),
                             os.path.basename(filelist))
        raise MortalError("problem reading file list (%s): %s" % (fname, str(ex)), 3)

    if opts.dodownload or opts.docheck:
        if opts.dodownload and todo == 0 and opts.verbosity > QUIET:
            print("No records found to be downloaded (use -w to re-download)")
            return failed

        select = opts.select or True
        failedfile = opts.errfile
        if failedfile and '/' not in failedfile:
            failedfile = os.path.join(opts.destdir, failedfile)
        if failedfile and opts.verbosity > QUIET:
            print("Records with download errors will be written to "+failedfile)

        if opts.verbosity > QUIET:
            process = "download" if opts.dodownload else "checks"
            print("Beginning %s of %i files..." % (process, todo if opts.dodownload else found))
        dlcount, failed = process_files(opts.dodownload, filelist, opts.destdir, select, failedfile, 
                                         opts.docheck, opts.clean, opts.force)

        try:
            if opts.verbosity > QUIET:
                summarize_done(opts.dodownload, dlcount, failed, failedfile)
        except OSError as ex:
            raise MortalError("problem reading failed table: "+str(ex), 4)

    if opts.verbosity > QUIET:
        if not opts.dodownload:
            print("Add the -D argument to actually download the %sdata." %
                  ("selected " if opts.select else ""))
            if not opts.docheck:
                print("Add the -C argument to just do a checksum check on the files downloaded so far.")

    return failed

def download_url_to(url, dest):
    """
    make a GET request against a public URL and save its contents to the given destination file.
    """
    with urlopen(url) as resp:
        if os.path.isdir(dest):
            dest = os.path.join(dest, get_url_outname(resp))
        with open(dest, 'wb') as fd:
            shutil.copyfileobj(resp, fd)
        return (dest, resp.info().get_content_type())

_disp_fname_re = re.compile(r'filename="([^"]+)"')
def get_url_outname(resp):
    """
    choose an output file name saving an HTTP response to.  This will choose the recommendation in 
    the disposition header item; if that does not exist, the last path field in the url will be used. 
    :param resp:  the HTTPResponse from the opened URL
    """
    disp = resp.info().get_content_disposition()
    if disp:
        m = _disp_fname_re.search(disp)
        if m:
            return m.group(1)
    return os.path.basename(resp.geturl())

def ensure_filelist(listfile, destdir):
    """
    if it appears not to exist, download the table file listing available files from the dataset
    to the destination directory.
    :param listfile:  the table file, either as a URL or a local path
    """
    try:
        url = urlparse(listfile)
        if url.scheme:
            # it's a URL
            return get_filelist(listfile, destdir)
    except ValueError as ex:
        # treat as a path
        pass
    except HTTPError as ex:
        raise MortalError("Failed to retrieve file list from %s: %s (%s)" % (listfile, ex.reason, ex.code), 3)
    except URLError as ex:
        raise MortalError("Failed to retrieve file list from %s: %s" % (listfile, ex.reason), 3)

    if not os.path.isfile(listfile):
        raise MortalError("File list table not found: "+listfile, 2)
    return listfile

def get_filelist(url, destdir):
    with urlopen(url) as resp:
        if resp.info().get_content_type() == "application/json" or \
           resp.info().get_content_type() == "application/ld+json":
            destfile = os.path.join(destdir, "_nerdm.json")
        else:
            destfile = os.path.join(destdir, get_url_outname(resp))
        with open(destfile, 'wb') as fd:
            shutil.copyfileobj(resp, fd)

    if os.path.basename(destfile) == "_nerdm.json":
        with open(destfile) as fd:
            try:
                nerdm = json.load(fd)
            except (ValueError, TypeError) as ex:
                raise MortalError("Unable to read NERDm resource metadata: "+str(ex), 2)

        if 'ResultData' in nerdm:
            nerdm = nerdm['ResultData']
            if not isinstance(nerdm, list) or len(nerdm) < 1:
                raise MortalError("NERDm metadata not found via "+url, 2)
            nerdm = nerdm[0]
            if '_id' in nerdm:
                del nerdm['_id']

        nerdfile = destfile
        destfile = os.path.join(destdir, "_filelisting.csv")
        nerdm_to_filelist(nerdm, destfile)
        os.remove(nerdfile)

    return destfile

def get_default_filelist(pdrid, destdir):
    """
    query some default URLs to create a file listing table in the destination directory
    """
    destfile = None
    flurl = def_filelist_url
    if not flurl:
        flurl = def_filelist_url_pat % pdrid
    try:
        destfile = get_filelist(flurl, destdir)
    except URLError as ex:
        pass

    if destfile:
        return destfile

    nrdurl = def_metadata_url_pat % pdrid
    try:
        destfile = get_filelist(nrdurl, destdir)
    except URLError as ex:
        raise MortalError("Unable to retrieve a metadata record to create file listing: "+str(ex), 2)

    return destfile

def summarize_todo(listfile, destdir, select=True, fordownload=True):
    """
    print to standard out a summary the status of the output directory and what is requested to be done
    :param listfile:   the table file listing the files in the dataset
    :param destdir:    the destination directory for downloaded files
    :param select:     either a list of filenames requested for download or a boolean indicating whether 
                         all files listed in the table file should be downloaded
    :param fordownload: if True, provide summary info assuming user has/will request downloads; otherwise,
                       assume this just for file checks.
    :return: a 2-tuple giving the number of files already downloaded and the number of files to be downloaded
    """
    listfile = ensure_filelist(listfile, destdir)

    found = 0
    total = 0
    selected = 0
    selfound = 0
    selsz = 0
    foundsz = 0
    totsz = 0

    requested = "No"
    if select:
        if isinstance(select, list):
            select = set(select)
        if isinstance(select, set):
            if len(select) == 0:
                requested = "No"
            else:
                requested = select
        else:
            requested = "All"

    selected = set()
    with open(listfile) as fd:
        rdr = csv.reader(fd)
        for row in rdr:
            if row[0].lstrip().startswith('#'):
                continue
            fname = row[COL_FILE].lstrip().lstrip('/')
            try:
                size = int(row[COL_SIZE])
            except ValueError as ex:
                size = 0
            total += 1
            totsz += size

            fileselected = False
            if (isinstance(requested, set) and 
                fname in requested or any([fname.startswith(r+'/') for r in requested])) or \
               requested == "All":
                fileselected = True
                
            if os.path.exists(os.path.join(destdir, fname)):
                found += 1
                foundsz += size
                if fileselected and (opts.force or not fordownload):
                    selected.add(fname)
                    selsz += size
            elif fordownload and fileselected:    
                selected.add(fname)
                selsz += size

    print("Dataset id: "+opts.pdrid)
    print("Output directory: "+destdir)
    print("File Table: "+listfile)
    if totsz == 0:
        print("  lists %i file%s, %i of which %s already downloaded." %
              (total, "s" if total != 1 else "", found, "are" if found != 1 else "is"))
        print("  (Warning: file table appears to not include proper file sizes.)")
    else:
        print("  lists %i file%s (%s), %i (%s) of which %s already downloaded." %
              (total, "s" if total != 1 else "", formatBytes(totsz),
               found, formatBytes(foundsz), "are" if found != 1 else "is"))

    todo = len(selected)
    if select:
        process = "downloaded" if fordownload else "checked"
        if totsz == 0:
            print("%i file%s selected to be %s" % (todo, "s" if todo != 1 else "", process))
        else:
            print("%i file%s (%s) selected to be %s" %
                  (todo, "s" if todo != 1 else "", formatBytes(selsz), process))
        if (opts.docheck):
            print("File downloads will be verified by checksums")

    return (found, todo)

def summarize_done(diddownload, successcount, failedcount, failedfile=None):
    """
    summarize the results of the downloads
    """
    if diddownload:
        print("Successfully downloaded %i file%s" % (successcount, "s" if successcount != 1 else ""))
    else:
        print("%i downloaded file%s passed integrity checks" % (successcount, "s" if successcount != 1 else ""))
    if failedcount > 0:
        print("%d file%s failed to download correctly"  % (failedcount, "s" if failedcount != 1 else ""))
        if failedfile:
            print("  See %s for a list of failed downloads" % failedfile)
    return failedcount

def check_files(listfile, destdir, failedtbl=None, rmonerr=True, select=True):
    """
    run checks on the files found in the output directory.  Unless particular files or directories are 
    specified (via `select`), this function will look for all files given in the `listfile` file.  Any 
    files not found are ignored; however files that are the wrong size or have the wrong checksum will 
    by default be removed and its info will be added to the failed table file.
    :param listfile:      the URL or local file path to file table listing the available files
    :param destdir:       the directory to look for files listed in `listfile`
    :param str failtbl:   the path to a local file where the list of files that failed to download should 
                          be written.  This file will be a CSV table where each row is the row from the 
                          input file table with an extra column appended, indicating the reason for the 
                          failure.  If None, no such record of failures will be written.
    :param bool rmonerr:  if True and a file download does not verify via a checksum check, the downloaded 
                          file will be removed.  
    :param select:        either a list of filepaths to download or a boolean indicating whether all 
                          available files should be downloaded.  If a list is given, each element should 
                          match a file path or an ancestor directory for one of the files listed in 
                          first column of the file table.
    """
    return process_files(False, listfile, destdir, select, failedtbl, True, rmonerr)

def download_files(listfile, destdir, select=True, failedtbl=None, docheck=True, rmonerr=True, force=False):
    """
    download selected or all files from the specified file table into the given destination directory
    :param listfile:      the URL or local file path to file table listing the available files
    :param destdir:       the directory to write files into
    :param select:        either a list of filepaths to download or a boolean indicating whether all 
                          available files should be downloaded.  If a list is given, each element should 
                          match a file path or an ancestor directory for one of the files listed in 
                          first column of the file table.
    :param str failtbl:   the path to a local file where the list of files that failed to download should 
                          be written.  This file will be a CSV table where each row is the row from the 
                          input file table with an extra column appended, indicating the reason for the 
                          failure.  If None, no such record of failures will be written.
    :param bool docheck:  if True, verify that the SHA-256 checksum of the downloaded file matches that 
                          given in fifth column of the table.
    :param bool rmonerr:  if True and a file download does not verify via a checksum check, the downloaded 
                          file will be removed.  
    :param bool force:    if True, all selected files will be downloaded, overwriting any previously 
                          downloaded versions.
    :return: 2-tuple of integers giving the number of files successfully downloaded and the number that 
                          failed to download successfully
    """
    return process_files(True, listfile, destdir, select, failedtbl, docheck, rmonerr, force)
                

def process_files(dodownload, listfile, destdir, select=True, failedtbl=None, docheck=True, rmonerr=True,
                  force=False):
    """
    either download or just check downloaded files, depending on the `dodownload` argument.  This provides
    the implementations for `download_files()` and `check_files()`.
    :param bool dodownload:  if True, download the files; otherwise, just check downloaded the files 
                             found in the destination directory.
    """
    listfile = ensure_filelist(listfile, destdir)

    dlcount = 0
    failed = 0

    if failedtbl:
        same = False
        try:
            same = os.path.samefile(failedtbl, listfile)
        except FileNotFoundError as ex:
            same = os.path.normcase(failedtbl) == os.path.normcase(listfile)
        if same:
            MortalError(("Can't read files from failed output file\n"
                         "  Rename %s to retry previously failed files (or use -e or -E)") %
                        os.path.join(os.path.basename(os.path.dirname(failedtbl)),
                                     os.path.basename(failedtbl)), 2)

    errfd = None
    if not select:
        return (0, 0);

    nmlen = 0
    try: 
        with open(listfile) as fd:
            # read each row in the file listing table
            tbl = csv.reader(fd)
            for row in tbl:
                if row[COL_FILE].lstrip().startswith('#'):
                    # comment row
                    continue
                if hasattr(select, '__contains__') and row[COL_FILE] not in select and \
                   not any([row[COL_FILE].startswith(s.rstrip('/')+'/') for s in select]):
                    # not requested by user
                    continue

                # we want this one
                reason = None
                parent = os.path.dirname(row[COL_FILE]).lstrip('/')
                if parent:
                    parent = os.path.join(destdir, parent)
                    if not os.path.exists(parent):
                        os.makedirs(parent)
                destfile = os.path.join(destdir, row[COL_FILE].lstrip('/'))
                if dodownload and not force and os.path.exists(destfile):
                    if opts.verbosity >= VERBOSE:
                        print("  skipping %s; already downloaded" % row[COL_FILE])
                    continue

                try:
                    size = int(row[COL_SIZE])
                except ValueError:
                    size = None

                if opts.verbosity > QUIET:
                    doing = "fetching" if dodownload else "checking"
                    end = "\r" if opts.verbosity < VERBOSE else "\n"
                    sp = ''
                    if opts.verbosity < VERBOSE and nmlen > len(row[COL_FILE]):
                        sp = ' ' * (nmlen - len(row[COL_FILE]) + 2)
                    nmlen = len(row[COL_FILE])
                    sz = " (%s)" % formatBytes(size) if size else ''
                    print("  %s %s%s...%s" % (doing, row[COL_FILE], sz, sp), end=end)

                if dodownload:
                    # download the file
                    try:
                        download_url_to(row[COL_URL], destfile)
                    except URLError as ex:
                        # failed to open the URL
                        failed += 1
                        reason = str(ex)
                        if opts.verbosity >= VERBOSE:
                            complain(row[COL_FILE] + ": " + reason)
                        if failedtbl:
                            if not errfd:
                                errfd = openfailed(failedtbl)
                            row.append(reason)
                            errfd.write(",".join(row)+"\n")
                        continue
                            
                    except IOError as ex:
                        # we'll check the result afterward
                        reason = "copy error: " + str(ex)
                        if opts.verbosity >= VERBOSE:
                            complain(row[COL_FILE] + ": " + reason)
                    except OSError as ex:
                        # we'll check the result afterward
                        reason = str(ex)
                        if opts.verbosity >= VERBOSE:
                            complain(row[COL_FILE] + ": " + reason)

                    # the file should now exist
                    if not os.path.isfile(destfile):
                        if failedtbl and not errfd:
                            errfd = openfailed(failedtbl)
                        if errfd:
                            row.append(reason)
                            errfd.write(",".join(row)+"\n")
                        continue

                elif not os.path.exists(destfile):
                    continue

                if size is None:
                    if opts.verbosity > SILENT:
                        complain("Warning: no size given in file table for "+row[COL_FILE])
                elif os.stat(destfile).st_size != size:
                    reason = "Wrong download size"

                if not reason and docheck:
                    if not row[COL_HASH]:
                        if opts.verbosity > SILENT:
                            complain("Warning: no checksum hash in file table for "+row[COL_FILE])
                    elif checksum_of(destfile) != row[COL_HASH]:
                        reason = "Checksum failure"

                if reason:
                    failed += 1
                    if opts.verbosity >= VERBOSE:
                        complain(row[COL_FILE] + ": " + reason)
                    if rmonerr:
                        os.remove(destfile)
                    if failedtbl:
                        if not errfd:
                            errfd = openfailed(failedtbl)
                        row.append(reason)
                        errfd.write(",".join(row)+"\n")
                else:
                    dlcount += 1

    finally:
        if opts.verbosity == NORMAL and nmlen > 0:
            print(' ' * (nmlen + len("  fetching (XXX.X XX)  ...")), end="\r")
        if errfd:
            errfd.close()

    return (dlcount, failed)

def openfailed(ffile):
    mode = 'w'
    if os.path.exists(ffile):
        mode = 'a'
    out = open(ffile, mode)
    if mode == 'w':
        try:
            out.write("# This table list requested files that failed to download\n")
            out.write("# \n")
        except Exception as ex:
            out.close()
            raise
    return out

def nerdm_to_filelist(nerdm, listfile=None):
    """
    convert a NERDm Resource record to a file listing CSV table
    :param nerdm:     either a dictionary contain NERDm Resource data or a string giving the path 
                      to a local file
    :param listfile:  a path to the file to write the CSV table to; if not provided, it will be 
                      written to "_filelisting.csv" in the currently set destination directory
    """

    if not isinstance(nerdm, Mapping):
        # assume str or Path filename provided
        with open(nerdm) as fd:
            data = json.load(fd)
        nerdm = data

    if not isinstance(nerdm, Mapping):
        raise ValueError("Input data does not look like a NERDm Resource: not a dictionary")
    if 'components' not in nerdm and 'title' not in nerdm:
        raise ValueError("Input data does not look like a NERDm Resource: no components nor title")

    return nerdmcomps_to_filelist(nerdm.get('components', []), listfile,
                                  nerdm.get('@id'), nerdm.get('title'))

def nerdmcomps_to_filelist(nerdm, listfile=None, dsid=None, title=None):
    """
    convert a NERDm Resource record to a file listing CSV table
    :param nerdm:     an array of NERDm Component objects including the downloadable files that 
                      should be exported to the output table.  Checksum files will be ignored.  
    :param listfile:  a path to the file to write the CSV table to; if not provided, it will be 
                      written to "_filelisting.csv" in the currently set destination directory
    :param dsid:      the dataset ID that the list of components are a part of; if provided, it 
                      will be written to the output table's header.
    :param title:     the title of the dataset that the list of components are a part of; if 
                      provided, it will be written to the output table's header.
    """
    if not isinstance(nerdm, list):
        raise TypeError("Input NERDm component list is not an array")
    if not listfile:
        listfile = os.path.join(opts.destdir, "_filelisting.csv")
    if os.path.exists(listfile):
        complain("Warning: over-writing "+
                 os.path.join(os.path.basename(os.path.dirname(listfile)), os.path.basename(listfile)))

    pfx = re.compile(r'^.*:')
    def isoftype(types, target):
        return pfx.sub('', target) in [pfx.sub('', t) for t in types]

    with open(listfile, 'w') as listfd:
        listfd.write("# Data file listing for NIST data publication")
        if title:
            listfd.write(",\n# ")
            listfd.write(title)
        if dsid:
            listfd.write("\n# (%s)" % dsid)
        listfd.write("\n# \n")
        listfd.write("# file path, file_size(bytes), file type, MIME type, SHA-256 hash, download URL\n")

        for comp in nerdm:
            types = comp.get('@type', [])
            if isoftype(types, "ChecksumFile") or isoftype(types, "Hidden"):
                continue
            if comp.get('downloadURL') and \
               (isoftype(types, "DownloadableFile") or isoftype(types, "DataFile")):
                filep = comp.get('filepath')
                fmt = comp.get('format',{}).get('description','')
                if ',' in fmt:
                    fmt = '"%s"' % fmt
                if not filep:
                    if opts.verbosity >= VERBOSE:
                        complain("warning: missing filepath property for url="+comp.get('downloadURL',''))
                    continue
                data = [
                    filep,
                    str(comp.get('size','')),
                    fmt,
                    comp.get('mediaType',''),
                    comp.get('checksum',{}).get('hash',''),
                    comp.get('downloadURL')
                ]
                listfd.write(",".join(data))
                listfd.write("\n")

    return listfile


def checksum_of(filepath):
    """
    return the checksum for the given file
    """
    bfsz = 10240000   # 10 MB buffer
    sum = hashlib.sha256()
    with open(filepath, mode='rb') as fd:
        while True:
            buf = fd.read(bfsz)
            if not buf: break
            sum.update(buf)
    return sum.hexdigest()
                
def formatBytes(nb, numAfterDecimal=-1):
    """
    format a byte count for display using metric byte units.  
    :param int nb:               the number of bytes to format
    :param int numAfterDecimal:  the number of digits to appear after the decimal if the value is 
                                 greater than 1000; if less than zero (default), the number will be 
                                 1 or 2.
    """
    if not isinstance(numAfterDecimal, int):
        numAfterDecimal = -1
    if not isinstance(nb, int):
        return ''
    if nb == 0:
        return "0 Bytes"
    if nb == 1:
        return "1 Byte"
    base = 1000
    e = ['Bytes', 'kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    f = math.floor(math.log10(nb) / math.log10(base))
    v = nb / math.pow(base, f)
    d = numAfterDecimal
    if d < 0:
        if f == 0:   # less than 1 kilobyte
            d = 0
        elif v < 10.0:
            d = 2
        else:
            d = 1
        v = round(v, d)
    return "%s %s" % ( (("%%.%df" % d) % v), e[f] )

def main(progname, args):
    global opts
    set_options(progname, args)

    failed = run();
    if failed > 0:
        raise MortalError("%d file%s failed to download" %
                          (failed, "s" if failed != 1 else ""), 2)
        

if __name__ == '__main__':
    try: 
        main(sys.argv[0], sys.argv[1:])

    except MortalError as ex:
        ex.die()

    except Exception as ex:
        if opts.verbosity > SILENT:
            print("Unexpected error: "+str(ex), file=sys.stderr)
            traceback.print_tb(sys.exc_info()[2])
        sys.exit(4)

