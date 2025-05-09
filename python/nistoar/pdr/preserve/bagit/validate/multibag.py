"""
This module implements a validator for the Multibag Profile
"""
import os, re
from collections import OrderedDict
from urllib.parse import urlparse

from .base import (Validator, BagValidatorBase, ALL, ValidationResults,
                   ERROR, WARN, REC, ALL, PROB)
from ..bag import NISTBag

class MultibagValidator(BagValidatorBase):
    """
    A validator that runs tests for compliance with the Multibag Bagit Profile.
    In particular, this validator tests whether a given bag can be consider
    part of a multibag aggregation.  
    """
    profile = ("Multibag", "0.4")

    def __init__(self, config=None):
        super(MultibagValidator, self).__init__(config)

    def test_version(self, bag, want=ALL, results=None, **kw):
        out = results
        if not out:
            out = ValidationResults(bag.name, want)

        data = bag.get_baginfo()

        t = self._err("2-Version",
                      "bag-info.txt field must have required element: Multibag-Version")
        t = out._add_applied(t, "Multibag-Version" in data and len(data["Multibag-Version"]) > 0)
        if t.failed():
            return out

        t = self._warn("2-Version", "bag-info.txt field, Multibag-Version, should only appear once")
        t = out._add_applied(t, len(data["Multibag-Version"]) == 1)

        t = self._err("2-Version-val",
                      "Multibag-Version must be set to a recognized version ('0.3', '0.4'")
        t = out._add_applied(t, data["Multibag-Version"][-1] in ["0.3", "0.4"])
        
        return out

    def test_reference(self, bag, want=ALL, results=None, **kw):
        out = results
        if not out:
            out = ValidationResults(bag.name, want)

        data = bag.get_baginfo()
        t = self._rec("2-Reference", "bag-info.txt should include field: Multibag-Reference")
        t = out._add_applied(t, "Multibag-Reference" in data and len(data["Multibag-Version"]) > 0)
        if t.failed():
            return out

        t = self._err("2-Reference-val", "Empty value given for Multibag-Reference")
        url = data["Multibag-Reference"][-1]
        t = out._add_applied(t, bool(url))

        t = self._err("2-Reference-val", "Multibag-Reference value is not an absolute URL")
        url = urlparse(url)
        t = out._add_applied(t, url.scheme and url.netloc)

        return out

    def test_tag_directory(self, bag, want=ALL, results=None, **kw):
        out = results
        if not out:
            out = ValidationResults(bag.name, want)

        data = bag.get_baginfo()
        headver = data.get("Multibag-Head-Version", [""])[-1]
        
        t = self._err("2-Tag-Directory",
                      "bag-info.txt: Multibag-Tag-Directory element should only be set for Head Bags")
        t = out._add_applied(t, "Multibag-Tag-Directory" not in data or headver)

        if "Multibag-Tag-Directory" in data:
            mdir = data["Multibag-Tag-Directory"]

            t = self._err("2-Tag-Directory",
                          "bag-info.txt: Value for Multibag-Tag-Directory should not be empty")
            t = out._add_applied(t, len(mdir) > 0 and mdir[-1])
            if t.failed():
                return out

            t = self._err("2-Tag-Directory",
                    "bag-info.txt: Multibag-Tag-Directory element should only appear no more than once")
            t = out._add_applied(t, len(mdir) == 1)

            t = self._err("2-Tag-Directory", "Multibag-Tag-Directory must exist as directory")
            t = out._add_applied(t, os.path.isdir(os.path.join(bag.dir, mdir[-1])))

        elif headver:
            t = self._err("2-Tag-Directory",
                          "Default Multibag-Tag-Directory, multibag, must exist as a directory")
            t = out._add_applied(t, os.path.exists(os.path.join(bag.dir, "multibag")))

        return out

    def test_head_version(self, bag, want=ALL, results=None, ishead=False, **kw):
        out = results
        if not out:
            out = ValidationResults(bag.name, want)

        data = bag.get_baginfo()
        if "Multibag-Head-Version" in data:
            value = data["Multibag-Head-Version"] 

            t = self._warn("2-Head-Version",
                           "bag-info.txt: Value for Multibag-Head-Version should not be empty")
            t = out._add_applied(t, len(value) > 0 and value[-1])
            if t.failed():
                return out

            t = self._warn("2-Head-Version",
                           "bag-info.txt: Multibag-Head-Version element should only appear once")
            t = out._add_applied(t, len(value) == 1)

        else:
            t = self._err("2-Head-Version",
                          "Head bag: bag-info.txt must have Multibag-Head-Version element")
            t = out._add_applied(t,  ishead)

        return out

    def test_head_deprecates(self, bag, want=ALL, results=None, **kw):
        out = results
        if not out:
            out = ValidationResults(bag.name, want)

        data = bag.get_baginfo()
        headver = data.get("Multibag-Head-Version", [""])[-1]

        t = self._err("2-Head-Deprecates",
                      "bag-info.txt: Multibag-Head-Deprecates element should only be set for Head Bags")
        t = out._add_applied(t, "Multibag-Head-Deprecates" not in data or headver)

        if "Multibag-Head-Deprecates" in data:
            values = data["Multibag-Head-Deprecates"]

            t = self._warn("2-Head-Deprecates",
                           "bag-info.txt: Value for Multibag-Head-Deprecates should not be empty")
            t = out._add_applied(t, len(values) > 0)
            if t.failed():
                return out

            empty = False
            selfdeprecating = False
            badfmt = []
            for val in values:
                empty = empty or not val
                parts = [p.strip() for p in val.split(',')]
                if len(parts) > 2:
                    badfmt.append(val)
                selfdeprecating = selfdeprecating or parts[0] == headver or \
                                  (len(parts) > 1 and parts[1] == bag.name)

            t = self._err("2-Head-Deprecates",
                          "bag-info.txt: Multibag-Head-Deprecates value must "+
                          "match format: VERSION, [BAGNAME]")
            comm = None
            if len(badfmt) > 0:
                comm = list(badfmt)
            t = out._add_applied(t, len(badfmt) == 0, comm)
            
            t = self._warn("2-Head-Deprecates",
                           "bag-info.txt: Value for Multibag-Head-Deprecates should not be empty")
            t = out._add_applied(t, not empty)

            t = self._warn("2-Head-Deprecates",
                           "bag-info.txt: Multibag-Head-Deprecates should not deprecate itself")
            t = out._add_applied(t, not selfdeprecating)

        return out

    def test_baginfo_recs(self, bag, want=ALL, results=None, **kw):
        out = results
        if not out:
            out = ValidationResults(bag.name, want)

        data = bag.get_baginfo()

        for el in ["Internal-Sender-Identifier",
                   "Internal-Sender-Description", "Bag-Group-Identifier"]:
            t = self._rec("2-2", "Recommed adding value for "+el+" into bag-info.txt file")
            t = out._add_applied(t, el in data and len(data[el]) > 0 and data[el][-1])
            if t.failed():
                continue
            
            t = self._err("2-2", "bag-info.txt: "+el+" element should not have empty values")
            t = out._add_applied(t, all(data[el]))

        return out

    def test_member_bags(self, bag, want=ALL, results=None, **kw):
        out = results
        if not out:
            out = ValidationResults(bag.name, want)

        mdir = bag.multibag_dir
        ishead = bag.is_headbag()
        
        assert mdir
        if os.path.isdir(mdir) != ishead:
            if ishead:
                t = self._err("2-Tag-Directory", "Multibag-Tag-Directory must exist as directory")
            else:
                t = self._err("2-Tag-Directory", "Multibag-Tag-Directory should only exist in a Head Bag")
            t = out._add_applied(t, False)
            return out

        mbemf = os.path.join(mdir, "member-bags.tsv")
        t = self._err("3.0-1", "Multibag tag directory must contain a member-bags.tsv file")
        t = out._add_applied(t, os.path.isfile(mbemf))
        if t.failed():
            return out

        badfmt = []
        badurl = []
        replicated = []
        found = set()
        foundme = False
        last = None
        with open(mbemf) as fd:
            i = 0
            for line in fd:
                i += 1
                parts = [f.strip() for f in line.strip().split('\t')]
                last = parts[0]
                if last == bag.name:
                    foundme = True
                if last in found:
                    replicated.append(i)
                else:
                    found.add(last)
                if len(parts) > 1 and len(parts[1]) > 0:
                    url = urlparse(parts[1])
                    if not url.scheme or url.netloc:
                        badurl.append(i)

        t = self._err("3.1-1", "member-bags.tsv lines must match format, BAGNAME[\tURL][\t...]")
        comm = None
        if badfmt:
            s = (len(badfmt) > 1 and "s") or ""
            if len(badfmt) > 4:
                badfmt[3] = '...'
                badfmt = badfmt[:4]
            comm = "line{0} {1}".format(s, ", ".join([str(b) for b in badfmt]))
        t = out._add_applied(t, len(badfmt) == 0, comm)

        t = self._err("3.1-2", "member-bags.txt: URL field must be an absolute URL")
        comm = None
        if badurl:
            s = (len(badurl) > 1 and "s") or ""
            if len(badurl) > 4:
                badurl[3] = '...'
                badurl = badurl[:4]
            comm = "line{0} {1}".format(s, ", ".join([str(b) for b in badurl]))
        t = out._add_applied(t, len(badurl) == 0, comm)

        t = self._err("3.1-3", "member-bags.tsv must list current bag name")
        t = out._add_applied(t, foundme)

        if ishead:
            t = self._err("3.1-4", "member-bags.tsv: Head bag must be listed last")
            t = out._add_applied(t, last == bag.name)

        t = self._warn("3.1-5", "member-bags.tsv: a bag name should only be listed once")
        comm = None
        if len(replicated) > 0:
            s = (len(replicated) > 1 and "s") or ""
            if len(replicated) > 4:
                replicated[3] = '...'
                replicated = replicated[:4]
            comm= "line{0} {1}".format(s,", ".join([str(b) for b in replicated]))
        t = out._add_applied(t, len(replicated) == 0, comm)

        return out

    def test_file_lookup(self, bag, want=ALL, results=None, ishead=False, **kw):
        out = results
        if not out:
            out = ValidationResults(bag.name, want)

        mdir = bag.multibag_dir
        ishead = bag.is_headbag()
        
        assert mdir
        if os.path.isdir(mdir) != ishead:
            if ishead:
                t = self._err("2-Tag-Directory", "Multibag-Tag-Directory must exist as directory")
            else:
                t = self._err("2-Tag-Directory", "Multibag-Tag-Directory should only exist in a Head Bag")
            t = out._add_applied(t, False)
            return out

        flirf = os.path.join(mdir, "file-lookup.tsv")
        t = self._err("3.0-2", "Multibag tag directory must contain a file-lookup.tsv file")
        t = out._add_applied(t, os.path.isfile(flirf))
        if t.failed():
            return out

        badfmt = []
        replicated = []
        missing = []
        paths = set()
        with open(flirf) as fd:
            i = 0
            for line in fd:
                i += 1
                parts = line.strip().split('\t')
                if parts[0] in paths:
                    replicated.append(i)
                else:
                    paths.add(parts[0])
                if len(parts) != 2:
                    badfmt.append(i)

                if len(parts) > 1 and parts[1] == bag.name and \
                   not os.path.isfile(os.path.join(bag.dir, parts[0])):
                    missing.append(i)

        t = self._err("3.2-1", "file-lookup.tsv lines must match format, FILEPATH\\tBAGNAME")
        comm = None
        if len(badfmt) > 0:
            s = (len(badfmt) > 1 and "s") or ""
            if len(badfmt) > 4:
                badfmt[3] = '...'
                badfmt = badfmt[:4]
            comm= "line{0} {1}".format(s,", ".join([str(b) for b in badfmt]))
        t = out._add_applied(t, len(badfmt) == 0, comm)

        t = self._err("3.2-2", "file-lookup.tsv: file path for current bag must exist as a file")
        comm = None
        if len(missing) > 0:
            s = (len(missing) > 1 and "s") or ""
            if len(missing) > 4:
                missing[3] = '...'
                missing = missing[:4]
            comm= "line{0} {1}".format(s,", ".join([str(b) for b in missing]))
        t = out._add_applied(t, len(missing) == 0, comm)

        t = self._warn("3.2-3", "file-lookup.tsv: a file path must be listed only once")
        comm = None
        if len(replicated) > 0:
            s = (len(replicated) > 1 and "s") or ""
            if len(replicated) > 4:
                replicated[3] = '...'
                replicated = replicated[:4]
            comm= "line{0} {1}".format(s,", ".join([str(b) for b in replicated]))
        t = out._add_applied(t, len(replicated) == 0, comm)
        
        # get a list of the payload files
        missing = []
        for root, subdirs, files in os.walk(bag.data_dir):
            for f in files:
                if f.startswith(".") or f.startswith("_"):
                    continue
                path = os.path.join(root, f)[len(bag.dir)+1:]
                if path not in paths:
                    missing.append(path)
        
        t = self._rec("3.2-4", "all payload file should be listed in the file-lookup.tsv file")
        comm = None
        if len(missing) > 0:
            s = (len(missing) > 1 and "s") or ""
            comm = [ "{0} payload file{1} missing from file-lookup.tsv"
                     .format(len(missing), s) ]
            comm += missing
        t = out._add_applied(t, len(missing) == 0)

        return out

