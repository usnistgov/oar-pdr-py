"""
This module implements a validator for the NIST-generated bags
"""
import os, re, json
from collections import OrderedDict
from collections.abc import Mapping
from urllib.parse import urlparse

import ejsonschema as ejs

from .base import (Validator, ValidatorBase, ALL, ValidationResults,
                   ERROR, WARN, REC, ALL, PROB, AggregatedValidator)
from .bagit import BagItValidator
from .multibag import MultibagValidator
from ..bag import NISTBag
from ..... import pdr
from .. import ConfigurationException

DEF_BASE_NERDM_SCHEMA = "https://data.nist.gov/od/dm/nerdm-schema/v0.1#"
DEF_NERDM_RESOURCE_SCHEMA = DEF_BASE_NERDM_SCHEMA + "/definitions/Resource"
DEF_PUB_NERDM_SCHEMA = "https://data.nist.gov/od/dm/nerdm-schema/pub/v0.1#"
DEF_NERDM_DATAFILE_SCHEMA = DEF_PUB_NERDM_SCHEMA + "/definitions/DataFile"
DEF_NERDM_SUBCOLL_SCHEMA = DEF_PUB_NERDM_SCHEMA + "/definitions/Subcollection"
DEF_BASE_POD_SCHEMA = "https://data.nist.gov/od/dm/pod-schema/v1.1#"
DEF_POD_DATASET_SCHEMA = DEF_BASE_POD_SCHEMA + "/definitions/Dataset"


class NISTBagValidator(ValidatorBase):
    """
    A validator that runs tests for compliance for the NIST Preservation Bag
    Profile.  Specifically, this validator only covers the NIST Profile-specific
    parts (excluding Multibag and basic BagIt compliance; see 
    NISTAIPValidator)
    """
    namere02 = re.compile("^(\w[\w\-]*).mbag(\d+)_(\d+)-(\d+)$")
    namere04 = re.compile("^(\w[\w\-]*).(\d+(_\d+)*).mbag(\d+)_(\d+)-(\d+)$")
    
    def __init__(self, config=None, profver="0.4"):
        super(NISTBagValidator, self).__init__(config)
        self._validatemd = self.cfg.get('validate_metadata', True)
        self.mdval = None
        self.profile = ("NIST", profver)
        if self._validatemd:
            schemadir = self.cfg.get('nerdm_schema_dir', pdr.def_schema_dir)
            if not schemadir:
                raise ConfigurationException("Need to set nerdm_schema_dir when "
                                             +"validate_metadata is True")
            if not os.path.exists(schemadir):
                raise ConfigurationException("nerdm_schema_dir directory does "+
                                             "exist: " + schemadir)
            self.mdval = {
                "_": ejs.ExtValidator.with_schema_dir(schemadir, ejsprefix='_'),
                "$": ejs.ExtValidator.with_schema_dir(schemadir, ejsprefix='$')
            }

    def test_name(self, bag, want=ALL, results=None, **kw):
        """
        Test that the bag has a compliant name
        """
        out = results
        if not out:
            out = ValidationResults(bag.name, want)

        pver = [int(v) for v in self.profile[1].split('.')]
        if pver > [0, 3]:
            t = self._warn("2-0", "Bag names should match format DSID.DM_DN.mbagMM_NN-SS")
            nm = self.namere04.match(bag.name)
            pverpos = (4, 5)
        else:
            t = self._warn("2-0", "Bag names should match format DSID.mbagMM_NN-SS")
            nm = self.namere02.match(bag.name)
            pverpos = (2, 3)

        t = out._add_applied(t, nm)

        if nm:
            t = self._warn("2-2",
                           "Bag name should include profile version 'mbag{0}_{1}'"
                            .format(*pver))
            t = out._add_applied(t, nm.group(pverpos[0]) == str(pver[0]) and
                                    nm.group(pverpos[1]) == str(pver[1]))

        return out

    def test_bagit_mdels(self, bag, want=ALL, results=None, **kw):
        """
        test that the bag-info.txt has the required/recommended metadata 
        elements defined by the BagIt standard.
        """
        out = results
        if not out:
            out = ValidationResults(bag.name, want)

        arkstart = "ark:/" + \
                   self.cfg.get("test_bagit_mdels",{}).get("ark_naan","")

        data = bag.get_baginfo()
        required = "Source-Organization Contact-Email Bagging-Date Bag-Group-Identifier Internal-Sender-Identifier".split()
        recommended = "Organization-Address External-Description External-Identifier Bag-Size Payload-Oxum".split()
        
        i = 0
        for name in required:
            i += 1
            t = self._err("3-1-"+str(i), "bag-info.txt file must include "+name)
            t = out._add_applied(t, name in data and len(data[name]) > 0 and data[name][-1])

        orgname = "National Institute of Standards and Technology"
        t = self._err("3-1-1-1", "'Source-Organization' must be set to '{0}'".format(orgname))
        t = out._add_applied(t, any([orgname in v for v in data.get('Source-Organization',[])]))

        i = 0
        for name in recommended:
            i += 1
            t = self._rec("3-2-"+str(i), "bag-info.txt file must include "+name)
            t = out._add_applied(t, name in data and len(data[name]) > 0 and data[name][-1])

        orgaddr = "Gaithersburg, MD 20899"
        t = self._warn("3-2-1-1", "'Organization-Address' must be set to the NIST address")
        t = out._add_applied(t, any([orgaddr in v for v in data.get('Organization-Address',[])]))

        t = self._rec("3-2-3-1", "'External-Identifier' values should include NIST ARK Identifier")
        t = out._add_applied(t, any([v.startswith(arkstart) for v in data.get('External-Identifier',[])]))

        return out;

    def test_profile_version(self, bag, want=ALL, results=None, **kw):
        """
        test that the bag-info.txt file includes the NIST BagIt Profile version
        """
        out = results
        if not out:
            out = ValidationResults(bag.name, want)

        data = bag.get_baginfo()

        t = self._err("3-3-1","bag-info.txt must include 'NIST-BagIt-Version'")
        t = out._add_applied(t, 'NIST-BagIt-Version' in data)

        val = data.get('NIST-BagIt-Version', [])
        if t.passed():
            t = self._err("3-3-1", "'NIST-BagIt-Version' value must not be empty")
            t = out._add_applied(t, len(val) > 0 and val[-1])
        if t.passed():
            t = self._err("3-3-1", "'NIST-BagIt-Version' should only be specified once")
            t = out._add_applied(t, len(val) == 1)
        if t.passed():
            t = self._err("3-3-1", "'NIST-BagIt-Version' value must be set to {0}".format(self.profile[1]))
            t = out._add_applied(t, val[-1] == self.profile[1])

        return out

    def test_dataset_version(self, bag, want=ALL, results=None, **kw):
        """
        Test that the version is of an acceptable form
        """
        out = results
        if not out:
            out = ValidationResults(bag.name, want)
        mdfile = os.path.join(bag.metadata_dir, "nerdm.json")
        version = None

        try:
            # if this fails, don't bother reporting it as another test
            # will
            with open(mdfile) as fd:
                data = json.load(fd)

            version = data.get('version')
            
        except Exception as ex:
            pass

        if version:
            t = self._warn("4.2-1", "version should be of form N.N.N...")
            comm = None
            if not re.match(r'^\d+(.\d+)*$', version):
                comm = [ "Offending version found in NERDm record: " +
                         str(version) ]
            t = out._add_applied(t, not comm, comm)
        
        data = bag.get_baginfo()

        if 'Multibag-Head-Version' in data:
            mbver = data['Multibag-Head-Version'][-1]

            t = self._warn("4.2-2", "Multibag-Head-Version info tag should be of form N.N.N...")
            comm = None
            if not re.match(r'^\d+(.\d+)*$', mbver):
                comm = [ "Offending version found in NERDm record: " +
                         str(version) ]
            t = out._add_applied(t, not comm, comm)

            if version:
                t = self._warn("4.2-3", "Multibag-Head-Version info tag should match NERDm version")
                comm = None
                if version != mbver:
                    comm = [ "{0} != {1}".format(mbver, version) ]
                t = out._add_applied(t, not comm, comm)

        return out
        
    def test_nist_md(self, bag, want=ALL, results=None, **kw):
        """
        Test that the bag-info.txt file includes the required NIST-specific
        metadata elements.
        """
        out = results
        if not out:
            out = ValidationResults(bag.name, want)

        data = bag.get_baginfo()

        self._check_nist_md('3-3-2', 'NIST-POD-Metadata',
                            "metadata/pod.json", bag, data, out)
        self._check_nist_md('3-3-3', 'NIST-NERDm-Metadata',
                            "metadata/nerdm.json", bag, data, out)

        return out

    def _check_nist_md(self, label, elname, expected, bag, data, out):
        if elname not in data:
            return out
        
        t = self._err(label, "'{0}' must not have an empty value".format(elname))
        t = out._add_applied(t, len(data[elname]) > 0 and data[elname][-1])
        if len(data[elname]) == 0:
            return out

        t = self._err(label, "'{0}' must be specified only once".format(elname))
        t = out._add_applied(t, len(data[elname]) == 1)

        t = self._warn(label, "'{0}' is typically set to '{1}'".format(elname, expected))
        t = out._add_applied(t, data[elname][-1] == expected)

        if data[elname][-1]:
            t = self._err(label, "File given in value of '{0}' must exist as a file".format(elname))
            t = out._add_applied(t, os.path.isfile(os.path.join(bag.dir,data[elname][-1])))

        return out

    def test_metadata_dir(self, bag, want=ALL, results=None, **kw):
        """
        test that the metadata tag directory exists.
        """
        out = results
        if not out:
            out = ValidationResults(bag.name, want)

        t = self._err("4.1-1", "Bag must have a tag directory named 'metadata'")
        t = out._add_applied(t, os.path.isdir(os.path.join(bag.dir, "metadata")))

        return out

    def test_pod(self, bag, want=ALL, results=None, **kw):
        """
        Test the existence and validity of the POD metadata file
        """
        out = results
        if not out:
            out = ValidationResults(bag.name, want)
        podfile = os.path.join(bag.metadata_dir, "pod.json")
        
        t = self._err("4.1-2-0", "Metadata tag directory must contain the file, pod.json")
        t = out._add_applied(t, os.path.isfile(podfile))
        if t.failed():
            return out

        t = self._err("4.1-2-1", "pod.json must contain a legal POD Dataset record") 
        try:
            with open(podfile) as fd:
                data = json.load(fd)
        except Exception as ex:
            comm = ["Failed reading JSON file: "+str(ex)]
            t = out._add_applied(t, False, comm)
            return out
        
        comm = None
        if self._validatemd:
            flav = self._get_mdval_flavor(data)
            schemauri = data.get(flav+"schema")
            if not schemauri:
                schemauri = DEF_POD_DATASET_SCHEMA
            verrs = self.mdval[flav].validate(data, schemauri=schemauri,
                                              strict=True, raiseex=False)
            if verrs:
                s = (len(verrs) > 1 and "s") or ""
                comm = ["{0} validation error{1} detected"
                        .format(len(verrs), s)]
                comm += [str(e) for e in verrs]
        t = out._add_applied(t, not comm, comm)
            
        t = self._err("4.1-3-2", "metadata/pod.json must have @type=dcat:Dataset")
        t = out._add_applied(t, "dcat:Dataset" in data.get("@type",[]))
        return out

    def _get_mdval_flavor(self, data):
        """
        return the prefix (or a default) used to identify meta-properties
        used for validation.
        """
        for prop in "schema extensionSchemas".split():
            mpfxs = [k[0] for k in data.keys() if k[1:] == prop and k[0] in "_$"]
            if len(mpfxs) > 0:
                return mpfxs[0]
        return "_"

    def test_nerdm(self, bag, want=ALL, results=None, **kw):
        """
        Test the existence and the validity of the resource-level NERDm
        metadata file.  
        """
        out = results
        if not out:
            out = ValidationResults(bag.name, want)
        mdfile = os.path.join(bag.metadata_dir, "nerdm.json")
        
        t = self._err("4.1-3-0", "Metadata tag directory must contain the file, nerdm.json")
        t = out._add_applied(t, os.path.isfile(mdfile))
        if t.failed():
            return out

        t = self._err("4.1-3-1", "metadata/nerdm.json must contain a legal NERDm Resource record") 
        try:
            with open(mdfile) as fd:
                data = json.load(fd)
        except Exception as ex:
            comm = ["Failed reading JSON file: "+str(ex)]
            t = out._add_applied(t, False, comm)
            return out

        if self._validatemd:
            flav = self._get_mdval_flavor(data)
            schemauri = data.get(flav+"schema")
            if not schemauri:
                schemauri = DEF_NERDM_RESOURCE_SCHEMA
            verrs = self.mdval[flav].validate(data, schemauri=schemauri,
                                              strict=True, raiseex=False)
            comm = None
            if verrs:
                s = (len(verrs) > 1 and "s") or ""
                comm = ["{0} validation error{1} detected"
                        .format(len(verrs), s)]
                comm += [str(e) for e in verrs]
            t = out._add_applied(t, not comm, comm)
            
        t = self._err("4.1-3-2", "metadata/nerdm.json must be a NERDm Resource record")
        t = out._add_applied(t, "nrdp:PublicDataResource" in data.get("@type",[]))

        t = self._err("4.1-3-3", "metadata/nerdm.json must not include any DataFile components")
        t = out._add_applied(t, not any(["nrdp:DataFile" in c['@type']
                                         for c in data.get("components",[])]))

        return out

    def test_metadata_tree(self, bag, want=ALL, results=None, **kw):
        """
        Test for the proper structure of the metadata tag directory tree.
        """
        out = results
        if not out:
            out = ValidationResults(bag.name, want)
        metadir = os.path.join(bag.dir, "metadata")
        datadir = os.path.join(bag.dir, "data")

        dotdir   = []
        dotfile  = []
        misngdir = []
        misngfil = []
        dnotadir = []
        fnotadir = []
        nonerd   = []
        for root, subdirs, files in os.walk(datadir):
            for dir in subdirs:
                path = os.path.join(root[len(datadir)-5:], dir)
                if dir.startswith('.'):
                    dotdir.append(path)
                    continue
                dir = os.path.join(root, dir)
                mdir = os.path.join(metadir, dir[len(datadir)+1:])
                if not os.path.exists(mdir):
                    misngdir.append(path)
                elif not os.path.isdir(mdir):
                    dnotadir.append("meta"+path)
                elif not os.path.exists(os.path.join(mdir,"nerdm.json")):
                    nonerd.append("meta"+path)

            for f in files:
                path = os.path.join(root[len(datadir)-4:], f)
                if f.startswith('.'):
                    dotfile.append(path)
                    continue
                f = os.path.join(metadir, root[len(datadir)+1:], f)
                if not os.path.exists(f):
                    misngfil.append(path)
                elif not os.path.isdir(f):
                    fnotadir.append("meta"+path)
                elif not os.path.exists(os.path.join(f,"nerdm.json")):
                    nonerd.append("meta"+path)

        t = self._warn("4.1-4-5", "Data directory should not contain files "+
                                  "or directories that start with '.'")
        comm = None
        if len(dotdir) > 0:
            s = (len(dotdir) > 1 and "ies") or "y"
            comm = [ "{0} dot-director{1} found".format(len(dotdir), s) ]
            comm += dotdir
        t = out._add_applied(t, len(dotdir) == 0, comm)
        
        t = self._err("4.1-4-1a", "Data directory must be mirrored by a metadata directory")
        comm = None
        if len(misngdir) > 0:
            s = (len(misngdir) > 1 and "ies are") or "y is"
            comm = [ "{0} data director{1} not mirrored in metadata directory"
                     .format(len(misngdir), s) ]
            comm += misngdir
        t = out._add_applied(t, len(misngdir) == 0, comm)
        
        t = self._err("4.1-4-1b", "Data file must be mirrored by a metadata directory")
        comm = None
        if len(misngfil) > 0:
            s = (len(misngfil) > 1 and "s are") or " is"
            comm = [ "{0} data file{1} not mirrored in metadata directory"
                     .format(len(misngfil), s) ]
            comm += misngfil
        t = out._add_applied(t, len(misngfil) == 0, comm)
        
        t = self._err("4.1-4-1a", "Data directory must be mirrored by a metadata directory")
        comm = None
        if len(dnotadir) > 0:
            s = (len(dnotadir) > 1 and "ies") or "y"
            comm = [ "{0} data director{1} mirrored by non-director{1}"
                     .format(len(dnotadir), s) ]
            comm += dnotadir
        t = out._add_applied(t, len(dnotadir) == 0, comm)
        
        t = self._err("4.1-4-1b", "Data file must be mirrored by a metadata directory")
        comm = None
        if len(fnotadir) > 0:
            fs = (len(fnotadir) > 1 and "s") or ""
            s = (len(fnotadir) > 1 and "ies") or "y"
            comm = [ "{0} data file{1} mirrored by non-director{2}"
                     .format(len(fnotadir), fs, s) ]
            comm += fnotadir
        t = out._add_applied(t, len(fnotadir) == 0, comm)
        
        t = self._err("4.1-5", "Every metadata subdirectory must contain a nerdm.json file")
        comm = None
        if len(nonerd) > 0:
            s = (len(fnotadir) > 1 and "s") or ""
            comm = [ "Missing nerdm.json file{1} for {0} data tree item{1}"
                     .format(len(nonerd), s) ]
            comm += nonerd
        t = out._add_applied(t, len(nonerd) == 0, comm)

        return out

    def test_nerdm_validity(self, bag, want=ALL, results=None, **kw):
        out = results
        if not out:
            out = ValidationResults(bag.name, want)
        metadir = os.path.join(bag.dir, "metadata")
        datadir = os.path.join(bag.dir, "data")

        vt = self._err("4.1-4-2b", "A data file directory must contain a valid NERDm metadata file.")
        ft = self._err("4.1-4-2c", "A data file's NERDm data must be of @type=nrdp:DataFile.")
        ct = self._err("4.1-4-2d", "A data directory's NERDm data must be of @type=nrdp:Subcollection.")
        kt = self._rec("4.1-4-2e", "_schema and @context fields recommended "+
                         "for inclusion in component NERDm data file")
        for root, subdirs, files in os.walk(datadir):
            for f in files:
                path = os.path.join(root[len(datadir):], f)
                mdf = os.path.join(metadir, path, "nerdm.json")
                if not os.path.isfile(mdf):
                    continue

                data = self._check_comp_legal(mdf, path, out)
                if data is None:
                    continue

                comm = None
                ok = '@type' in data and \
                     ("nrdp:DataFile" in data['@type'] or \
                      "nrdp:ChecksumFile" in data['@type'])
                if not ok:
                    comm = [path + ": " + str(data['@type'])]
                t = out._add_applied(ft, ok, comm)

                comm = None
                ok = ('_schema' in data or '$schema' in data) and \
                     '@context' in data;
                if not ok:
                    comm = ["filepath: " +path]
                t = out._add_applied(kt, ok, comm)

                if self._validatemd:
                    flav = self._get_mdval_flavor(data)
                    schemauri = data.get(flav+"schema")
                    if not schemauri:
                        schemauri = DEF_NERDM_DATAFILE_SCHEMA
                    verrs = self.mdval[flav].validate(data, schemauri=schemauri,
                                                      strict=True, raiseex=False)
                    comm = None
                    if verrs:
                        s = (len(verrs) > 1 and "s") or ""
                        comm = ["{0} validation error{1} detected"
                                .format(len(verrs), s)]
                        comm += [str(e) for e in verrs]
                    t = out._add_applied(vt, not comm, comm)
            
            for d in subdirs:
                path = os.path.join(root[len(datadir):], d)
                mdf = os.path.join(metadir, path, "nerdm.json")
                if not os.path.isfile(mdf):
                    continue

                data = self._check_comp_legal(mdf, path, out)
                if data is None:
                    continue

                comm = None
                ok = '@type' in data and "nrdp:Subcollection" in data['@type']
                if not ok:
                    comm = [path + ": " + str(data['@type'])]
                t = out._add_applied(ct, ok, comm)

                if self._validatemd:
                    flav = self._get_mdval_flavor(data)
                    schemauri = data.get(flav+"schema")
                    if not schemauri:
                        schemauri = DEF_NERDM_SUBCOLL_SCHEMA
                    verrs = self.mdval[flav].validate(data, schemauri=schemauri,
                                                      strict=True, raiseex=False)
                    comm = None
                    if verrs:
                        s = (len(verrs) > 1 and "s") or ""
                        comm = ["{0} validation error{1} detected"
                                .format(len(verrs), s)]
                        comm += [str(e) for e in verrs]
                    t = out._add_applied(vt, not comm, comm)
            
        return out

    def _check_comp_legal(self, nerdmf, path, res):
        vt = self._err("4.1-4-2a", "A data file directory must contain a legal NERDm metadata file.")
        pt = self._err("4.1-4-2f", "A data component's NERDm data must have a correct filepath property")
        try:
            with open(nerdmf) as fd:
                data = json.load(fd, object_pairs_hook=OrderedDict)
        except ValueError as ex:
            t = res._add_applied(vt, False, ["metadata/"+path+"/nerdm.json: Not a legal JSON file"])
            return None

        comm = None
        ok = isinstance(data, Mapping)
        if not ok:
            comm = ["metadata/"+path+"/nerdm.json: content is not a JSON object"]
        vt = res._add_applied(vt, ok, comm)
        if vt.failed():
            return None

        comm = None
        ok = 'filepath' in data and data['filepath'] == path
        if not ok:
            comm = [path + ": filepath: " + str(data.get('filepath'))]
        t = res._add_applied(pt, ok, comm)

        return data


class NISTAIPValidator(AggregatedValidator):
    """
    An AggregatedValidator that validates the complete profile for bags 
    created by the NIST preservation service.  
    """
    def __init__(self, config=None):
        if not config:
            config = {}
        bagit = BagItValidator(config=config.get("bagit", {}))
        multibag = MultibagValidator(config=config.get("multibag", {}))
        nist = NISTBagValidator(config=config.get("nist", {}))

        super(NISTAIPValidator, self).__init__(
            bagit,
            multibag,
            nist
        )

