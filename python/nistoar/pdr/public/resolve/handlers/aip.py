"""
Handlers for resolving PDR resource identifiers
"""
import sys, os, re, json, tempfile
from collections import OrderedDict

from .base import Handler, Format, FormatSupport, TextSupport, Unacceptable, UnsupportedFormat, Ready
from nistoar.pdr import constants as const
from nistoar.pdr.exceptions import ConfigurationException, IDNotFound, StateException
from nistoar.pdr import distrib
from nistoar.pdr.publish.bagger import utils as bagutils
import nistoar.pdr.distrib as distrib

import multibag

VER_DELIM  = const.RELHIST_EXTENSION.lstrip('/')
DIST_DELIM = "pdr:d"
HEAD_DELIM = "pdr:h"

class AIPHandler(Handler):
    """
    A Handler for resolving PDR AIP (Archive Information Package) identifiers.  This is intended 
    to handle endpoints under "/aip/"; however, the path passed is expected to be relative to this base.
    """

    def __init__(self, path, wsgienv, start_resp, config={}, log=None):
        super(AIPHandler, self).__init__(path, wsgienv, start_resp, config)
        self._dsep = self.cfg.get('locations', {}).get('distributionService')
        if not self._dsep:
            raise ConfigurationException("Missing config param: locations.distributionService")
        self._dsep = self._dsep.rstrip('/')
        self._svccli = distrib.RESTServiceClient(self._dsep)
        self.log = log

    def do_GET(self, path, ashead=False, format=None):
        """
        handle a GET (or HEAD) request of a resource
        :param str path:    the path to the resource to get.  In general, this path is not necessarily
                            the original PATH_INFO, but has a base stripped from it.  
        :param bool ashead: If True, handle the resolution of the resource as a HEAD request by omitting
                            the body from the response
        :param format:      A label or MIME-type for the format to return.  If None, the 
                            format will be determined from the client's request.
                            :type format: str or Format
        :return:  the body
        """
        path = path.lstrip('/')
        if not path:
            return Ready('', self._env, self._start).handle()

        parts = path.split('/')
        if len(parts) == 1:
            if bagutils.is_legal_bag_name(parts[0]):
                return self.resolve_aip_file(parts[0])
            return self.resolve_aip_id(parts[0])

        aipid = parts[0]
        if parts[1] == DIST_DELIM:
            return self.resolve_aip_distrib(aipid, '/'.join(parts[2:]))

        if parts[1] == HEAD_DELIM:
            return self.resolve_aip_head(aipid, path='/'.join(parts[2:]))

        if parts[1] == VER_DELIM:
            ver = None
            if len(parts) > 2:
                ver = parts[2]
            return self.resolve_aip_version(aipid, ver, '/'.join(parts[3:]))

        return send_error(404, "Not Found")

    def _determine_request_formats(self, format=None):
        if isinstance(format, str):
            return [ format ]
        elif isinstance(format, list):
            # allow format param to be a list of desired formats
            return [(isinstance(f, Format) and f.name) or f for f in format
                    if isinstance(f, str) or isinstance(f, Format)]
        else:
            return self.ordered_formats()

    def resolve_aip_id(self, aipid, version=None, ashead=False, format=None):
        """
        return information about the AIP with the given identifier
        :param str aipid:   the identifier for the AIP of interest
        :param format:      A label or MIME-type for the format to return.  If None, the 
                            format will be determined from the client's request.
                            :type format: str or Format
        """
        supp_fmts = TextSupport()
        supp_fmts.support(Format("json", "application/json"), ["text/json", "application/json"], True)

        if not isinstance(format, Format):
            reqformats = self._determine_request_formats(format)
            try:
                format = supp_fmts.select_format(reqformats, self.ordered_accepts())
            except Unacceptable as ex:
                return self.send_error(406, "Not Acceptable", str(ex))
            except UnsupportedFormat as ex:
                return self.send_error(400, "Unsupported Format", str(ex))

        if format is None:
            # set the default return format
            format = supp_fmts.default_format()

        distcli = distrib.BagDistribClient(aipid, self._svccli)
        try:

            vers = distcli.list_versions()
            head = distcli.describe_head_for_version(version)

        except distrib.DistribResourceNotFound as ex:
            return self.send_error(404, "AIP Not Found")
        except distrib.DistribServerError as ex:
            if self.log:
                self.log.exception("Trouble accessing distrib service: "+str(ex))
            return self.send_error(502, "Failure from upstream service")
        except distrib.DistribClientError as ex:
            if self.log:
                self.log.exception("Failure using distrib service: "+str(ex))
            return self.send_error(502, "Internal Failure")
        
        if format.name == "json" or format.name == "text":
            out = OrderedDict([
                ("aipid", aipid),
                ("maxMultibagSequence", head.get("multibagSequence")),
            ])
            if head.get('sinceVersion'):
                if version:
                    out['version'] = head.get('sinceVersion')
                else:
                    out['latestVersion'] = head.get('sinceVersion')
            del head['aipid']
            out['headBag'] = head
            out['versions'] = vers

            return self.send_ok(content=json.dumps(out, indent=2),
                                contenttype=format.ctype, ashead=ashead)

        # log unwired format!
        raise StateException("No dataset handler implemented for format="+format.name)

    _nativects = {
        "zip": "application/zip",
        "tgz": "application/gzip",
        "gz":  "application/gzip",
        "7z":  "application/7z",
        "":    "application/octet-stream"
    }
    
    def resolve_aip_file(self, aipbag, ashead=False, format=None):
        """
        return an AIP bag file
        :param str aipbag:  the name of the AIP bag file to send
        :param format:      A label or MIME-type for the format to return.  If None, the 
                            format will be determined from the client's request.
                            :type format: str or Format
        """
        default2native = True
        bag = bagutils.BagName(aipbag)
        if not bag.serialization:
            # resolve the bag name into an available serialized bag name
            try:
                aipbag = self._find_serialized_bag(bag.aipid, aipbag)
                default2native = False
            except distrib.DistribResourceNotFound as ex:
                return self.send_error(404, "AIP Not Found")
            except distrib.DistribServerError as ex:
                if self.log:
                    self.log.exception("Trouble accessing distrib service: "+str(ex))
                return self.send_error(502, "Failure from upstream service")
            except distrib.DistribClientError as ex:
                if self.log:
                    self.log.exception("Failure using distrib service: "+str(ex))
                return self.send_error(502, "Internal Failure")
        
        supp_fmts = FormatSupport()
        supp_fmts.support(Format("json", "application/json"), ["text/json", "application/json"])

        nct = self._nativects.get(os.path.splitext(aipbag)[1].lstrip('.'))
        supp_fmts.support(Format("native", nct), [nct], default2native)

        if not isinstance(format, Format):
            reqformats = self._determine_request_formats(format)
            try:
                format = supp_fmts.select_format(reqformats, self.ordered_accepts())
            except Unacceptable as ex:
                return self.send_error(406, "Not Acceptable", str(ex))
            except UnsupportedFormat as ex:
                return self.send_error(400, "Unsupported Format", str(ex))
                
        if format is None:
            # set the default return format
            format = supp_fmts.default_format()

        bagep = self._dsep + "/_aip/" + aipbag

        if format.name == "json":
            self.add_header("Location", bagep + "/_info")
            self.set_response(307, "Found")
            self.end_headers()
            return []
            
        if format.name == "native":
            self.add_header("Location", bagep)
            self.set_response(307, "Found")
            self.end_headers()
            return []

        # log unwired format!
        raise StateException("No dataset handler implemented for format="+format.name)

    def _find_serialized_bag(self, aipid, aipbag):
        distcli = distrib.BagDistribClient(aipid, self._svccli)
        aipbag += "."
        matched = [b for b in distcli.list_all() if b.startswith(aipbag)]
        if len(matched) == 0:
            raise distrib.DistribResourceNotFound(aipbag.rstrip('.'))
        return matched[0]

    def resolve_aip_distrib(self, aipid, distid='', ashead=False, format=None):
        """
        return listing of the AIP distributions (bag files) available for the given AIP
        :param str aipid:   the identifier for the AIP of interest
        :param str distid:  any further path given after the distributions endpoint indicating the 
                            which distribution to retrieve.  This is either an integer, indicating the 
                            bag sequence number or the name of the bag.
        :param format:      A label or MIME-type for the format to return.  If None, the 
                            format will be determined from the client's request.
                            :type format: str or Format
        """
        distcli = distrib.BagDistribClient(aipid, self._svccli)
        try:

            dists = distcli.describe_all()

        except distrib.DistribResourceNotFound as ex:
            return self.send_error(404, "AIP Not Found")
        except distrib.DistribServerError as ex:
            if self.log:
                self.log.exception("Trouble accessing distrib service: "+str(ex))
            return self.send_error(502, "Failure from upstream service")
        except distrib.DistribClientError as ex:
            if self.log:
                self.log.exception("Failure using distrib service: "+str(ex))
            return self.send_error(502, "Internal Failure")

        dist = None
        if distid:
            try:
                seq = int(distid)
                dist = [d for d in dists if d.get('multibagSequence') == seq]
                dist = (len(dist) > 0 and dist[0]) or None
            except ValueError as ex:
                dist = [d for d in dists if d.get('name') == distid]
                dist = (len(dist) > 0 and dist[0]) or None
            if not dist:
                return self.send_error(404, "Not Found")
                
        supp_fmts = TextSupport()
        supp_fmts.support(Format("json", "application/json"), ["text/json", "application/json"], True)
        if dist:
            nct = self._nativects.get(os.path.splitext(dist.get('name',''))[1].lstrip('.'))
            supp_fmts.support(Format("native", nct), [nct])

        if not isinstance(format, Format):
            reqformats = self._determine_request_formats(format)
            try:
                format = supp_fmts.select_format(reqformats, self.ordered_accepts())
            except Unacceptable as ex:
                return self.send_error(406, "Not Acceptable", str(ex))
            except UnsupportedFormat as ex:
                return self.send_error(400, "Unsupported Format", str(ex))
        if not format:
            format = supp_fmts.default_format()
                
        if format.name == "json" or format.name == "text":
            if dist:
                return self.send_ok(content=json.dumps(dist, indent=2),
                                    contenttype=format.ctype, ashead=ashead)
                
            return self.send_ok(content=json.dumps(dists, indent=2),
                                contenttype=format.ctype, ashead=ashead)
        
        if format.name == "native":
            if not dist:
                raise StateException("Programming error: native format not available for this resource")

            bagep = dist.get('downloadURL', self._dsep + "/_aip/" + dist['name'])
            self.add_header("Location", bagep)
            self.set_response(307, "Found")
            self.end_headers()
            return []

        # log unwired format!
        raise StateException("No dataset handler implemented for format="+format.name)

    def resolve_aip_head(self, aipid, version=None, path='', ashead=False, format=None):
        """
        resolve the head bag of the latest version of a given AIP
        :param str aipid:   the identifier for the AIP of interest
        :param str version: the version of the AIP to find the head bag for; if empty or None (default),
                            the head bag for the latest version is resolved
        :param str path:    any further path given after the headbag request endpoint.  Currently,
                            this should be empty or None; otherwise, a 404 is returned.
        :param format:      A label or MIME-type for the format to return.  If None, the 
                            format will be determined from the client's request.
                            :type format: str or Format
        """
        if path:
            return self.send_error(403, "Not a supported resource")

        distcli = distrib.BagDistribClient(aipid, self._svccli)
        try:

            head = distcli.describe_head_for_version(version)

        except distrib.DistribResourceNotFound as ex:
            return self.send_error(404, "AIP Not Found")
        except distrib.DistribServerError as ex:
            if self.log:
                self.log.exception("Trouble accessing distrib service: "+str(ex))
            return self.send_error(502, "Failure from upstream service")
        except distrib.DistribClientError as ex:
            if self.log:
                self.log.exception("Failure using distrib service: "+str(ex))
            return self.send_error(502, "Internal Failure")

        supp_fmts = TextSupport()
        supp_fmts.support(Format("json", "application/json"), ["text/json", "application/json"], True)
        nct = self._nativects.get(os.path.splitext(head.get('name',''))[1].lstrip('.'))
        supp_fmts.support(Format("native", nct), [nct])
        
        if not isinstance(format, Format):
            reqformats = self._determine_request_formats(format)
            try:
                format = supp_fmts.select_format(reqformats, self.ordered_accepts())
            except Unacceptable as ex:
                return self.send_error(406, "Not Acceptable", str(ex))
            except UnsupportedFormat as ex:
                return self.send_error(400, "Unsupported Format", str(ex))
        if not format:
            format = supp_fmts.default_format()
                
        if format.name == "json" or format.name == "text":
            return self.send_ok(content=json.dumps(head, indent=2),
                                contenttype=format.ctype, ashead=ashead)

        if format.name == "native":
            bagep = head.get('downloadURL', self._dsep + "/_aip/" + head['name'])
            self.add_header("Location", bagep)
            self.set_response(307, "Found")
            self.end_headers()
            return []

        # log unwired format!
        raise StateException("No dataset handler implemented for format="+format.name)

    def resolve_aip_version(self, aipid, version=None, path='', ashead=False, format=None):
        """
        resolve to information regarding distributions for a particular version of an AIP
        """
        if path is None:
            path = ''

        if not version and not path:
            # just list the versions available (e.g. ['1.0.0', '1.0.1', '1.2.0'])
            self.add_header("Location", self._dsep +'/'+ aipid +'/_aip/_v')
            self.set_response(307, "Found")
            self.end_headers()
            return []

        parts = path.strip('/').split('/')
        if parts[0] == HEAD_DELIM:
            # asked for [aipid]/pdr:v/[x.x.x]/pdr:h
            if len(parts) > 1:
                return self.send_error(403, "Not supported")
            return self.resolve_aip_head(aipid, version, ashead=ashead, format=format)
            
        if parts[0] == DIST_DELIM:
            # asked for [aipid]/pdr:v/[x.x.x]/pdr:d
            if len(parts) > 1:
                return self.send_error(403, "Not supported")
            return self.resolve_dists_for_version(aipid, version, ashead=ashead, format=format)

        # return info about the given version
        return self.resolve_aip_id(aipid, version, ashead=ashead, format=format)

    def resolve_dists_for_version(self, aipid, version, ashead=False, format=format):
        """
        return a list of the distributions that are part of a given version of an AIP
        """
        distcli = distrib.BagDistribClient(aipid, self._svccli)
        try:

            head = distcli.describe_head_for_version(version)
            dists = distcli.describe_all()

        except distrib.DistribResourceNotFound as ex:
            return self.send_error(404, "AIP Not Found")
        except distrib.DistribServerError as ex:
            if self.log:
                self.log.exception("Trouble accessing distrib service: "+str(ex))
            return self.send_error(502, "Failure from upstream service")
        except distrib.DistribClientError as ex:
            if self.log:
                self.log.exception("Failure using distrib service: "+str(ex))
            return self.send_error(502, "Internal Failure")
        
        supp_fmts = TextSupport()
        supp_fmts.support(Format("json", "application/json"), ["text/json", "application/json"], True)

        if not isinstance(format, Format):
            reqformats = self._determine_request_formats(format)
            try:
                format = supp_fmts.select_format(reqformats, self.ordered_accepts())
            except Unacceptable as ex:
                return self.send_error(406, "Not Acceptable", str(ex))
            except UnsupportedFormat as ex:
                return self.send_error(400, "Unsupported Format", str(ex))

        if format is None:
            # set the default return format
            format = supp_fmts.default_format()

        tmpdir = self.cfg.get('tmp_dir', tempfile.gettempdir())
        if not os.path.isdir(tmpdir):
            if self.log:
                self.log.warning("Configured 'tmp_dir' is not an existing directory; using %s",
                                 tempfile.gettempdir())
            tmpdir = tempfile.gettempdir()

        members = []
        with tempfile.TemporaryDirectory(prefix="resolveaip", dir=tmpdir) as td:
            distcli.save_bag(head['name'], td)

            bag = multibag.open_headbag(os.path.join(td, head['name']))
            members = bag.member_bag_names

        out = [d for d in dists if os.path.splitext(d['name'])[0] in members]

        if format.name == "json" or format.name == "text":
            return self.send_ok(content=json.dumps(out, indent=2),
                                contenttype=format.ctype, ashead=ashead)

        # log unwired format!
        raise StateException("No dataset handler implemented for format="+format.name)


        
