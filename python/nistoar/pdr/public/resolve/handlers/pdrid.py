"""
Handlers for resolving PDR resource identifiers
"""
import sys, re, json

from .base import (Handler, Format, FormatSupport, XHTMLSupport, TextSupport,
                   Unacceptable, UnsupportedFormat, Ready)
from nistoar.pdr import constants as const
from nistoar.pdr.exceptions import ConfigurationException, IDNotFound, StateException
from nistoar.pdr.describe import MetadataClient

ark_naan = const.ARK_NAAN
VER_DELIM = const.RELHIST_EXTENSION.lstrip('/')
FILE_DELIM = const.FILECMP_EXTENSION.lstrip('/')
LINK_DELIM = const.LINKCMP_EXTENSION.lstrip('/')
AGG_DELIM = const.AGGCMP_EXTENSION.lstrip('/')

class PDRIDHandler(Handler):
    """
    A Handler for resolving a PDR resource identifier.  This is intended to handle endpoints 
    under "/id/"; however, the path passed is expected to be relative to this base and represent
    a PDR identifier (or a short-hand version of one) to be resolved. 
    """
    ark_id_re = re.compile(const.ARK_ID_PAT)
    old_ver_ext_re = re.compile(r'\.(\d+(_\d+(_\d+)?)?)$')
    ver_path_delim_re = re.compile(VER_DELIM + r'(/+|$)')
    filepath_delim_re = re.compile(FILE_DELIM+ r'(/+|$)')

    def __init__(self, path, wsgienv, start_resp, config={}, log=None):
        super(PDRIDHandler, self).__init__(path, wsgienv, start_resp, config)
        self._naan = str(self.cfg.get('naan', ark_naan))
        self.log = log
        self._mdcachedir = self.cfg.get("metadata_cache_dir")

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
        """
        path = path.lstrip('/')
        if not path:
            return Ready('', self._env, self._start).handle()

        idm = self.ark_id_re.match(path)     # match allowed ARK identifiers (ark:/NNNNN/dsid/...)
        if idm:
            if not idm.group(const.ARK_ID_DS_GRP):
                return self.send_error(403, "Missing dataset ID")
            if not self.cfg.get('ignore_naan', False) and idm.group(const.ARK_ID_NAAN_GRP) != self._naan:
                return self.send_error(404, "Unrecognized ID NAAN")
            dsid = path[:idm.end(const.ARK_ID_DS_GRP)]               # base ARK ID 
            path = path[idm.end(const.ARK_ID_DS_GRP):].lstrip('/')   # the rest of the path

        else:
            # ark: prefix not included; determine which kind of "short-hand"
            parts = path.split('/', 1)
            dsid = parts[0]
            path = (len(parts) > 1 and parts[1]) or ""

            if len(dsid) < 32:
                # longer than 32, assume this is an old-style EDI record;
                # other-wise, it's a reduced PDR ID
                dsid = "ark:/%s/%s" % (self._naan, dsid)

        # backward compatibility: support version access via ".vN_N_N" extension
        m = self.old_ver_ext_re.search(dsid)
        if m:
            ver = m.group(1).replace('_', '.')
            path = "%s/%s/%s" % (VER_DELIM, ver, path)
            dsid = self.old_ver_ext_re.sub('', dsid)

        # Does path point to a version or release history?
        version = None
        m = self.ver_path_delim_re.search(path)
        if m:
            # Yes.
            if path == m.group():
                # path = "pdr:v/": resolve to release history
                return self.resolve_release_history(dsid, ashead, format)
            parts = path[m.end():].split('/', 1)
            version = parts[0]
            path = (len(parts) > 1 and parts[1]) or ""

        if not path:
            # resolving a dataset-level identifier (possibly specific version)
            return self.resolve_dataset(dsid, version, ashead, format)

        # backward compatibility: support old components delimiter
        if path.startswith("cmps/"):
            path = FILE_DELIM + path[4:]
        
        # Does the path point to a component identifier?
        return self.resolve_component(dsid, path, version, ashead, format)

    def resolve_dataset(self, dsid, version=None, ashead=False, format=None):
        """
        send a representation of an identified dataset
        :param str dsid:  either a PDR or EDI identifier for the dataset; if it starts with "ark:", 
                          it will be taken as its PDR identifier.  
        :param str version:  the version of the dataset that is desired.  If None, the latest is sent.
        :param format:  the format for the view of the dataset that is desired.  If None, the 
                        format will be determined from the client's request.
                        :type format: str or Format
        """
        supp_fmts = XHTMLSupport()
        TextSupport.add_support(supp_fmts)
        supp_fmts.support(Format("nerdm", "application/json"), ["text/json", "application/json"], True)
        # supp_fmts.support(Format("datacite", "application/json"))

        reqformats = []
        if isinstance(format, str):
            reqformats = [ format ]
        elif isinstance(format, list):
            # allow format param to be a list of desired formats
            reqformats = [(isinstance(f, Format) and f.name) or f for f in format
                          if isinstance(f, str) or isinstance(f, Format)]
        else:
            reqformats = self.ordered_formats()

        if not isinstance(format, Format):
            try:
                format = supp_fmts.select_format(reqformats, self.ordered_accepts())
            except Unacceptable as ex:
                return self.send_error(406, "Not Acceptable", str(ex))
            except UnsupportedFormat as ex:
                return self.send_error(400, "Unsupported Format", str(ex))

        if format is None:
            # set the default return format
            format = supp_fmts.default_format()

        if format.name == supp_fmts.FMT_HTML:
            baseurl = self.cfg.get("locations", {}).get("landingPageService")
            if not baseurl:
                raise ConfigurationException("Missing required configuration: locations.landingPageService")
            if not baseurl.endswith('/'):
                baseurl += '/'

            redirect = baseurl+dsid
            redirect = redirect.rstrip('/')
            if version:
                if not redirect.endswith(VER_DELIM):
                    redirect += '/'+VER_DELIM
                redirect += '/'+version

            self.add_header("Location", redirect)
            self.set_response(307, "Temporary Redirect")
            self.end_headers()
            return []

        elif format.name == "nerdm" or format.name == "text":
            baseurl = self.cfg.get("APIs", {}).get("mdSearch")
            if not baseurl:
                raise ConfigurationException("Missing required configuration: APIs.mdSearch")
            try:
                nerdm = MetadataClient(baseurl, self._mdcachedir).describe(dsid, version)
            except IDNotFound as ex:
                return self.send_error(404, "Dataset ID Not Found")
            except Exception as ex:
                msg = "Trouble accessing metadata service: "+str(ex)
                if self.log:
                    self.log.exception(msg)
                else:
                    print(msg, file=sys.stderr)
                return self.send_error(503, "Metadata Service Temporarily Unavailable")

            if format.name == "nerdm":
                return self.send_ok(content=json.dumps(nerdm, indent=2),
                                    contenttype=format.ctype, ashead=ashead)

            # plain text wanted
            return self._send_text_dataset(nerdm)
                
        # log unwired format!
        raise StateException("No dataset handler implemented for format="+format.name)

    def _send_text_dataset(self, nerdm, ashead=False):
        from nistoar.pdr.publish import readme
        from io import StringIO

        out = StringIO()
        try: 
            gen = readme.ReadmeGenerator()
            gen.generate(nerdm, out, False, False)
        except Exception as ex:
            msg = "Trouble generating plain text: "+str(ex)
            if self.log:
                self.log.exception(msg)
            else:
                print(msg, file=sys.stderr)
            return self.send_error(500, "Internal Error",
                                   content="Trouble generating plain text description",
                                   contenttype="text/plain")

        return self.send_ok(content=out.getvalue(), contenttype="text/plain", ashead=ashead)
            
    def resolve_release_history(self, dsid, ashead=False, format=None):
        """
        send a view summarizing the release history for a dataset
        """
        supp_fmts = FormatSupport()
        supp_fmts.support(Format("nerdm", "application/json"), ["text/json", "application/json"])
        # TextSupport.add_support(supp_fmts)
        # XHTMLSupport.add_support(supp_fmts)

        reqformats = []
        if isinstance(format, str):
            reqformats = [ format ]
        elif isinstance(format, list):
            # allow format param to be a list of desired formats
            reqformats = [(isinstance(f, Format) and f.name) or f for f in format
                          if isinstance(f, str) or isinstance(f, Format)]
        else:
            reqformats = self.ordered_formats()

        if not isinstance(format, Format):
            try:
                format = supp_fmts.select_format(reqformats, self.ordered_accepts())
            except Unacceptable as ex:
                return self.send_error(406, "Not Acceptable", str(ex))
            except UnsupportedFormat as ex:
                return self.send_error(400, "Unsupported Format", str(ex))
                
        if format is None:
            # set the default return format
            format = supp_fmts.default_format()

        if format.name == "nerdm":   # FUTURE: or format.name == "text":
            baseurl = self.cfg.get("APIs", {}).get("mdSearch")
            if not baseurl:
                raise ConfigurationException("Missing required configuration: APIs.mdSearch")
            try:
                nerdm = MetadataClient(baseurl, self._mdcachedir).describe(dsid + const.RELHIST_EXTENSION)
            except IDNotFound as ex:
                return self.send_error(404, "Dataset ID Not Found")
            except Exception as ex:
                msg = "Trouble accessing metadata service: "+str(ex)
                if self.log:
                    self.log.exception(msg)
                else:
                    print(msg, file=sys.stderr)
                return self.send_error(503, "Metadata Service Temporarily Unavailable")

            # if format.name == "nerdm":
            return self.send_ok(content=json.dumps(nerdm, indent=2),
                                contenttype=format.ctype, ashead=ashead)

            # FUTURE: plain text wanted
            # return self._send_text_dataset(nerdm)
                
        # log unwired format!
        raise StateException("No dataset handler implemented for format="+format.name)

    def resolve_component(self, dsid, path, version=None, ashead=False, format=None):
        """
        send a view of a dataset component (which could be a file)
        """
        cmpid = '/'.join([dsid, path])
        
        baseurl = self.cfg.get("APIs", {}).get("mdSearch")
        if not baseurl:
            raise ConfigurationException("Missing required configuration: APIs.mdSearch")
        try:
            cmpmd = MetadataClient(baseurl, self._mdcachedir).describe(cmpid, version)
        except IDNotFound as ex:
            return self.send_error(404, "Component ID Not Found")
        except Exception as ex:
            msg = "Trouble accessing metadata service: "+str(ex)
            if self.log:
                self.log.exception(msg)
            else:
                print(msg, file=sys.stderr)
            return self.send_error(503, "Metadata Service Temporarily Unavailable")

        reqformats = []
        if isinstance(format, str):
            reqformats = [ format ]
        elif isinstance(format, list):
            # allow format param to be a list of desired formats
            reqformats = [(isinstance(f, Format) and f.name) or f for f in format
                          if isinstance(f, str) or isinstance(f, Format)]
        else:
            reqformats = self.ordered_formats()

        if path.startswith(AGG_DELIM):
            # this is an included resource; does the client want nerdm format?
            if reqformats and (reqformat[0] == "nerdm" or reqformat[0] == "application/json"):
                # looks like it
                return self.send_ok(content=json.dumps(cmpmd, indent=2),
                                    contenttype="application/json", ashead=ashead)
            
            # otherwise, redirect to it if possible
            if cmpmd.get('@id'):
                idm = self.ark_id_re.match(cmpmd.get('@id'))
                if idm:
                    baseurl = self.cfg.get("locations", {}).get("resolverService")
                    if not baseurl:
                        raise ConfigurationException("Missing required config: locations.resolverService")
                    if not baseurl.endswith('/'):
                        baseurl += '/'
                    self.add_header("Location", baseurl + cmpmd.get('@id'))
                    self.set_response(302, "Found Included Resource")
                    self.end_headers()
                    return []

            if cmpmd.get('location'):
                self.add_header("Location", cmpmd.get('location'))
                self.set_response(302, "Found Included Resource")
                self.end_headers()
                return[]

        nrdfmt = Format("nerdm", "application/json")
        supp_fmts = TextSupport()
        supp_fmts.support(nrdfmt, ["text/json", "application/json"], True)

        native = Format("native", "")
        redirurl = cmpmd.get('downloadURL') or cmpmd.get('accessURL')
        if redirurl or isinstance(cmpmd.get('mediaType'), str):
            native = Format("native", cmpmd['mediaType'])
            supp_fmts.support(native, [cmpmd['mediaType']], True)

        reqformats = []
        if isinstance(format, str):
            reqformats = [ format ]
        elif isinstance(format, list):
            # allow format param to be a list of desired formats
            reqformats = [(isinstance(f, Format) and f.name) or f for f in format
                          if isinstance(f, str) or isinstance(f, Format)]
        else:
            reqformats = self.ordered_formats()

        if not isinstance(format, Format):
            try:
                format = supp_fmts.select_format(reqformats, self.ordered_accepts())
            except Unacceptable as ex:
                return self.send_error(406, "Not Acceptable", str(ex))
            except UnsupportedFormat as ex:
                return self.send_error(400, "Unsupported Format", str(ex))

        if not format:
            format = (redirurl and native) or nrdfmt
                
        if format.name == nrdfmt.name or format.name == "text":
            # send the JSON metadata
            return self.send_ok(content=json.dumps(cmpmd, indent=2),
                                contenttype=format.ctype, ashead=ashead)

        if format.name == native.name and redirurl:
            # redirect to the URL pointed to by this component
            self.add_header("Location", redirurl)
            self.set_response(302, "Found component")
            self.end_headers()
            return[]

        # log unwired format!
        raise StateException("No dataset handler implemented for format="+format.name)
        
    
