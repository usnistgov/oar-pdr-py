"""
The base Handler class for handler implementations used by the Resolver WSGI app
"""
import sys, re
from functools import reduce
from collections import namedtuple
from wsgiref.headers import Headers
from urllib.parse import parse_qs
from typing import List

from .utils import is_content_type, match_accept, acceptable, order_accepts

class UnsupportedFormat(Exception):
    """
    An exception indicating that none of the client-requested formats are supported by the handler
    for the requested resource.  This exception is expected to result in a 400 (Bad Request) response
    to the client.
    """
    pass

class Unacceptable(Exception):
    """
    An expection indicating that the requested (or otherwise selected) format corresponds to a 
    content-type that is not acceptable to the client.  This exception is expected to result in a 
    406 (Not Acceptable) response to the client.  
    """
    pass
            
Format = namedtuple("Format", ["name", "ctype"])

class FormatSupport(object):
    """
    a class that encapsulates the formats supported by a Handler and which can be 
    used to select the most appropriate format among those acceptable to the client.
    """

    def __init__(self):
        """
        create an instance with no formats registered as supported
        """
        self._lu = {}
        self._ctps = {}
        self._deffmt = None

    def support(self, format: Format, cts=[], asdefault=False, raiseonconflict=False):
        """
        add support for a named format.   
        :param Format format:  the format to support (providing its name and default content type label)
        :param cts:  a list of the content types that, when requested, should result in the given format
                     to be returned.  
                     :type cts: a list of str
        :param bool asdefault:  if True, set this to be the default Format (i.e. the format returned by
                     :py:meth:`default_format`); this is meant to be the Format returned to the client 
                     when the client has not specified a desired format.
        :param bool raiseonconflict: as a content type provided in cts can be associated with only one 
                     format, this value controls what happens if a content type is already associated 
                     with a format.  If False (default), the association will be overridden by this new 
                     call.  If True, then a ValueError will be raised before the format is registered.  
        """
        if raiseonconflict:
            if format.name in self._lu:
                raise ValueError("Format already registered as supported: " + format.name)
            registered = [c for c in cts if c in self._lu]
            if registered:
                raise ValueError("Content types already supported by a registered format: "+
                                 str(registered))

        if format.name in self._lu:
            # clean out the previous registration
            self._lu = dict([item for item in self._lu.items() if item[1] != format.name])

        for ct in cts:
            self._lu[ct] = format
        self._lu[format.name] = format
        self._ctps[format.name] = set(cts)
        self._ctps[format.name].add(format.ctype)

        if asdefault or not self._deffmt:
            self._deffmt = format

    _wildc_ct_re = re.compile(r'^(\w+)/\*$')

    def match(self, fmtreq: str) -> Format:
        """
        return the Format object that best matches the given content type or format name
        :param str fmtreq:  the requested format to match.  This should either be a MIME-type label
                            or a format's logical name (e.g. "html", "text").  
        :return:  the supported Format associated with the given format identifier, or None if the 
                  name or MIME-type is not registered as supported.  
        """
        if fmtreq == '*/*' or fmtreq == '*':
            return self.default_format()

        m = self._wildc_ct_re.match(fmtreq)
        if m:
            # requested format looks like "XXX/*"
            mimestart = m.group(1) + '/'
            if self.default_format().ctype.startswith(mimestart):
                # the requested format matches our default content type; pick it
                return self.default_format()

            # crap shoot a match
            mts = [c for c in self._lu.keys() if c.startswith(mimestart)]
            if mts:
                return self._lu.get(mts[0])
        else:
            fmt = self._lu.get(fmtreq)
            if fmt and is_content_type(fmtreq):
                fmt = Format(fmt.name, fmtreq)
            return fmt

        return None
        

    def default_format(self) -> Format:
        """
        the format that should be considered the default one to return to the client when the client 
        has not indicated a specific format in its request.
        """
        return self._deffmt

    def select_format(self, formats, accepts):
        """
        given format choices ordered by precedence by the client, pick a supported format to return.
        Note that if both `formats` and `accepts` are empty or None, None is returned; the caller can 
        then choose to call :py:meth:`default_format` to as the selected format to return to the client.
        
        :param formats:  the formats requested, in order of precedence, by the client via URL query 
                         parameters.  While these values take precedence over the content types provided
                         via `accepts`, the format selected must correspond to a content-type that is 
                         included in accepts if the latter has values.  This should be an empty list or 
                         None if a format was not requested.  
                         :type formats: list of requested format names or content-types/MIME-types.
        :param accepts:  the content-types, in order of precedence, that were indicated by the client 
                         as acceptable to be returned.  If provided, the returned format will correspond 
                         to most preferred content type that is supported.  
        :return:  the Format that best matches the client's request, or None if no formats or accepts are 
                         provided.
                         :rtype: Format
        :raise UnsupportedFormat:  if all values given in `formats` indicate unsupported formats
        :raise Unacceptable:       if none of the supported values in `formats` correspond to a content 
                                   type that is not included in the (non-empty) `accepts` list.  If a 
                                   format was not requested (i.e. `formats` is None or empty), then this
                                   exception is raised if none of the content types in `accepts` are 
                                   supported.  
        """
        
        if formats:
            unacceptable = []
            for label in formats:
                fmt = self.match(label)
                if not fmt:
                    # no match
                    continue

                if not accepts or '*' in accepts or '*/*' in accepts:
                    # anything is acceptable
                    return fmt

                if is_content_type(label):
                    # requested format is in form of T/S (a MIME-type)
                    mct = acceptable(label, accepts)
                    if mct:
                        if mct.endswith('/*') and match_accept(mct, fmt.ctype):
                            return fmt
                        return Format(fmt.name, mct)
                else:
                    # client asked for a format via its logical name; match the acceptable content types
                    # with all those associated with the format
                    for ct in accepts:
                        mct = acceptable(ct, self._ctps.get(fmt.name, []))
                        if mct and not mct.endswith('/*'):
                            return Format(fmt.name, mct)

                unacceptable.append(label)

            if unacceptable:
                raise Unacceptable("format parameter is inconsistent with Accept header")
            raise UnsupportedFormat("Unsupported format requested")

        if accepts:
            for label in accepts:
                fmt = self.match(label)
                if fmt:
                    if is_content_type(label) and not label.endswith('/*'):
                       fmt = Format(fmt.name, label)
                    return fmt

            raise Unacceptable("No given Accept types supported")

        return None

class XHTMLSupport(FormatSupport):
    """
    Support HTML as an output format assuming the HTML will be legal XHTML.  
    """
    FMT_HTML = "html"

    def __init__(self):
        super(XHTMLSupport, self).__init__()
        XHTMLSupport.add_support(self)

    @classmethod
    def add_support(cls, fmtsup: FormatSupport, asdefault=False):
        """
        Add support for HTML content types to the given FormatSupport instance
        :param bool asdefault:  if True, set this to be the default Format (i.e. the format returned by
                     :py:meth:`default_format`); this is meant to be the Format returned to the client 
                     when the client has not specified a desired format.
        """
        fmtsup.support(Format(XHTMLSupport.FMT_HTML, "text/html"), [
            "application/html",
            "text/html",
            "application/xhtml",
            "application/xhtml+xml"
        ], asdefault, True)

class TextSupport(FormatSupport):
    """
    Support a plain text output format
    """
    FMT_TEXT = "text"

    def __init__(self):
        super(TextSupport, self).__init__()
        TextSupport.add_support(self)

    @classmethod
    def add_support(cls, fmtsup: FormatSupport, asdefault: bool=False):
        """
        Add support for plain text content types to the given FormatSupport instance
        """
        fmtsup.support(Format(TextSupport.FMT_TEXT, "text/plain"), ["text/plain"], asdefault, True)
  

class JSONSupport(FormatSupport):
    """
    Support a plain text output format
    """
    FMT_JSON = "json"
    DEF_CONTENT_TYPE = "application/json"

    def __init__(self, ctypes=[]):
        super(JSONSupport, self).__init__()
        JSONSupport.add_support(self, ctypes)

    @classmethod
    def add_support(cls, fmtsup: FormatSupport, ctypes: List[str]=[], asdefault: bool=False):
        """
        Add support for JSON content types to the given FormatSupport instance
        """
        if not ctypes:
            ctypes = [ JSONSupport.DEF_CONTENT_TYPE ]
        fmtsup.support(Format(JSONSupport.FMT_JSON, JSONSupport.DEF_CONTENT_TYPE),
                       ctypes, asdefault, True)
  
