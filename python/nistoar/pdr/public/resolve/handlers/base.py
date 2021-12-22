"""
The base Handler class for handler implementations used by the Resolver WSGI app
"""
import sys, re
from functools import reduce
from collections import namedtuple
from wsgiref.headers import Headers
from urllib.parse import parse_qs

def is_content_type(label):
    """
    return True if the given format label should be interpreted as a content type (i.e. using 
    MIME-type syntax).  This implementation returns if it contains a '/' character.
    """
    return '/' in label

def match_accept(ctype, acceptable):
    """
    return the most specific content type of the two inputs if the two match each other, taking in 
    account wildcards, or None if the two do not match.  The returned content type will end in "/*" 
    if both input values end in "/*".
    """
    if ctype == acceptable or (acceptable.endswith('/*') and ctype.startswith(acceptable[:-1])):
        return ctype
    if ctype.endswith('/*') and acceptable.startswith(ctype[:-1]):
        return acceptable
    return None

def acceptable(ctype, acceptable):
    """
    return the first match of a given content type value to a list of acceptable content types
    """
    if len(acceptable) == 0:
        return ctype
    if ctype in ['*', '*/*']:
        return acceptable[0]
    for ct in acceptable:
        m = match_accept(ctype, ct)
        if m:
            return m

    return None

def order_accepts(accepts):
    """
    order the given accept values according to their q-value.  
    :param accepts:  the list of accept values with their q-values attached.  This can be given either 
                     as a str or a list of str, each representing the value of the HTTP Accept request 
                     header value.
                     :type accepts: str or list of str
    :return:  a list of the mime types in order of q-value.  (The q-values will be dropped.)
    """
    if isinstance(accepts, str):
        accepts = [a.strip() for a in accepts.split(',') if a]
    else:
        acc = []
        for a in accepts:
            acc.extend([b.strip() for b in a.split(',') if b])
        accepts = acc

    for i in range(len(accepts)):
        q = 1.0
        m = re.search(r';q=(\d+(\.\d+)?)', accepts[i])
        if m:
            q = float(m.group(1))
        accepts[i] = (re.sub(r';.*$', '', accepts[i]), q)

    accepts.sort(key=lambda a: a[1], reverse=True)
    return [a[0] for a in accepts if a[1] > 0]

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
    Support HTML as an output format assuming the HTML will be legal XHTML
    """
    FMT_TEXT = "text"

    def __init__(self):
        super(TextSupport, self).__init__()
        TextSupport.add_support(self)

    @classmethod
    def add_support(cls, fmtsup: FormatSupport, asdefault: bool=False):
        """
        Add support for HTML content types to the given FormatSupport instance
        """
        fmtsup.support(Format(TextSupport.FMT_TEXT, "text/plain"), ["text/plain"], asdefault, True)
  
class Handler(object):
    """
    a default web request handler that also serves as a base class for the 
    handlers specialized for the supported resource paths.
    """

    def __init__(self, path, wsgienv, start_resp, config={}):
        self._path = path
        self._env = wsgienv
        self._start = start_resp
        self._hdr = Headers([])
        self._code = 0
        self._msg = "unknown status"
        self.cfg = config
        self._handle_format_as = {}

        self._meth = self._env.get('REQUEST_METHOD', 'GET')

    def send_error(self, code, message, content=None, contenttype=None, ashead=None, encoding='utf-8'):
        """
        respond to the client with an error of a given code and reason

        This method is meant to be called by a method handler (or an override of :py:meth:`handle`) 
        and is provided as a simple way to send an error response (instead of calling 
        :py:meth:`set_response` and :py:meth:`end_headers` directly).  

        :param int code:        the HTTP response code to assign
        :param str message:     the briefly-stated reason to give for the error; this text
                                is sent as the message that accompanies the code in the HTTP 
                                response header
        :param content:         Content to return as the body.  
                                :type content: str or byte or a list of either
        :param str contenttype: the MIME type to associate with the returned content.
        :param bool ashead:     True if this is being sent as if in response to a HEAD request; if so,
                                the size and type of the content will be included in the headers, but 
                                the actual content will be withheld.  If not provided, it will be set 
                                to True if the originally requested method is "HEAD"; otherwise it is 
                                False
        :param str encoding:    The encoding required to turn the content--when given as str--into bytes.
                                The default is 'utf-8'.  
        """
        return self._send(code, message, content, contenttype, ashead, encoding)

    def send_ok(self, content=None, contenttype=None, message="OK", code=200, ashead=None, encoding='utf-8'):
        """
        respond to the client a response of success.  

        This method is meant to be called by a method handler (or an override of :py:meth:`handle`) 
        and is provided as a short-cut for small, simple successful responses instead of calling 
        :py:meth:`set_response` and :py:meth:`end_headers` directly.  

        :param str message:     the briefly-stated reason to give for the error; this text
                                is sent as the message that accompanies the code in the HTTP 
                                response header.  The default if not specified is "OK". 
        :param content:         Content to return as the body.  If not provided, the body will be
                                empty.
                                :type content: str or byte
        :param int code:        the HTTP response code to assign.  This should be between greater
                                than or equal to 200 and less than 300; the default is 200.
        :param str contenttype: the MIME type to associate with the returned content.
        :param bool ashead:     True if this is being sent as if in response to a HEAD request; if so,
                                the size and type of the content will be included in the headers, but 
                                the actual content will be withheld.  If not provided, it will be set 
                                to True if the originally requested method is "HEAD"; otherwise it is 
                                False
        :param str encoding:    The encoding required to turn the content--when given as str--into bytes.
                                The default is 'utf-8'.  
        """
        return self._send(code, message, content, contenttype, ashead, encoding)

    def _send(self, code, message, content, contenttype, ashead, encoding):
        if ashead is None:
            ashead = self._meth.upper() == "HEAD"
        status = "{0} {1}".format(str(code), message)

        if content:
            if not isinstance(content, list):
                content = [ content ]
            badtype = [type(c) for c in content if not isinstance(c, (str, bytes))]
            if badtype:
                raise TypeError("send_*: non-str/bytes found in content")
            if not contenttype:
                contenttype = (isinstance(content[0], str) and "text/plain") or "application/octet-stream"
        elif content is None:
            content = []
        # convert to bytes
        content = [(isinstance(c, str) and c.encode(encoding)) or c for c in content]

        hdrs = []
        if contenttype:
            hdrs = Headers([])
            hdrs.add_header("Content-Type", contenttype)
            hdrs = hdrs.items()
        if len(content) > 0:
            hdrs.append(("Content-Length", str(reduce(lambda x, t: x+len(t), content, 0))))

        self._start(status, hdrs, None)
        return (not ashead and content) or []

    def add_header(self, name, value):
        """
        record a name-value pair to be sent as part of the response header.

        :param str name:  the name of the header field to cache
        :param str value: the value to give to the header field
        :raises UnicodeEncodeError:  if name or value includes Unicode characters (see PEP 333)
        """
        # Caution: HTTP does not support Unicode characters (see
        # https://www.python.org/dev/peps/pep-0333/#unicode-issues);
        # thus, this will raise a UnicodeEncodeError if the input strings
        # include Unicode (char code > 255).
        #
        # make sure values are encodable
        e = "ISO-8859-1"
        (name.encode(e), value.encode(e))

        self._hdr.add_header(name, value)

    def set_response(self, code, message):
        """
        record the response code and message to be sent when the response is triggered to push out.
        """
        self._code = code
        self._msg = message

    def end_headers(self):
        """
        trigger the delivery of response's header to the web client.  

        This method is meant to be called by a method handler (or an override of :py:meth:`handle`).
        It should be preceded with a call to :py:meth:`set_response`; afterward, the handler should 
        return the body content (as an iterable).  
        """
        status = "{0} {1}".format(str(self._code), self._msg)
        self._start(status, self._hdr.items())

    def handle(self):
        """
        handle the request encapsulated in this Handler (at construction time).  

        The default implementation looks for a Handler method of the form, `do_`METH(), where METH is 
        is the HTTP method requested (e.g. GET, HEAD, etc.) and calls it with the requested URL path
        (as set at construction).  If the requested method is HEAD and there is no HEAD, `do_GET()` 
        is called with a second argument set to True which should prevent the content from the 
        path to be excluded.  
        """
        meth_handler = 'do_'+self._meth

        if hasattr(self, meth_handler):
            return getattr(self, meth_handler)(self._path)
        elif self._meth == "HEAD":
            return self.do_GET(self._path, ashead=True)
        else:
            return self.send_error(405, self._meth + " not supported on this resource")

    def ordered_formats(self):
        """
        return a list of output formats requested by the client via the `format` query parameter, 
        ordered by their occurrence in the query string portion of the request URL.  This order 
        will usually be taken as the order of preference by the client.  
        """
        format = []
        if 'QUERY_STRING' in self._env:
            params = parse_qs(self._env['QUERY_STRING'])
            if 'format' in params:
                format = params['format']
        return format

    def ordered_accepts(self):
        """
        return a list of acceptable content types requested by client via the Accept request header,
        ordered by preference according to their q-values
        :return:  a list of the MIME types in order of q-value.  (The q-values will be dropped.)
        """
        return order_accepts(self._env.get('HTTP_ACCEPT', []))


class Ready(Handler):
    """
    a default handler for handling unsupported paths or proof-of-life responses
    """

    def __init__(self, path, wsgienv, start_resp, config={}, log=None):
        """
        instantiate the handler
        """
        super(Ready, self).__init__(path, wsgienv, start_resp, config)
        self._fmtsup = FormatSupport()
        XHTMLSupport.add_support(self._fmtsup)
        TextSupport.add_support(self._fmtsup)

    def do_GET(self, path, ashead=False, format=None):
        path = path.lstrip('/')
        if path:
            return self.send_error(404, "Not found")

        if isinstance(format, str):
            format = self._fmtsup.match(format)
            if not format:
                return self.send_error(400, "Unsupported Format")

        if not format:
            try:
                format = self._fmtsup.select_format(self.ordered_formats(), self.ordered_accepts())
            except Unacceptable as ex:
                return self.send_error(406, "Not Acceptable", str(ex))
            except UnsupportedFormat as ex:
                return self.send_error(400, "Unsupported Format", str(ex))

        if format is None:
            # set the default return format
            format = self._fmtsup.default_format()

        if format.name == XHTMLSupport.FMT_HTML:
            return self.get_ready_html(format.ctype, ashead)

        if format.name == TextSupport.FMT_TEXT:
            msg = "Resolver service is ready"
            return self.send_ok(msg, format.ctype, "Ready", ashead=ashead)

        self.send_error(400, "Unsupported format requested")

    def get_ready_html(self, contenttype, ashead=None):
        out = """
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
  <head>
    <title>Resolver Service: Ready</title>
  </head>
  <body>
    <h1>Resolver Service Is Ready</h1>
    <p>
       Available Resolver Endpoints Include:
       <ul>
         <li> <a href="id/">/id/</a> -- for resolving PDR (ARK-based) identifiers </li>
         <li> <a href="aip/">/aip/</a> -- for resolving PDR AIP identifiers </li>
       </ul>
    </p>
  </body>
</html>
"""      
        return self.send_ok(out, contenttype, "Ready", ashead=ashead)
        
