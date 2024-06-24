"""
helper functions for access the file manager's WebDAV interface
"""
import re
from collections import OrderedDict

from webdav3.client import Client, WebDavXmlUtils, RemoteResourceNotFound

info_request = """<?xml version="1.0"?>
<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:nc="http://nextcloud.org/ns">
  <d:prop>
    <d:resourcetype/><d:getlastmodified/><d:getlastmodified/>
    <oc:fileid/><oc:size/><oc:permissions/>
  </d:prop>
</d:propfind>
"""

_re_ns = re.compile(r'^\{[^\}]+\}')

def propfind_resp_to_dict(respel):
    """
    convert a propfind response etree element into a dictionary of properties
    """
    dav_props = {
        '{DAV:}creationdate':    "created",
        '{DAV:}getlastmodified': "modified",
        '{DAV:}resourcetype':    "type",
    }
    props = respel.xpath('.//d:prop', namespaces={"d": "DAV:"})
    if not props:
        raise ValueError("propfind_resp_to_dict(): Input Element does not look like a PROPFIND response: "+
                         "missing d:prop descendent element")
    props = respel.xpath('.//d:prop[contains(../d:status,"200 OK")]',
                         namespaces={"d": "DAV:"})
    if not props:
        raise ValueError("propfind_resp_to_dict(): Input Element contains no valid property values")
    props = props[0]

    out = OrderedDict()
    for child in props:
        if child.tag in dav_props:
            name = dav_props[child.tag]
        else:
            name = _re_ns.sub('', child.tag)

        if child.tag == "{DAV:}resourcetype":
            if len(child) > 0 and child[0].tag == "{DAV:}collection":
                value = "folder"
            else:
                value = "file"
        else:
            value = child.text
        out[name] = value

    return out

def parse_propfind(content, reqpath, davbase):
    """
    Extract the properties in a PROPFIND XML response into a dicitonary
    :param str content:  the XML response message to parse
    :param str reqpath:  the path that properties were requested 
    :param str davbase:  the base WebDAV endpoint URL
    """
    path = f"/{reqpath.strip('/')}/"
    respel = WebDavXmlUtils.extract_response_for_path(content, path, davbase)
    return propfind_resp_to_dict(respel)

