"""
This module provides a client class, :py:class:`FMWebDAVClient`, designed to perform WebDAV operations 
that interact with a Nextcloud instance to manage directories and files.  It leverages the 
``webdav3.client`` package to carry out standard WebDAV operations.  The module also includes some 
utility functions assisting with authentication and resource property processing.

Clients can authenticate to Nextcloud's WebDAV interface either with a long-term password or a temporary
password; in the MIDAS system, long-term passwords are not made available to external clients.  The 
Nextcloud's (NIST-enhanced) generic layer provides a special endpoint for retrieving a temporary password
with requires X.509 client certificate authentication.  The 
"""
import logging, os, re
from datetime import datetime
from urllib.parse import urlparse, urlunparse, urljoin, unquote, urlsplit
from collections import OrderedDict
from typing import Mapping, List

from webdav3 import client as wd3c
from webdav3.client import etree
import OpenSSL
import requests

from nistoar.base.config import ConfigurationException
from ..exceptions import *

def get_webdav_password(ep: str, certpath: str, keypath: str, capath: str=None,
                        log: logging.Logger=None):
    """
    obtain a temporary password for accessing the file manager's WebDAV interface.
    :param str       ep:  the URL for the password-generating endpoint.  This should be the 
                          full endpoint URL (i.e. ending in "/auth").
    :param str certpath:  the file path to the X.509 certificate (in PEM format) to use to 
                          authenticate to the password-generating endpoint.  The certificate
                          subject's common name (CN) must match an existing nextcloud username.
    :param str  keypath:  the file path to the X.509 private key (in PEM format) to use with 
                          the certificate.
    :param str   capath:  the file path to a CA certificate bundle for validating the endpoint's 
                          site certificate.  If not provided, the OS-installed bundle will be 
                          used.
    :raises FileManagerServiceException:  if an error occurs during retrieval; see the 
                          :py:mod:`~nistoar.midas.dap.fm.exceptions` module for the list of 
                          possible subclass exceptions that can be thrown.  In particular,
                          :py:class:`~nistoar.midas.dap.fm.exceptions.FileManagerClientError`
                          will be raised if the client certificate (given by ``certpath``) is 
                          not verifiable.
    :raises ValueError:   if the certificate or key files can not be read.
    """
    # check for readability of cert, key
    pass
    
    try:
        response = requests.post(ep, cert=(certpath, keypath), verify=capath)

        err_msg = None
        if response.status_code >= 400 and response.text:
            try:
                err_msg = response.json().get('error')
            except ValueError as ex:
                pass

        if response.status_code >= 500:
            if err_msg:
                err_msg = "File Manager Server Error: "+err_msg
            raise FileManagerServerError(response.status_code, url, response.text, err_msg)
        elif response.status_code == 405 or response.status_code == 401:
            if not err_msg:
                err_msg = "ensure client credentials are correct"
            raise FileManagerClientError("File manager WebDAV Authentication failed (%d); %s" %
                                         (response.status_code, err_msg), response.status)
        elif response.status_code >= 400:
            if not err_msg:
                err_msg = response.reason
            raise FileManagerClientError("File manager access failure, %s (programmer error?): %s"
                                          % (response.status_code, err_msg), response.status_code, ep)

        if '/json' not in response.headers['content-type']:
            err_msg = "WebDav authentication request failure on %:\n  non-JSON response type: %s"
            raise FileManagerClientError(err_msg % (ep, response.headers['content-type']))

        temp_password_data = response.json()
        pwd = temp_password_data.get('temporary_password')
        if not pwd:
            raise UnexpectedFileManagerResponse("File Manager Authentication Failure: empty password",
                                                auth_url, response.text)

        if log:
            log.info(f"Authenticated successfully; temporary password retrieved")
            log.debug(pwd)
        return pwd

    except requests.RequestException as ex:
        if log:
            log.error(f"Error during authentication: {str(ex)}")
        raise FileManagerCommError("File Manager communication failure during authentication: " +
                                   str(ex), ep) from ex

def extract_cert_cn(cert_path: str):
    """ Extract CN (Common Name) from the given X.509 certificate (in PEM format """
    with open(cert_path, 'rb') as cert_file:
        cert_data = cert_file.read()

    cert = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, cert_data)
    subject = cert.get_subject()
    return subject.CN
    

class FMWebDAVClient:
    """
    A client for accessing the file manager's WebDAV interface.  It is a wrapper around the 
    webdav3.client interface which is available via the ``wdcli`` property.  If a username and 
    password is included in the configuration, then the ``wdcli`` property is available at
    construction; otherwise, the configuration must include a user X.509 certificate (and key), 
    and the ``wdcli`` property is created after calling the :py:meth:`authenticate` method.  

    The class looks for the following configuration parameters:

    ``service_endpoint``
        _str_ (required).  the WebDAV endpoint URL pointing to the remote collection (directory) 
        that should be the root of subsequent accesses.
    ``ca_bundle``
        _str_ (optional).  the path to a CA certificate bundle which should be used to verify 
        the WebDAV's site certificate.
    ``public_prefix``
        _str_ (optional).  the root path of the service endpoint that is part of a proxy web 
        server's routing.  This may be needed (depending on the WebDAV server configuration) 
        to properly parse PROPFIND responses if they do will not include this path when listing 
        resource URLs being described.
    ``authentication``
        _dict_ (required).  the data required to authenticated to the WebDAV service (see below).

    The ``authentication`` parameter is a dictionary which can contain the following parameters:

    ``client_cert_path``
        _str_ (optional).  the path to an X.509 certificate (in PEM format) representing the client 
        identity to authenticate to the WebDAV service as.  This is required if ``user`` and ``pass`` 
        are not provided.
    ``client_cert_path``
        _str_ (optional).  the path to the PEM-encoded private key for the certificate given in 
        ``client_cert_path``.
    ``client_auth_url``
        _str_ (optional).  the URL to use to authenticate using an X.509 certificate.  If not provided,
        a default will be constructed based on the ``service_endpoint`` and assuming the NIST Nextcloud 
        generic layer API.
    ``user``
        _str_ (optional).  the user name of the identity to authenticate to the WebDAV service as.
        If ``pass`` is not provided, it must match the CN (common name) of the identity in the 
        certificate given via ``client_cert_path``.  
    ``pass``
        _str_ (optional).  the password to authenticate with.  If provided, ``client_cert_path`` is 
        ignored.

    :param dict config:  the configuration dictionary
    :param Logger  log:  the Logger to use for log messages

    :raises ConfigurationException  if there are missing or inconsistent comfiguration parameters 
                                    (including if the cert files can not be found on disk).
    """
    def __init__(self, config: Mapping, log: logging.Logger=None):
        """
        initialize the client

        :param dict config:  the configuration parameters for this client; see class documentation for 
                             the parameter descriptions.
        :param Logger log:   the Logger object to use for messages from this client.  If not provided,
                             a default logger with the name "webdavcli" will be used. 
        """
        if not log:
            log = logging.getLogger("webdavcli")
        self.log = log
        self.cfg = config

        if not config.get("service_endpoint"):
            raise ConfigurationException("FMWebDAVClient: Missing required config parameter: "+
                                         "service_endpoint")
        try:
            ep = urlparse(config["service_endpoint"])
        except ValueError as ex:
            raise ConfigurationException("FMWebDAVClient: config param service_endpoint not a URL")

        self._wdcopts = {
            'webdav_hostname': urlunparse((ep[0], ep[1], '', '', '', '')),
            'webdav_root': ep.path or '/',
            'webdav_password': None
        }

        self._add_auth_opts(config.get('authentication', {}), self._wdcopts) # may raise ConfigurationException

        self.wdcli = None
        if self._wdcopts['webdav_password']:
            self.wdcli = wd3c.Client(self._wdcopts)
            self.wdcli.verify = self.cfg.get('ca_bundle', True)

    def _add_auth_opts(self, authcfg, wd3opts):
        if not authcfg:
            self.log.warning("No authentication parameters provided; assuming none are needed")

        wd3opts['webdav_password'] = authcfg.get('pass')
        userid = authcfg.get('user')

        if not wd3opts['webdav_password'] or not userid:
            if not authcfg.get("client_cert_path"):
                raise ConfigurationException("FMWebDAVClient: missing required config parameter: "
                                             "authentication.client_cert_path")
            if not os.path.isfile(authcfg["client_cert_path"]):
                raise ConfigurationException(f"{authcfg['client_cert_path']}: client cert file not found")

            try:
                certuser = extract_cert_cn(authcfg['client_cert_path'])
            except Exception as ex:
                raise ConfigurationException("%s: trouble reading client cert: %s" %
                                             (authcfg['client_cert_path'], str(ex))) from ex
            if not userid:
                userid = certuser
            elif userid != certuser:
                # Certificate must have CN matching nextcloud admin username
                raise ConfigurationException("%s: CN does not match configured user, %s" %
                                             (authcfg['client_cert_path'], userid))

        if userid:
            wd3opts['webdav_login'] = userid

        if not wd3opts['webdav_password']:
            if not authcfg.get("client_key_path"):
                raise ConfigurationException("FMWebDAVClient: missing required config parameter: "
                                             "authentication.client_key_path")
            if not os.path.isfile(authcfg["client_key_path"]):
                raise ConfigurationException(f"{authcfg['client_key_path']}: client key file not found")

    def authenticate(self):
        """
        present X.509 credentials to the remote file manager service to get back temporary 
        credentials for using the WebDAV API.  This creates a new client session with the service.
        """
        authcfg = self.cfg.get('authentication', {})
        auth_url = authcfg.get('client_auth_url')
        if not auth_url:
            auth_url = self.cfg.get('service_endpoint').split("remote.php/dav/file")[0] + \
                       "api/genapi.php/auth"

        certpath = authcfg.get('client_cert_path')
        keypath = authcfg.get('client_key_path')
        capath = self.cfg.get('ca_bundle')
        if not certpath or not keypath:
            raise ConfigurationException("FMWebDAVClient.authenticate() requires config params: "+
                                         "client_cert_path, client_key_path")

        try:
            self._wdcopts['webdav_password'] = get_webdav_password(auth_url, certpath, keypath,
                                                                   capath, self.log)
        except OSError as ex:
            raise FileManagerClientError("Unable to get temp password: "+str(ex)) from ex

        self.wdcli = wd3c.Client(self._wdcopts)
        if self.cfg.get('ca_bundle'):
            self.wdcli.verify = self.cfg['ca_bundle']
            
    def is_directory(self, path):
        """Check if arg path leads to a directory, returns bool accordingly"""
        if not self.wdcli:
            self.authenticate()

        try:
            return self.wdcli.is_dir(path)
        except wd3c.RemoteResourceNotFound:
            return False

    def is_file(self, path):
        """Check if arg path leads to a file, returns bool accordingly"""
        return not self.is_directory(path)

    def exists(self, path):
        """
        return True if the given path exists on the server
        """
        if not self.wdcli:
            self.authenticate()

        try:
            return self.wdcli.check(path)
        except (wd3c.NoConnection, wd3c.ConnectionException) as ex:
            raise FileManagerCommError("Failed to create directory: "+str(ex)) from ex

        # Note wdcli.check() will return False if any error code > 400 is returned (not ideal)

    def ensure_directory(self, path):
        """
        Ensure that a directory with given a path exists, creating it if necessary
        """
        if not self.wdcli:
            self.authenticate()

        try:
            self.wdcli.mkdir(path)
        except (wd3c.NoConnection, wd3c.ConnectionException) as ex:
            raise FileManagerCommError("Failed to create directory: "+str(ex)) from ex
        except wd3c.NotEnoughSpace as ex:
            raise FileManagerServerError("Failed to create directory: "+str(ex)) from ex
        except wd3c.RemoteResourceNotFound as ex:
            raise FileManagerResourceNotFound("Unable to create directory: "+str(ex)) from ex
        except wd3c.ResponseErrorCode as ex:
            if ex.code < 500:
                raise FileManagerClientError("Unable to create directory: "+str(ex)) from ex
            else:
                raise FileManagerServerError("Failed to create directory: "+str(ex)) from ex

    def list_folder_info(self, path):
        """
        retrieve resource info for a resource (file or directory) with the given path
        """
        if not self.wdcli:
            self.authenticate()

        if not path:
            path = "/"
        elif not path.startswith('/'):
            path = "/"+path

        try:
            resp = self.wdcli.execute_request("list", path, info_request)  # default Depth: 1
        except (wd3c.NoConnection, wd3c.ConnectionException) as ex:
            raise FileManagerCommError("Failed to get resource info: "+str(ex), ep=path) from ex
        except wd3c.NotEnoughSpace as ex:
            raise FileManagerServerError("Failed to get resource info: "+str(ex), ep=path) from ex
        except wd3c.RemoteResourceNotFound as ex:
            raise FileManagerResourceNotFound(path, "Unable to get resource info: "+str(ex)) from ex
        except wd3c.ResponseErrorCode as ex:
            if ex.code < 500:
                raise FileManagerClientError("Unable to get resource info: "+str(ex), ep=path) from ex
            else:
                raise FileManagerServerError("Failed to get resource info: "+str(ex), ep=path) from ex
            
        base = self.cfg['service_endpoint']
        if self.cfg.get('public_prefix'):
            base = urljoin(self.cfg['public_prefix'], urlparse(base).path.lstrip('/'))

        try:
            return parse_propfind_info(resp.text, base)
        except wd3c.RemoteResourceNotFound as ex:
            raise FileManagerResourceNotFound(path, str(ex)) from ex
        except etree.XMLSyntaxError as ex:
            raise FileSpaceServerError("Server returned unparseable XML") from ex
            
    def get_resource_info(self, path):
        """
        return a dictionary of metadata describing the resource (file or directory) with the 
        given path.
        """
        if not self.wdcli:
            self.authenticate()

        if not path:
            path = "/"
        elif not path.startswith('/'):
            path = "/"+path

        try:
            resp = self.wdcli.execute_request("info", path, info_request)
        except (wd3c.NoConnection, wd3c.ConnectionException) as ex:
            raise FileManagerCommError("Failed to get resource info: "+str(ex), ep=path) from ex
        except wd3c.NotEnoughSpace as ex:
            raise FileManagerServerError("Failed to get resource info: "+str(ex), ep=path) from ex
        except wd3c.RemoteResourceNotFound as ex:
            raise FileManagerResourceNotFound(path, "Unable to get resource info: "+str(ex)) from ex
        except wd3c.ResponseErrorCode as ex:
            if ex.code < 500:
                raise FileManagerClientError("Unable to get resource info: "+str(ex), ep=path) from ex
            else:
                raise FileManagerServerError("Failed to get resource info: "+str(ex), ep=path) from ex
            
        base = self.cfg['service_endpoint']
        if self.cfg.get('public_prefix'):
            base = urljoin(self.cfg['public_prefix'], urlparse(base).path.lstrip('/'))

        try:
            return parse_propfind_for(resp.text, path, base)
        except wd3c.RemoteResourceNotFound as ex:
            raise FileManagerResourceNotFound(path, str(ex)) from ex
        except etree.XMLSyntaxError as ex:
            raise FileSpaceServerError("Server returned unparseable XML") from ex

    def delete_resource(self, path):
        """
        delete the named resource.
        """
        if not self.wdcli:
            self.authenticate()

        try:
            resp = self.wdcli.clean(path)
        except (wd3c.NoConnection, wd3c.ConnectionException) as ex:
            raise FileManagerCommError("Failed to delete resource: "+str(ex)) from ex
        except wd3c.NotEnoughSpace as ex:
            raise FileManagerServerError("Failed to delete resource: "+str(ex)) from ex
        except wd3c.RemoteResourceNotFound as ex:
            raise FileManagerResourceNotFound("Unable to delete resource: "+str(ex)) from ex
        except wd3c.ResponseErrorCode as ex:
            if ex.code < 500:
                raise FileManagerClientError("Unable to delete resource: "+str(ex)) from ex
            else:
                raise FileManagerServerError("Failed to delete resource: "+str(ex)) from ex




info_request = """<?xml version="1.0"?>
<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:nc="http://nextcloud.org/ns">
  <d:prop>
    <d:resourcetype/><d:creationdate/><d:getlastmodified/>
    <d:getetag/><d:getcontentlength/>
    <oc:fileid/><oc:size/><oc:permissions/>
  </d:prop>
</d:propfind>
"""

_re_ns = re.compile(r'^\{[^\}]+\}')

def propfind_resp_to_dict(respel):
    """
    convert a propfind response etree element into a dictionary of properties
    """
    href_el = next(iter(respel.findall(".//{DAV:}href")), None)
    path = unquote(urlsplit(href_el.text).path) if href_el is not None else None

    dav_props = {
        '{DAV:}creationdate':    "created",
        '{DAV:}getlastmodified': "modified",
        '{DAV:}getetag':         "etag",
        '{DAV:}getcontenttype':  "contenttype",
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
    if path:
        out['urlpath'] = path
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

def parse_propfind_info(content, davbase):
    """
    Extract the properties in a PROPFIND XML response into a dictionary where each key is 
    a file paths and the value is a dictionary of properties for that file.
    :param str content:  the XML response message to parse
    :param str davbase:  the base WebDAV endpoint URL path; the file paths appearing as keys in the 
                         response will be relative to this base endpoint path.  
    """
    if davbase:
        davbase = urlparse(davbase).path.strip('/')
    davbase = f"/{davbase}/" if davbase else "/"

    out = OrderedDict()
    entries = extract_propfind_responses(content)
    for respel in entries:
        info = propfind_resp_to_dict(respel)
        path = info.get('urlpath')
        if path and path.startswith(davbase):
            path = path[len(davbase):].rstrip('/')
        else:
            # unexpected
            continue
        info['path'] = path
        out[path] = info
        
    return out

def extract_propfind_responses(content) -> List:
    """
    return a list of the response elements found in the given propfind request response
    :param str content:  the XML response message to parse
    """
    # adapted from webdavclient3's WebDavXmlUtils.parse_get_list_info_response()
    out = []
    try:
        tree = etree.fromstring(content)
        for resp in tree.findall(".//{DAV:}response"):
            href_el = next(iter(resp.findall(".//{DAV:}href")), None)
            if href_el is not None:
                out.append(resp)

    except etree.XMLSyntaxError as ex:
        raise FileSpaceServerError("Server returned unparseable XML") from ex

    return out

def parse_propfind_for(content, reqpath, davbase):
    """
    Extract the properties in a PROPFIND XML response into a dictionary
    :param str content:  the XML response message to parse
    :param str reqpath:  the path that properties were requested for
    :param str davbase:  the base WebDAV endpoint URL
    """
    path = f"/{reqpath.strip('/')}/"
    respel = wd3c.WebDavXmlUtils.extract_response_for_path(content, path, davbase)
    out = propfind_resp_to_dict(respel)
    out['path'] = out['urlpath']
    davbase = urlparse(davbase).path
    if out['path'].startswith(davbase):
        out['path'] = out['path'][len(davbase):].strip('/')
    return out

