"""
A RESTful web service that resolves a DOI to a NERDm reference
"""
import logging, re
from logging import Logger
from collections import OrderedDict
from typing import List, Mapping, Callable

from nistoar.nerdm.convert.doi import DOIResolver
from nistoar.doi import DOIDoesNotExist, DOIClientException, DOIResolutionException
from nistoar.web.rest import ServiceApp, Handler, HandlerWithJSON, Ready
from nistoar.base.config import ConfigurationException
from nistoar.nerdm.constants import BIB_SCHEMA_URI
from nistoar.pdr.utils.prov import Agent
from ..dbio.wsgi import DBIOHandler
from .. import system

deflog = logging.getLogger(system.system_abbrev)   \
                .getChild('doi').getChild('wsgi')

DEF_BASE_PATH = '/'

_doi_pat = re.compile("(doi:)?(10\.\d+/[^#\s]+)")
_doi_baseurl_pat = re.compile("^https?://(dx\.)?doi\.org/")

class DOI2NERDmHandler(HandlerWithJSON):
    """
    a base handler for converting DOIs to NERDm metadata
    """
    def __init__(self, doiresolver, path: str, wsgienv: dict, start_resp: Callable, who=None, 
                 log: Logger=None, app=None):
        super(DOI2NERDmHandler, self).__init__(path, wsgienv, start_resp, who, {}, log, app)
        self._doires = doiresolver

    def send_reference(self, doi, ashead=False):
        """
        Convert a DOI to a NERDm Reference object and send it to the client

        Note that if the given DOI is found not to exist, the returned object will 
        contain the DOI (in the ``@id`` property) but with no other metadata (such 
        as title, location, citation text, etc.).

        :param str doi:  the DOI to convert, stripped of its prefix and component identifier; 
                         that is, it must begin with "10.".  
        :param str ashead:  send a response appropriate for a HEAD request
        """
        try:

            ref = self._doires.to_reference(doi)
            
        except DOIDoesNotExist as ex:
            
            ref = OrderedDict([
                ("@id", "doi:"+doi),
                ("_extensionSchemas",
                 [ BIB_SCHEMA_URI+"#/definitions/DCiteReference" ]),
                ("pdr:comment", "DOI does not exist yet")
            ])

        return self.send_json(ref, ashead=ashead)

    def send_authors(self, doi, ashead=False):
        """
        convert the given DOI to a list of NERDm authors (People objects) and send it to the client

        :param str doi:  the DOI to convert, stripped of its prefix and component identifier; 
                         that is, it must begin with "10.".  
        :param str ashead:  send a response appropriate for a HEAD request
        """
        auths = self._doires.to_authors(doi)
        return self.send_json(auths, ashead=ashead)

    def do_GET(self, path, ashead=False):
        """
        Convert a DOI to the requested NERDm object
        """
        if not path:
            return self.send_error_obj(405, "Method Not Allowed"
                                       "GET request must include a NERDm type, authors or ref",
                                       ashead=ashead)

        parts = path.strip('/').split('/', 1)
        path = parts[1] if len(parts) > 1 else ""
        outtype = parts[0]

        if outtype not in ["ref", "authors"]:
            return self.send_error_obj(404, "Not Found",
                                       "Not a supportted NERDm output type", ashead=ashead)
        if not path:
            return self.send_error_obj(405, "Method Not Allowed"
                                       "GET request must include a NERDm type, authors or ref",
                                       ashead=ashead)

        # interpret path as a DOI 
        path = _doi_baseurl_pat.sub('', path)  # strip https://doi.org/
        m = _doi_pat.search(path)              # confirm legal DOI 
        if not m:
            return self.send_error_obj(400, "Not a DOI",
                                       "Identifier not recognized as a DOI",
                                       ashead=ashead)
        doi = m.group(2)
        try:
            if outtype == "authors":
                return self.send_authors(doi)

            # should be "ref" but we can allow for synonyms; see outtype check above
            return self.send_reference(doi)
                                  
        except DOIDoesNotExist as ex:
            return self.send_error_obj(404, "DOI Not Found", "DOI appears not to be registered",
                                       ashead=ashead)
            
        except DOIClientException as ex:
            self.log.error("Problem using DOI resolver (prog error?) %s: %s",
                           doi, str(ex))
            return self.send_error_obj(500, "Server Error",
                                       "Server error while resolving OOI",
                                       ashead=ashead)
        
        except DOIResolutionException as ex:
            self.log.error("Unexpected DOI resolution response to %s: %s",
                           doi, str(ex))
            return self.send_error_obj(503, "DOI Resolver Failure",
                                       "Trouble accessing remote DOI resolver service",
                                       ashead=ashead)

class DOI2NERDmApp(ServiceApp):
    """
    a web app for converting a DOI to NERDm metadata
    """

    def __init__(self, log: Logger, config: dict={}):
        super(DOI2NERDmApp, self).__init__("doi2nerdm", log, config)
        if not self.cfg.get("doi_resolver"):
            raise ConfigurationException("DOI2NERDmApp: missing required parameter: doi_resolver")
        self._doires = DOIResolver.from_config(self.cfg["doi_resolver"])

    def create_handler(self, env: dict, start_resp: Callable, path: str, who: Agent=None) -> Handler:
        """
        return a handler instance to handle a particular request to a path
        :param Mapping env:  the WSGI environment containing the request
        :param Callable start_resp:  the start_resp function to use initiate the response
        :param str    path:  the path to the resource being requested.  This is usually 
                             relative to a parent path that this ServiceApp is configured to 
                             handle.  
        :param Agent   who:  the authenticated user agent making the request
        """
        if not path:
            return Ready(path, env, start_resp, who, log=self.log, app=self)

        return DOI2NERDmHandler(self._doires, path, env, start_resp, who, self.log, self)
