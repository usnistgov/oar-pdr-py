"""
The WSGI implementation of the web API to the NSD Indexer Service.  See :py:mod:`nistoar.midas.nsdi`
for a description of what this service does and :py:mod:`nistoar.midas.dbio.index` for what the index 
looks like. 

In this implementation, all endpoints feature the same interface: a GET request will return an index
document based on a prompt string given by a ``prompt`` query parameter.  The endpoint relects the 
NSD query endpoint that will be indexed.  The endpoints are:

  ``/People`` 
       indexes people whose last or first name begins with the prompt string
  ``/OU``
       indexes Organizational Unit (OU) records base on the OU's full name, abbreviation, or number
  ``/Division``
       indexes division records base on the division's full name, abbreviation, or number
  ``/Group``
       indexes division records base on the group's full name, abbreviation, or number

For example, a GET to ``/People?prompt=pla`` will return an index to all NSD people entries whose 
first or last name begins with "pla" (case-insensitive).  A GET to ``/Division?prompt=mat`` returns
an index to all division entries whose full name, abbreviation starts with "mat".  
``/Division?prompt=64`` returns index where the division number starts with "64".  

The default format for the returned index is JSON.  A format can be explicitly returned either via 
the ``format`` query parameter or by requesting a media type with the ``Accept`` HTTP request header. 
Supported ``format`` values are "json" and "csv"; their corresponding media types are "application/json"
(or "text/json") and "text/csv".

This service looks for the follow configuration parameters:
  ``nsd`` 
      an object that configures access to the NSD search web service.

The ``nsd`` object supports the following subparameters:
  ``service_endpoint``
      the base URL for the NSD search service.  For the official NSD service, this URL should end
      in "/api/v1".  
"""
from logging import Logger
from collections.abc import Mapping, Callable
from urllib.parse import parse_qs

from nistoar.web.rest import ServiceApp, Handler, ErrorHandling, FatalError
from nistoar.web.formats import FormatSupport, Format, Unacceptable, UnsupportedFormat
from nistoar.nsd.client import NSDClient, NSDServerError
from nistoar.pdr.publish.prov import PubAgent
from nistoar.midas.dbio.index import NSDPeopleIndexClient, NSDOrgIndexClient

class PeopleIndexHandler(Handler, ErrorHandling):
    """
    a handler for index requests on peoples names
    """

    def __init__(self, nsdclient: NSDClient, path: str, wsgienv: dict, start_resp: Callable, who=None, 
                 config: dict={}, log: Logger=None, app=None):
        super(PeopleIndexHandler, self).__init__(path, wsgienv, start_resp, who, config, log, app)

        idxprops = self.cfg.get("index_properties")  # None will default to last/first names
        self.idxcli = NSDPeopleIndexClient(nsdclient, idxprops)
        supp_fmts = FormatSupport()
        supp_fmts.support(Format("json", "application/json"), ["text/json", "application/json"], True)
        supp_fmts.support(Format("csv", "text/csv"), ["text/csv", "application/csv"], True)
        self._set_default_format_support(supp_fmts)

    def do_GET(self, path, ashead=False, format=None):
        """
        return an index based on the given prompt
        """
        path = path.lower()
        if path != "people":
            return self.send_error_obj(404, "Not Found")

        try:
            format = self.select_format(format)
            if not format:
                if self.log:
                    self.log.error("Failed to determine output format")
                return self.send_error(500, "Server Error")
        except Unacceptable as ex:
            return self.send_unacceptable(content=str(ex))
        except UnsupportedFormat as ex:
            return self.send_error(400, "Unsupported Format", str(ex))

        prompt = ''
        qstr = self._env.get('QUERY_STRING')
        if qstr:
            params = parse_qs(qstr)
            prompt = params.get('prompt')[-1]

        try:
            idx = self.idxcli.get_index_for(prompt)
        except NSDServerError as ex:
            self.log.error("Failure accessing NSD service: %s", str(ex))
            return self.send_error_obj(503, "Upstream service error",
                                  "Failure accessing the NSD service: "+str(ex))
        except Exception as ex:
            self.log.exception("Unexpected error accessing indexer client: %s", str(ex))
            return self.send_error_obj(500, "Internal Server Error")

        if format.name == "csv":
            return self.send_ok(idx.export_as_csv(), "text/csv")
        else:
            return self.send_ok(idx.export_as_json(), "application/json")


class OrgIndexHandler(Handler, ErrorHandling):
    """
    a handler for index requests on organizational units
    """

    def __init__(self, nsdclient: NSDClient, path: str, wsgienv: dict, start_resp: Callable, who=None, 
                 config: dict={}, log: Logger=None, app=None):
        super(OrgIndexHandler, self).__init__(path, wsgienv, start_resp, who, config, log, app)

        idxprops = self.cfg.get("index_properties")  # None will default to orG_Name, orG_ACRNM, orG_CD
        self.idxcli = NSDOrgIndexClient(nsdclient, idxprops)
        supp_fmts = FormatSupport()
        supp_fmts.support(Format("json", "application/json"), ["text/json", "application/json"], True)
        supp_fmts.support(Format("csv", "text/csv"), ["text/csv", "application/csv"], True)
        self._set_default_format_support(supp_fmts)

    def do_GET(self, path, ashead=False, format=None):
        """
        return an index based on the given prompt
        """
        path = path.lower()
        if path not in "ou division group organization".split():
            return self.send_error_obj(404, "Not Found", "Not a recognized organization type: "+path)
        if path == "organization":
            path = "ou division group".split()

        try:
            format = self.select_format(format)
            if not format:
                if self.log:
                    self.log.error("Failed to determine output format")
                return self.send_error(500, "Server Error")
        except Unacceptable as ex:
            return self.send_unacceptable(content=str(ex))
        except UnsupportedFormat as ex:
            return self.send_error(400, "Unsupported Format", str(ex))

        prompt = ''
        qstr = self._env.get('QUERY_STRING')
        if qstr:
            params = parse_qs(qstr)
            prompt = params.get('prompt')[-1]

        try:
            idx = self.idxcli.get_index_for(path, prompt)
        except NSDServerError as ex:
            self.log.error("Failure accessing NSD service: %s", str(ex))
            return self.send_error_obj(503, "Upstream service error",
                                  "Failure accessing the NSD service: "+str(ex))
        except Exception as ex:
            self.log.exception("Unexpected error accessing indexer client: %s", str(ex))
            return self.send_error_obj(500, "Internal Server Error")

        if format.name == "csv":
            return self.send_ok(idx.export_as_csv(), "text/csv")
        else:
            return self.send_ok(idx.export_as_json(), "application/json")

    

class NSDIndexerApp(ServiceApp):
    """
    a web app interface for handling NSD indexing requests
    """
    _supported_eps = {
        "people":        PeopleIndexHandler,
        "ou":            OrgIndexHandler,
        "division":      OrgIndexHandler,
        "group":         OrgIndexHandler,
        "organization":  OrgIndexHandler
    }

    def __init__(self, log: Logger, config: Mapping={}, nsdclient: NSDClient=None):
        """
        initialize that app
        :param Logger  log:  the Logger object to use for log messages
        :param dict config:  the configuration dictionary to configure this web app
        :param NSDClient nsdclient:  the NSDClient instance to use to submit queries.  This 
                             should be compatible with the v1 version of the NSD service.  
                             If not provided, one will be constructed based on the 
                             configuration provided in ``config``.
        """
        super(NSDIndexerApp, self).__init__("NSD-indexer", log, config)

        if not nsdclient:
            ep = self.cfg.get("nsd", {}).get("service_endpoint")
            if not ep:
                raise ConfigurationException("Missing required configuration parameter: nsd.service_endpoint")
            nsdclient = NSDClient(ep)
        self.nsdcli = nsdclient

    def create_handler(self, env: Mapping, start_resp: Callable, path: str, who: PubAgent) -> Handler:
        """
        return a handler instance to handle a particular request to a path
        :param Mapping env:  the WSGI environment containing the request
        :param Callable start_resp:  the start_resp function to use initiate the response
        :param str path:     the path to the resource being requested.  This is expected to 
                             be :py:mod:`one of the endpoints supported by this class<.v1>`.
        """
        path = path.strip('/')
        pathels = path.split('/')

        if len(pathels) == 0:
            return Ready(self.cfg.get('ready', {}), path)

        pathels[0] = pathels[0].lower()  # path case-insensitive
        if pathels[0] not in self._supported_eps:
            return Unsupported(env, start_resp, path, self.cfg.get('unsupported', {}), self.log, self)

        return self._supported_eps[pathels[0]](self.nsdcli, path, env, start_resp, None, 
                                               self.cfg.get(pathels[0], {}), self.log, self)


class Unsupported(Handler, ErrorHandling):

    def __init__(self, env: Mapping, start_resp: Callable, path: str, config: Mapping=None,
                 log: Logger=None, app=None):
        Handler.__init__(self, path, env, start_resp, None, config, log, app)

    def do_GET(self, path, ashead=False):
        return self.send_error_obj(404, "Not Found", ashead=ashead)


def NSDIndexerAppFactory(dbio_client_factory, log: Logger, config: Mapping, projname):
    """
    a factory function that fits an NSDIndexerApp into the MIDAS web app framework.  The 
    ``dbio_client_factory`` and ``projname`` argument are ignored as they are not needed.
    """
    return NSDIndexerApp(log, config)

