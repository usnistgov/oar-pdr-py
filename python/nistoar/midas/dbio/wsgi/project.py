"""
A web service interface to various MIDAS project records.  

A _project record_ is a persistable record that is compliant with the MIDAS Common Database project 
data model, where examples of a "project record" types include DMP records and data publication drafts.
The :py:class:`MIDASProjectApp` encapsulates the handling of requests to create and manipulate project 
records.  If desired, this class can be specialized for a particular project type; as an example, see
:py:mod:`nistoar.midas.dap.service.mds3`.

This implementation uses the simple :py:mod:`nistoar-internal WSGI 
framework<nistoar.pdr.publish.service.wsgi>` to handle the specific web service endpoints.  The 
:py:class:`MIDASProjectApp` is the router for the Project collection endpoint: it analyzes the relative 
URL path and delegates the handling to a more specific handler class.  In particular, these endpoints
are handled accordingly:

``/`` -- :py:class:`ProjectSelectionHandler`
     responds to to project search queries to find project records matching search criteria (GET) 
     as well as accepts requests to create new records (POST).

``/{projid}`` -- :py:class:`ProjectHandler`
     returns the full project record (GET) or deletes it (DELETE).

``/{projid}/name`` -- :py:class:`ProjectNameHandler`
     returns (GET) or updates (PUT) the user-supplied name of the record.

``/{projid}/owner`` -- :py:class:`ProjectNameHandler`
     returns ID of current owner (GET) or reassigns ownership to given ID (PUT).

``/{projid}/data[/...]`` -- :py:class:`ProjectDataHandler`
     returns (GET), updates (PUT, PATCH), or clears (DELETE) the data content of the record.  This
     implementation supports updating individual parts of the data object via PUT, PATCH, DELETE 
     based on the path relative to ``data``.   Subclasses (e.g. with the 
     :py:mod:`DAP specialization<nistoar.midas.dap.service.mds3>`) may also support POST for certain
     array-type properties within ``data``.  

``/{projid}/acls[/...]`` -- :py:class:`ProjectACLsHandler`
     returns (GET) or updates (PUT, PATCH, POST, DELETE) access control lists for the record.

``/{projid}/*`` -- :py:class`ProjectInfoHandler`
     returns other non-editable parts of the record via GET (including the ``meta`` property).
"""
from logging import Logger
from collections import OrderedDict
from collections.abc import Mapping, Sequence, Callable
from typing import Iterator
from urllib.parse import parse_qs
import json

from nistoar.web.rest import ServiceApp, Handler, Agent
from nistoar.web.formats import JSONSupport, TextSupport, UnsupportedFormat, Unacceptable
from ... import dbio
from ...dbio import ProjectRecord, ProjectService, ProjectServiceFactory
from .base import DBIOHandler
from .search_sorter import SortByPerm

__all__ = ["MIDASProjectHandler", "ProjectDataHandler"]

class ProjectRecordHandler(DBIOHandler):
    """
    base handler class for all requests on project records.  
    """
    def __init__(self, service: ProjectService, svcapp: ServiceApp, wsgienv: dict, start_resp: Callable, 
                 who: Agent, path: str="", config: dict=None, log: Logger=None):
        """
        Initialize this handler with the request particulars.  

        :param ProjectService service:  the ProjectService instance to use to get and update
                               the project data.
        :param ServiceApp svcapp:  the web service ServiceApp receiving the request and calling this 
                               constructor
        :param dict  wsgienv:  the WSGI request context dictionary
        :param Callable start_resp:  the WSGI start-response function used to send the response
        :param Agent     who:  the authenticated user making the request.  
        :param str      path:  the relative path to be handled by this handler; typically, some starting 
                               portion of the original request path has been stripped away to handle 
                               produce this value.
        :param dict   config:  the handler's configuration; if not provided, the inherited constructor
                               will extract the configuration from ``svcapp``.  Normally, the constructor
                               is called without this parameter.
        :param Logger    log:  the logger to use within this handler; if not provided (typical), the 
                               logger attached to the ServiceApp will be used.  
        """

        super(ProjectRecordHandler, self).__init__(svcapp, service.dbcli, wsgienv, start_resp, who,
                                                   path, config, log)
        self.svc = service

class ProjectHandler(ProjectRecordHandler):
    """
    handle access to the whole project record
    """

    def __init__(self, service: ProjectService, svcapp: ServiceApp, wsgienv: dict, start_resp: Callable, 
                 who: Agent, id: str, config: dict=None, log: Logger=None):
        """
        Initialize this handler with the request particulars.  This constructor is called 
        by the webs service ServiceApp.  

        :param ProjectService service:  the ProjectService instance to use to get and update
                               the project data.
        :param ServiceApp svcapp:  the web service ServiceApp receiving the request and calling this 
                               constructor
        :param dict  wsgienv:  the WSGI request context dictionary
        :param Callable start_resp:  the WSGI start-response function used to send the response
        :param Agent     who:  the authenticated user making the request.  
        :param str        id:  the ID of the project record being requested
        :param dict   config:  the handler's configuration; if not provided, the inherited constructor
                               will extract the configuration from ``svcapp``.  Normally, the constructor
                               is called without this parameter.
        :param Logger    log:  the logger to use within this handler; if not provided (typical), the 
                               logger attached to the ServiceApp will be used.  
        """

        super(ProjectHandler, self).__init__(service, svcapp, wsgienv, start_resp, who, "", config, log)

        self._id = id
        if not id:
            # programming error
            raise ValueError("Missing ProjectRecord id")

    def do_OPTIONS(self, path):
        return self.send_options(["GET", "DELETE"])

    def do_GET(self, path, ashead=False):
        try:
            prec = self.svc.get_record(self._id)
        except dbio.NotAuthorized as ex:
            return self.send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return self.send_error_resp(404, "ID not found", "Record with requested identifier not found", 
                                        self._id, ashead=ashead)

        return self.send_json(prec.to_dict(), ashead=ashead)

    def do_DELETE(self, path):
        try:
            prec = self.svc.get_record(self._id)
            out = prec.to_dict()
            self.svc.delete_record(self._id)
        except dbio.NotAuthorized as ex:
            return self.send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return self.send_error_resp(404, "ID not found", "Record with requested identifier not found", 
                                        self._id)
        except NotImplementedError as ex:
            return self.send_error(501, "Not Implemented")

        return self.send_json(out)


class ProjectInfoHandler(ProjectRecordHandler):
    """
    handle retrieval of simple parts of a project record.  Only GET requests are allowed via this handler.
    """

    def __init__(self, service: ProjectService, svcapp: ServiceApp, wsgienv: dict, start_resp: Callable, 
                 who: Agent, id: str, attribute: str, config: dict={}, log: Logger=None):
        """
        Initialize this handler with the request particulars.  This constructor is called 
        by the webs service ServiceApp.  

        :param ProjectService service:  the ProjectService instance to use to get and update
                               the project data.
        :param ServiceApp svcapp:  the web service ServiceApp receiving the request and calling this 
                               constructor
        :param dict  wsgienv:  the WSGI request context dictionary
        :param Callable start_resp:  the WSGI start-response function used to send the response
        :param Agent     who:  the authenticated user making the request.  
        :param str        id:  the ID of the project record being requested
        :param str attribute:  a recognized project model attribute
        :param dict   config:  the handler's configuration; if not provided, the inherited constructor
                               will extract the configuration from ``svcapp``.  Normally, the constructor
                               is called without this parameter.
        :param Logger    log:  the logger to use within this handler; if not provided (typical), the 
                               logger attached to the ServiceApp will be used.  
        """

        super(ProjectInfoHandler, self).__init__(service, svcapp, wsgienv, start_resp, who, attribute,
                                                 config, log)
        self._id = id
        if not id:
            # programming error
            raise ValueError("Missing ProjectRecord id")

    def do_OPTIONS(self, path):
        return self.send_options(["GET"])

    def do_GET(self, path, ashead=False):
        if not path:
            # programming error
            raise ValueError("Missing ProjectRecord attribute")
        try:
            prec = self.svc.get_record(self._id)
        except dbio.NotAuthorized as ex:
            return self.send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return self.send_error_resp(404, "ID not found",
                                        "Record with requested identifier not found",
                                        self._id, ashead=ashead)

        parts = path.split('/')
        data = prec.to_dict()
        while len(parts) > 0:
            attr = parts.pop(0)
            if not isinstance(data, Mapping) or attr not in data:
                return self.send_error(404, "Record attribute not available",
                                       "Requested record attribute not found", self._id, ashead=ashead)
            data = data[attr]

        return self.send_json(data, ashead=ashead)

class ProjectNameHandler(ProjectRecordHandler):
    """
    handle retrieval/update of a project records owner or mnemonic name
    """

    MAX_NAME_LENGTH = 1024
    ALLOWED_PROPS = ["name", "owner"]

    def __init__(self, service: ProjectService, svcapp: ServiceApp, wsgienv: dict, start_resp: Callable,
                 who: Agent, id: str, prop: str, config: dict=None, log: Logger=None):
        """
        Initialize this handler with the request particulars.  This constructor is called 
        by the webs service ServiceApp.  

        :param ProjectService service:  the ProjectService instance to use to get and update
                               the project data.
        :param ServiceApp svcapp:  the web service ServiceApp receiving the request and calling this 
                               constructor
        :param dict  wsgienv:  the WSGI request context dictionary
        :param Callable start_resp:  the WSGI start-response function used to send the response
        :param Agent     who:  the authenticated user making the request.  
        :param str        id:  the ID of the project record being requested
        :param str      prop:  the project record property to be updated; restricted to "name" or "owner"
        :param dict   config:  the handler's configuration; if not provided, the inherited constructor
                               will extract the configuration from ``svcapp``.  Normally, the constructor
                               is called without this parameter.
        :param Logger    log:  the logger to use within this handler; if not provided (typical), the 
                               logger attached to the ServiceApp will be used.  
        """
        if prop not in self.ALLOWED_PROPS:
            raise ValueError("ProjectNameHandler(): Disallowed prop: "+str(prop))
        super(ProjectNameHandler, self).__init__(service, svcapp, wsgienv, start_resp, who, prop, config, log)
                                                   
        self._id = id
        if not id:
            # programming error
            raise ValueError("Missing ProjectRecord id")

        # allow output in JSON (default) or plain text
        fmtsup = JSONSupport()
        TextSupport.add_support(fmtsup)
        self._set_default_format_support(fmtsup)

        # allows client to request format via query parameter "format"
        self._set_format_qp("format")

    def acceptable(self):
        # not sure if this is really needed
        if super(ProjectNameHandler, self).acceptable():
            return True
        return "text/plain" in self.get_accepts()
        
    def do_OPTIONS(self, path):
        return self.send_options(["GET", "PUT"])

    def do_GET(self, path, ashead=False):
        if path not in self.ALLOWED_PROPS:
            self.log.error("ProjectNameHandler: unexpected property request (programmer error?): %s", path)
            return self.send_error(500, "Internal Server Error")

        try:
            format = self.select_format()
        except Unacceptable as ex:
            return self.send_unacceptable(content=str(ex))
        except UnsupportedFormat as ex:
            return self.send_error(400, "Unsupported Format", str(ex))

        try:
            prec = self.svc.get_record(self._id)
        except dbio.NotAuthorized as ex:
            return self.send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return self.send_error_resp(404, "ID not found", "Record with requested identifier not found", 
                                        self._id, ashead=ashead)

        if format.name == "text":
            return self.send_ok(getattr(prec, path), contenttype=format.ctype, ashead=ashead)
        else:
            return self.send_json(getattr(prec, path), ashead=ashead)

    def do_PUT(self, path):
        if path not in self.ALLOWED_PROPS:
            self.log.error("ProjectNameHandler: unexpected property request (programmer error?): %s", path)
            return self.send_error(500, "Internal Server Error")

        try:
            format = self.select_format()
        except Unacceptable as ex:
            return self.send_unacceptable(content=str(ex))
        except UnsupportedFormat as ex:
            return self.send_error(400, "Unsupported Format", str(ex))

        rctype = self._env.get('CONTENT_TYPE', 'application/json')
        if rctype == "application/x-www-form-urlencoded":  # default for python requests
            rctype = "application/json"
        try:
            if rctype == "text/plain":
                name = self.get_text_body(self.MAX_NAME_LENGTH+1)
            elif "/json" in rctype:
                name = self.get_json_body()
            else:
                self.log.debug("Attempt to change %s with unsupported content type: %s", path, rctype)
                return self.send_error_resp(415, "Unsupported Media Type",
                                            "Input content-type is not supported", self._id)
        except self.FatalError as ex:
            return self.send_fatal_error(ex)

        if len(name) > self.MAX_NAME_LENGTH:
            return self.send_error_resp(400, "Value length too long"
                                        "Supplied value is tool long for "+path, self._id)

        try:
            prec = self.svc.get_record(self._id)
            if path == "owner":
                self.log.info("Reassigning ownership of %s from %s to %s", self._id, prec.owner, name)
                prec.reassign(name)
            else:
                self.log.info("Changing short name of %s from %s to %s", self._id, prec.name, name)
                setattr(prec, path, name)
            if not prec.authorized(dbio.ACLs.ADMIN):
                raise dbio.NotAuthorized(self._dbcli.user_id, "change record "+path)
            prec.save()
        except dbio.NotAuthorized as ex:
            return self.send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return self.send_error_resp(404, "ID not found",
                                        "Record with requested identifier not found", self._id)
        except dbio.InvalidUpdate as ex:
            return self.send_error_resp(400, "Invalid Input",
                                        "Invalid value provided for %s: %s" % (path, str(ex)), self._id)

        if format.name == "text":
            return self.send_ok(getattr(prec, path), contenttype=format.ctype)
        else:
            return self.send_json(getattr(prec, path))

        
class ProjectDataHandler(ProjectRecordHandler):
    """
    handle retrieval/update of a project record's data content
    """

    def __init__(self, service: ProjectService, svcapp: ServiceApp, wsgienv: dict, start_resp: Callable, 
                 who: Agent, id: str, datapath: str, config: dict=None, log: Logger=None):
        """
        Initialize this data request handler with the request particulars.  This constructor is called 
        by the webs service ServiceApp in charge of the project record interface.  

        :param ProjectService service:  the ProjectService instance to use to get and update
                               the project data.
        :param ServiceApp svcapp:  the web service ServiceApp receiving the request and calling this 
                               constructor
        :param dict  wsgienv:  the WSGI request context dictionary
        :param Callable start_resp:  the WSGI start-response function used to send the response
        :param Agent     who:  the authenticated user making the request.  
        :param str        id:  the ID of the project record being requested
        :param str  datapath:  the subpath pointing to a particular piece of the project record's data;
                               this will be a '/'-delimited identifier pointing to an object property 
                               within the data object.  This will be an empty string if the full data 
                               object is requested.
        :param dict   config:  the handler's configuration; if not provided, the inherited constructor
                               will extract the configuration from ``svcapp``.  Normally, the constructor
                               is called without this parameter.
        :param Logger    log:  the logger to use within this handler; if not provided (typical), the 
                               logger attached to the ServiceApp will be used.  
        """
        super(ProjectDataHandler, self).__init__(service, svcapp, wsgienv, start_resp, who, datapath,
                                                 config, log)
        self._id = id
        if not id:
            # programming error
            raise ValueError("Missing ProjectRecord id")

    def do_OPTIONS(self, path):
        return self.send_options(["GET", "PUT", "PATCH", "DELETE"])

    def do_GET(self, path, ashead=False):
        """
        respond to a GET request
        :param str path:  a path to the portion of the data to get.  This is the same as the `datapath`
                          given to the handler constructor.  This will be an empty string if the full
                          data object is requested.
        :param bool ashead:  if True, the request is actually a HEAD request for the data
        """
        try:
            out = self.svc.get_data(self._id, path)
        except dbio.NotAuthorized as ex:
            return self.send_unauthorized()
        except dbio.ObjectNotFound as ex:
            if ex.record_part:
                return self.send_error_resp(404, "Data property not found",
                                            "No data found at requested property", self._id, ashead=ashead)
            return self.send_error_resp(404, "ID not found",
                                        "Record with requested identifier not found", self._id, ashead=ashead)
        return self.send_json(out, ashead=ashead)

    def do_DELETE(self, path):
        """
        respond to a DELETE request.  This is used to clear the value of a particular property 
        within the project data or otherwise reset the project data to its initial defaults.
        :param str path:  a path to the portion of the data to clear
        """
        try:
            cleared = self.svc.clear_data(self._id, path)
        except dbio.NotAuthorized as ex:
            return self._send_unauthorized()
        except dbio.PartNotAccessible as ex:
            return self.send_error_resp(405, "Data part not deletable",
                                        "Requested part of data cannot be deleted")
        except dbio.ObjectNotFound as ex:
            if ex.record_part:
                return self.send_error_resp(404, "Data property not found",
                                            "No data found at requested property", self._id, ashead=ashead)
            return self.send_error_resp(404, "ID not found",
                                        "Record with requested identifier not found", self._id, ashead=ashead)

        return self.send_json(cleared, "Cleared", 201)

    def do_PUT(self, path):
        try:
            newdata = self.get_json_body()
        except self.FatalError as ex:
            return self.send_fatal_error(ex)

        try:
            data = self.svc.replace_data(self._id, newdata, path)  
        except dbio.NotAuthorized as ex:
            return self.send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return self.send_error_resp(404, "ID not found",
                                        "Record with requested identifier not found", self._id)
        except dbio.InvalidUpdate as ex:
            return self.send_error_resp(400, "Invalid Input Data", ex.format_errors())
        except dbio.PartNotAccessible as ex:
            return self.send_error_resp(405, "Data part not updatable",
                                        "Requested part of data cannot be updated")
        except dbio.NotEditable as ex:
            return self.send_error_resp(409, "Not in editable state", "Record is not in state=edit")
                                        

        return self.send_json(data)

    def do_PATCH(self, path):
        try:
            newdata = self.get_json_body()
        except self.FatalError as ex:
            return self.send_fatal_error(ex)

        try:
            data = self.svc.update_data(self._id, newdata, path)
        except dbio.NotAuthorized as ex:
            return self.send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return self.send_error_resp(404, "ID not found",
                                        "Record with requested identifier not found", self._id)
        except dbio.InvalidUpdate as ex:
            return self.send_error_resp(400, "Submitted data creates an invalid record",
                                        ex.format_errors())
        except dbio.PartNotAccessible as ex:
            return self.send_error_resp(405, "Data part not updatable",
                                        "Requested part of data cannot be updated")
        except dbio.NotEditable as ex:
            return self.send_error_resp(409, "Not in editable state", "Record is not in state=edit")

        return self.send_json(data)


class ProjectSelectionHandler(ProjectRecordHandler):
    """
    handle collection-level access searching for project records and creating new ones
    """

    def __init__(self, service: ProjectService, svcapp: ServiceApp, wsgienv: dict, start_resp: Callable,
                 who: Agent, iftype: str=None, config: dict=None, log: Logger=None):
        """
        Initialize this record request handler with the request particulars.  This constructor is called 
        by the webs service ServiceApp in charge of the project record interface.  

        :param ServiceApp svcapp:  the web service ServiceApp receiving the request and calling this 
                               constructor
        :param dict  wsgienv:  the WSGI request context dictionary
        :param Callable start_resp:  the WSGI start-response function used to send the response
        :param Agent     who:  the authenticated user making the request.  
        :param str    iftype:  a name for the search interface type that is requested.  Recognized values
                               include ":selected" which supports Mongo-like filter syntax.  If not provided,
                               a simple query-parameter-based filter syntax is used.
        :param dict   config:  the handler's configuration; if not provided, the inherited constructor
                               will extract the configuration from ``svcapp``.  Normally, the constructor
                               is called without this parameter.
        :param Logger    log:  the logger to use within this handler; if not provided (typical), the 
                               logger attached to the ServiceApp will be used.  
        """
        if iftype is None:
            iftype = ""
        super(ProjectSelectionHandler, self).__init__(service, svcapp, wsgienv, start_resp, who, iftype,
                                                      config, log)

    def do_OPTIONS(self, path):
        return self.send_options(["GET", "POST"])

    def _select_records(self, perms, **constraints) -> Iterator[ProjectRecord]:
        """
        submit a search query in a project specific way.  This method is provided as a 
        hook to subclasses that may need to specialize the search strategy or manipulate the results.  
        This implementation passes the query directly to the generic DBClient instance.
        :return:  a generator that iterates through the matched records
        """
        return self._dbcli.select_records(perms, **constraints)

    def do_GET(self, path, ashead=False):
        """
        respond to a GET request, interpreted as a search for records accessible by the user
        :param str path:  a path to the portion of the data to get.  This is the same as the `datapath`
                          given to the handler constructor.  This will always be an empty string.
        :param bool ashead:  if True, the request is actually a HEAD request for the data
        """
        perms = []
        qstr = self._env.get('QUERY_STRING')
        if qstr:
            params = parse_qs(qstr)
            perms = params.get('perm')
        if not perms:
            perms = dbio.ACLs.OWN

        # sort the results by the best permission type permitted
        sortd = SortByPerm()
        for rec in self._select_records(perms):
            sortd.add_record(rec)
        out = [rec.to_dict() for rec in sortd.sorted()]

        return self.send_json(out, ashead=ashead)

    def do_POST(self, path):
        """
        create a new project record given some initial data
        """
        try:
            input = self.get_json_body()
        except self.FatalError as ex:
            print("fatal Error")
            return self.send_fatal_error(ex)
        
        path = path.rstrip('/')
        if not path:
            return self.create_record(input)
        elif path == ':selected':
            try: 
                return self.adv_select_records(input)
            except SyntaxError as syntax:
                return self.send_error(400, "Wrong query structure for filter")
            except ValueError as value:
                return self.send_error(204, "No Content")
            except AttributeError as attribute:
                return self.send_error(400, "Attribute Error, not a json")
        
        else:
            return self.send_error(400, "Selection resource not found")

    def adv_select_records(self, input: Mapping):
        """
        submit a record search 
        :param dict filter:   the search constraints for the search.
        """
        filter = input.get("filter", {})
        perms = input.get("permissions", [])
        if not perms:
            perms = [ dbio.ACLs.OWN ]

        # sort the results by the best permission type permitted
        sortd = SortByPerm()
        for rec in self._adv_select_records(filter, perms):
            sortd.add_record(rec)

        out = [rec.to_dict() for rec in sortd.sorted()]
        if not out:
            raise ValueError("Empty Set")
        else:
            return self.send_json(out)

    def _adv_select_records(self, filter, perms) -> Iterator[ProjectRecord]:
        """
        submit the advanced search query in a project-specific way. This method is provided as a 
        hook to subclasses that may need to specialize the search strategy or manipulate the results.  
        This base implementation passes the query directly to the generic DBClient instance.
        :return:  a generator that iterates through the matched records
        """
        return self._dbcli.adv_select_records(filter, perms)

    def create_record(self, newdata: Mapping):
        """
        create a new record
        :param dict newdata:  the initial data for the record.  This data is expected to conform to
                              the DBIO record schema (with ``data`` and ``meta`` content appropriate 
                              for the project type). 
        """
        if not newdata.get('name'):
            return self.send_error_resp(400, "Bad POST input", "No mneumonic name provided")

        try:
            prec = self.svc.create_record(newdata['name'], newdata.get("data"), newdata.get("meta"))
        except dbio.NotAuthorized as ex:
            self.log.debug("Authorization failure: "+str(ex))
            return self.send_unauthorized()
        except dbio.AlreadyExists as ex:
            return self.send_error_resp(400, "Name already in use", str(ex))
        except dbio.InvalidUpdate as ex:
            return self.send_error_resp(400, "Submitted data creates an invalid record",
                                        ex.format_errors())
    
        return self.send_json(prec.to_dict(), "Project Created", 201)


class ProjectACLsHandler(ProjectRecordHandler):
    """
    handle retrieval/update of a project record's data content
    """

    def __init__(self, service: ProjectService, svcapp: ServiceApp, wsgienv: dict, start_resp: Callable, 
                 who: Agent, id: str, datapath: str="", config: dict=None, log: Logger=None):
        """
        Initialize this data request handler with the request particulars.  This constructor is called 
        by the webs service ServiceApp in charge of the project record interface.  

        :param ProjectService service:  the ProjectService instance to use to get and update
                               the project data.
        :param ServiceApp svcapp:  the web service ServiceApp receiving the request and calling this 
                               constructor
        :param dict  wsgienv:  the WSGI request context dictionary
        :param Callable start_resp:  the WSGI start-response function used to send the response
        :param Agent     who:  the authenticated user making the request.  
        :param str        id:  the ID of the project record being requested
        :param str  permpath:  the subpath pointing to a particular permission ACL; it can either be
                               simply a permission name, PERM (e.g. "read"), or a p
                               this will be a '/'-delimited identifier pointing to an object property 
                               within the data object.  This will be an empty string if the full data 
                               object is requested.
        :param dict   config:  the handler's configuration; if not provided, the inherited constructor
                               will extract the configuration from ``svcapp``.  Normally, the constructor
                               is called without this parameter.
        :param Logger    log:  the logger to use within this handler; if not provided (typical), the 
                               logger attached to the ServiceApp will be used.  
        """
        super(ProjectACLsHandler, self).__init__(service, svcapp, wsgienv, start_resp, who, datapath,
                                                 config, log)
        self._id = id
        if not id:
            # programming error
            raise ValueError("Missing ProjectRecord id")

        
    def do_OPTIONS(self, path):
        return self.send_options(["GET", "POST", "PUT", "PATCH", "DELETE"])

    def do_GET(self, path, ashead=False):
        try:
            prec = self.svc.get_record(self._id)
        except dbio.NotAuthorized as ex:
            return self.send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return self.send_error_resp(404, "ID not found",
                                        "Record with requested identifier not found",
                                        self._id, ashead=ashead)

        recd = prec.to_dict()
        if not path:
            return self.send_json(recd.get('acls', {}), ashead=ashead)

        path = path.strip('/')
        parts = path.split('/', 1)
        acl = recd.get('acls', {}).get(parts[0])
        if acl is None:
            if parts[0] not in [dbio.ACLs.READ, dbio.ACLs.WRITE, dbio.ACLs.ADMIN, dbio.ACLs.DELETE]:
                return self.send_error_resp(404, "Unsupported ACL type",
                                            "Request for unsupported ACL type", ashead=ashead)
            acl = []

        if len(parts) < 2:
            return self.send_json(acl, ashead=ashead)

        return self.send_json(parts[1] in acl, ashead=ashead)

    def do_POST(self, path):
        """
        add an identity to the acl for a specified permission.  This handles POST ID/acls/PERM; 
        `path` should be set to PERM.  
        """
        try:
            # the input should be a single string giving a user or group identity to add to PERM ACL
            identity = self.get_json_body()
        except self.FatalError as ex:
            return self.send_fatal_error(ex)

        # make sure a permission type, and only a permission type, is specified
        path = path.strip('/')
        if not path or '/' in path:
            return self.send_error_resp(405, "POST not allowed",
                                        "ACL POST request should not specify a user/group identifier")

        if not isinstance(identity, str):
            return self.send_error_resp(400, "Wrong input data type"
                                        "Input data is not a string providing a user or group identifier")

        # TODO: ensure input value is a bona fide user or group name

        try:
            prec = self.svc.get_record(self._id)
        except dbio.NotAuthorized as ex:
            return self.send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return self.send_error_resp(404, "ID not found",
                                        "Record with requested identifier not found", self._id)

        if path in [dbio.ACLs.READ, dbio.ACLs.WRITE, dbio.ACLs.ADMIN, dbio.ACLs.DELETE]:
            prec.acls.grant_perm_to(path, identity)
            prec.save()
            return self.send_json(prec.to_dict().get('acls', {}).get(path,[]))

        return self.send_error_resp(405, "POST not allowed on this permission type",
                                    "Updating specified permission is not allowed")
        
    def do_PUT(self, path):
        """
        replace the list of identities in a particular ACL.  This handles PUT ID/acls/PERM; 
        `path` should be set to PERM.  Note that previously set identities are removed. 
        """
        # make sure a permission type, and only a permission type, is specified
        path = path.strip('/')
        if not path or '/' in path:
            return self.send_error_resp(405, "PUT not allowed", "Unable set ACL membership")

        try:
            identities = self.get_json_body()
        except self.FatalError as ex:
            return self.send_fatal_error(ex)

        if isinstance(identities, str):
            identities = [identities]
        if not isinstance(identities, list):
            return self.send_error_resp(400, "Wrong input data type"
                                        "Input data is not a string providing a user/group list")

        # TODO: ensure input value is a bona fide user or group name

        try:
            prec = self.svc.get_record(self._id)
        except dbio.NotAuthorized as ex:
            return self.send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return self.send_error_resp(404, "ID not found",
                                        "Record with requested identifier not found", self._id)

        if path in [dbio.ACLs.READ, dbio.ACLs.WRITE, dbio.ACLs.ADMIN, dbio.ACLs.DELETE]:
            try:
                prec.acls.revoke_perm_from_all(path)
                prec.acls.grant_perm_to(path, *identities)
                prec.save()
                return self.send_json(prec.to_dict().get('acls', {}).get(path,[]))
            except dbio.NotAuthorized as ex:
                return self.send_unauthorized()

        return self.send_error_resp(405, "PUT not allowed on this permission type",
                                    "Updating specified permission is not allowed")
        

    def do_PATCH(self, path):
        """
        fold given list of identities into a particular ACL.  This handles PATCH ID/acls/PERM; 
        `path` should be set to PERM.
        """
        try:
            # input is a list of user and/or group identities to add the PERM ACL
            identities = self.get_json_body()
        except self.FatalError as ex:
            return self.send_fatal_error(ex)

        # make sure path is a permission type (PERM), and only a permission type
        path = path.strip('/')
        if not path or '/' in path:
            return self.send_error_resp(405, "PATCH not allowed",
                                        "ACL PATCH request should not a member name")

        if isinstance(identities, str):
            identities = [identities]
        if not isinstance(identities, list):
            return self.send_error_resp(400, "Wrong input data type"
                                        "Input data is not a list of user/group identities")

        # TODO: ensure input value is a bona fide user or group name

        try:
            prec = self.svc.get_record(self._id)
        except dbio.NotAuthorized as ex:
            return self.send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return self.send_error_resp(404, "ID not found",
                                        "Record with requested identifier not found", self._id)

        if path in [dbio.ACLs.READ, dbio.ACLs.WRITE, dbio.ACLs.ADMIN, dbio.ACLs.DELETE]:
            try:
                prec.acls.grant_perm_to(path, *identities)
                prec.save()
                return self.send_json(prec.to_dict().get('acls', {}).get(path, []))
            except dbio.NotAuthorized as ex:
                return self.send_unauthorized()

        return self.send_error_resp(405, "PATCH not allowed on this permission type",
                                    "Updating specified permission is not allowed")
        
    def do_DELETE(self, path):
        """
        remove an identity from an ACL.  This handles DELETE ID/acls/PERM/USER; `path` should 
        be set to PERM/USER.
        """
        if path is None:
            path = ""

        path = path.strip('/')
        if not path or '/' not in path:
            return self.send_error_resp(405, "DELETE not allowed on permission type",
                                        "DELETE requires a group or user id after the permission type")
        parts = path.split('/', 1)

        # TODO: ensure user value is a bona fide user or group name

        # retrieve the record
        try:
            prec = self.svc.get_record(self._id)
        except dbio.NotAuthorized as ex:
            return self.send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return self.send_error_resp(404, "ID not found",
                                        "Record with requested identifier not found", self._id)

        if parts[0] in [dbio.ACLs.READ, dbio.ACLs.WRITE, dbio.ACLs.ADMIN, dbio.ACLs.DELETE]:
            # remove the identity from the ACL
            try:
                prec.acls.revoke_perm_from(parts[0], parts[1])
                prec.save()
                return self.send_ok(message="ID removed")
            except dbio.NotAuthorized as ex:
                return self.send_unauthorized()

        return self.send_error_resp(405, "DELETE not allowed on this permission type",
                                    "Updating specified permission is not allowed")

class ProjectStatusHandler(ProjectRecordHandler):
    """
    handle status requests and actions
    """
    _requestable_actions = [ ProjectService.STATUS_ACTION_FINALIZE, ProjectService.STATUS_ACTION_SUBMIT ] 

    def __init__(self, service: ProjectService, svcapp: ServiceApp, wsgienv: dict, start_resp: Callable, 
                 who: Agent, id: str, datapath: str="", config: dict=None, log: Logger=None):
        """
        Initialize this data request handler with the request particulars.  This constructor is called 
        by the webs service ServiceApp in charge of the project record interface.  

        :param ProjectService service:  the ProjectService instance to use to get and update
                               the project data.
        :param ServiceApp svcapp:  the web service ServiceApp receiving the request and calling this 
                               constructor
        :param dict  wsgienv:  the WSGI request context dictionary
        :param Callable start_resp:  the WSGI start-response function used to send the response
        :param Agent     who:  the authenticated user making the request.  
        :param str        id:  the ID of the project record being requested
        :param str  permpath:  the subpath pointing to a particular permission ACL; it can either be
                               simply a permission name, PERM (e.g. "read"), or a p
                               this will be a '/'-delimited identifier pointing to an object property 
                               within the data object.  This will be an empty string if the full data 
                               object is requested.
        :param dict   config:  the handler's configuration; if not provided, the inherited constructor
                               will extract the configuration from ``svcapp``.  Normally, the constructor
                               is called without this parameter.
        :param Logger    log:  the logger to use within this handler; if not provided (typical), the 
                               logger attached to the ServiceApp will be used.  
        """
        super(ProjectStatusHandler, self).__init__(service, svcapp, wsgienv, start_resp, who, datapath,
                                                   config, log)
        self._id = id
        if not id:
            # programming error
            raise ValueError("Missing ProjectRecord id")

    def do_OPTIONS(self, path):
        return self.send_options(["GET", "PUT", "PATCH"])

    def do_GET(self, path, ashead=False):
        """
        return the status object in response to a GET request
        """
        try:
            out = self.svc.get_status(self._id)
        except dbio.NotAuthorized as ex:
            return self.send_unauthorized()
        except dbio.ObjectNotFound as ex:
            if ex.record_part:
                return self.send_error_resp(404, "Data property not found",
                                            "No data found at requested property", self._id, ashead=ashead)
            return self.send_error_resp(404, "ID not found",
                                        "Record with requested identifier not found", self._id, ashead=ashead)

        if path == "state":
            out = out.state
        elif path == "action":
            out = out.action
        elif path == "message":
            out = out.action
        elif path:
            return self.send_error_resp(404, "Status property not accessible",
                                        "Requested status property is not accessible", self._id, ashead=ashead)
            
        return self.send_json(out.to_dict(), ashead=ashead)

    def do_PUT(self, path):
        """
        request an action to be applied to the record 
        """
        path = path.strip('/')
        if path:
            return self.send_error_resp(405, "PUT not allowed", "PUT is not allowed on a status property")
                                        
        try:
            req = self.get_json_body()
        except self.FatalError as ex:
            return self.send_fatal_error(ex)

        if not req.get('action'):
            return self.send_error_resp(400, "Invalid input: missing action property"
                                        "Input record is missing required action property")
        return self._apply_action(req['action'], req.get('message'))

    def do_PATCH(self, path):
        """
        request an action to be applied to the record or just update the associated message
        """
        path = path.strip('/')
        if path:
            return self.send_error_resp(405, "PATCH not allowed", "PATCH is not allowed on a status property")
                                        
        try:
            req = self.get_json_body()
        except self.FatalError as ex:
            return self.send_fatal_error(ex)

        # if action is not set, the message will just get updated.
        return self._apply_action(req.get('action'), req.get('message'))
        
    def _apply_action(self, action, message=None):
        try:
            if message and action is None:
                stat = self.svc.update_status_message(self._id, message)
            elif action == 'finalize':
                stat = self.svc.finalize(self._id, message)
            elif action == 'submit':
                stat = self.svc.submit(self._id, message)
            else:
                return self.send_error_resp(400, "Unrecognized action",
                                            "Unrecognized action requested")
        except dbio.NotAuthorized as ex:
            return self.send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return self.send_error_resp(404, "ID not found",
                                        "Record with requested identifier not found", self._id)
        except dbio.InvalidUpdate as ex:
            return self.send_error_resp(400, "Request creates an invalid record", ex.format_errors())
        except dbio.NotEditable as ex:
            return self.send_error_resp(409, "Not in editable state", "Record is not in state=edit or ready")
        except dbio.NotSubmitable as ex:
            return self.send_error_resp(409, "Not in editable state", "Record is not in state=edit or ready")

        return self.send_json(stat.to_dict())

        
        
class MIDASProjectApp(ServiceApp):
    """
    a base web app for an interface handling project record.
    """
    _selection_handler = ProjectSelectionHandler
    _update_handler = ProjectHandler
    _name_update_handler = ProjectNameHandler
    _data_update_handler = ProjectDataHandler
    _acls_update_handler = ProjectACLsHandler
    _info_update_handler = ProjectInfoHandler
    _status_handler = ProjectStatusHandler
    # _history_handler = ProjectHistoryHandler

    def __init__(self, service_factory: ProjectServiceFactory, log: Logger, config: dict={}):
        super(MIDASProjectApp, self).__init__(service_factory.project_type, log, config)
        self.svcfact = service_factory

    def create_handler(self, env: dict, start_resp: Callable, path: str, who: Agent) -> Handler:
        """
        return a handler instance to handle a particular request to a path
        :param Mapping env:  the WSGI environment containing the request
        :param Callable start_resp:  the start_resp function to use initiate the response
        :param str    path:  the path to the resource being requested.  This is usually 
                             relative to a parent path that this ServiceApp is configured to 
                             handle.  
        :param Agent   who:  the authenticated user agent making the request
        """

        # create a service on attached to the user
        service = self.svcfact.create_service_for(who)

        # now parse the requested path; we have different handlers for different types of paths
        path = path.strip('/')
        idattrpart = path.split('/', 2)
        if len(idattrpart) < 2:
            if not idattrpart[0]:
                # path is empty: this is used to list all available projects or create a new one
                return self._selection_handler(service, self, env, start_resp, who)
            elif idattrpart[0][0] == ":":
                # path starts with ":" (e.g. ":selected"); this indicates a search resource
                return self._selection_handler(service, self, env, start_resp, who, idattrpart[0])
            else:
                # path is just an ID: 
                return self._update_handler(service, self, env, start_resp, who, idattrpart[0])
            
        elif idattrpart[1] == "name" or idattrpart[1] == "owner":
            # path=ID/name: get/change the owner or the mnemonic name of record ID
            return self._name_update_handler(service, self, env, start_resp, who, idattrpart[0], idattrpart[1])
        elif idattrpart[1] == "data":
            # path=ID/data[/...]: get/change the content of record ID
            if len(idattrpart) == 2:
                idattrpart.append("")
            return self._data_update_handler(service, self, env, start_resp, who,
                                             idattrpart[0], idattrpart[2])
        elif idattrpart[1] == "status":
            # path=ID/status: get or act on the status of the record
            if len(idattrpart) == 2:
                idattrpart.append("")
            return self._status_handler(service, self, env, start_resp, who, idattrpart[0], idattrpart[2])
        elif idattrpart[1] == "acls":
            # path=ID/acls: get/update the access control on record ID
            if len(idattrpart) < 3:
                idattrpart.append("")
            return self._acls_update_handler(service, self, env, start_resp, who,
                                             idattrpart[0], idattrpart[2])

        # the fallback handler will return some arbitrary part of the record
        if len(idattrpart) > 2:
            idattrpart[1] = "/".join(idattrpart[1:])
        return self._info_update_handler(service, self, env, start_resp, who,
                                         idattrpart[0], idattrpart[1])

    class _factory:
        def __init__(self, project_coll):
            self._prjcoll = project_coll
        def __call__(self, dbcli_factory: dbio.DBClientFactory, log: Logger, config: dict={},
                     project_coll: str=None):
            if not project_coll:
                project_coll = self._prjcoll
            service_factory = ProjectServiceFactory(project_coll, dbcli_factory, config, log)
            return MIDASProjectApp(service_factory, log, config)

    @classmethod
    def factory_for(cls, project_coll):
        """
        return a factory function that instantiates this class connected to the given DBIO collection.  
        This is intended for plugging this ServiceApp into the main WSGI app as is.  
        :param str project_coll:  the name of the DBIO project collection to use for creating and 
                                  updating project records.
        """
        return cls._factory(project_coll)

        

    

        
