"""
A web service interface to various MIDAS project records.  

A _project record_ is a persistable record that is compliant with the MIDAS Common Database project 
data model, where examples of "project record" types include DMP records and data publication drafts.
The :py:class:`MIDASProjectApp` encapsulates the handling of requests to create and manipulate project 
records.  If desired, this class can be specialized for a particular project type, and the easiest way 
to do that is by sub-classing the :py:class:`~nistoar.midas.dbio.wsgi.project.ProjectRecordBroker` and 
passing that class to the :py:class:`MIDASProjectApp` constructor.  This is because the 
:py:class:`~nistoar.midas.dbio.wsgi.project.ProjectRecordBroker` class isolates the business logic for 
retrieving and manipulating project records.  
"""
from logging import Logger
from collections import OrderedDict
from collections.abc import Mapping, Sequence

from nistoar.pdr.publish import SubApp, Handler   # use same sevice infrastructure as the publishing service
from nistoar.pdr.utils.webrecord import WebRecorder
from .. import dbio
from ..dbio import ProjectRecord
from .base import DBIOHandler
from .broker import ProjectRecordBroker


class MIDASProjectApp(SubApp):
    """
    a base web app for an interface handling project record
    """
    def_project_broker_class = ProjectRecordBroker

    def __init__(self, servicetype, log: Logger, dbcli_factory: DBClientFactory,
                 foruser: str, config: dict={}, project_broker_cls=None):
        super(MIDASApp, self).__init__(servicetype, log, config)

        ## create dbio client from config
        self._prjbrkr_cls = self.cfg.get('project_handler_class', self.def_project_handler_class)
        self._dbfact = dbcli_factory

    def create_handler(self, env: dict, start_resp: Callable, path: str, who: PubAgent) -> Handler:
        """
        return a handler instance to handle a particular request to a path
        :param Mapping env:  the WSGI environment containing the request
        :param Callable start_resp:  the start_resp function to use initiate the response
        :param str path:     the path to the resource being requested.  This is usually 
                             relative to a parent path that this SubApp is configured to 
                             handle.  
        :param PubAgent who  the authenticated user agent making the request
        """

        # set up dbio client and the request handler that will mediate with it
        dbcli = self._dbfact.create_client(self._name, who.actor)
        pbroker = self._prjbrkr_cls(dbcli, self.cfg, env, self.log)

        # now parse the requested path; we have different handlers for different types of paths
        idattrpart = path.split('/', 2)
        if len(idattrpart) < 2:
            if not idattrpart:
                # path is empty: this is used to list all available projects or create a new one
                return ProjectSelectionHandler(pbroker, self, env, start_resp, who)
            else:
                # path is just an ID: 
                return ProjectHandler(pbroker, self, env, start_resp, who, idattrpart[0])
            
        elif idattrpart[1] == "name":
            # path=ID/name: get/change the mnumonic name of record ID
            return ProjectNameHandler(pbroker, self, env, start_resp, who, idattrpart[0])
        elif idattrpart[1] == "data":
            # path=ID/data[/...]: get/change the content of record ID
            if len(idattrpart) == 2:
                idattrpart.append("")
            return ProjectDataHandler(pbroker, self, env, start_resp, who, idattrpart[0], idattrpart[2])
        elif idattrpart[1] == "acls":
            # path=ID/acls: get/update the access control on record ID
            if len(idattrpart) < 3:
                idattrpart.append(None)
            return ProjectACLsHandler(self, env, start_resp, who, idattrpart[0], idattrpart[2])

        # the fallback handler will return some arbitrary part of the record
        if len(idattrpart) > 2:
            idattrpart[1] = "/".join(idattrpart[1:])
        return ProjectInfoHandler(self, env, start_resp, who, idattrpart[0], idattrpart[1])

class ProjectRecordHandler(DBIOHandler):
    """
    base handler class for all requests on project records.  This base allows requests to be funneled 
    through a :py:class:`~nistoar.midas.dbio.wsgi.project.ProjectRecordBroker` instance.  
    """
    def __init__(self, broker: ProjectRecordBroker, subapp: SubApp, wsgienv: dict, start_resp: Callable, 
                 who: PubAgent, path: str="", config: dict=None, log: Logger=None):
        """
        Initialize this handler with the request particulars.  

        :param ProjectRecordBroker broker:  the ProjectRecordBroker instance to use to get and update
                               the project data through.
        :param SubApp subapp:  the web service SubApp receiving the request and calling this constructor
        :param dict  wsgienv:  the WSGI request context dictionary
        :param Callable start_resp:  the WSGI start-response function used to send the response
        :param PubAgent  who:  the authenticated user making the request.  
        :param str      path:  the relative path to be handled by this handler; typically, some starting 
                               portion of the original request path has been stripped away to handle 
                               produce this value.
        :param dict   config:  the handler's configuration; if not provided, the inherited constructor
                               will extract the configuration from `subapp`.  Normally, the constructor
                               is called without this parameter.
        :param Logger    log:  the logger to use within this handler; if not provided (typical), the 
                               logger attached to the SubApp will be used.  
        """

        super(ProjectHandler, self).__init__(subapp, wsgienv, start_resp, who, path, config, log)
        self._pbrkr =  broker

class ProjectInfoHandler(ProjectRecordHandler):
    """
    handle retrieval of simple parts of a project record
    """

    def __init__(self, broker: ProjectRecordBroker, subapp: SubApp, wsgienv: dict, start_resp: Callable, 
                 who: PubAgent, id: str, attribute: str, config: dict={}, log: Logger=None):
        """
        Initialize this handler with the request particulars.  This constructor is called 
        by the webs service SubApp.  

        :param ProjectRecordBroker broker:  the ProjectRecordBroker instance to use to get and update
                               the project data through.
        :param SubApp subapp:  the web service SubApp receiving the request and calling this constructor
        :param dict  wsgienv:  the WSGI request context dictionary
        :param Callable start_resp:  the WSGI start-response function used to send the response
        :param PubAgent  who:  the authenticated user making the request.  
        :param str        id:  the ID of the project record being requested
        :param dict   config:  the handler's configuration; if not provided, the inherited constructor
                               will extract the configuration from `subapp`.  Normally, the constructor
                               is called without this parameter.
        :param Logger    log:  the logger to use within this handler; if not provided (typical), the 
                               logger attached to the SubApp will be used.  
        """

        super(ProjectInfoHandler, self).__init__(broker, subapp, attribute, wsgienv, start_resp,
                                                 who, attribute, config, self._app.log)
        self._id = id
        if not id:
            # programming error
            raise ValueError("Missing ProjectRecord id")

    def do_GET(self, path, ashead=False):
        if not path:
            # programming error
            raise ValueError("Missing ProjectRecord attribute")
        try:
            prec = self._pbrkr.get_record(self._id)
        except dbio.NotAuthorized as ex:
            return send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return send_error_resp(404, "ID not found",
                                   "Record with requested identifier not found", self._id, ashead=ashead)

        parts = path.split('/')
        data = prec.to_dict()
        while len(parts) > 0:
            attr = parts.pop(0)
            if not isinstance(data, Mapping) or attr not in data:
                return send_error(404, "Record attribute not available",
                                  "Requested record attribute not found", self._id, ashead=ashead)
            data = data[attr]

        return send_json(data, ashead=ashead)

class ProjectNameHandler(ProjectRecordHandler):
    """
    handle retrieval/update of a project records mnumonic name
    """

    def __init__(self, broker: ProjectRecordBroker, subapp: SubApp, wsgienv: dict, start_resp: Callable,
                 who: PubAgent, id: str, config: dict={}, log: Logger=None):
        """
        Initialize this handler with the request particulars.  This constructor is called 
        by the webs service SubApp.  

        :param ProjectRecordBroker broker:  the ProjectRecordBroker instance to use to get and update
                               the project data through.
        :param SubApp subapp:  the web service SubApp receiving the request and calling this constructor
        :param dict  wsgienv:  the WSGI request context dictionary
        :param Callable start_resp:  the WSGI start-response function used to send the response
        :param PubAgent  who:  the authenticated user making the request.  
        :param str        id:  the ID of the project record being requested
        :param dict   config:  the handler's configuration; if not provided, the inherited constructor
                               will extract the configuration from `subapp`.  Normally, the constructor
                               is called without this parameter.
        :param Logger    log:  the logger to use within this handler; if not provided (typical), the 
                               logger attached to the SubApp will be used.  
        """
        
        super(ProjectNameHandler, self).__init__(broker, subapp, wsgienv, start_resp, who, "", config, log)
                                                   
        self._id = id
        if not id:
            # programming error
            raise ValueError("Missing ProjectRecord id")

    def do_GET(self, path, ashead=False):
        try:
            prec = self._pbrkr.get_record(self._id)
        except dbio.NotAuthorized as ex:
            return send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return send_error_resp(404, "ID not found",
                                   "Record with requested identifier not found", self._id, ashead=ashead)

        return self.send_json(prec.name)

    def do_PUT(self, path):
        try:
            name = self.get_json_body()
        except self.FatalError as ex:
            return self.send_fatal_error(ex)

        try:
            prec = self._dbcli.get_record_for(self._id)
            prec.name = name
            if not prec.authorized(dbio.ACLs.ADMIN):
                raise dbio.NotAuthorized(self._dbcli.user_id, "change record name")
            prec.save()
        except dbio.NotAuthorized as ex:
            return send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return send_error_resp(404, "ID not found",
                                   "Record with requested identifier not found", self._id, ashead=ashead)

class ProjectDataHandler(ProjectRecordHandler):
    """
    handle retrieval/update of a project record's data content
    """

    def __init__(self, broker: ProjectRecordBroker, subapp: SubApp, wsgienv: dict, start_resp: Callable, 
                 who: PubAgent, id: str, datapath: str, config: dict=None, log: Logger=None):
        """
        Initialize this data request handler with the request particulars.  This constructor is called 
        by the webs service SubApp in charge of the project record interface.  

        :param ProjectRecordBroker broker:  the ProjectRecordBroker instance to use to get and update
                               the project data through.
        :param SubApp subapp:  the web service SubApp receiving the request and calling this constructor
        :param dict  wsgienv:  the WSGI request context dictionary
        :param Callable start_resp:  the WSGI start-response function used to send the response
        :param PubAgent  who:  the authenticated user making the request.  
        :param str        id:  the ID of the project record being requested
        :param str  datapath:  the subpath pointing to a particular piece of the project record's data;
                               this will be a '/'-delimited identifier pointing to an object property 
                               within the data object.  This will be an empty string if the full data 
                               object is requested.
        :param dict   config:  the handler's configuration; if not provided, the inherited constructor
                               will extract the configuration from `subapp`.  Normally, the constructor
                               is called without this parameter.
        :param Logger    log:  the logger to use within this handler; if not provided (typical), the 
                               logger attached to the SubApp will be used.  
        """
        super(ProjectDataHandler, self).__init__(broker, subapp, wsgienv, start_resp, who, datapath,
                                                 config, log)
        self._id = id
        if not id:
            # programming error
            raise ValueError("Missing ProjectRecord id")

    def do_GET(self, path, ashead=False):
        """
        respond to a GET request
        :param str path:  a path to the portion of the data to get.  This is the same as the `datapath`
                          given to the handler constructor.  This will be an empty string if the full
                          data object is requested.
        :param bool ashead:  if True, the request is actually a HEAD request for the data
        """
        try:
            out = self.get_data(self._id, part)
        except dbio.NotAuthorized as ex:
            return send_unauthorized()
        except dbio.ObjectNotFound as ex:
            if ex.record_part:
                return send_error_resp(404, "Data property not found",
                                       "No data found at requested property", self._id, ashead=ashead)
            return send_error_resp(404, "ID not found",
                                   "Record with requested identifier not found", self._id, ashead=ashead)
        return self.send_json(out)

    def do_PUT(self, path):
        try:
            newdata = self.get_json_body()
        except self.FatalError as ex:
            return self.send_fatal_error(ex)

        try:
            return self.replace_data(self._id, newdata, path)  
        except dbio.NotAuthorized as ex:
            return send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return send_error_resp(404, "ID not found",
                                   "Record with requested identifier not found", self._id, ashead=ashead)
        except InvalidUpdate as ex:
            return send_error_resp(400, "Invalid Input Data", str(ex))
        except PartNotAccessible as ex:
            return send_error_resp(405, "Data part not updatable",
                                   "Requested part of data cannot be updated")

    def do_PATCH(self, path):
        try:
            newdata = self.get_json_body()
        except self.FatalError as ex:
            return self.send_fatal_error(ex)

        try:
            return self.update_data(self._id, newdata, path)
        except dbio.NotAuthorized as ex:
            return send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return send_error_resp(404, "ID not found",
                                   "Record with requested identifier not found", self._id, ashead=ashead)
        except InvalidUpdate as ex:
            return send_error_resp(400, "Invalid Input Data", str(ex))
        except PartNotAccessible as ex:
            return send_error_resp(405, "Data part not updatable",
                                   "Requested part of data cannot be updated")


class ProjectSelectionHandler(ProjectRecordHandler):
    """
    handle collection-level access searching for project records and creating new ones
    """

    def __init__(self, broker: ProjectRecordBroker, subapp: SubApp, wsgienv: dict, start_resp: Callable,
                 who: PubAgent, config: dict=None, log: Logger=None):
        """
        Initialize this record request handler with the request particulars.  This constructor is called 
        by the webs service SubApp in charge of the project record interface.  

        :param SubApp subapp:  the web service SubApp receiving the request and calling this constructor
        :param dict  wsgienv:  the WSGI request context dictionary
        :param Callable start_resp:  the WSGI start-response function used to send the response
        :param PubAgent  who:  the authenticated user making the request.  
        :param dict   config:  the handler's configuration; if not provided, the inherited constructor
                               will extract the configuration from `subapp`.  Normally, the constructor
                               is called without this parameter.
        :param Logger    log:  the logger to use within this handler; if not provided (typical), the 
                               logger attached to the SubApp will be used.  
        """
        super(ProjectSelectionHandler, self).__init__(broker, subapp, wsgienv, start_resp, who, "",
                                                      config, log)

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
            perms = [ dbio.ACLs.READWRITE ]

        # sort the results by the best permission type permitted
        selected = OrderedDict()
        for rec in self._dbcli.select_records(perms):
            if rec.owner == _dbcli.user_id:
                rec['maxperm'] = "owner"
            elif rec.authorized(dbio.ACLs.ADMIN):
                rec['maxperm'] = dbio.ACLs.ADMIN
            elif rec.authorized(dbio.ACLs.WRITE):
                rec['maxperm'] = dbio.ACLs.WRITE
            else:
                rec['maxperm'] = dbio.ACLs.READ

            if rec['perm'] not in selected:
                selected[rec['perm']] = []
            selected[rec['perm']].append(rec)

        # order the matched records based on best permissions
        out = []
        for perm in ["owner", dbio.ACLs.ADMIN, dbio.ACLs.WRITE, dbio.ACLs.READ]:
            for rec in selected.get(perm, []):
                out.append(rec.to_dict())

        return send_json(out, ashead=ashead)

    def do_POST(self, path):
        """
        create a new project record given some initial data
        """
        try:
            newdata = self.get_json_body()
        except self.FatalError as ex:
            return self.send_fatal_error(ex)

        if not newdata['name']:
            return send_error_resp(400, "Bad POST input", "No mneumonic name provided")

        try:
            prec = self.create_record(newdata['name'], newdata.get("data"), newdata.get("meta"))
        except dbio.NotAuthorized as ex:
            return send_unauthorized()
        except dbio.AlreadyExists as ex:
            return send_error_resp(400, "Name already in use", str(ex))
    
        return send_json(prec.to_dict())


class ProjectACLsHandler(ProjectRecordHandler):
    """
    handle retrieval/update of a project record's data content
    """

    def __init__(self, broker: ProjectRecordBroker, subapp: SubApp, wsgienv: dict, start_resp: Callable, 
                 who: PubAgent, id: str, datapath: str, config: dict=None, log: Logger=None):
        """
        Initialize this data request handler with the request particulars.  This constructor is called 
        by the webs service SubApp in charge of the project record interface.  

        :param ProjectRecordBroker broker:  the ProjectRecordBroker instance to use to get and update
                               the project data through.
        :param SubApp subapp:  the web service SubApp receiving the request and calling this constructor
        :param dict  wsgienv:  the WSGI request context dictionary
        :param Callable start_resp:  the WSGI start-response function used to send the response
        :param PubAgent  who:  the authenticated user making the request.  
        :param str        id:  the ID of the project record being requested
        :param str  datapath:  the subpath pointing to a particular piece of the project record's data;
                               this will be a '/'-delimited identifier pointing to an object property 
                               within the data object.  This will be an empty string if the full data 
                               object is requested.
        :param dict   config:  the handler's configuration; if not provided, the inherited constructor
                               will extract the configuration from `subapp`.  Normally, the constructor
                               is called without this parameter.
        :param Logger    log:  the logger to use within this handler; if not provided (typical), the 
                               logger attached to the SubApp will be used.  
        """
        super(ProjectDataHandler, self).__init__(broker, subapp, wsgienv, start_resp, who, datapath,
                                                 config, log)
        self._id = id
        if not id:
            # programming error
            raise ValueError("Missing ProjectRecord id")

        
    def do_GET(self, path, ashead=False):
        try:
            prec = self._pbrkr.get_record(self._id)
        except dbio.NotAuthorized as ex:
            return send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return send_error_resp(404, "ID not found",
                                   "Record with requested identifier not found", self._id, ashead=ashead)

        recd = prec.to_dict()
        if not path:
            return self.send_json(recd.get('acls', {}))

        path = path.strip('/')
        parts = path.split('/', 1)
        acl = recd.get('acls', {}).get(parts[0])
        if acl is None:
            if parts[0] not in [dbio.ACLs.READ, dbio.ACLs.WRITE, dbio.ACLs.ADMIN, dbio.ACLs.DELETE]:
                return self.send_error_resp(404, "Unsupported ACL type", "Request for unsupported ACL type")
            acl = []

        if len(parts) < 2:
            return self.send_json(acl)

        return self.send_json(parts[1] in acl)

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
            prec = self._pbrkr.get_record(self._id)
        except dbio.NotAuthorized as ex:
            return send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return send_error_resp(404, "ID not found",
                                   "Record with requested identifier not found", self._id, ashead=ashead)

        if path in [dbio.ACLs.READ, dbio.ACLs.WRITE, dbio.ACLs.ADMIN, dbio.ACLs.DELETE]:
            pres.acls.grant_perm_to(path, identity)
            pres.save()
            return send_json(prec.to_dict().get('acls', {}))

        return self.send_error_resp(405, "POST not allowed on this permission type",
                                    "Updating specified permission is not allowed")
        
    def do_PUT(self, path):
        """
        replace the list of identities in a particular ACL.  This handles PUT ID/acls/PERM; 
        `path` should be set to PERM.  Note that previously set identities are removed. 
        """
        try:
            identities = self.get_json_body()
        except self.FatalError as ex:
            return self.send_fatal_error(ex)

        # make sure a permission type, and only a permission type, is specified
        path = path.strip('/')
        if not path or '/' in path:
            return self.send_error_resp(405, "PUT not allowed", "Unable set ACL membership")

        if isinstance(identities, str):
            identities = [identities]
        if not isinstance(identity, list):
            return self.send_error_resp(400, "Wrong input data type"
                                        "Input data is not a string providing a user/group list")

        # TODO: ensure input value is a bona fide user or group name

        try:
            prec = self._pbrkr.get_record(self._id)
        except dbio.NotAuthorized as ex:
            return send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return send_error_resp(404, "ID not found",
                                   "Record with requested identifier not found", self._id, ashead=ashead)

        if path in [dbio.ACLs.READ, dbio.ACLs.WRITE, dbio.ACLs.ADMIN, dbio.ACLs.DELETE]:
            try:
                pres.acls.revoke_perm_for_alll(path)
                pres.acls.grant_perm_to(path, *identities)
                pres.save()
                return send_json(prec.to_dict().get('acls', {}))
            except dbio.NotAuthorized as ex:
                return send_unauthorized()

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
        if not isinstance(identity, list):
            return self.send_error_resp(400, "Wrong input data type"
                                        "Input data is not a list of user/group identities")

        # TODO: ensure input value is a bona fide user or group name

        try:
            prec = self._pbrkr.get_record(self._id)
        except dbio.NotAuthorized as ex:
            return send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return send_error_resp(404, "ID not found",
                                   "Record with requested identifier not found", self._id, ashead=ashead)

        if path in in [dbio.ACLs.READ, dbio.ACLs.WRITE, dbio.ACLs.ADMIN, dbio.ACLs.DELETE]:
            try:
                pres.acls.grant_perm_to(path, *identities)
                pres.save()
                return send_json(prec.to_dict().get('acls', {}))
            except dbio.NotAuthorized as ex:
                return send_unauthorized()

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
            prec = self._pbrkr.get_record(self._id)
        except dbio.NotAuthorized as ex:
            return send_unauthorized()
        except dbio.ObjectNotFound as ex:
            return send_error_resp(404, "ID not found",
                                   "Record with requested identifier not found", self._id, ashead=ashead)

        if path in in [dbio.ACLs.READ, dbio.ACLs.WRITE, dbio.ACLs.ADMIN, dbio.ACLs.DELETE]:
            # remove the identity from the ACL
            try:
                pres.acls.revoke_perm_from(parts[0], parts[1])
                pres.save()
                return send_ok()
            except dbio.NotAuthorized as ex:
                return send_unauthorized()

        return self.send_error_resp(405, "DELETE not allowed on this permission type",
                                    "Updating specified permission is not allowed")
        
        
        

    

        
