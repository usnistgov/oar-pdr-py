"""
A WSGI application providing the MIDAS-application layer to the file manager, implemented 
with Flask

The :py:func:`create_app` function instantiates the WSGI application that can be provided
to a WSGI server (e.g. uWSGI).  This function requires a configuration dictionary and 
looks for the following configuration parameters:

``name``
   (str) _optional_.  a name to the flask app; it is also used as the root name of 
   the default logger.

``flask``
   (dict) _required_.  The paramters specific to the flask-specific; this includes 
   ``secret_key`` and ``debug``.

``service``
   (dict) _required_.  The parameters for configuring the underlying 
   ``MIDASFileManagerService``;

``allowed_service_users``
   (list or str) _optional_.  a list of client identities that are authorized to 
   use this service.  When x509 certificates are used to authenticate, the client's 
   CN must appear in this list.

``debug``
   (bool) _optional_.  If True, debugging will be turned onin the Flask infrastructure.
"""
import logging
from logging import Logger
from functools import wraps
from abc import ABC, abstractmethod
from collections import OrderedDict
from collections.abc import Mapping
from typing import List

from flask import Flask, request, current_app, jsonify, make_response, Blueprint
from flask_restful import Api, Resource, url_for

from nistoar.base.config import configure_log, ConfigurationException
from nistoar.midas.dap.fm import service as fmservice
from nistoar.midas.dap.fm.exceptions import *

class AuthHandler(ABC):
    """
    an abstract base class for authentication decorators
    """

    @abstractmethod
    def authenticate(self):
        """
        return the identifer for the authenticated user if the user successfully authenticated
        """
        raise NotImplemented()

    @abstractmethod
    def authorize(self, clientid):
        """
        return True if the given user is authorized to access the requested resource
        """
        raise NotImplemented()

    def authentication_required(self, f):
        def wrapper(*args, **kw):
            if self.authenticate():
                return f(*args, **kw)
            return make_error_response("Not authenticated", 401)
        return wrapper

    def authorization_required(self, f):
        def wrapper(*args, **kw):
            if self.authorize(self.authenticate()):
                return f(*args, **kw)
            return make_error_response("Not authorized", 401)
        return wrapper

class AllowedBaseAuthHandler(AuthHandler):
    def __init__(self, allowed_users: List[str]):
        self.allowed = allowed_users

    def authorize(self, clientid):
        return clientid in self.allowed


class DelegatedX509AuthHandler(AllowedBaseAuthHandler):
    """
    An AuthHandler that relies on an reverse proxy service to handle parsing and validation of 
    a client X.509 certificate.
    """

    def authenticate(self):
        """
        return the identifer for the authenticated user if the user successfully authenticated.
        In this implementation, the identifier returned is the Common Name (CN) of the user
        """
        if request.headers.get("X_CLIENT_VERIFY") != "SUCCESS":
            return None
        return request.headers.get("X_CLIENT_CN")

class NoAuthNeededAuthHandler(AllowedBaseAuthHandler):
    """
    An AuthHandler that requires no credentials from client and sets a default user
    """

    def __init__(self, assume_user: str):
        self.user = assume_user
        super(NoAuthNeededAuthHandler, self).__init__([self.user])

    def authenticate(self):
        return self.user


auth = DelegatedX509AuthHandler(['admin'])
# auth = NoAuthNeededAuthHandler('admin')

def create_app(config: Mapping, fmsvc: fmservice.MIDASFileManagerService=None, 
               log: Logger=None):
    """
    create the Flask application

    :param dict config:  the configuration data for the app
    :param log  Logger:  the logger to use (optional)
    :param fmsvc MIDASFileManagerService:  the file-manager service to use; if not
             provided, one will be created
    """
    missing = []
    for param in "flask service".split():
        if param not in config:
            missing.append(param)
    if not config.get('flask', {}).get('secret_key') and \
       not config.get('flask', {}).get('SECRET_KEY'):
        missing.append("flask.secret_key")
    if missing:
        raise ConfigurationException("Missing required config parameters: " +
                                     ", ".join(missing))
    if not isinstance(config.get('allowed_service_endpoints', []), list):
        raise ConfigurationException("Config param, allowed_service_endpoints, not a list: " +
                                     str(type(config.get('allowed_service_endpoints'))))
    if not isinstance(config.get('allowed_service_users', []), list):
        raise ConfigurationException("Config param, allowed_service_users, not a list: " +
                                     str(type(config.get('allowed_service_users'))))

    if config.get('debug'):
        config['flask']['DEBUG'] = True
    config.update(config.get('flask'))
    del config['flask']

    app = Flask(__name__)
    app.name = config.get('name', 'midasfm')
    if not log:
        log = logging.getLogger(app.name)
    app.logger = log
    app.config.update(config)
    if config.get('allowed_service_users'):
        auth.allowed_users = config['allowed_service_users']

    if not fmsvc:
        fmsvc = fmservice.MIDASFileManagerService(config.get('service', {}), app.logger)
    app.service = fmsvc

    app.register_blueprint(SpacesBlueprint(), url_prefix=config.get('endpoint_path', '/mfm1'))

    return app

def make_error_content(message: str, code: int=0, intent: str=None):
    out = OrderedDict([("message", message)])
    if code > 0:
        out['code'] = code
    if intent:
        out['intent'] = intent
    return out

def make_error_response(message: str, code: int, intent: str=None):
    out = make_error_content(message, code, intent)
    return out, code

def server_error(message: str=None, intent: str=None, code: int=500):
    if not message:
        message = "Internal server error"
    return make_error_response(message, code, intent)

def not_found(message: str, intent: str=None, code: int=404):
    return make_error_response(message, code, intent)

def bad_input(message: str, intent: str=None, code: int=400):
    return make_error_response(message, code, intent)

class SpacesResource(Resource):
    """
    the ``/spaces`` endpoint handler
    """
    @auth.authorization_required
    def get(self):
        """
        return a list of available spaces
        """
        svc = current_app.service
        return svc.space_ids()

    @auth.authorization_required
    def head(self):
        """
        test that this service is available
        """
        if current_app.service.test():
            return "", 200
        return "", 500

    @auth.authorization_required
    def post(self):
        """
        create a new file manager space.  The input is a JSON object that must contain two properties:
        
        ``id``
             the identifier to assign to the space
        ``foruser``
             the identifier for the user that will need full access permissions on the space
        """
        svc = current_app.service
        rec = request.json
        if not rec:
            return {"message": "Create space request missing input data" }, 400

        if "id" not in rec:
            return {"message": "Create space request missing 'id' property" }, 400
        if "for_user" not in rec:
            print("create_space: "+str(rec))
            return {"message": "Create space request missing 'for_user' property" }, 400

        try:
            sp = svc.create_space_for(rec['id'], rec['for_user'])
            return sp.summarize()

        except FileManagerOpConflict as ex:
            return make_error_response(str(ex), 409, "creating a new space")
            return {"message": str(ex)}, 405
        except FileManagerException as ex:
            current_app.logger.error(str(ex))
            return server_error(intent="creating a new space")
        except Exception as ex:
            current_app.logger.exception(ex)
            return server_error(intent="creating a new space")

class SpaceResource(Resource):
    """
    access to a particular space (/spaces/[id])
    """
    @auth.authorization_required
    def get(self, id):
        """
        return some summary information about the identified space
        """
        svc = current_app.service
        try:
            sp = svc.get_space(id)
            return sp.summarize()

        except FileManagerResourceNotFound as ex:
            return not_found("Requested space not found", "summarizing a space")
        except Exception as ex:
            self.log.exception(ex)
            return server_error(intent="summarizing a space")

    @auth.authorization_required
    def head(self, id):
        """
        test for the existence of the identified space
        """
        svc = current_app.service
        try:
            if not svc.space_exists(id):
                return "", 404
            return "", 200
        except Exception as ex:
            self.log.exception(ex)
            return "", 500

    @auth.authorization_required
    def delete(self, id):
        """
        delete the file manager space having the given ID.  404 is returned if the space does not 
        exist.  
        """
        svc = current_app.service
        try:
            svc.delete_space(id)
            return "", 200
        except FileManagerResourceNotFound as ex:
            return not_found("Requested space not found", "deleting a space")
        except Exception as ex:
            self.log.exception(ex)
            return "", 500

    @auth.authorization_required
    def put(self, id):
        """
        create a new file manager space, assigning it a given identifier.  The input is a JSON object that 
        must contain the property, ``foruser``, which represents the identifier for the user that will need 
        full access permissions on the space.  If the input object includes an ``id`` property, it will be 
        ignored.
        """
        svc = current_app.service
        rec = request.json
        if not rec:
            return {"message": "Create space request missing input data" }, 400
        if "for_user" not in rec:
            return {"message": "Create space request missing 'for_user' property" }, 400

        try:
            sp = svc.create_space_for(id, rec['for_user'])
            return sp.summarize()

        except FileManagerOpConflict as ex:
            return make_error_response(str(ex), 405, "creating a new space")
            return {"message": str(ex)}, 405
        except FileManagerException as ex:
            current_app.logger.error(str(ex))
            return server_error(intent="creating a new space")
        except Exception as ex:
            current_app.logger.exception(ex)
            return server_error(intent="creating a new space")

class SpaceScansResource(Resource):
    """
    Access to the available scans of a particular space (/spaces/[id]/scans)
    """
    @auth.authorization_required
    def get(self, id):
        """
        return a list of the available scan reports.  
        """
        svc = current_app.service

        try: 
            sp = svc.get_space(id)
            # todo
            return [ ]
        except FileManagerResourceNotFound as ex:
            return not_found("Requested space not found", "requesting space scanning")

    @auth.authorization_required
    def post(self, id):
        """
        request the creation of a new scan of the uploads directory.  An initial version of the scan
        report is returned, but the scanning will continue asynchronously.
        """
        svc = current_app.service

        try: 
            sp = svc.get_space(id)
            return sp.launch_scan()
            
        except FileManagerResourceNotFound as ex:
            return not_found("Requested space not found", "requesting space scanning")
        except FileManagerScanException as ex:
            self.log.error(str(ex))
            return server_error()
        except Exception as ex:
            self.log.exception(ex)
            return server_error()

class SpaceScanReportResource(Resource):
    """
    Acces a specific scan report for a space (/spaces/[id]/scans/[scanid])
    """
    
    @auth.authorization_required
    def get(self, spaceid, scanid):
        """
        return the report for the scan having the given identifier.  If the scan is still in progress,
        the returned report will be a preliminary version.
        :param str spaceid:  the identifier for the space that was (or is being) scanned
        :param str  scanid:  the identifier for the scan that was launched on the specified space
        """
        svc = current_app.service

        try: 
            sp = svc.get_space(spaceid)
            return sp.get_scan(scanid)

        except FileManagerResourceNotFound as ex:
            return not_found("Requested space not found", "requesting scan report")
        except FileNotFoundError as ex:
            return not_found("Scan report not found (may have been purged)", "requesting scan report")
        except FileManagerScanException as ex:
            self.log.error(str(ex))
            return server_error()
        except Exception as ex:
            self.log.exception(ex)
            return server_error()

    @auth.authorization_required
    def delete(self, spaceid, scanid):
        """
        test for the existence of the identified space
        """
        svc = current_app.service
        try:
            sp = svc.get_space(spaceid)
            sp.delete_scan(scanid)
            return "", 200
        except FileManagerResourceNotFound as ex:
            return not_found("Requested space not found", "deleting a scan report")
        except Exception as ex:
            self.log.exception(ex)
            return "", 500

class SpacePermsResource(Resource):
    """
    Access to the permissions of a particular space (/spaces/[id]/perms)
    """

    def get(self, id):
        """
        return the permissions for the known users of the given space.  The output will be a JSON 
        object whose keys are user identifiers and values are labels indicating their respective 
        access permission--one of "None", "Read", "Write", "Delete", "Share", ore "All".
        """
        svc = current_app.service

        try: 
            sp = svc.get_space(id)
            perms = sp.get_permissions(sp.uploads_folder)
        except FileManagerResourceNotFound as ex:
            return not_found("Requested space not found", "requesting scan report")
        except FileManagerScanException as ex:
            self.log.error(str(ex))
            return server_error()
        except Exception as ex:
            self.log.exception(ex)
            return server_error()
            
        # convert numeric permissions to strings
        for user in perms:
            if perms[user] in fmservice.perm_name:
                perms[user] = fmservice.perm_name[perms[user]]

        return perms

    def patch(self, id):
        """
        update the permissions on the identified space.  The input must be a JSON object whose keys
        are the identifiers of the users to update permissions for and values are the labels of the 
        new permissions to assign, one of "None", "Read", "Write", "Delete", "Share", ore "All".
        User IDs are only needed for those users that are to be changed.  The output, on the other hand,
        will provide permissions of all known users with permissions set for this space.
        """
        svc = current_app.service
        rec = request.json

        if not isinstance(rec, Mapping):
            return bad_input("Input record is not a JSON object: "+str(rec), "updating permissions")
        if any(not isinstance(p, str) or p not in fmservice.perm_code for p in rec.values()):
            return bad_input("Unrecognized permission values found in input: "+str(rec),
                             "updating permissions")
        try:
            sp = svc.get_space(id)
            for user in rec:
                perm = fmservice.perm_code[rec[user]]
                sp.set_permissions_for(sp.uploads_folder, user, perm)

        except FileManagerResourceNotFound as ex:
            return not_found("Requested space not found", "request scan report")
        except FileManagerScanException as ex:
            self.log.error(str(ex))
            return server_error()
        except Exception as ex:
            self.log.exception(ex)
            return server_error()
                
        return self.get(id)
            

def SpacesBlueprint():
    bp = Blueprint("spaces", __name__)
    api = Api(bp)
    api.add_resource(SpacesResource, '/spaces')
    api.add_resource(SpaceResource, '/spaces/<string:id>')
    api.add_resource(SpaceScansResource, '/spaces/<string:id>/scans')
    api.add_resource(SpaceScanReportResource, '/spaces/<string:spaceid>/scans/<string:scanid>')
    api.add_resource(SpacePermsResource, '/spaces/<string:id>/perms')
    return bp

