"""
A WSGI application providing the MIDAS-application layer to the file manager, implemented with Flask
"""
import logging
from functools import wraps
from abc import ABC, abstractmethod
from collections import OrderedDict
from collections.abc import Mapping

from flask import Flask, request, current_app, jsonify, make_response, Blueprint
from flask_restful import Api, Resource, url_for

from nistoar.base.config import configure_log, ConfigurationException
from nistoar.midas.dap.fm import service as fmservice

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

class DelegatedX509AuthHandler(AuthHandler):
    """
    An AuthHandler that relies on an reverse proxy service to handle parsing and validation of 
    a client X.509 certificate.
    """

    def __init__(self, allowed_users):
        self.allowed = allowed_users

    def authenticate(self):
        """
        return the identifer for the authenticated user if the user successfully authenticated.
        In this implementation, the identifier returned is the Common Name (CN) of the user
        """
        if request.headers.get("X_CLIENT_VERIFY") != "SUCCESS":
            return None
        return request.headers.get("X_CLIENT_CN")

    def authorize(self, clientid):
        return clientid in self.allowed

auth = DelegatedX509AuthHandler(['admin'])

def create_app(config: Mapping, fmsvc: fmservice.MIDASFileManagerService=None):
    """
    create the Flask application

    :param dict config:  the configuration data for the app
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
        raise ConfigurationException("Config param, allowed_service_endpoints, not a str: " +
                                     str(type(config.get('allowed_service_endpoints'))))

    configure_log(config=config)

    if config.get('debug'):
        config['flask']['DEBUG'] = True
    config.update(config.get('flask'))
    del config['flask']

    app = Flask(__name__)
    app.name = config.get('name', 'midasfm')
    app.logger = logging.getLogger(app.name)
    app.config.update(config)

    if not fmsvc:
        fmsvc = fmservice.MIDASFileManagerService(config.get('service', {}), app.logger)
    app.service = fmsvc

    app.register_blueprint(SpacesBlueprint(), url_prefix='/mfm1')

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
    return make_error_resonse(message, code, intent)

def not_found(message: str, intent: str=None, code: int=404):
    return make_error_resonse(message, code, intent)

def bad_input(message: str, intent: str=None, code: int=400):
    return make_error_resonse(message, code, intent)

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
    def post(self):
        svc = current_app.service
        rec = request.json
        if not rec:
            return {"message": "Create space request missing input data" }, 400

        if "id" not in rec:
            return {"message": "Create space request missing 'id' property" }, 400
        if "for_user" not in rec:
            return {"message": "Create space request missing 'for_user' property" }, 400

        try:
            sp = svc.create_space_for(rec['id'], rec['for_user'])
            return sp.summarize()

        except FileManagerOpConflict as ex:
            return make_error_response(str(ex), 405, "Create a new space")
            return {"message": str(ex)}, 405
        except FileManagerException as ex:
            current_app.logger.error(str(ex))
            return server_error()
        except Exception as ex:
            current_app.logger.exception(ex)
            return server_error()

class SpaceResource(Resource):
    """
    access to a particular space (/spaces/[id])
    """
    @auth.authorization_required
    def get(self, id):
        svc = current_app.service
        try:
            sp = svc.get_space(id)
            return sp.summarize()

        except FileManagerResourceNotFound as ex:
            return not_found("Requested space not found", "request space by identifier")

    @auth.authorization_required
    def put(self, id):
        svc = current_app.service
        rec = request.json
        if not rec:
            return {"message": "Create space request missing input data" }, 400
        if "for_user" not in rec:
            return {"message": "Create space request missing 'for_user' property" }, 400

        try:
            sp = svc.create_space_for(id, rec['for_user'])
            return sp.summarize()

        except FileManagerResourceNotFound as ex:
            return not_found("Requested space not found", "request space by identifier")

class SpaceScansResource(Resource):
    """
    Access to the available scans of a particular space (/spaces/[id]/scans)
    """
    @auth.authorization_required
    def get(self, id):
        svc = current_app.service

        try: 
            sp = svc.get_space(id)
            # todo
            return [ ]
        except FileManagerResourceNotFound as ex:
            return not_found("Requested space not found", "request space scanning")

    @auth.authorization_required
    def post(self, id):
        svc = current_app.service

        try: 
            sp = svc.get_space(id)
            return sp.launch_scan()
            
        except FileManagerResourceNotFound as ex:
            return not_found("Requested space not found", "request space scanning")
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
        svc = current_app.service

        try: 
            sp = svc.get_space(spaceid)
            return sp.get_scan(scanid)

        except FileManagerResourceNotFound as ex:
            return not_found("Requested space not found", "request scan report")
        except FileNotFoundError as ex:
            return not_found("Scan report not found (may have been purged)", "request scan report")
        except FileManagerScanException as ex:
            self.log.error(str(ex))
            return server_error()
        except Exception as ex:
            self.log.exception(ex)
            return server_error()

class SpacePermsResource(Resource):
    """
    Access to the permissions of a particular space (/spaces/[id]/perms)
    """

    def get(self, id):
        svc = current_app.service

        try: 
            sp = svc.get_space(id)
            perms = sp.get_permissions(sp.uploads_folder)
        except FileManagerResourceNotFound as ex:
            return not_found("Requested space not found", "request scan report")
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
                perm = perm_code[rec[user]]
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
    api.add_resource(SpacePermsResource, '/spaces/<string:spaceid>/perms')
    return bp

