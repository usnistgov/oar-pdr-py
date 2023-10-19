"""
Registers all API endpoints
"""

from flask import Blueprint
from flask_restful import Api

from app.api.auth import Authentication
from app.api.permissions import Permissions
from app.api.record_space import RecordSpace
from app.api.scan import ScanFiles, ScanStatus
from app.api.test import Test

api_bp = Blueprint("api", __name__)
api = Api(api_bp)

api.add_resource(Authentication, "/auth")
api.add_resource(RecordSpace,
                 "/record-space/<string:user_name>/<string:record_name>",
                 "/record-space/<string:record_name>"
                 )
api.add_resource(ScanFiles, "/scan-files/<string:user_name>/<string:record_name>")
api.add_resource(ScanStatus, "/scan-status/<string:task_id>")
api.add_resource(Permissions,
                 "/permissions/<string:user_name>/<string:record_name>/<string:permission_type>",
                 "/permissions/<string:user_name>/<string:record_name>"
                 )

api.add_resource(Test, "/test")