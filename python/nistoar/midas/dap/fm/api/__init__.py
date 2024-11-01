
"""
Registers all API endpoints
"""

from flask import Blueprint
from flask_restful import Api

from app.api.auth import Authentication
from app.api.file import File
from app.api.permissions import Permissions
from app.api.record_space import RecordSpace
from app.api.scan import ScanFiles
from app.api.test import Test

api_bp = Blueprint("api", __name__)
api = Api(api_bp)

api.add_resource(Authentication, "/auth")
api.add_resource(RecordSpace,
                 "/record-space/<string:record_name>/user/<string:user_name>/",
                 "/record-space/<string:record_name>",
                 )
api.add_resource(File,
                 "/file/<string:destination_path>",
                 )
api.add_resource(ScanFiles,
                 "/record-space/<string:record_name>/scan/<string:scan_id>",
                 "/record-space/<string:record_name>/scan",
                 )
api.add_resource(Permissions,
                 "/record-space/<string:record_name>/user/<string:user_name>/permissions/<string:permission_type>",
                 "/record-space/<string:record_name>/user/<string:user_name>/permissions",
                 )

api.add_resource(Test, "/test")
