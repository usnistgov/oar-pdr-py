"""
/scan-files endpoint returns details about record space files
/scan-status endpoint returns the status of async scan task
"""
import uuid
from multiprocessing import Process

from flask_jwt_extended import jwt_required
from flask_restful import Resource

from app.utils import files

scanning = {}


# Async task
def scan_checksum(task_id):
    checksum = 'I am a checksum'
    # status types
    status = 'completed'

    scanning[task_id] = {
        'status': status,
        'checksum': checksum
    }

    return scanning[task_id]


class ScanFiles(Resource):
    @jwt_required()
    def put(self, user_name, record_name):
        try:
            #TODO: make sure to NOT start a scan for a user-record if another one is already ongoing
            task_id = str(uuid.uuid4())
            # run async
            p = Process(target=scan_checksum(task_id))
            p.start()

            scan_user = files.put_scan(user_name)

            direct_result = {
                'scan_user': scan_user,
                'task_id': task_id
            }

            success_response = {
                'success': 'PUT',
                'message': direct_result
            }

            return success_response, 201
        except Exception as error:
            error_response = {
                'error': 'Bad Request',
                'message': str(error)
            }
            return error_response, 400


class ScanStatus(Resource):
    @jwt_required()
    def get(self, task_id):
        try:
            scan_status = {}
            result = scan_checksum(task_id)
            scan_status['status'] = result['status']

            if scan_status['status'] == 'completed':
                scan_status['checksum'] = result['checksum']

            return scan_status

        except Exception as error:
            error_response = {
                'error': 'Bad Request',
                'message': str(error)
            }
            return error_response, 400
