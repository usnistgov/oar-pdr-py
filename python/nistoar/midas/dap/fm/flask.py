"""
Create Flask App
"""

from dotenv import load_dotenv
from flask import Flask
from flask_jwt_extended import JWTManager

from app.api import api_bp
from config import Config
import logging

load_dotenv()


def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    logging.basicConfig(level=logging.INFO)

    jwt = JWTManager(app)
    logging.info("JWTManager initialized")

    app.config['PROPAGATE_EXCEPTIONS'] = True

    # JWT error handling
    @jwt.expired_token_loader
    def expired_token_callback(jwt_header, jwt_payload):
        logging.error("Expired token")
        return {
            'error': 'token_expired',
            'message': 'The token has expired.'
        }, 401

    @jwt.invalid_token_loader
    def invalid_token_callback(error):
        logging.error("Invalid token")
        return {
            'error': 'invalid_token',
            'message': 'Signature verification failed.'
        }, 401

    @jwt.unauthorized_loader
    def unauthorized_callback(error):
        logging.error("Unauthorized access")
        return {
            'error': 'authorization_required',
            'message': 'Request does not contain an access token.'
        }, 401

    @jwt.needs_fresh_token_loader
    def needs_fresh_token_callback(jwt_header, jwt_payload):
        logging.error("Fresh token required")
        return {
            'error': 'fresh_token_required',
            'message': 'The token is not fresh.'
        }, 401

    @jwt.revoked_token_loader
    def revoked_token_callback(jwt_header, jwt_payload):
        logging.error("Revoked token")
        return {
            'error': 'token_revoked',
            'message': 'The token has been revoked.'
        }, 401

    app.register_blueprint(api_bp, url_prefix="/api")
    logging.info("Blueprint registered")

    return app
