"""
Create Flask App
"""

from dotenv import load_dotenv
from flask import Flask
from flask_jwt_extended import JWTManager

from app.api import api_bp
from config import Config

load_dotenv()


def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)
    app.register_blueprint(api_bp, url_prefix="/api")

    jwt = JWTManager(app)

    return app
