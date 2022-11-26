# Flask Imports
from flask import Flask, Response, jsonify, request
from flask_jwt import JWT
from flask_restful import Api

# Third Party Imports
import os
from dotenv import load_dotenv
load_dotenv()
# import pandas as pd
# from servicepytan import Report,auth

# Local Imports
from .errors import errors
from .base import bp as base_bp

# Create Flask App
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('SQLALCHEMY_DATABASE_URI')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = os.getenv('SQLALCHEMY_TRACK_MODIFICATIONS')

# Register Blueprints
app.register_blueprint(errors)
app.register_blueprint(base_bp)


