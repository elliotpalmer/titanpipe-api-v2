from flask import Blueprint, Response, jsonify, request
import json
import pandas as pd
from .db import db
from .auth import get_servicetitan_config
from servicepytan import Report, auth as st_auth

bp = Blueprint("reports", __name__)

def get_servicetitan_connection(authorization):
    config = get_servicetitan_config(authorization)
    config = json.loads(config)[0]
    st_conn = st_auth.servicepytan_connect(
      app_id=config['app_id'],
      app_key=config['app_key'],
      client_id=config['client_id'],
      client_secret=config['client_secret'],
      tenant_id=config['tenant_id'],
      timezone=config['timezone']
    )
    return st_conn

@bp.route("/servicetitan")
def servicetitan_reports():
    authorization = request.headers.get('Authorization')
    st_conn = get_servicetitan_connection(authorization)
    report = Report("operations", "94428310", conn=st_conn)
    report.add_params("From", "2022-11-01")
    report.add_params("To", "2022-11-07")
    report.add_params("DateType", "3")
    data = report.get_all_data()
    print(data)
    return Response(data, status=200)