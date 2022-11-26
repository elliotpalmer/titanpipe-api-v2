from flask import Blueprint, Response, jsonify, request
import json
import pandas as pd
from .db import db, get_database_engine
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

def get_servietitan_report_data(authorization, report_category, report_id, report_params=[]):
    st_conn = get_servicetitan_connection(authorization)
    report = Report(report_category, report_id, st_conn)
    for param in report_params:
        report.add_params(param['name'], param['value'])
    report_data = report.get_all_data()
    return report_data

def get_report_data_as_dataframe(report_data):
    fields = [field["name"] for field in report_data["fields"]]
    df = pd.DataFrame(report_data['data'], columns=fields)
    return df

def upload_report_data_to_database(table_name, df, engine):
    df.to_sql(
      table_name,
      engine,
      if_exists='replace',
      index=False
    )
    return {"success": True, "message": f"Uploaded {len(df)} rows to {table_name}"}

@bp.route("/test")
def servicetitan_reports():
    authorization = request.headers.get('Authorization')
    parameters = [
      { "name": "From", "value": "2022-11-01" },
      { "name": "To", "value": "2022-11-04" },
      { "name": "DateType", "value": "3" }
    ]
    report_category = "accounting" #"operations"
    report_id =  "48751764" #"94428310"
    # report_category = "operations"
    # report_id = "94428310"
    report_data = get_servietitan_report_data(authorization, report_category, report_id, parameters)
    report_df = get_report_data_as_dataframe(report_data)
    # report_df = pd.DataFrame({"a": [1,2,3]})
    engine = get_database_engine(authorization)
    response = upload_report_data_to_database("test_table_2", report_df, engine)
    print(response)
    return Response(response, status=200)

@bp.route("/", methods=["POST"])
def sync_report():
    body = request.get_json()
    print(body)
    authorization = request.headers.get('Authorization')
    parameters = body['parameters']
    report_category = body['report_category'] #"operations"
    report_id =  body['report_id'] #"94428310"
    table_name = body['table_name']
    # report_category = "operations"
    # report_id = "94428310"
    report_data = get_servietitan_report_data(authorization, report_category, report_id, parameters)
    report_df = get_report_data_as_dataframe(report_data)
    # report_df = pd.DataFrame({"a": [1,2,3]})
    engine = get_database_engine(authorization)
    response = upload_report_data_to_database(table_name, report_df, engine)
    print(response)
    return Response(response, status=200)