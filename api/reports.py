from flask import Blueprint, Response, jsonify, request
from flask_json import json_response
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

def sync_report_data(authorization, report_config):
    parameters = report_config['parameters']
    report_category = report_config['report_category'] #"operations"
    report_id =  report_config['report_id'] #"94428310"
    table_name = report_config['table_name']
    report_data = get_servietitan_report_data(authorization, report_category, report_id, parameters)
    report_df = get_report_data_as_dataframe(report_data)
    engine = get_database_engine(authorization)
    response = upload_report_data_to_database(table_name, report_df, engine)
    return response

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

@bp.route("/sync", methods=["GET","POST"])
def sync_report():
  if request.method == "POST":
    body = request.get_json()
    # print(body)
    authorization = request.headers.get('Authorization')
    response = sync_report_data(authorization, body)
    # print(response)
    return json_response(status_=200, response=response)
  elif request.method == "GET":
    return Response("GET", status=200)

@bp.route("/queue", methods=["GET","POST"])
def queue_report():
  if request.method == "POST":
    body = request.get_json()
    print(body)
    authorization = request.headers.get('Authorization')
    body['authorization'] = authorization
    engine = db.engine
    engine.execute(f"""
      INSERT INTO request_queue
        (request_type, request_config, request_status) VALUES ('report', '{json.dumps(body)}', 'queued')
      """)
    return json_response(status_=200, response={"success": True, "message": "Report queued"})
  elif request.method == "GET":
    return Response("GET", status=200)

def get_next_queued_report():
  engine = db.engine
  result = pd.read_sql("""
    SELECT request_uuid, request_config 
    FROM request_queue 
    WHERE request_type = 'report' 
      AND request_status = 'queued' 
      and active
    """, engine)
  queue_length = len(result)

  if len(result) == 0:
    return {
    "request_uuid": None,
    "request_config": None,
    "has_more": queue_length > 0,
    "queue_count": queue_length
  }
  
  return {
    "request_uuid": result['request_uuid'][0],
    "request_config": result['request_config'][0],
    "has_more": queue_length > 0,
    "queue_count": queue_length
  }

def set_queued_report_status(request_uuid, status, active=True):
  engine = db.engine
  engine.execute(f"""
    UPDATE request_queue 
    SET request_status = '{status}', updated_at = current_timestamp, active = {active} 
    WHERE request_uuid = '{request_uuid}'
    """)
  return True

@bp.route("/queue/runnext", methods=["GET","POST"])
def run_next_report():
  
  queue_config = get_next_queued_report()
  if request.method == "GET":
    return json_response(status_=200, response=queue_config)

  if request.method == "POST":
    engine = db.engine
    
    request_uuid = queue_config['request_uuid']
    request_config = queue_config['request_config']
    has_more = queue_config['has_more']
    queue_count = queue_config['queue_count']

    if request_uuid is None:
      return json_response(status_=200, response={"success": True, "message": "No queued reports", "has_more": has_more, "queue_count": queue_count})

    set_queued_report_status(request_uuid, 'running')
    try:
      response = sync_report_data(request_config['authorization'], request_config)
      set_queued_report_status(request_uuid, 'complete', active=False)
      return json_response(status_=200, response={"success": True, "message": "Report Synced", "has_more": has_more, "queue_count": queue_count})
    except Exception as e:
      print(e)
      set_queued_report_status(request_uuid, 'error')
      return json_response(status_=500, response={"success": False, "message": "Report Failed",  "has_more": has_more, "queue_count": queue_count})