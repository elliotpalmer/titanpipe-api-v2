from flask import Blueprint, Response, jsonify, request
import pandas as pd
from .db import db

bp = Blueprint("auth", __name__)

def get_servicetitan_config(authorization):
    json = (
      pd
      .read_sql(
        f"""
          with org as (
            select organization_id 
            from api_keys 
            where 'Bearer ' || api_key = '{authorization}'
          )
          select * 
          from public.servicetitan_config
          where organization_id in (select organization_id from org)
          and active = true
        """, db.engine)
      .to_json(orient="records",default_handler=str)
    )
    return json

@bp.route("/servicetitan/config")
def servicetitan_config():
    authorization = request.headers.get('Authorization')
    json = get_servicetitan_config(authorization)
    return json

@bp.route("/database/config")
def database_config():
    authorization = request.headers.get('Authorization')
    json = (
      pd
      .read_sql(
        f"""
          with org as (
            select organization_id 
            from api_keys 
            where 'Bearer ' || api_key = '{authorization}'
          )
          select * 
          from public.databases
          where organization_id in (select organization_id from org)
        """, db.engine)
      .to_json(orient="records",default_handler=str)
    )
    return json
