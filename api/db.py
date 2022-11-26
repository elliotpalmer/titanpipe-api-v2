from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
import json
import pandas as pd

db = SQLAlchemy()

def get_database_config(authorization):
    response = (
      pd
      .read_sql(
        f"""
          with org as (
            select organization_id 
            from api_keys 
            where 'Bearer ' || api_key = '{authorization}'
          )
          select database_config, database_type
          from databases
          where organization_id in (select organization_id from org)
        """, db.engine)
      .to_json(orient="records",default_handler=str)
      )
    json_data = json.loads(response)[0]
    return json_data

def get_postgres_url(database_config):
    PG_USER = database_config['database_config']['PG_USER']
    PG_PASSWORD = database_config['database_config']['PG_PASSWORD']
    PG_HOST = database_config['database_config']['PG_HOST']
    PG_PORT = database_config['database_config']['PG_PORT']
    PG_DATABASE = database_config['database_config']['PG_DATABASE']
    postgres_url = f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}'
    return postgres_url

def get_database_engine(authorization):
    database_config = get_database_config(authorization)
    if database_config['database_type'] == 'postgres':
        postgres_url = get_postgres_url(database_config)
        engine = create_engine(postgres_url)
    else:
        raise Exception(f'Unsupported database type: {database_config["database_type"]}')
        return False

    return engine
