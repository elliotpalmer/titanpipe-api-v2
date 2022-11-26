from flask import Blueprint, Response, jsonify, request
import pandas as pd
from .db import db

bp = Blueprint("base", __name__)

@bp.route("/")
def base():
    return Response("Hello, world!", status=200)


@bp.route("/custom", methods=["POST"])
def custom():
    payload = request.get_json()

    if payload.get("say_hello") is True:
        output = jsonify({"message": "Hello!"})
    else:
        output = jsonify({"message": "..."})

    return output


@bp.route("/health")
def health():
    return Response("OK, cowboy #1!", status=200)

@bp.route("/dbtest")
def dbtest():
    df = pd.read_sql("select * from test_table", db.engine)
    return Response(df.to_json(orient="records"), status=200)