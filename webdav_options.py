from app import app
from flask import Response


@app.route("/", methods=["OPTIONS"], defaults={"path": ""})
@app.route("/<path>", methods=["OPTIONS"])
def options(path):
    response = Response()
    response.headers["DAV"] = "1"
    response.headers["Allow"] = "GET,HEAD,PROPFIND,OPTIONS"
    return response
