from app import app, auth
from flask import Response


@app.route("/", methods=["OPTIONS"], defaults={"path": ""})
@app.route("/<path>", methods=["OPTIONS"])
@auth.login_required
def options(path):
    response = Response()
    response.headers["DAV"] = "1"
    response.headers["Allow"] = "GET,HEAD,DELETE,MKCOL,PROPFIND,OPTIONS"
    return response
