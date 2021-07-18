from app import app, config, auth
from flask import Response, abort
import azure.storage.filedatalake


@app.route("/", methods=["MKCOL"], defaults={"path": "/"})
@app.route("/<path:path>", methods=["MKCOL"])
@auth.login_required
def mkcol(path):
    if path == "/":
        abort(405)
    if not path.endswith("/"):
        abort(409)

    directory_client = azure.storage.filedatalake.DataLakeDirectoryClient(
        "https://{}.dfs.core.windows.net".format(config.storage_account_name), config.storage_container, path, credential=config.storage_account_key)

    try:
        directory_client.create_directory(
            match_condition=azure.core.MatchConditions.IfMissing)
    except azure.core.exceptions.ResourceExistsError:
        abort(405)

    return Response(status=201)
