from app import app, config, auth
from flask import request, Response, abort
import azure.storage.filedatalake


@app.route("/", methods=["PUT"], defaults={"path": "/"})
@app.route("/<path:path>", methods=["PUT"])
@auth.login_required
def put(path):
    if path.endswith("/"):
        abort(405)

    blob_client = azure.storage.blob.BlobClient("https://{}.blob.core.windows.net".format(
        config.storage_account_name), config.storage_container, path, credential=config.storage_account_key)

    total_length = request.content_length
    blob_client.upload_blob(request.stream, length=request.content_length, overwrite=True)

    return Response(status=204)
