from app import app, config, auth
from flask import request, Response, abort
import azure.storage.filedatalake


@app.route("/", methods=["DELETE"], defaults={"path": "/"})
@app.route("/<path:path>", methods=["DELETE"])
@auth.login_required
def delete(path):
    if path == "/":
        # Delete the container
        abort(501)

    file_client = azure.storage.filedatalake.DataLakeFileClient("https://{}.dfs.core.windows.net".format(
        config.storage_account_name), config.storage_container, path, credential=config.storage_account_key)

    try:
        is_directory = file_client.get_file_properties().metadata.get("hdi_isfolder", False)
    except azure.core.exceptions.ResourceNotFoundError:
        abort(404)

    if path.endswith("/") and not is_directory:
        abort(409)
    elif not path.endswith("/") and is_directory:
        abort(409)

    if not is_directory:
        # Delete a file
        file_client.delete_file()
    else:
        # Delete a directory
        if request.headers.get("Depth") and request.headers.get("Depth") != "infinity":
            abort(400)

        directory_client = azure.storage.filedatalake.DataLakeDirectoryClient(
            "https://{}.dfs.core.windows.net".format(config.storage_account_name), config.storage_container, path, credential=config.storage_account_key)
        directory_client.delete_directory()

    return Response(status=204)
