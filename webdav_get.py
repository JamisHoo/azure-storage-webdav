from app import app, config
from flask import Response, stream_with_context, abort
import azure.storage.filedatalake


@app.route("/<path:path>")
def get(path):
    file_client = azure.storage.filedatalake.DataLakeFileClient(
        "https://{}.dfs.core.windows.net".format(config.storage_account_name), config.storage_container, path, credential=config.storage_account_key)
    try:
        download_response = file_client.download_file()
    except azure.core.exceptions.ResourceNotFoundError:
        abort(404)

    properties = download_response.properties
    content_settings = dict(properties["content_settings"].items())

    is_directory = properties.metadata.get("hdi_isfolder", False)
    if is_directory:
        raise NotImplementedError

    response = Response(stream_with_context(download_response.chunks()))
    response.accept_ranges = "bytes"
    response.set_etag(properties.etag.strip("\""))
    if content_settings["cache_control"]:
        response.cache_control = content_settings["cache_control"]
    if content_settings["content_encoding"]:
        response.content_encoding = content_settings["content_encoding"]
    if content_settings["content_language"]:
        response.content_language = content_settings["content_language"]
    response.content_length = download_response.size
    if content_settings["content_md5"]:
        response.content_md5 = content_settings["content_md5"]
    if content_settings["content_type"]:
        response.content_type = content_settings["content_type"]

    return response
