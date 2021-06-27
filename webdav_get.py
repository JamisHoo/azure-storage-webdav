from app import app, config
import parse
from flask import request, Response, stream_with_context, abort
import azure.storage.filedatalake


@app.route("/<path:path>")
def get(path):
    file_client = azure.storage.filedatalake.DataLakeFileClient(
        "https://{}.dfs.core.windows.net".format(config.storage_account_name), config.storage_container, path, credential=config.storage_account_key)

    file_offset = None
    file_length = None
    if request.headers.get("Range"):
        nums = parse.parse("bytes={:d}-{:d}", request.headers.get("Range"))
        if not nums:
            nums = parse.parse("bytes={:d}-", request.headers.get("Range"))
        if not nums or len(nums.fixed) < 1 or len(nums.fixed) > 2:
            abort(400)
        file_offset = nums[0]
        if len(nums.fixed) == 2:
            file_length = nums[1] - file_offset + 1

    raw_response = None

    def callback(response):
        nonlocal raw_response
        raw_response = response.http_response

    try:
        download_response = file_client.download_file(
            offset=file_offset, length=file_length, raw_response_hook=callback)
    except azure.core.exceptions.ResourceNotFoundError:
        abort(404)

    properties = download_response.properties
    content_settings = dict(properties["content_settings"].items())

    is_directory = properties.metadata.get("hdi_isfolder", False)
    if is_directory:
        raise NotImplementedError

    response = Response(stream_with_context(
        download_response.chunks()), status=206 if file_offset else 200)
    response.accept_ranges = "bytes"
    response.set_etag(properties.etag.strip("\""))
    if content_settings["cache_control"]:
        response.cache_control = content_settings["cache_control"]
    if content_settings["content_encoding"]:
        response.content_encoding = content_settings["content_encoding"]
    if content_settings["content_language"]:
        response.content_language = content_settings["content_language"]
    if content_settings["content_md5"]:
        response.content_md5 = content_settings["content_md5"]
    if content_settings["content_type"]:
        response.content_type = content_settings["content_type"]
    if file_offset:
        nums = parse.parse("bytes {:d}-{:d}/{:d}",
                           raw_response.headers["Content-Range"])
        response.content_range.set(nums[0], nums[1] + 1, nums[2])
    response.content_length = int(raw_response.headers["Content-Length"])

    return response
