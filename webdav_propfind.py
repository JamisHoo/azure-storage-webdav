from app import app, config, auth, compress
from flask import request, Response, abort
from wsgiref.handlers import format_date_time
import os
import time
import lxml.etree
import azure.storage.filedatalake


@app.route("/", methods=["PROPFIND"], defaults={"path": "/"})
@app.route("/<path:path>", methods=["PROPFIND"])
@auth.login_required
@compress.compressed()
def list(path):
    depth = request.headers.get("Depth")
    if not depth:
        depth = "infinity"
    if depth not in ["0", "1", "infinity"]:
        abort(400)

    filesystem_client = azure.storage.filedatalake.FileSystemClient(
        "https://{}.dfs.core.windows.net".format(config.storage_account_name), config.storage_container, credential=config.storage_account_key)

    xml_multistatus = lxml.etree.Element(lxml.etree.QName(
        "DAV:", "multistatus"), nsmap={"D": "DAV:"})

    file_client = azure.storage.filedatalake.DataLakeFileClient(
        "https://{}.dfs.core.windows.net".format(config.storage_account_name), config.storage_container, path, credential=config.storage_account_key)
    try:
        properties = file_client.get_file_properties()
    except azure.core.exceptions.ResourceNotFoundError:
        abort(404)

    is_directory = properties.metadata.get("hdi_isfolder", False)

    xml_response = lxml.etree.SubElement(
        xml_multistatus, lxml.etree.QName("DAV:", "response"))
    lxml.etree.SubElement(
        xml_response, lxml.etree.QName("DAV:", "href")).text = os.path.join("/", path)
    xml_propstat = lxml.etree.SubElement(
        xml_response, lxml.etree.QName("DAV:", "propstat"))
    xml_prop = lxml.etree.SubElement(
        xml_propstat, lxml.etree.QName("DAV:", "prop"))
    lxml.etree.SubElement(
        xml_prop, lxml.etree.QName("DAV:", "displayname")).text = os.path.basename(path)
    xml_resourcetype = lxml.etree.SubElement(
        xml_prop, lxml.etree.QName("DAV:", "resourcetype"))
    if is_directory:
        xml_collection = lxml.etree.SubElement(
            xml_resourcetype, lxml.etree.QName("DAV:", "collection"))
    else:
        xml_resourcetype.text = ""
        lxml.etree.SubElement(xml_prop, lxml.etree.QName(
            "DAV:", "getcontentlength")).text = str(properties.size)
    lxml.etree.SubElement(xml_prop, lxml.etree.QName(
        "DAV:", "getetag")).text = properties.etag
    lxml.etree.SubElement(
        xml_prop, lxml.etree.QName("DAV:", "getlastmodified")).text = format_date_time(time.mktime(properties.last_modified.timetuple()))
    lxml.etree.SubElement(xml_prop, lxml.etree.QName(
        "DAV:", "creationdate")).text = properties.creation_time.isoformat()
    if properties.content_settings.content_language:
        lxml.etree.SubElement(xml_prop, lxml.etree.QName(
            "DAV:", "getcontentlanguage")).text = properties.content_settings.content_language
    if properties.content_settings.content_type:
        lxml.etree.SubElement(xml_prop, lxml.etree.QName(
            "DAV:", "getcontenttype")).text = properties.content_settings.content_type
    lxml.etree.SubElement(xml_prop, lxml.etree.QName("DAV:", "lockdiscovery"))
    lxml.etree.SubElement(xml_prop, lxml.etree.QName(
        "DAV:", "supportedlock")).text = ""
    lxml.etree.SubElement(xml_propstat, lxml.etree.QName(
        "DAV:", "status")).text = "HTTP/1.1 200 OK"
    if is_directory and depth != "0":
        paths = filesystem_client.get_paths(
            path=path, recursive=(depth == "infinity"))
        for p in paths:
            xml_response = lxml.etree.SubElement(
                xml_multistatus, lxml.etree.QName("DAV:", "response"))
            lxml.etree.SubElement(
                xml_response, lxml.etree.QName("DAV:", "href")).text = os.path.join("/", p.name)
            xml_propstat = lxml.etree.SubElement(
                xml_response, lxml.etree.QName("DAV:", "propstat"))
            xml_prop = lxml.etree.SubElement(
                xml_propstat, lxml.etree.QName("DAV:", "prop"))
            lxml.etree.SubElement(
                xml_prop, lxml.etree.QName("DAV:", "displayname")).text = os.path.basename(p.name)
            xml_resourcetype = lxml.etree.SubElement(
                xml_prop, lxml.etree.QName("DAV:", "resourcetype"))
            if p.is_directory:
                xml_collection = lxml.etree.SubElement(
                    xml_resourcetype, lxml.etree.QName("DAV:", "collection"))
            else:
                xml_resourcetype.text = ""
                lxml.etree.SubElement(
                    xml_prop, lxml.etree.QName("DAV:", "getcontentlength")).text = str(p.content_length)
            lxml.etree.SubElement(xml_prop, lxml.etree.QName(
                "DAV:", "getetag")).text = properties.etag
            lxml.etree.SubElement(
                xml_prop, lxml.etree.QName("DAV:", "getlastmodified")).text = format_date_time(time.mktime(p.last_modified.timetuple()))
            lxml.etree.SubElement(xml_prop, lxml.etree.QName(
                "DAV:", "creationdate")).text = properties.creation_time.isoformat()
            if properties.content_settings.content_language:
                lxml.etree.SubElement(xml_prop, lxml.etree.QName(
                    "DAV:", "getcontentlanguage")).text = properties.content_settings.content_language
            if properties.content_settings.content_type:
                lxml.etree.SubElement(xml_prop, lxml.etree.QName(
                    "DAV:", "getcontenttype")).text = properties.content_settings.content_type
            lxml.etree.SubElement(
                xml_prop, lxml.etree.QName("DAV:", "lockdiscovery"))
            lxml.etree.SubElement(
                xml_prop, lxml.etree.QName("DAV:", "supportedlock")).text = ""
            lxml.etree.SubElement(
                xml_propstat, lxml.etree.QName("DAV:", "status")).text = "HTTP/1.1 200 OK"

    return Response(lxml.etree.tostring(xml_multistatus, pretty_print=True, encoding="utf-8", xml_declaration=True), status=207, mimetype="text/xml")
