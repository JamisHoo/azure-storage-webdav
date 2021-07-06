#!/usr/bin/env python3

import hmac
import json
import flask
import flask_compress
import flask_httpauth


class Config:
    pass


config = Config()

with open("config.json") as f:
    config_data = json.load(f)
    if config_data["cloud_service"]["type"] != "azure storage datalake":
        raise NotImplementedError("unsupported cloud storage type")
    config.storage_account_name = config_data["cloud_service"]["account_name"]
    config.storage_account_key = config_data["cloud_service"]["account_key"]
    config.storage_container = config_data["cloud_service"]["container_name"]
    config.auth = dict()
    for u in config_data["auth"]:
        config.auth[u] = config_data["auth"][u]

app = flask.Flask(__name__)
app.config["COMPRESS_REGISTER"] = False
compress = flask_compress.Compress()
compress.init_app(app)
auth = flask_httpauth.HTTPBasicAuth()


@auth.verify_password
def verify_password(username, password):
    if username in config.auth and hmac.compare_digest(password, config.auth[username]):
        return username


import webdav_options  # noqa
import webdav_get  # noqa
import webdav_propfind  # noqa
