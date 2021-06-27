#!/usr/bin/env python3

import json
import flask


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

app = flask.Flask(__name__)

import webdav_options  # noqa
import webdav_get  # noqa
import webdav_propfind  # noqa
