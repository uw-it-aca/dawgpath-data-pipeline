# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import os

# Mirrors django-container's SWS_ENV mapping so both processes share one knob.
SWS_HOSTS = {
    "PROD": "https://ws.api.uw.edu:443",
    "EVAL": "https://wseval.s.uw.edu:443",
}

BOOLEAN_SETTINGS = ("DB_DEBUG", "RESTCLIENTS_SWS_VERIFY_HTTPS")


class AppSettings:
    EDW_PASSWORD = ""
    EDW_USER = ""
    EDW_SERVER = ""

    AZSQL_SERVER = "studentanalytics-prod.database.windows.net"
    AZSQL_PORT = "1433"
    AZSQL_HOSTNAME_IN_CERTIFICATE = "*.database.windows.net"
    AZSQL_DATABASE = "StudentAnalytics"
    AZSQL_USER = ""
    AZSQL_PASSWORD = ""
    AZSQL_AUTHENTICATION = "ActiveDirectoryPassword"
    AZSQL_DRIVER = "ODBC Driver 18 for SQL Server"

    DB_CLASS = "sqlite3"
    DB_FILE = "db.sqlite"
    DB_DEBUG = False

    DB_USER = "postgres"
    DB_PASSWORD = "postgres"
    DB_HOST = "postgres"
    DB_PORT = "5432"
    DB_DATABASE = ""

    RESTCLIENTS_SWS_DAO_CLASS = "Live"
    RESTCLIENTS_SWS_VERIFY_HTTPS = True

    GCS_BUCKET_NAME = None

    def get(self, key):
        if key in os.environ:
            value = os.environ[key]
            if key in BOOLEAN_SETTINGS:
                return value.strip().lower() in ("1", "true", "yes", "on")
            return value
        if key == "RESTCLIENTS_SWS_HOST" and os.getenv("SWS_ENV") in SWS_HOSTS:
            return SWS_HOSTS[os.environ["SWS_ENV"]]
        # AttributeError for unset keys lets getattr(settings, key, default) work
        return getattr(AppSettings, key)
