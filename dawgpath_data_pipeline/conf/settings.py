# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import os


class AppSettings:
    EDW_PASSWORD = ""
    EDW_USER = ""
    EDW_SERVER = ""

    DB_CLASS = "postgres"
    DB_FILE = ""
    DB_DEBUG = False

    DB_USER = "postgres"
    DB_PASSWORD = "postgres"
    DB_HOST = "postgres"
    DB_PORT = "5432"
    DB_DATABASE = ""

    RESTCLIENTS_SWS_DAO_CLASS = "Live"
    RESTCLIENTS_SWS_CERT_FILE = ""
    RESTCLIENTS_SWS_KEY_FILE = ""
    RESTCLIENTS_SWS_HOST = ""
    RESTCLIENTS_SWS_VERIFY_HTTPS = False

    def get(self, attr, default=None):
        # deployed environments inject config as env vars; local uses app.conf
        if attr in os.environ:
            return os.environ[attr]
        return getattr(AppSettings, attr)
