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
        return getattr(AppSettings, attr)
