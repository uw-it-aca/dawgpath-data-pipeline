import os
import pyodbc
import pandas
from commonconf import settings

DB = "UWSDBDataStore"


def get_prereqs():
    db_query = """
        SELECT
            *
        FROM sec.sr_course_prereq
        WHERE
            last_eff_yr = 9999
            AND pr_not_excl != \'E\'
    """
    return _run_query(DB, db_query)


def get_course_titles():
    db_query = """
        SELECT
            *
        FROM sec.sr_course_titles
        WHERE
            last_eff_yr = 9999
    """
    return _run_query(DB, db_query)


def get_curric_info():
    db_query = """
        SELECT
            curric_abbr,
            curric_name,
            curric_branch,
            curric_url,
            curric_home_url
        FROM sec.sr_curric_code
        WHERE
            curric_last_yr = 9999
    """
    return _run_query(DB, db_query)


def _run_query(database, query):
    os.environ["FREETDSCONF"] = "db_config/freetds.conf"
    os.environ["ODBCSYSINI"] = "db_config"

    password = getattr(settings, "EDW_PASSWORD")
    user = getattr(settings, "EDW_USER")
    server = getattr(settings, "EDW_SERVER")
    constring = "Driver={FreeTDS};" \
                f"SERVERNAME={server};" \
                f"Database={database};" \
                "Port=1433;" \
                "TDS_Version=7.2;" \
                f"UID={user};" \
                f"PWD={password}"

    con = pyodbc.connect(constring)
    df = pandas.read_sql(query, con)
    con.close()
    return df
