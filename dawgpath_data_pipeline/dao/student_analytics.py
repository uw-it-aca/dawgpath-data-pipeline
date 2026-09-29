# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import time

import pandas
import pyodbc
from commonconf import settings

CONNECTION_ATTEMPTS = 3

# Azure SQL gateway sheds load with these during failover/throttling.
TRANSIENT_SQLSTATES = ("08001", "08S01", "08003", "40001", "40197", "40501",
                       "40613", "HYT00", "HYT01")


def get_bottleneck_gateway_courses():
    # One row per course per scoring term; keep each course's newest scoring
    # term, and only course versions still in effect (last_eff_yr 9999).
    db_query = """
            WITH scored AS (
                SELECT
                    department_abbrev,
                    course_number,
                    course_branch,
                    bottleneck_indicator,
                    bottleneck_severity,
                    gateway_indicator,
                    gateway_significance,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            department_abbrev,
                            course_number,
                            course_branch
                        ORDER BY scoring_year DESC, scoring_qtr DESC
                    ) AS scoring_rank
                FROM [dbo].[BottleneckGatewayCourses]
                WHERE last_eff_yr = 9999
                    AND department_abbrev IS NOT NULL
                    AND course_number IS NOT NULL
            )
            SELECT
                department_abbrev,
                course_number,
                course_branch,
                bottleneck_indicator AS is_bottleneck,
                bottleneck_severity,
                gateway_indicator AS is_gateway,
                gateway_significance
            FROM scored
            WHERE scoring_rank = 1
    """
    return _run_query(db_query)


def _connection_string():
    return (
        f"DRIVER={{{settings.AZSQL_DRIVER}}};"
        f"SERVER=tcp:{settings.AZSQL_SERVER},{settings.AZSQL_PORT};"
        f"DATABASE={settings.AZSQL_DATABASE};"
        f"UID={settings.AZSQL_USER};"
        f"PWD={settings.AZSQL_PASSWORD};"
        f"Authentication={settings.AZSQL_AUTHENTICATION};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        f"HostNameInCertificate={settings.AZSQL_HOSTNAME_IN_CERTIFICATE};"
        "Connection Timeout=30;"
    )


def _is_transient(error):
    return bool(error.args) and error.args[0] in TRANSIENT_SQLSTATES


def _run_query(query):
    for attempt in range(1, CONNECTION_ATTEMPTS + 1):
        try:
            con = pyodbc.connect(_connection_string())
            break
        except pyodbc.Error as error:
            # Never surface the connection string; it carries the password.
            if not _is_transient(error) or attempt == CONNECTION_ATTEMPTS:
                raise
            time.sleep(2 ** attempt)
    df = pandas.read_sql(query, con)
    con.close()
    return df
