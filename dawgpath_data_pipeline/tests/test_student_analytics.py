# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase
from unittest.mock import MagicMock, call, patch

import pandas
import pyodbc

from dawgpath_data_pipeline.dao.student_analytics import (
    _connection_string,
    _run_query,
    get_bottleneck_gateway_courses,
)


def _settings(settings_mock):
    settings_mock.AZSQL_DRIVER = "ODBC Driver 18 for SQL Server"
    settings_mock.AZSQL_SERVER = "studentanalytics-prod.database.windows.net"
    settings_mock.AZSQL_PORT = "1433"
    settings_mock.AZSQL_DATABASE = "StudentAnalytics"
    settings_mock.AZSQL_USER = "a_azsql_dawgpath@uw.edu"
    settings_mock.AZSQL_PASSWORD = "sekrit"
    settings_mock.AZSQL_AUTHENTICATION = "ActiveDirectoryPassword"


class TestStudentAnalytics(TestCase):
    @patch("dawgpath_data_pipeline.dao.student_analytics.settings")
    def test_connection_string_uses_entra_id_and_encryption(
            self, settings_mock):
        _settings(settings_mock)

        conn_str = _connection_string()

        self.assertIn("DRIVER={ODBC Driver 18 for SQL Server};", conn_str)
        self.assertIn(
            "SERVER=tcp:studentanalytics-prod.database.windows.net,1433;",
            conn_str)
        self.assertIn("DATABASE=StudentAnalytics;", conn_str)
        self.assertIn("UID=a_azsql_dawgpath@uw.edu;", conn_str)
        self.assertIn("Authentication=ActiveDirectoryPassword;", conn_str)
        self.assertIn("Encrypt=yes;", conn_str)
        self.assertIn("TrustServerCertificate=no;", conn_str)
        # ODBC rejects the ADO.NET True/False spelling; off is the default.
        self.assertNotIn("MultipleActiveResultSets", conn_str)

    @patch("dawgpath_data_pipeline.dao.student_analytics.time.sleep")
    @patch("dawgpath_data_pipeline.dao.student_analytics.pandas.read_sql")
    @patch("dawgpath_data_pipeline.dao.student_analytics.pyodbc.connect")
    @patch("dawgpath_data_pipeline.dao.student_analytics.settings")
    def test_run_query_retries_transient_connection_failures(
            self, settings_mock, connect_mock, read_sql_mock, sleep_mock):
        _settings(settings_mock)
        connection = MagicMock()
        connect_mock.side_effect = [
            pyodbc.Error("40613", "Database unavailable"),
            pyodbc.Error("08S01", "Communication link failure"),
            connection,
        ]
        expected = pandas.DataFrame({"course_number": [142]})
        read_sql_mock.return_value = expected

        actual = _run_query("SELECT course_number FROM source")

        self.assertIs(actual, expected)
        self.assertEqual(connect_mock.call_count, 3)
        self.assertEqual(sleep_mock.call_args_list, [call(2), call(4)])
        connection.close.assert_called_once_with()

    @patch("dawgpath_data_pipeline.dao.student_analytics.time.sleep")
    @patch("dawgpath_data_pipeline.dao.student_analytics.pyodbc.connect")
    @patch("dawgpath_data_pipeline.dao.student_analytics.settings")
    def test_run_query_does_not_retry_auth_failures(
            self, settings_mock, connect_mock, sleep_mock):
        _settings(settings_mock)
        connect_mock.side_effect = pyodbc.Error("28000", "Login failed")

        with self.assertRaises(pyodbc.Error):
            _run_query("SELECT 1")

        self.assertEqual(connect_mock.call_count, 1)
        sleep_mock.assert_not_called()

    @patch("dawgpath_data_pipeline.dao.student_analytics.pyodbc.connect")
    @patch("dawgpath_data_pipeline.dao.student_analytics.settings")
    def test_connection_failure_does_not_leak_password(
            self, settings_mock, connect_mock):
        _settings(settings_mock)
        connect_mock.side_effect = pyodbc.Error("28000", "Login failed")

        with self.assertRaises(pyodbc.Error) as ctx:
            _run_query("SELECT 1")

        self.assertNotIn("sekrit", str(ctx.exception))

    @patch("dawgpath_data_pipeline.dao.student_analytics._run_query")
    def test_query_keeps_one_current_row_per_course(self, run_query_mock):
        get_bottleneck_gateway_courses()

        query = run_query_mock.call_args.args[0]
        self.assertIn("last_eff_yr = 9999", query)
        self.assertIn(
            "PARTITION BY\n                            department_abbrev,\n"
            "                            course_number,\n"
            "                            course_branch",
            query)
        self.assertIn("ORDER BY scoring_year DESC, scoring_qtr DESC", query)
        self.assertIn("WHERE scoring_rank = 1", query)
