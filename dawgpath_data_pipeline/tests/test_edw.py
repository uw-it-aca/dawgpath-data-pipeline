# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase
from unittest.mock import MagicMock, call, patch

import pandas
import pymssql

from dawgpath_data_pipeline.dao.edw import _run_query


class TestEDW(TestCase):
    @patch("dawgpath_data_pipeline.dao.edw.time.sleep")
    @patch("dawgpath_data_pipeline.dao.edw.pandas.read_sql")
    @patch("dawgpath_data_pipeline.dao.edw.pymssql.connect")
    @patch("dawgpath_data_pipeline.dao.edw.settings")
    def test_run_query_retries_transient_connection_failures(
            self, settings_mock, connect_mock, read_sql_mock, sleep_mock):
        settings_mock.EDW_PASSWORD = "password"
        settings_mock.EDW_USER = "user"
        settings_mock.EDW_SERVER = "server"
        connection = MagicMock()
        connect_mock.side_effect = [
            pymssql.OperationalError(20002, b"TDS server connection failed"),
            pymssql.OperationalError(20002, b"TDS server connection failed"),
            connection,
        ]
        expected = pandas.DataFrame({"system_key": [1]})
        read_sql_mock.return_value = expected

        actual = _run_query("database", "SELECT system_key FROM source")

        self.assertIs(actual, expected)
        self.assertEqual(connect_mock.call_count, 3)
        connect_mock.assert_called_with(
            "server", "user", "password", "database")
        self.assertEqual(sleep_mock.call_args_list, [call(2), call(4)])
        read_sql_mock.assert_called_once_with(
            "SELECT system_key FROM source", connection)
        connection.close.assert_called_once_with()