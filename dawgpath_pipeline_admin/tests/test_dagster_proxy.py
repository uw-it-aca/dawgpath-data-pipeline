# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import patch

from django.contrib.auth.models import User
from django.test import TestCase, override_settings


class DagsterProxyTest(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="javerage")

    def _set_saml_groups(self, groups):
        session = self.client.session
        session["samlUserdata"] = {"isMemberOf": groups}
        session.save()

    def test_anonymous_is_redirected_to_login(self):
        response = self.client.get("/dagster/runs")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/saml/login", response["Location"])

    @override_settings(DAGSTER_ACCESS_GROUP="u_acadev_dawgpath")
    def test_authenticated_without_group_is_denied(self):
        self.client.force_login(self.user)
        self._set_saml_groups(["u_acadev_other"])
        response = self.client.get("/dagster/runs")
        self.assertEqual(response.status_code, 403)

    @override_settings(DAGSTER_ACCESS_GROUP="u_acadev_dawgpath")
    @patch("dawgpath_pipeline_admin.views.dagster.requests.request")
    def test_authorized_group_is_proxied(self, mock_request):
        mock_request.return_value.status_code = 200
        mock_request.return_value.headers = {"content-type": "text/html"}
        mock_request.return_value.iter_content.return_value = iter(
            [b"dagster"])

        self.client.force_login(self.user)
        self._set_saml_groups(["u_acadev_dawgpath"])
        response = self.client.get("/dagster/runs")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(b"".join(response.streaming_content), b"dagster")
        self.assertEqual(
            mock_request.call_args.kwargs["url"],
            "http://127.0.0.1:3000/dagster/runs")

    @patch("dawgpath_pipeline_admin.views.dagster.requests.request")
    def test_upstream_status_and_headers_are_preserved(self, mock_request):
        mock_request.return_value.status_code = 502
        mock_request.return_value.headers = {
            "content-type": "application/json",
            "x-dagster-header": "kept",
            "transfer-encoding": "chunked",
        }
        mock_request.return_value.iter_content.return_value = iter([b"{}"])

        self.client.force_login(self.user)
        response = self.client.get("/dagster/graphql")

        self.assertEqual(response.status_code, 502)
        self.assertEqual(response["x-dagster-header"], "kept")
        self.assertNotIn("transfer-encoding", response)
