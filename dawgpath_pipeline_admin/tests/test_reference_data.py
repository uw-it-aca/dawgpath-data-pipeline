# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

from django.contrib.auth.models import User
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import Client, TestCase, override_settings
from django.urls import reverse

from dawgpath_data_pipeline.utilities.reference_data import ReferenceDataStore


@override_settings(DAGSTER_ACCESS_GROUP="pipeline-admin")
class ReferenceViewTest(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="operator")
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        env = patch.dict(os.environ, {
            "GCS_BUCKET_NAME": "", "ARTIFACT_ROOT": self.tmp.name})
        env.start()
        self.addCleanup(env.stop)
        self.url = reverse("reference-data")

    def authorize(self, client=None):
        client = client or self.client
        client.force_login(self.user)
        session = client.session
        session["samlUserdata"] = {"isMemberOf": ["pipeline-admin"]}
        session.save()

    def upload(self, client=None, content=b"INFO-0-1-6,informatics\n",
               **extra):
        client = client or self.client
        return client.post(self.url, {
            "kind": "career",
            "file": SimpleUploadedFile("arbitrary.csv", content),
            **extra,
        })

    def test_authentication_and_authorization_on_get_and_post(self):
        self.assertEqual(self.client.get(self.url).status_code, 302)
        self.assertEqual(self.upload().status_code, 302)
        self.client.force_login(self.user)
        self.assertEqual(self.client.get(self.url).status_code, 403)
        self.assertEqual(self.upload().status_code, 403)
        self.authorize()
        with override_settings(DAGSTER_ACCESS_GROUP=None):
            self.assertEqual(self.client.get(self.url).status_code, 403)
        self.assertEqual(ReferenceDataStore().history(), [])

    def test_upload_is_staged_and_history_displays_identity(self):
        self.authorize()
        response = self.upload()
        self.assertRedirects(response, self.url)
        history = ReferenceDataStore().history()
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["uploader"], "operator")
        self.assertEqual(history[0]["filename"], "admit_major_mapping.csv")
        self.assertFalse((Path(self.tmp.name) / "latest").exists())
        response = self.client.get(self.url)
        self.assertContains(response, "Next complete run")
        self.assertContains(response, history[0]["version"])

    def test_invalid_upload_is_visible_and_not_selected(self):
        self.authorize()
        response = self.upload(content=b"invalid")
        self.assertEqual(response.status_code, 400)
        self.assertContains(response, "two nonempty columns", status_code=400)
        self.assertEqual(ReferenceDataStore().history(), [])

    def test_normalization_overflow_is_visible_and_not_staged(self):
        self.authorize()
        content = b"INFO-0-1-6,informatics"
        with patch(
                "dawgpath_data_pipeline.utilities.reference_data."
                "MAX_UPLOAD_BYTES", len(content)):
            response = self.upload(content=content)
        self.assertContains(
            response, "Normalized CSV must be at most 10 MiB.",
            status_code=400)
        self.assertEqual(ReferenceDataStore().history(), [])
        self.assertFalse((Path(self.tmp.name) / "reference-data").exists())

    def test_csrf_is_required_not_inherited_from_proxy(self):
        client = Client(enforce_csrf_checks=True)
        self.authorize(client)
        self.assertEqual(self.upload(client).status_code, 403)
        client.get(self.url)
        token = client.cookies["csrftoken"].value
        self.assertEqual(self.upload(
            client, csrfmiddlewaretoken=token).status_code, 302)

    def test_storage_error_is_visible(self):
        self.authorize()
        with patch.object(ReferenceDataStore, "upload", side_effect=OSError):
            response = self.upload()
        self.assertContains(response, "not confirmed", status_code=503)
