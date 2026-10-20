# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import logging

from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.exceptions import PermissionDenied
from django.shortcuts import redirect, render
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_protect
from google.api_core.exceptions import GoogleAPIError
from google.auth.exceptions import GoogleAuthError

from dawgpath_data_pipeline.utilities.reference_data import ReferenceDataStore
from dawgpath_pipeline_admin.forms import ReferenceUploadForm
from dawgpath_pipeline_admin.views.dagster import has_dagster_access

logger = logging.getLogger(__name__)


@method_decorator(csrf_protect, name="dispatch")
class ReferenceDataView(LoginRequiredMixin, View):
    def dispatch(self, request, *args, **kwargs):
        if request.user.is_authenticated and not has_dagster_access(request):
            raise PermissionDenied("Not authorized to manage reference data.")
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        return self._render(request, ReferenceUploadForm())

    def post(self, request):
        form = ReferenceUploadForm(request.POST, request.FILES)
        if form.is_valid():
            try:
                ReferenceDataStore().upload(
                    form.cleaned_data["kind"], form.cleaned_data["content"],
                    request.user.get_username())
            except (GoogleAPIError, GoogleAuthError, OSError):
                logger.exception("Reference upload storage failed")
                return self._render(
                    request, form, error="Storage unavailable. The upload was "
                    "not confirmed; check history before retrying.",
                    status=503)
            return redirect("reference-data")
        return self._render(request, form, status=400)

    def _render(self, request, form, error=None, status=200):
        try:
            history = ReferenceDataStore().history()
        except (GoogleAPIError, GoogleAuthError, OSError):
            logger.exception("Reference upload history unavailable")
            history = []
            error = "Storage unavailable; upload history cannot be displayed."
            status = 503
        selected = {}
        for entry in history:
            selected.setdefault(entry["kind"], entry["version"])
        return render(request, "reference_data.html", {
            "form": form, "history": history, "selected": selected,
            "error": error,
        }, status=status)
