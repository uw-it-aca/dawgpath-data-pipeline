# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import requests
from django.conf import settings
from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.exceptions import PermissionDenied
from django.http import StreamingHttpResponse
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt

# The webserver binds to loopback in the same pod, so this proxy is its only
# reachable entrance and SAML stays the sole authentication boundary.
DAGSTER_BASE_URL = "http://127.0.0.1:3000"

# https://datatracker.ietf.org/doc/html/rfc2616#section-13.5.1
HOP_BY_HOP_HEADERS = frozenset([
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-encoding",
    "content-length",
])

STREAM_CHUNK_SIZE = 8192


def has_dagster_access(request):
    access_group = getattr(settings, "DAGSTER_ACCESS_GROUP", None)
    if not access_group:
        return True
    saml_data = request.session.get("samlUserdata", {})
    return access_group in saml_data.get("isMemberOf", [])


@method_decorator(csrf_exempt, name="dispatch")
class DagsterProxyView(LoginRequiredMixin, View):
    """SAML-authenticated reverse proxy to the in-pod Dagster webserver."""

    http_method_names = ["get", "post", "head", "options"]

    def dispatch(self, request, *args, **kwargs):
        if request.user.is_authenticated and not has_dagster_access(request):
            raise PermissionDenied("Not authorized for the Dagster UI")
        return super().dispatch(request, *args, **kwargs)

    def _proxy(self, request):
        base_url = getattr(
            settings, "DAGSTER_BASE_URL", DAGSTER_BASE_URL).rstrip("/")
        headers = {
            key: value for key, value in request.headers.items()
            if key.lower() not in HOP_BY_HOP_HEADERS
        }

        upstream = requests.request(
            method=request.method,
            url=f"{base_url}{request.get_full_path()}",
            headers=headers,
            data=request.body or None,
            allow_redirects=False,
            stream=True,
            timeout=getattr(settings, "DAGSTER_PROXY_TIMEOUT", 30),
        )

        response = StreamingHttpResponse(
            upstream.iter_content(chunk_size=STREAM_CHUNK_SIZE),
            status=upstream.status_code,
            content_type=upstream.headers.get("content-type"),
        )
        for key, value in upstream.headers.items():
            if key.lower() not in HOP_BY_HOP_HEADERS | {"content-type"}:
                response[key] = value
        return response

    def get(self, request, *args, **kwargs):
        return self._proxy(request)

    def post(self, request, *args, **kwargs):
        return self._proxy(request)

    def head(self, request, *args, **kwargs):
        return self._proxy(request)

    def options(self, request, *args, **kwargs):
        return self._proxy(request)
