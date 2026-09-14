# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from django.conf import settings


def google_analytics(request):
    return {"google_analytics": getattr(settings, "GOOGLE_ANALYTICS_KEY", " ")}


def django_debug(request):
    return {"django_debug": getattr(settings, "DEBUG", False)}
