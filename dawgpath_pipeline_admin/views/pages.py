# Copyright 2022 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from django.views.generic import TemplateView


class PageView(TemplateView):
    template_name = "index.html"


class DefaultPageView(PageView):
    template_name = "index.html"
