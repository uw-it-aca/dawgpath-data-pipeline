# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import os
import django

os.environ.setdefault(
    "DJANGO_SETTINGS_MODULE", "dawgpath_pipeline_admin.test_settings")
django.setup()
