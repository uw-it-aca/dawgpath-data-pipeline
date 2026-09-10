import os
import django

os.environ.setdefault(
    "DJANGO_SETTINGS_MODULE", "dawgpath_pipeline_admin.test_settings")
django.setup()
