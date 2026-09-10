# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

SECRET_KEY = "test-secret-key-for-admin-tests"
ROOT_URLCONF = "dawgpath_pipeline_admin.urls"
LOGIN_URL = "/saml/login"
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}
INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django.contrib.sessions",
    "dawgpath_pipeline_admin",
]
MIDDLEWARE = [
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
]
TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [],
        },
    }
]
