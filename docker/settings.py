from .base_settings import *

INSTALLED_APPS += [
    "dawgpath_pipeline_admin.apps.AppNameConfig",
]

# If you have file data, define the path here
# DATA_ROOT = os.path.join(BASE_DIR, "dawgpath_pipeline_admin/data")

GOOGLE_ANALYTICS_KEY = os.getenv("GOOGLE_ANALYTICS_KEY", default=" ")

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "APP_DIRS": True,
        "OPTIONS": {
            "debug": True,
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
                "dawgpath_pipeline_admin.context_processors.google_analytics",
                "dawgpath_pipeline_admin.context_processors.django_debug",
                # "dawgpath_pipeline_admin.context_processors.auth_user",
            ],
        },
    }
]

if os.getenv("ENV") == "localdev":
    DEBUG = True
    VITE_MANIFEST_PATH = os.path.join(
        BASE_DIR, "dawgpath_pipeline_admin", "static", "manifest.json"
    )
else:
    VITE_MANIFEST_PATH = os.path.join(os.sep, "static", "manifest.json")
