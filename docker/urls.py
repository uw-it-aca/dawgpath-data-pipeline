from .base_urls import *
from django.conf.urls import include
from django.urls import re_path

urlpatterns += [re_path(r"^", include("dawgpath_pipeline_admin.urls"))]
