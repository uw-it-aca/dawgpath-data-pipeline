from django.urls import include, re_path

urlpatterns = [
    re_path(r'^', include('prereq_data_pipeline.urls')),
]