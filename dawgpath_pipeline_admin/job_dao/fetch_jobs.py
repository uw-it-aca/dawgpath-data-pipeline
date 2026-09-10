# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from dawgpath_data_pipeline.jobs.fetch_course_data import FetchCourseData

# from dawgpath_data_pipeline.conf.settings import AppSettings as settings
# from commonconf.backends import use_configuration_backend
# # setup app settings
# use_configuration_backend('dawgpath_data_pipeline.conf.settings.AppSettings')


def fetch_course_data():
    FetchCourseData().run()
