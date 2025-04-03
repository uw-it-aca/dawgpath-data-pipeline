from dawgpath_data_pipeline.jobs.fetch_course_data import FetchCourseData
# from dawgpath_data_pipeline.conf.settings import AppSettings as settings  # noqa
# from commonconf.backends import use_configuration_backend
# # setup app settings
# use_configuration_backend('dawgpath_data_pipeline.conf.settings.AppSettings')


def fetch_course_data():
    FetchCourseData().run()
