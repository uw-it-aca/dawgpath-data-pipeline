import logging
from dawgpath_data_pipeline.conf.settings import AppSettings as settings  # noqa
from commonconf.backends import use_configuration_backend
# setup app settings
use_configuration_backend('dawgpath_data_pipeline.conf.settings.AppSettings')

MINIMUM_DATA_COUNT = 8
