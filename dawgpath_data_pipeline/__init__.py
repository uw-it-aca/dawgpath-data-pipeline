# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from commonconf.backends import use_configuration_backend

from dawgpath_data_pipeline.conf.settings import AppSettings as settings  # noqa

use_configuration_backend("dawgpath_data_pipeline.conf.settings.AppSettings")

MINIMUM_DATA_COUNT = 8
