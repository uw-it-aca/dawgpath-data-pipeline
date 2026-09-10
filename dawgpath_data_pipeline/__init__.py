# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from dawgpath_data_pipeline.conf.settings import AppSettings as settings  # noqa
from commonconf.backends import (
    use_configuration_backend,
    use_configparser_backend,
)

conf_path = os.path.join(os.path.dirname(__file__), "conf", "app.conf")
if os.path.exists(conf_path):
    use_configparser_backend(conf_path, "PDP-Settings")
else:
    use_configuration_backend(
        "dawgpath_data_pipeline.conf.settings.AppSettings"
    )

MINIMUM_DATA_COUNT = 8
