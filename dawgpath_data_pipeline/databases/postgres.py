# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import numpy
from commonconf import settings
from psycopg2.extensions import adapt, register_adapter
from sqlalchemy import create_engine

from dawgpath_data_pipeline.databases import Database

URL_PATTERN = "postgresql://{username}:{password}@{host}:{port}/{database}"

NUMPY_SCALAR_TYPES = (
    numpy.bool_,
    numpy.int8, numpy.int16, numpy.int32, numpy.int64,
    numpy.float16, numpy.float32, numpy.float64,
)


def _adapt_numpy_scalar(value):
    # NumPy 2 reprs scalars as "np.float64(1.0)", which psycopg2 emits as SQL.
    return adapt(value.item())


for _numpy_type in NUMPY_SCALAR_TYPES:
    register_adapter(_numpy_type, _adapt_numpy_scalar)


class Postgres(Database):
    engine = None
    url = ""

    def __init__(self):
        echo = getattr(settings, "DB_DEBUG", None) == "True"
        self.url = URL_PATTERN.format(
            username=settings.DB_USER,
            password=settings.DB_PASSWORD,
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            database=settings.DB_DATABASE
        )
        self.engine = create_engine(self.url,
                                    echo=echo)
