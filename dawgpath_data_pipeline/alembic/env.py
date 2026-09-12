# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from logging.config import fileConfig  # nopep8

from alembic import context  # nopep8

from dawgpath_data_pipeline.databases.implementation import (
    get_db_implementation,  # nopep8
)
from dawgpath_data_pipeline.models.base import Base
from dawgpath_data_pipeline.models.common_course_major import CommonCourseMajor
from dawgpath_data_pipeline.models.concurrent_courses import (
    ConcurrentCourses,
    ConcurrentCoursesMajor,
)
from dawgpath_data_pipeline.models.course import Course

# models must me imported for autogenerate to detect changes
from dawgpath_data_pipeline.models.curriculum import Curriculum
from dawgpath_data_pipeline.models.gpa_distro import GPADistribution
from dawgpath_data_pipeline.models.graph import Graph
from dawgpath_data_pipeline.models.major import Major
from dawgpath_data_pipeline.models.prereq import Prereq
from dawgpath_data_pipeline.models.regis_major import RegisMajor
from dawgpath_data_pipeline.models.registration import Registration
from dawgpath_data_pipeline.models.sr_major import SRMajor
from dawgpath_data_pipeline.models.sws_course import SWSCourse
from dawgpath_data_pipeline.models.transcript import Transcript

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    # url = config.get_main_option("sqlalchemy.url")
    # context.configure(
    #     url=url,
    #     target_metadata=target_metadata,
    #     literal_binds=True,
    #     dialect_opts={"paramstyle": "named"},
    # )
    #
    # with context.begin_transaction():
    #     context.run_migrations()

    # not sure we want/need this
    raise NotImplementedError()



def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = get_db_implementation().engine

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
