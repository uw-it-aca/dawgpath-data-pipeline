from commonconf.backends import use_configparser_backend
use_configparser_backend("conf/app.conf", "PDP-Settings")

from logging.config import fileConfig # nopep8
from alembic import context # nopep8
from prereq_data_pipeline.databases.implementation import get_db_implemenation # nopep8
from prereq_data_pipeline.models.base import Base

# models must me imported for autogenerate to detect changes
from prereq_data_pipeline.models.curriculum import Curriculum
from prereq_data_pipeline.models.course import Course
from prereq_data_pipeline.models.prereq import Prereq
from prereq_data_pipeline.models.graph import Graph
from prereq_data_pipeline.models.registration import Registration
from prereq_data_pipeline.models.concurrent_courses import ConcurrentCourses
from prereq_data_pipeline.models.gpa_distro import GPADistribution
from prereq_data_pipeline.models.major import Major

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
    raise NotImplemented()



def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = get_db_implemenation().engine

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
