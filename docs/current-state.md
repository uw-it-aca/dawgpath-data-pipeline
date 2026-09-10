# DawgPath Data Pipeline Current State

Last reviewed: 2026-09-09

This repository is in an in-progress refactor from the original manually run `prereq_data_pipeline` Python package toward a `dawgpath_data_pipeline` package with a Django administration gateway. The ETL logic is still implemented as plain Python `DataJob` subclasses backed by SQLAlchemy models and Alembic migrations. It has not yet been converted into Django management commands, Kubernetes CronJobs, or a production-ready job runner.

## Branch State

The working branch reviewed here is `feature/web-ui`.

Relative to `origin/develop`, this branch is ahead by these commits:

- `7e422f5` `reafactor wip`
- `bd64943` merge from remote `feature/web-ui`
- `885017f` `adding config files`
- `3f3ce24` `scss deprication fix`
- `fe64ad0` `adding dockerignore`
- `145c6ae` `web ui based on the axdd-django template`
- `90fe5ea` `dawgpath refactor`

It is also one commit behind `origin/develop` (`318c1a9 changes`). The local worktree had only untracked local/editor/environment files at review time: `.idea/`, `bin/`, `dawgpath-data-pipeline.code-workspace`, and `lib64`.

The main branch comparison is noisy because most source files moved from `prereq_data_pipeline/` into `dawgpath_data_pipeline/`. Current branch additions include the Django administration package, Dockerfile, docker-compose setup, new settings locations, and several expanded job/model modules.

## Architecture Snapshot

Current runtime layers:

- `dawgpath_data_pipeline/`: ETL package, SQLAlchemy models, DAO functions, Alembic migrations, tests, and job classes.
- `dawgpath_pipeline_admin/`: Django app shell introduced by the web UI branch.
- `docker/`: Django container settings and lifecycle scripts.

The ETL jobs use `DataJob` from `dawgpath_data_pipeline.jobs`. `DataJob` opens a SQLAlchemy session through `get_db_implemenation()` and provides bulk-save and delete helpers. The database backend is selected with `DB_CLASS` from `commonconf` settings and can be `sqlite3`, `memory`, or `postgres`.

The default application settings currently point to Postgres:

- host: `postgres`
- port: `5432`
- user/password: `postgres` / `postgres`
- database: empty by default

The package initializes `commonconf` in `dawgpath_data_pipeline/__init__.py` and sets `MINIMUM_DATA_COUNT = 8`, which is used to suppress small-count GPA/common-course/concurrency data.

## Data Sources

The ETL code reads from two upstream sources:

- EDW through `dawgpath_data_pipeline.dao.edw`, using `pymssql` and `pandas.read_sql` against `sec.*` tables.
- Student Web Service through `dawgpath_data_pipeline.dao.sws`, using `uw_sws.course.get_course_by_label`.

EDW query functions currently cover:

- registration major declarations since a year
- transcripts since a year
- curriculum management major/program data
- SDB major codes and home URLs
- registrations by year/quarter
- course prerequisites
- course titles
- curriculum code metadata

## Current Execution Model

The job system has been standardized during preflight cleanup:

- All 25 job classes inherit from `DataJob` and expose a consistent `run()` method returning a structured `JobResult` object with `status`, `rows_affected`, and `metadata`.
- Destructive single-table refresh jobs use `_atomic_replace()` to perform in-memory fetch/build before executing `DELETE` and `INSERT` within a single atomic SQLAlchemy transaction with automatic rollback on error.
- Export jobs write to temporary `.tmp` files first and perform atomic `os.replace()` operations to prevent partially rendered files from being published.
- The unsafe Django `PageView.get_context_data()` HTTP job trigger was removed.

## Validation Status

Local validation performed on 2026-09-09:

- Python 3.12 virtual environment configured with clean package installation via `pip install -e .`.
- Full Python test suite (`python -m unittest discover -s dawgpath_data_pipeline/tests`): 72/72 passed (including job contract tests and export JSON/pickle snapshot tests).
- Django admin test suite (`python -m unittest discover -s dawgpath_pipeline_admin/tests`): 3/3 passed.

## Deployment Fit

The included knowledge base describes the target UW-IT ACA deployment model: images built in GitHub Actions, published to Google Artifact Registry, rendered through the shared Django production chart, and reconciled into GKE by Flux.

This branch has started toward that model with a `django-container` based Dockerfile and a Django gateway, but the job system is not yet aligned with the shared background-job pattern. To run reliably on the cluster, the ETL work should be exposed as explicit non-HTTP workloads, probably Django management commands or another single-purpose command runner that the shared chart can schedule as CronJobs or invoke as controlled Jobs.

Do not rely on the current page-render hook as an operational runner.
