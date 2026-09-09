# DawgPath Data Pipeline

![Tests](https://github.com/uw-it-aca/dawgpath-data-pipeline/actions/workflows/cicd.yml/badge.svg?branch=main) [![Coverage Status](https://coveralls.io/repos/github/uw-it-aca/dawgpath-data-pipeline/badge.svg)](https://coveralls.io/github/uw-it-aca/dawgpath-data-pipeline)

Data pipeline for DawgPath and the Prereq Map application.

This repository is currently in an in-progress refactor from manually run ETL scripts toward a deployable application/job structure. The existing ETL behavior is still implemented in `dawgpath_data_pipeline` as SQLAlchemy-backed Python job classes. The Django/Vue admin shell exists, but it is not yet a production job runner.

## Documentation

- [Current state](docs/current-state.md): branch status, architecture snapshot, validation status, and deployment fit.
- [ETL job catalog](docs/jobs.md): current jobs, inputs, outputs, dependencies, and inferred workflow order.
- [Job safety and transaction audit](docs/job-safety-audit.md): destructive execution audit, transaction strategies, and staging table plan.
- [Operations and deployment notes](docs/operations.md): local operation, cluster fit, and suggested lockdown plan.
- [Known issues and immediate fixes](docs/known-issues.md): items to address before deployment or deeper refactoring.

## Current Validation Snapshot

As of 2026-09-09, `python -m compileall dawgpath_data_pipeline/` passes. The historical test runner, `python dawgpath_data_pipeline/test.py -v`, fails in the active local environment before tests run because `commonconf` is not installed.
