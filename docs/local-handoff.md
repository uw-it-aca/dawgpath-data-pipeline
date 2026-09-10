# DawgPath Local Handoff / Resume Notes

## Current status
- Dagster orchestration is implemented in `dawgpath_data_pipeline/orchestration/`.
- Asset groups: `source_refreshes`, `derived_assets`, `published_artifacts`.
- Job groups: `daily_catalog_refresh`, `full_pipeline_job`, `publish_artifacts_job`.
- Artifact versioning and manifest publishing are implemented via `dawgpath_data_pipeline/utilities/artifact_publisher.py`.
- Local Dagster dev runner is validated; port 3000 responds with HTTP 200.
- Deployment config is split into `docker/test-values.yml` and `docker/prod-values.yml`.
- GitHub workflow was updated to a Compass-style modern ACA pattern in `.github/workflows/cicd.yml`.
- Generated runtime directories are ignored in `.gitignore`.

## Important decisions / constraints
- Actual production deployment is intentionally deferred until local validation is complete.
- Hostnames, usernames, and other runtime secrets are intentionally not hardcoded into source-controlled values files.
- Secret values should be supplied by External Secrets / HashiCorp Vault via ESO patterns.
- The repo is in a local-dev-ready mode, not a production-deployed mode.

## Verification evidence
- `./venv/bin/dagster dev -h 0.0.0.0 -p 3000 -w workspace.yaml` served HTTP 200 on port 3000.
- `./venv/bin/python -m unittest discover -s dawgpath_data_pipeline/tests` -> 77 tests OK.
- `DJANGO_SETTINGS_MODULE=dawgpath_pipeline_admin.test_settings ./venv/bin/python -m django test dawgpath_pipeline_admin.tests` -> 5 tests OK.

## Files to pay attention to
- `dawgpath_data_pipeline/orchestration/assets.py`
- `dawgpath_data_pipeline/orchestration/jobs.py`
- `dawgpath_data_pipeline/orchestration/definitions.py`
- `dawgpath_data_pipeline/utilities/artifact_publisher.py`
- `dawgpath_data_pipeline/jobs/__init__.py`
- `docker/test-values.yml`
- `docker/prod-values.yml`
- `.github/workflows/cicd.yml`
- `docs/migration-plan.md`
- `docs/privacy-data-release.md`

## Recommended next actions on resume
1. Start the local Dagster dev runner and confirm status in the web UI.
2. Re-run unit tests after checking out on another machine.
3. Validate that Dagster job selection (`daily_catalog_refresh`, `full_pipeline_job`) still resolves correctly.
4. Review remaining TODOs for artifact publisher integration and metadata extraction if there are follow-up changes.
5. Only then proceed to test/staging deployment and eventual production deploy.
