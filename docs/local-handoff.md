# DawgPath Local Handoff / Resume Notes

## Current status
- Dagster orchestration is implemented in `dawgpath_data_pipeline/orchestration/`.
- Asset groups (pipeline stages): `source_refreshes` -> `derived_assets` -> `published_artifacts`. These are independent of the `TIER_*_K8S_TAGS` in `orchestration/tags.py`, which are per-asset pod sizing only.
- Jobs (no single job is a full refresh):
  - `enrollment_history_refresh` (quarter-partitioned) - stage 1 of a full refresh.
  - `catalog_and_analytics_refresh` - stage 2; every non-history asset, catalog/SWS fetch through derived builds to exports. Reads the history tables rather than refreshing them.
  - `catalog_refresh` - cheap catalog-only subset of stage 2, for between full refreshes.
  - `sws_course_refresh` - single SWS fetch, for throttled manual runs.
  - `export_artifacts` - re-export published files from existing tables only.
  - `sync_history_quarters` - maintenance; rolls the quarter partition list to the current lookback window. Run after a fresh deploy and before a manual backfill.
- Schedules: `monthly_full_refresh` (04:00 on the 1st; one `enrollment_history_refresh` run per quarter) and `weekly_catalog_refresh` (05:00 Sundays), both shipped stopped.
- Sensor: `full_refresh_after_history` launches `catalog_and_analytics_refresh` once every quarter in the latest history batch has succeeded; shipped stopped.
- Failure capture: `run_failure_capture` (`orchestration/monitors.py`) ships **running** and logs one structured line per failed run - job, run id, partition, batch, failed step keys, and a `suspected OOM` flag when the worker was SIGKILLed rather than raising. Every materialization also records `peak_rss_mib`, so you can compare actual peaks against the daemon memory limit.

### Running a full fetch -> build -> export
- Scheduled: start both `monthly_full_refresh` and `full_refresh_after_history`. The schedule tags its runs `dawgpath/history_batch=<tick date>`; the sensor waits for all quarters in that batch, then launches `catalog_and_analytics_refresh`.
- Manual: run `sync_history_quarters`, then backfill `enrollment_history_refresh` across all quarters from the UI. The sensor treats the `dagster/backfill` id as a batch, so with the sensor started the backfill chains into `catalog_and_analytics_refresh` automatically. Without the sensor, launch that job by hand once the backfill is green.
- Partial backfills never chain: the sensor requires every quarter in the current lookback window to have a successful finished run.
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
3. Validate that Dagster job selection (`catalog_refresh`, `catalog_and_analytics_refresh`) still resolves correctly.
4. Review remaining TODOs for artifact publisher integration and metadata extraction if there are follow-up changes.
5. Only then proceed to test/staging deployment and eventual production deploy.
