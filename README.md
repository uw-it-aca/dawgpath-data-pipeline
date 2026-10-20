# DawgPath Data Pipeline

![Tests](https://github.com/uw-it-aca/dawgpath-data-pipeline/actions/workflows/cicd.yml/badge.svg?branch=main) [![Coverage Status](https://coveralls.io/repos/github/uw-it-aca/dawgpath-data-pipeline/badge.svg)](https://coveralls.io/github/uw-it-aca/dawgpath-data-pipeline)

Data pipeline for DawgPath and the Prereq Map application.

This repository is currently in an in-progress refactor from manually run ETL scripts toward a deployable application/job structure. The existing ETL behavior is still implemented in `dawgpath_data_pipeline` as SQLAlchemy-backed Python job classes. The Django/Vue admin shell exists, but it is not yet a production job runner.

## Documentation

- [Current state](docs/current-state.md): branch status, architecture snapshot, validation status, and deployment fit.
- [Pipeline runner plan](docs/pipeline-runner-plan.md): architecture plan for Dagster orchestrator, GCS artifact delivery, and SAML SSO boundary.
- [ETL job catalog](docs/jobs.md): current jobs, inputs, outputs, dependencies, and inferred workflow order.
- [Job safety and transaction audit](docs/job-safety-audit.md): destructive execution audit, transaction strategies, and staging table plan.
- [Privacy & data release policy](docs/privacy-data-release.md): `MINIMUM_DATA_COUNT = 8` privacy threshold rules and enforcement.
- [Operations and deployment notes](docs/operations.md): local operation, cluster fit, and suggested lockdown plan.
- [EDW SSH tunnel reliability](docs/edw-ssh-tunnel.md): failure symptoms, reconnect fix, verification, and diagnosis.
- [Dagster UI WebSocket plan](docs/dagster-websocket-plan.md): deprioritized nginx `auth_request` approach for live log streaming behind SAML; Dagster's built-in HTTP-polling fallback already covers this.
- [Known issues and immediate fixes](docs/known-issues.md): items to address before deployment or deeper refactoring.

## Dagster Jobs and Automations

Dagster definitions live in `dawgpath_data_pipeline/orchestration/`. A complete
history-backed refresh is two jobs:

1. `stage_1_refresh_enrollment_history` refreshes enrollment, major-declaration,
   and transcript history, with one partitioned run per quarter.
2. `stage_2_refresh_catalog_analytics_and_exports` refreshes the non-history
   sources, rebuilds analytics, and publishes the exports. It uses the history
   already loaded by stage 1.

The other registered jobs are narrower/manual actions:

- `refresh_catalog_only` refreshes catalog data and its selected derived
  artifacts; the weekly schedule uses this instead of running the full stage 2.
- `refresh_sws_course_descriptions` fetches missing SWS descriptions and can be
  run separately when those requests need to be controlled.
- `reexport_published_artifacts` republishes artifact files from existing
  tables without fetching or rebuilding.
- `sync_history_partitions` updates the quarter partitions used by stage 1.

Automations:

- `monthly_history_refresh` starts stage 1 at 04:00 on the first of each month.
- `launch_stage_2_after_history` starts stage 2 after every quarter in the
  latest scheduled batch or full manual backfill has succeeded.
- `weekly_catalog_refresh` runs `refresh_catalog_only` at 05:00 on Sundays.
- `run_failure_capture` logs details about failed runs, including failed steps
  and likely out-of-memory kills.

The monthly and weekly schedules and `launch_stage_2_after_history` are shipped
stopped; enable the monthly schedule and its sensor together to automate the
full refresh. `run_failure_capture` is running by default. For a manual full
refresh, run `sync_history_partitions`, backfill stage 1 across all quarters,
and then let the sensor launch stage 2 or launch stage 2 manually. If history is
already current, stage 2 can be run on its own. `reexport_published_artifacts`
is useful when only republishing is needed.

## Reference data uploads

Pipeline users in `DAGSTER_ACCESS_GROUP` can upload COI and career-center CSVs
at `/reference-data/` on the pipeline host. This page is separate from the
Dagster proxy and uses SAML authentication, group authorization, and CSRF
protection.

- **COI:** UTF-8 CSV with the header `course_id,score`. IDs such as `INFO 201`
  must be unique. Scores must be finite and between -5 and 5 (the existing
  scale includes negative values); -1 is the missing-score sentinel. Each of
  levels 100, 200, 300 and 400 needs a non-sentinel score for Pathways' aggregate
  calculations.
- **Career mapping:** UTF-8 CSV without a header, with a unique credential code
  and career slug per row, e.g. `INFO-0-1-6,informatics`.
- Both formats reject empty rows, duplicate IDs, extra columns and surrounding
  whitespace, and are limited to 10 MiB / 100000 data rows. A UTF-8 BOM and CRLF
  line endings are accepted and normalized before checksumming/storage.

Validated uploads are versioned and staged, **not immediately published**.
The newest valid upload of each type committed before a complete export run
was created is selected for that run. Later uploads apply to later runs.
Run reexecution retains the root run's selection. Upload history displays the
uploader, creation time, version, checksum, row count, and next selected versions.

Stage 2 and `reexport_published_artifacts` now include `pin_reference_data` and
`export_reference_data`. They publish `coi_scores.csv` and
`admit_major_mapping.csv` alongside the generated exports, retaining the
selected upload metadata in `reference-data/runs/<root-run-id>.json` and
Dagster asset metadata. The weekly catalog-only job remains a partial export
and does not publish reference files. Bootstrap **both** uploads before starting
a complete export; missing files fail the reference export explicitly.

Storage uses the existing `GCS_BUCKET_NAME` and Application Default Credentials.
Uploads live under `reference-data/uploads/`; metadata JSON is the immutable
commit marker and CSV bytes are verified against their recorded SHA-256.
The UI service needs object create/list/read access there, and Dagster workers
need list/read plus create/read under `reference-data/runs/`. Do not configure
deletion/overwrite access for these records; retain referenced uploads for
reexecution and future release rollback.

Without a bucket, uploads/snapshots use `ARTIFACT_ROOT` (default `artifacts`).
Local Compose shares `/app/artifacts` between the UI and Dagster containers;
configure `DAGSTER_ACCESS_GROUP` and a matching authenticated SAML/mock session
to use the upload page. Configured GCS failures never fall back to local files.

This is feature 1 of the GCS consumer work: it does not yet introduce an atomic
complete-release manifest, a prebuilt search index, or changes to Pathways.
The existing mutable `latest/manifest.json` is still not a complete-release
boundary and must not be used as such. Cross-checking reference IDs against a
specific generated catalog belongs in the complete-release finalizer; upload
validation currently establishes CSV structure and import prerequisites only.
Pathways' Azure similar-majors refresh is unchanged.

Before bootstrapping, correct the conflicting duplicates in the legacy COI
file: investigation found 18 duplicated course IDs with different scores.
Uploads deliberately reject these rather than silently picking a winner.

## Historical validation snapshot

As of 2026-09-09, `python -m compileall dawgpath_data_pipeline/` passes. The historical test runner, `python dawgpath_data_pipeline/test.py -v`, fails in the active local environment before tests run because `commonconf` is not installed.
