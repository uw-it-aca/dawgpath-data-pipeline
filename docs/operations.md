# Operations and Deployment Notes

Last reviewed: 2026-09-09

This project is not currently ready to run as a scheduled production job on the cluster without additional work. The ETL behavior exists, but the operational boundary is still implicit Python class calls rather than a stable command interface.

## Local Operation

The historical test entry point is:

```bash
python dawgpath_data_pipeline/test.py -v
```

The test runner configures `commonconf` from `dawgpath_data_pipeline/conf/test.conf` and then runs `nose2` discovery.

The current active shell validation on 2026-09-09 passed compilation but failed test startup because `commonconf` was missing from the active environment. Install project dependencies into the selected Python environment before using the test command.

The Docker Compose file defines:

- `app`: Django app container built from the `app-container` target.
- `postgres`: local Postgres 13.4.

The Compose app service only passes `GOOGLE_ANALYTICS_KEY` explicitly. ETL database and EDW/SWS configuration still need to be provided through the app's settings mechanism before live fetch jobs can run.

## Cluster Fit

The shared UW-IT ACA architecture uses GitHub Actions, Google Artifact Registry, Helm rendering through the shared Django production chart, and Flux reconciliation into GKE. Background jobs normally run as Django management commands, management-daemon workloads, or Kubernetes CronJobs described by deployment values.

This project currently has a Django container shell, but its ETL jobs are not exposed as management commands. A production job deployment should add an explicit runner first, then schedule the runner through the shared chart. Recommended shape:

- one command entry point for the full refresh workflow
- optional command entry points for individual jobs or job groups
- non-HTTP execution through Kubernetes Job/CronJob resources
- clear environment-specific configuration for EDW, SWS, and the target database
- logs that identify job name, upstream source, row counts, elapsed time, and failure cause
- a lock or concurrency guard so two full refreshes cannot overlap

## Production Safety Requirements

Before scheduling this pipeline, verify or add:

- An explicit command runner that does not depend on page rendering.
- Dry-run or test-mode coverage for each fetch/build/export phase.
- Ordered workflow orchestration with failure handling between destructive delete and replacement load.
- A staging database refresh path that can be compared against current production output.
- Dependency installation that matches the declared Python/Django/runtime versions.
- Secrets and service credentials supplied through Vault/External Secrets rather than committed configuration.
- Monitoring and alerting for failed jobs, stale output, row-count anomalies, and upstream connection failures.

## Suggested Lockdown Plan

To preserve current behavior before larger refactoring:

1. Get the existing test suite running in a clean environment.
2. Add tests around job ordering and output shape for each exported JSON/pickle artifact.
3. Capture fixture-based snapshots for representative course, curriculum, major, registration, transcript, and SWS data.
4. Add a command runner that invokes existing classes without changing job internals.
5. Run the command runner against a disposable local database and compare exported artifacts with known-good outputs.
6. Only then refactor internals toward Django management commands or a better orchestration layer.
