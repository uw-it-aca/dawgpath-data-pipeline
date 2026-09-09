## Plan: Pipeline Runner Direction

Recommended approach: move DawgPath ETL toward a dedicated Python orchestration model, preferably Dagster, with object-storage artifact delivery. Do not reuse the existing Airflow Google Cloud project, but use it as a reference architecture for worker separation, scheduler behavior, logging, secrets, operational runbooks, and artifact delivery. Keep the current DataJob classes as the initial execution surface, but wrap them in explicit ops/assets, add run state and artifact metadata, and stop treating Django as the runner. Django can remain only if the admin UI is needed for app-specific controls.

**Preflight Cleanup Before Runner Work (COMPLETED)**
1. [x] Remove the unsafe page-render execution path in `dawgpath_pipeline_admin/views/pages.py`; no HTTP request should delete/reload ETL tables.
2. [x] Get the test environment reproducible: clean virtualenv/container install, resolve missing `commonconf`, confirm declared package names, and run the existing compile/test commands.
3. [x] Reconcile packaging/runtime declarations before adding Dagster: ensure `setup.py`/requirements install both `dawgpath_data_pipeline` and any optional admin package correctly, and pick a supported Python/Django/container version.
4. [x] Normalize obvious public names that would leak into commands/assets/docs, especially `get_db_implementation`, `build_major_dec_grade_distro.py`, `major_counts`, `test_gpa_distro.py`, and `test_utilities.py`.
5. [x] Split job concerns before orchestration: make each job expose a clear `run()` contract returning `JobResult` with row-count/result metadata, and ensure no hidden saves of `None` in `FetchSWSCourseData.run()`.
6. [x] Add fixture/snapshot coverage for current export outputs (`course_export.json`, `curric_export.json`, `major_export.json`, pickles) before changing delivery or internals.
7. [x] Identify destructive jobs and mark their safety strategy: added `_atomic_replace()` for single-transaction table updates, atomic `.tmp` file replaces for exports, and documented staging table strategy in `docs/job-safety-audit.md`.
8. [x] Clean repository hygiene separately from code behavior: updated `.gitignore` and removed generated static assets and `pyvenv.cfg` from git tracking.

**Steps**


1. [x] Remove unsafe HTTP-triggered job execution from the Django page path before any deployment. The current `PageView.get_context_data()` path runs `FetchCourseData().run()` while rendering a page. (Completed during Preflight 1).
2. [ ] Choose orchestration model and hosting model:
   - Recommended: Dagster for DAG/assets, schedules, retries, run history, logs, and built-in web UI.
   - Host Dagster as its own Flux-managed workload set: webserver/control plane, daemon, run workers, Postgres metadata DB, and GCS/object-storage artifact target.
   - Model worker pod design after the existing UWDP Airflow/LRS pod templates where useful, especially EDW sidecars, worker sizing tiers, ServiceAccounts, ExternalSecrets, and network-access patterns; do not share its scheduler, workers, metadata DB, queues, or deployment lifecycle.
   - Use the shared `django-container` only for the SAML-authenticated admin/gateway if needed; do not force Dagster workers into the Django management-daemon pattern unless platform policy requires it.
   - Simpler fallback: Django management commands plus Kubernetes CronJobs if platform constraints reject Dagster.
   - Avoid: hand-rolled Django cron/status tracking as the primary design.
3. Define pipeline assets around current outputs:
   - source refreshes: course, curriculum, prereq, major, SR major, registration, regis-major, transcript, SWS course
   - derived assets: prereq graphs, curriculum prereq lists/graphs, concurrent courses, student model, common courses/majors, GPA distributions
   - exported artifacts: course JSON, curriculum JSON, major JSON, legacy pickle artifacts only while consumers still require them
4. Add artifact delivery via Google Cloud Storage or equivalent object storage:
   - write versioned artifacts under run IDs/timestamps
   - publish a small manifest with checksums, row counts, schema version, and current pointers
   - make consuming apps read `current` artifacts or a manifest URL instead of receiving committed files
5. Make destructive jobs production-safe:
   - use staging tables, run IDs, or transaction boundaries
   - avoid delete-then-load windows becoming visible to exports
   - add idempotency and concurrency guards
6. Add operational metadata:
   - per-job status, start/end time, row counts, upstream source, output artifact URI, exception details
   - preserve `MINIMUM_DATA_COUNT = 8` behavior and document it as a privacy/data-release rule
7. Deploy in phases:
   - keep `dawgpath-data-pipeline` as the application/source repo that builds the Dagster code image and optional Django admin/gateway image
   - prefer adding DawgPath Dagster infrastructure under the existing Flux desired-state repos/paths, likely `gcp-flux-dev` and `gcp-flux-prod`, because creating a dedicated Flux repo such as `gcp-flux-dawgpath` would likely require platform/GKE tenant wiring by the team that owns the cluster integration; only pursue a new Flux repo if platform owners require isolation
   - use the shared `django-container` as the natural base for the SAML-authenticated Django admin/gateway because it already matches UW-IT ACA auth, static, Nginx/Gunicorn, Vault/External Secrets, and Flux deployment patterns
   - evaluate a separate pipeline-runner image for Dagster workers/daemon if the Django base image adds unnecessary web-server assumptions or makes worker lifecycle harder
   - local Dagster dev runner wrapping current classes
   - test/staging deployment with object-storage delivery
   - production schedule after output snapshots match known behavior
8. Keep Django optional:
   - if retained, use it for a lightweight domain/admin surface that links to orchestrator status or reads run metadata
   - use Django with `uw-django-saml2` as the UW SSO front door if browser access must use existing SAML SSO
   - do not build core scheduling/retry/state tracking in Django unless required by platform constraints
9. Add an authentication boundary:
   - expose public/browser traffic only through a Django SAML-authenticated admin/gateway app or an ingress-level SSO proxy if platform-supported
   - keep Dagster/runner webserver and worker APIs internal to the namespace or cluster
   - map UW groups/access groups to roles such as view runs, launch runs, cancel runs, and administer schedules
   - store SAML certificates and service credentials through Vault/External Secrets; document Secret names and keys only, never values

**Repo Documentation Handoff**
- Target file to create in the data-pipeline repo: `/home/devights/devel/dawgpath-data-pipeline/docs/pipeline-runner-plan.md`.
- Purpose: shareable architecture plan for replacing ad hoc/manual ETL execution with a Dagster-based runner, UW SSO/admin boundary, Flux deployment strategy, and GCS artifact delivery.
- Also update `/home/devights/devel/dawgpath-data-pipeline/README.md` to add this file to the Documentation list.
- Keep this as documentation only; no runtime code changes in the same commit unless explicitly requested.

**Relevant files**

- `/home/devights/devel/dawgpath-data-pipeline/dawgpath_data_pipeline/jobs/__init__.py` — current `DataJob` session/save/delete base.
- `/home/devights/devel/dawgpath-data-pipeline/docs/jobs.md` — inferred current workflow order and job catalog.
- `/home/devights/devel/dawgpath-data-pipeline/docs/known-issues.md` — production blockers and immediate fixes.
- `/home/devights/devel/dawgpath-data-pipeline/dawgpath_pipeline_admin/views/pages.py` — unsafe page-render job trigger.
- `/home/devights/devel/dawgpath-data-pipeline/dawgpath_pipeline_admin/context_processors.py` — current commented SAML user/signout placeholders.
- `/home/devights/devel/copilot-knowledge/docs/architecture.md` — target UW-IT ACA GKE/Flux deployment model.
- `/home/devights/devel/copilot-knowledge/docs/authentication.md` — shared UW SAML/OIDC authentication guidance.
- `/home/devights/devel/copilot-knowledge/docs/jobs.md` — shared background job expectations.
- `/home/devights/devel/copilot-knowledge/inventory/deployments/repos/gcp-flux-uwdp.yaml` — Airflow deployment inventory, including Deployment, Ingress, ExternalSecret, ServiceAccount, Role/RoleBinding, StatefulSet, and Airflow/Postgres/git-sync/statsd/NFS images.
- `/home/devights/devel/copilot-knowledge/inventory/deployments/repos/uwdp-lrs-dags.yaml` — Airflow worker pod-template inventory with EDW/HANA sidecar worker pod specs, `uw-ssh-client`, and `openvpn-client-sidecar` references.
- `/home/devights/devel/copilot-knowledge/docs/deployment-patterns.md` — generated org deployment patterns showing CronJob, Deployment, ExternalSecret, ServiceAccount, scheduler, and worker pod-template usage.

**Verification**
1. Run the current tests in a clean environment after dependencies are installed.
2. Add fixture/snapshot tests for every exported artifact before replacing delivery.
3. Run the full pipeline against a disposable database and compare artifacts to known-good hand-generated outputs.
4. Validate object-storage writes with checksums and manifest resolution.
5. Deploy to test cluster and verify orchestrator UI run state, retries, logs, and failed-run alerts.

**Decisions**
- Recommend Dagster over Django cron/Celery for this project because the primary need is ETL orchestration with a status UI, dependencies, retries, assets, and artifact materialization.
- Do not reuse the existing Airflow Google Cloud project, but inspect it as the closest internal reference for how UW runs scheduled workers, handles secrets, exposes logs, and operationalizes data jobs.
- Prefer GCS/object storage delivery over committing generated files into a consuming repository.
- Preserve existing `DataJob` behavior first; refactor internals only after behavior and artifacts are locked down.

**Further Considerations**
1. If UW-IT platform policy strongly prefers the shared Django chart only, use Django management commands plus Kubernetes CronJobs as the first production step, and accept that monitoring will be thinner unless a separate dashboard is built.
2. If this pipeline grows into cross-app data products, Dagster asset definitions and object-storage manifests will scale better than command-only jobs.
