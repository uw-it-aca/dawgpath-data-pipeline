# Known Issues and Immediate Fixes

Last reviewed: 2026-09-09

These are documentation-time findings from reading the current `feature/web-ui` branch and running limited local validation. They are not code changes.

## Immediate Fixes Before Any Deployment

1. Remove job execution from Django page rendering.

   `dawgpath_pipeline_admin.views.pages.PageView.get_context_data()` calls `fetch_course_data()`, which calls `FetchCourseData().run()`. Rendering the app page can therefore delete and reload the `Course` table. This must be removed or gated before deploying the web shell anywhere with a real database.

2. Add a real job entry point.

   The ETL jobs are classes, but there is no supported CLI, Django management command, or scheduler wrapper. Cluster deployment should not call class methods ad hoc. Add a command layer that can run the full ordered workflow and individual jobs intentionally.

3. Reconcile packaging and dependency declarations.

   `setup.py` names/packages `dawgpath_pipeline_admin`, while tests and ETL imports depend on `dawgpath_data_pipeline`. The current local test run also failed because `commonconf` was absent from the active environment. Verify editable install behavior in a clean virtual environment or container.

4. Modernize CI/runtime assumptions.

   The GitHub Actions workflow still uses Ubuntu 18.04 and Python 3.6, while `setup.py` declares Django 4.2 and modern dependencies. The Dockerfile uses `django-container:1.4.1`, while the shared architecture docs describe newer `django-container` generations for current apps. Decide the supported runtime before deploying.

5. Fix destructive refresh failure windows.

   Many jobs delete their target table first and then reload. If a job fails after delete, downstream exports can publish empty or partial data. A production runner should load into staging tables, use transactions where practical, or otherwise prevent partially refreshed output from becoming current.

6. Guard long-running and multiprocessing jobs.

   Graph building and student preparation use `multiprocessing.Pool()`. The course graph job comments mention database connection depletion with small chunks. Production execution needs resource limits, pool sizing, connection cleanup, and timeout behavior.

7. Review `FetchSWSCourseData.run()` behavior.

   `_get_sws_courses()` saves chunks internally and returns `None`, while `run()` then calls `_save_sws_course(courses)`. The bulk-save helper currently swallows `TypeError`, so this likely hides the extra save of `None`. Make the method contract explicit and add tests.

8. Fix naming and typo issues before exposing commands.

   Examples include `get_db_implemenation`, `build_major_dec_grade_ditro.py`, `major_courts`, and inconsistent export names. These are tolerable internally but confusing for command names, docs, and operational dashboards.

9. Confirm privacy threshold behavior.

   `MINIMUM_DATA_COUNT = 8` suppresses some aggregate outputs. Lock this down in tests and deployment docs before producing public or app-consumed artifacts.

10. Establish output artifact ownership.

    JSON and pickle export paths are caller-provided and not documented as cluster artifacts. Decide whether exports are written to disk, object storage, another application database, or consumed directly by the DawgPath/Prereq Map app.

## Current Validation Gaps

- The test suite did not run locally because the active environment lacked `commonconf`.
- No live EDW, SWS, or Postgres fetch was attempted.
- No Docker image build was attempted.
- No Alembic migration was run against a clean database during this review.
- No exported artifact was generated or compared against a known-good baseline.

## Refactor Guidance

Continuing the Django refactor is useful only if Django becomes the operational shell for commands, configuration, and deployment. The ETL core does not currently need HTTP request handling. The safest path is to preserve the existing job classes, add a thin command/orchestration layer, lock down tests and fixtures, and defer deeper model/session refactoring until after behavior is captured.
