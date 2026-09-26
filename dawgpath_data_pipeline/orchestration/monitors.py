# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

"""
Failure capture for the DawgPath pipeline.

run_monitoring in dagster.yaml flips runs whose worker died to FAILURE but
records nothing about the cause. This sensor fires on every run failure and
writes one structured line naming the failed steps, the partition, and
whether the process was killed by the kernel rather than raising -- which is
what an OOM looks like here, since all steps run as subprocesses of the
single daemon pod.
"""

from dagster import (
    DagsterEventType,
    DefaultSensorStatus,
    RunFailureSensorContext,
    run_failure_sensor,
)

from dawgpath_data_pipeline.orchestration.partitions import HISTORY_BATCH_TAG

PARTITION_TAG = "dagster/partition"

# A Python exception produces a traceback; a SIGKILL produces one of these.
# The kernel OOM killer and a container memory-limit kill both land here.
OOM_MARKERS = (
    "exited with code -9",
    "signal 9",
    "sigkill",
    "out of memory",
    "oomkilled",
    "run worker process",
)


def _failure_messages(context: RunFailureSensorContext):
    messages = [context.failure_event.message or ""]
    steps = []
    logs = context.instance.all_logs(
        context.dagster_run.run_id,
        of_type={DagsterEventType.STEP_FAILURE,
                 DagsterEventType.ENGINE_EVENT},
    )
    for entry in logs:
        messages.append(entry.user_message or "")
        event = entry.dagster_event
        if event and event.event_type == DagsterEventType.STEP_FAILURE:
            steps.append(event.step_key)
    return messages, steps


def _oom_suspected(messages):
    joined = " ".join(messages).lower()
    return any(marker in joined for marker in OOM_MARKERS)


@run_failure_sensor(
    name="run_failure_capture",
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Logs the failed steps, partition, and probable cause of every run "
        "failure, flagging kills that look like the process running out of "
        "memory rather than raising."
    ),
)
def run_failure_capture(context: RunFailureSensorContext):
    run = context.dagster_run
    messages, steps = _failure_messages(context)
    detail = [f"job={run.job_name}", f"run_id={run.run_id}"]
    if run.tags.get(PARTITION_TAG):
        detail.append(f"partition={run.tags[PARTITION_TAG]}")
    if run.tags.get(HISTORY_BATCH_TAG):
        detail.append(f"batch={run.tags[HISTORY_BATCH_TAG]}")
    detail.append(f"failed_steps={','.join(steps) if steps else 'none'}")

    if _oom_suspected(messages):
        detail.append("probable_cause=worker killed (suspected OOM)")
        detail.append(
            "remediation=check daemon pod memory limit in docker/*-values.yml "
            "and the pool limits in dagster.yaml")
    else:
        detail.append(f"probable_cause={context.failure_event.message}")
    context.log.error("Run failure: " + "; ".join(detail))
