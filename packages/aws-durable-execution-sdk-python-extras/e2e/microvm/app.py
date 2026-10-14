# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The application inside the MicroVM image that the end-to-end test builds.

Lambda starts it at image build and snapshots it. The run hook then delivers
each job. The routes and the results are the same as those of the JavaScript
SDK's ``extras/e2e/microvm-app.ts``.
"""

from __future__ import annotations

import json
import logging
import os
import platform
import threading
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from aws_durable_execution_sdk_python_microvm_worker import (
    MicrovmJobContext,
    start_microvm_worker,
)


SESSION_DIR = Path("/tmp/session")  # noqa: S108
STARTED = time.monotonic()

# Only the process memory holds this value. A job that reads it back after a
# suspend shows that the resume restored the memory.
remembered: dict[str, str] = {}

# Counts the resume lifecycle hooks that the worker logged. A job that reports
# a count above zero shows that the service called the hook after the worker
# suspended its own MicroVM.
resume_hooks = 0


class CountingLogger:
    """The worker's logger, which also counts the resume hooks."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def info(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        global resume_hooks  # noqa: PLW0603
        if msg == "lifecycle hook" and (extra or {}).get("hook") == "resume":
            resume_hooks += 1
        self._logger.info(msg, *args, extra=extra)

    def warning(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        self._logger.warning(msg, *args, extra=extra)

    def error(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        self._logger.error(msg, *args, extra=extra)


def run_job(job: dict[str, Any], context: MicrovmJobContext) -> dict[str, Any]:
    context.logger.info("job received")
    if job.get("mode") == "crash":
        # Exits after the first heartbeat. The durable function must then fail
        # on the heartbeat timeout, well before the callback timeout.
        threading.Timer(2.0, lambda: os._exit(1)).start()
        threading.Event().wait()
    context.cancelled.wait(job.get("sleepSeconds", 0))
    if job.get("mode") == "fail":
        msg = f"job {job.get('label')} failed on purpose"
        raise RuntimeError(msg)
    return {
        "label": job.get("label"),
        "microvmId": context.microvm_id,
        "python": platform.python_version(),
        # The length of the job as JSON, in characters, as the JavaScript app
        # measures it.
        "inputLength": len(json.dumps(job, separators=(",", ":"), ensure_ascii=False)),
        # Echoed so that a test can compare what the MicroVM received.
        "padding": job.get("padding"),
        "finishedAt": datetime.now(UTC).isoformat(),
    }


# A session writes a file with one job and reads it with the next.
def write(job: dict[str, Any], context: MicrovmJobContext) -> dict[str, Any]:
    SESSION_DIR.mkdir(parents=True, exist_ok=True)
    (SESSION_DIR / job["key"]).write_text(job["value"])
    return {"microvmId": context.microvm_id}


def read(job: dict[str, Any], context: MicrovmJobContext) -> dict[str, Any]:
    return {
        "microvmId": context.microvm_id,
        "value": (SESSION_DIR / job["key"]).read_text(),
    }


def remember(job: dict[str, Any], context: MicrovmJobContext) -> dict[str, Any]:
    remembered.update(value=job["value"], storedAt=datetime.now(UTC).isoformat())
    return {"microvmId": context.microvm_id, "pid": os.getpid()}


def recall(_job: dict[str, Any], context: MicrovmJobContext) -> dict[str, Any]:
    return {
        "microvmId": context.microvm_id,
        "pid": os.getpid(),
        "uptimeSeconds": round(time.monotonic() - STARTED),
        "resumeHooks": resume_hooks,
        **remembered,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # The worker runs `handler` for a job that names no route: a small job
    # arrives in the run hook, and a large one over HTTP on the worker's
    # default job path. The /job route receives jobs that name it.
    worker = start_microvm_worker(
        handler=run_job,
        routes={
            "/job": run_job,
            "/write": write,
            "/read": read,
            "/remember": remember,
            "/recall": recall,
        },
        logger=CountingLogger(logging.getLogger("microvm-worker")),
    )
    worker.wait()
