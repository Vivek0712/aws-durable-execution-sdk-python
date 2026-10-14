# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The durable function that the end-to-end test deploys.

Each event selects one scenario, so one deployment covers every case. The
events are the same as those of the JavaScript SDK's ``extras/e2e``. So the two
runners share their scenarios.
"""

from __future__ import annotations

import os
from typing import Any

from aws_durable_execution_sdk_python.concurrency.models import BatchResult
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution

from aws_durable_execution_sdk_python_extras.microvm import MicrovmConfig, microvm


# The scenarios that need an operation this package does not have yet. The
# runner skips them unless E2E_SCENARIOS selects them.
NOT_AVAILABLE = {
    "session": "microvm_session",
    "suspend-session": "microvm_session",
    "idle-session": "microvm_session",
}


@durable_execution
def handler(event: dict[str, Any], context: DurableContext) -> Any:
    scenario = event["scenario"]
    if scenario in NOT_AVAILABLE:
        msg = f"the {scenario} scenario needs {NOT_AVAILABLE[scenario]}"
        raise NotImplementedError(msg)
    if event.get("requestPath") is not None:
        msg = "delivery over HTTP is not available yet"
        raise NotImplementedError(msg)

    heartbeat_timeout = event.get("heartbeatTimeoutSeconds")
    config = MicrovmConfig(
        image_identifier=_require_env("MICROVM_IMAGE_ARN"),
        execution_role_arn=_require_env("MICROVM_ROLE_ARN"),
        timeout=Duration.from_seconds(event["timeoutSeconds"]),
        heartbeat_timeout=(
            None
            if heartbeat_timeout is None
            else Duration.from_seconds(heartbeat_timeout)
        ),
    )
    job = dict(event["job"])
    if event.get("paddingLength"):
        job["padding"] = (event.get("paddingChar") or "x") * event["paddingLength"]

    if scenario == "parallel":
        # Each map item is a child context. The operation takes the item's
        # context, so each job runs inside its item.
        batch: BatchResult[Any] = context.map(
            ["a", "b"],
            lambda item_context, label, _index, _items: microvm(
                item_context,
                f"job-{label}",
                job_input={**job, "label": label},
                config=config,
            ),
            name="jobs",
        )
        batch.throw_if_error()
        return batch.get_results()

    return microvm(context, "job", job_input=job, config=config)


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        msg = f"{name} is not set"
        raise RuntimeError(msg)
    return value
