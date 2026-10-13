# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The job document that the operation sends to the MicroVM.

The worker in the MicroVM reads the same document in every SDK language. The
Python worker in ``aws-durable-execution-sdk-python-microvm-worker`` and the
JavaScript worker in ``@aws/durable-execution-sdk-js-microvm-worker`` parse
it. A change to the document must be made in every language.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


PAYLOAD_VERSION = 1
"""The version of the job document."""

MAX_RUN_HOOK_PAYLOAD_LENGTH = 4_096
"""``RunMicrovm`` accepts a ``runHookPayload`` of at most 4,096 code points.

The service counts Unicode code points, not UTF-8 bytes. A probe against the
service from the JavaScript SDK accepted 4,096 code points of an emoji, and
rejected 4,097. A Python ``str`` has one item per code point. So ``len()``
counts what the service counts.
"""


@dataclass(frozen=True, kw_only=True)
class RunHookPayload:
    """The ``runHookPayload`` document that carries one job in ``RunMicrovm``.

    Wire names: ``version``, ``region``, and ``job``, whose fields are
    ``callbackId``, ``heartbeatTimeoutSeconds``, and ``input``.
    """

    region: str
    """The Region of the durable function, for the callback API calls."""
    callback_id: str
    """The callback that the worker completes with the job's outcome."""
    job_input: Any
    """The caller's input. It must be JSON-serializable."""
    heartbeat_timeout_seconds: int | None = None
    """The heartbeat timeout, when the caller set one."""

    def to_json(self) -> str:
        """Serialize the payload as compact JSON.

        1. Non-ASCII text stays as it is, as ``JSON.stringify`` writes it. So
           the length counts the same code points as the JavaScript SDK's
           payload.
        2. A lone surrogate is not valid in the UTF-8 request body. So a
           payload that contains one is written with ``\\uXXXX`` escapes.
        3. ``allow_nan=False`` rejects ``nan`` and infinite floats, because
           the worker rejects the ``NaN`` and ``Infinity`` tokens.

        Raises:
            TypeError: When the input is not JSON-serializable.
            ValueError: When the input holds ``nan`` or an infinite float.
        """
        job: dict[str, Any] = {"callbackId": self.callback_id}
        if self.heartbeat_timeout_seconds is not None:
            job["heartbeatTimeoutSeconds"] = self.heartbeat_timeout_seconds
        job["input"] = self.job_input
        document = {"version": PAYLOAD_VERSION, "region": self.region, "job": job}
        text = json.dumps(
            document, allow_nan=False, ensure_ascii=False, separators=(",", ":")
        )
        try:
            text.encode()
        except UnicodeEncodeError:
            return json.dumps(document, allow_nan=False, separators=(",", ":"))
        return text


def fits_run_hook(payload: str) -> bool:
    """Whether ``payload`` fits in the ``runHookPayload`` of ``RunMicrovm``."""
    return len(payload) <= MAX_RUN_HOOK_PAYLOAD_LENGTH
