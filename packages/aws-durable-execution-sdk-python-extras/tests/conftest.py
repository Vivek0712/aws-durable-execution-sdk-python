# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Test doubles for the Lambda MicroVMs client."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

import pytest
from botocore.exceptions import ClientError


def client_error(code: str, status: int = 400) -> ClientError:
    """A botocore ClientError with a code and an HTTP status."""
    response: Any = {
        "Error": {"Code": code, "Message": f"{code} message"},
        "ResponseMetadata": {"HTTPStatusCode": status},
    }
    return ClientError(response, "RunMicrovm")


Answer = BaseException | Callable[[dict[str, Any]], Any] | None


class FakeMicrovmsClient:
    """Records Lambda MicroVMs API calls, and answers from scripted lists.

    ``launches`` and ``terminates`` hold the answers of ``run_microvm`` and
    ``terminate_microvm``, in order. Each answer is an exception to raise, a
    function that receives the request, or ``None`` for success. After a list
    ends, every call succeeds.
    """

    def __init__(
        self,
        *,
        launches: list[Answer] | None = None,
        terminates: list[Answer] | None = None,
    ) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._answers: dict[str, list[Answer]] = {
            "run_microvm": list(launches or []),
            "terminate_microvm": list(terminates or []),
        }
        self._lock = threading.Lock()

    def requests(self, name: str) -> list[dict[str, Any]]:
        with self._lock:
            return [request for call, request in self.calls if call == name]

    def _answer(self, name: str, request: dict[str, Any]) -> Any:
        with self._lock:
            self.calls.append((name, request))
            answers = self._answers[name]
            answer = answers.pop(0) if answers else None
        if isinstance(answer, BaseException):
            raise answer
        if callable(answer):
            return answer(request)
        return None

    def run_microvm(self, **request: Any) -> dict[str, Any]:
        self._answer("run_microvm", request)
        return {"microvmId": "mvm-1", "state": "PENDING"}

    def terminate_microvm(self, **request: Any) -> dict[str, Any]:
        self._answer("terminate_microvm", request)
        return {}


# The fakes are fixtures, not imports. Several packages in this repository
# have a top-level ``tests`` package, so ``from tests.conftest import ...``
# could resolve to another package's tests.


@pytest.fixture(autouse=True)
def aws_region(monkeypatch: pytest.MonkeyPatch) -> None:
    # The local runner's execution ARN is not an ARN. So the operation takes
    # the Region from AWS_REGION, as it does outside Lambda.
    monkeypatch.setenv("AWS_REGION", "us-west-2")


@pytest.fixture
def fake_client() -> type[FakeMicrovmsClient]:
    return FakeMicrovmsClient


@pytest.fixture
def make_error() -> Callable[..., ClientError]:
    return client_error
