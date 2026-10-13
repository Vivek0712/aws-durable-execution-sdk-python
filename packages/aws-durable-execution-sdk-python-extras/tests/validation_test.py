# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the checks that run before any durable operation."""

from __future__ import annotations

import math
from typing import Any

import pytest
from aws_durable_execution_sdk_python.config import Duration

from aws_durable_execution_sdk_python_extras.microvm import MicrovmConfig, microvm
from aws_durable_execution_sdk_python_extras.microvm import operation


class NoOperations:
    """A context that fails the test when the operation starts any durable
    operation."""

    def __init__(self, arn: str = "arn:aws:lambda:eu-west-1:1:function:f") -> None:
        self.execution_context = type("ExecutionContext", (), {})()
        self.execution_context.durable_execution_arn = arn

    def run_in_child_context(self, *args: Any, **kwargs: Any) -> Any:
        msg = "no durable operation may start"
        raise AssertionError(msg)


def config(**fields: Any) -> MicrovmConfig:
    values: dict[str, Any] = {
        "image_identifier": "img",
        "execution_role_arn": "role",
        "timeout": Duration.from_minutes(5),
        **fields,
    }
    return MicrovmConfig(**values)


@pytest.mark.parametrize(
    ("name", "fields", "message"),
    [
        ("", {}, "requires a non-empty name"),
        ("build", {"image_identifier": ""}, "image_identifier is required"),
        ("build", {"execution_role_arn": ""}, "execution_role_arn is required"),
        ("build", {"timeout": Duration(0)}, "timeout must be between 1 second"),
        (
            "build",
            {"timeout": Duration.from_seconds(28_801)},
            "Got 28801 seconds",
        ),
        (
            "build",
            {"heartbeat_timeout": Duration(0)},
            "heartbeat_timeout must be at least 1 second",
        ),
    ],
)
def test_rejects_an_invalid_call_before_any_durable_operation(
    name: str, fields: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        microvm(NoOperations(), name, job_input={}, config=config(**fields))  # type: ignore[arg-type]


@pytest.mark.parametrize("job_input", [{1, 2}, math.nan, {"x": math.inf}])
def test_rejects_an_input_that_is_not_json(job_input: Any) -> None:
    with pytest.raises(TypeError, match="must be JSON-serializable"):
        microvm(NoOperations(), "build", job_input=job_input, config=config())  # type: ignore[arg-type]


def test_rejects_a_call_without_a_region(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AWS_REGION")

    with pytest.raises(ValueError, match="cannot determine the AWS Region"):
        microvm(NoOperations("not-an-arn"), "build", job_input={}, config=config())  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("arn", "expected"),
    [
        ("arn:aws:lambda:eu-west-1:1:function:f:$LATEST/x", ("aws", "eu-west-1")),
        ("arn:aws-cn:lambda:cn-north-1:1:function:f", ("aws-cn", "cn-north-1")),
        ("not-an-arn", ("aws", "us-west-2")),
        ("arn:aws:lambda::1:function:f", ("aws", "us-west-2")),
    ],
)
def test_reads_the_partition_and_region(arn: str, expected: tuple[str, str]) -> None:
    assert operation._partition_and_region(arn) == expected  # noqa: SLF001


def test_creates_one_default_client_per_region(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[tuple[str, str]] = []
    configs: list[Any] = []

    class Session:
        def client(self, service: str, region_name: str, config: Any) -> object:
            created.append((service, region_name))
            configs.append(config)
            return object()

    monkeypatch.setattr(operation.boto3.session, "Session", Session)
    clients = operation._DefaultClients()  # noqa: SLF001

    first = clients.get("us-east-1")

    assert clients.get("us-east-1") is first
    assert clients.get("eu-west-1") is not first
    assert created == [
        ("lambda-microvms", "us-east-1"),
        ("lambda-microvms", "eu-west-1"),
    ]
    # botocore's defaults are 60 seconds each, with legacy retries.
    assert (configs[0].connect_timeout, configs[0].read_timeout) == (5, 30)
    assert configs[0].retries == {"mode": "standard", "max_attempts": 3}


def test_takes_the_input_and_the_config_by_keyword_only() -> None:
    with pytest.raises(TypeError, match="positional argument"):
        microvm(NoOperations(), "build", {}, config())  # type: ignore[arg-type, misc]
