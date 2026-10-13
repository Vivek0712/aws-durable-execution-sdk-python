# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the microvm operation, run on the local test runner."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)

from aws_durable_execution_sdk_python_extras.microvm import (
    MAX_RUN_HOOK_PAYLOAD_LENGTH,
    MicrovmConfig,
    MicrovmError,
    microvm,
)


WAIT = 30


def config_with(client: Any, **fields: Any) -> MicrovmConfig:
    values: dict[str, Any] = {
        "image_identifier": "arn:aws:lambda:us-west-2:123456789012:microvm-image:img",
        "execution_role_arn": "arn:aws:iam::123456789012:role/microvm",
        "timeout": Duration.from_minutes(5),
        "client": client,
        # One quick retry keeps the retry tests fast.
        "retry_strategy": lambda error, attempt: (
            RetryDecision.retry(Duration.from_seconds(1))
            if attempt < 2
            and getattr(error, "response", {}).get("Error", {}).get("Code")
            == "ThrottlingException"
            else RetryDecision.no_retry()
        ),
        **fields,
    }
    return MicrovmConfig(**values)


def outcome(result: DurableFunctionTestResult) -> Any:
    assert result.status is InvocationStatus.SUCCEEDED, result.error
    return json.loads(result.result) if result.result is not None else None


@pytest.fixture
def run() -> Iterator[Callable[..., Any]]:
    """Run a handler on the local test runner, and close the runner after."""
    runners: list[DurableFunctionTestRunner] = []

    def _run(
        handler: Callable[..., Any],
        event: Any,
        complete: Callable[[DurableFunctionTestRunner, str], None] | None = None,
    ) -> DurableFunctionTestResult:
        runner = DurableFunctionTestRunner(handler=handler, poll_interval=0.05)
        runners.append(runner)
        arn = runner.run_async(input=json.dumps(event), execution_timeout=WAIT)
        if complete is not None:
            callback_id = runner.wait_for_callback(
                arn, name="build.callback", timeout=WAIT
            )
            complete(runner, callback_id)
        return runner.wait_for_result(arn, timeout=WAIT)

    yield _run
    for runner in runners:
        runner.close()


def succeed_with(value: Any) -> Callable[[DurableFunctionTestRunner, str], None]:
    def complete(runner: DurableFunctionTestRunner, callback_id: str) -> None:
        runner.send_callback_success(callback_id, json.dumps(value).encode())

    return complete


def fail_with(
    error_type: str | None, message: str
) -> Callable[[DurableFunctionTestRunner, str], None]:
    def complete(runner: DurableFunctionTestRunner, callback_id: str) -> None:
        runner.send_callback_failure(
            callback_id,
            ErrorObject(message=message, type=error_type, data=None, stack_trace=None),
        )

    return complete


def catching(config: MicrovmConfig) -> Callable[..., Any]:
    """A handler that returns the class and message of a MicroVM error."""

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        try:
            return {"result": microvm(context, "build", event, config)}
        except MicrovmError as error:
            return {"error": type(error).__name__, "message": error.message}

    return handler


def test_launches_returns_the_result_and_terminates(run: Any, fake_client: Any) -> None:
    client = fake_client()
    config = config_with(client, heartbeat_timeout=Duration.from_seconds(60))

    result = run(catching(config), {"repo": "org/app"}, succeed_with({"passed": True}))

    assert outcome(result) == {"result": {"passed": True}}
    [launch] = client.requests("run_microvm")
    payload = json.loads(launch["runHookPayload"])
    callback_id = payload["job"]["callbackId"]
    assert payload == {
        "version": 1,
        "region": "us-west-2",
        "job": {
            "callbackId": callback_id,
            "heartbeatTimeoutSeconds": 60,
            "input": {"repo": "org/app"},
        },
    }
    assert launch["clientToken"] == hashlib.sha256(callback_id.encode()).hexdigest()
    assert launch["imageIdentifier"] == config.image_identifier
    assert launch["executionRoleArn"] == config.execution_role_arn
    assert launch["maximumDurationInSeconds"] == 300 + 300
    connector = "arn:aws:lambda:us-west-2:aws:network-connector:aws-network-connector"
    assert launch["ingressNetworkConnectors"] == [f"{connector}:NO_INGRESS"]
    assert launch["egressNetworkConnectors"] == [f"{connector}:INTERNET_EGRESS"]
    assert "imageVersion" not in launch
    assert "logging" not in launch
    assert client.requests("terminate_microvm") == [{"microvmIdentifier": "mvm-1"}]


def test_records_the_operations_in_one_child_context(
    run: Any, fake_client: Any
) -> None:
    result = run(catching(config_with(fake_client())), {}, succeed_with(1))

    context = result.get_context("build")
    assert [operation.name for operation in context.child_operations] == [
        "build.callback",
        "build.launch",
        "build.terminate",
    ]


def test_passes_the_optional_launch_settings(run: Any, fake_client: Any) -> None:
    client = fake_client()
    config = config_with(
        client,
        image_version="7",
        logging={"cloudWatch": {"logGroup": "/microvm"}},
        ingress_network_connectors=("arn:ingress",),
        egress_network_connectors=("arn:egress",),
        timeout=Duration.from_hours(8),
    )

    run(catching(config), {}, succeed_with(None))

    [launch] = client.requests("run_microvm")
    assert launch["imageVersion"] == "7"
    assert launch["logging"] == {"cloudWatch": {"logGroup": "/microvm"}}
    assert launch["ingressNetworkConnectors"] == ["arn:ingress"]
    assert launch["egressNetworkConnectors"] == ["arn:egress"]
    # The maximum duration is capped at 8 hours.
    assert launch["maximumDurationInSeconds"] == 28_800
    assert "heartbeatTimeoutSeconds" not in json.loads(launch["runHookPayload"])["job"]


def test_fails_with_a_microvm_error_for_a_result_that_is_not_json(
    run: Any, fake_client: Any
) -> None:
    client = fake_client()

    def complete(runner: DurableFunctionTestRunner, callback_id: str) -> None:
        runner.send_callback_success(callback_id, b"not json")

    result = run(catching(config_with(client)), {}, complete)

    error = outcome(result)
    assert error["error"] == "MicrovmError"
    assert error["message"].startswith(
        'MicroVM "build": the job result could not be read: '
    )
    assert len(client.requests("terminate_microvm")) == 1


def test_uses_the_default_client_of_the_region(
    run: Any, fake_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from aws_durable_execution_sdk_python_extras.microvm import operation

    client = fake_client()
    regions: list[str] = []

    class Clients:
        def get(self, region: str) -> Any:
            regions.append(region)
            return client

    monkeypatch.setattr(operation, "_DEFAULT_CLIENTS", Clients())

    result = run(catching(config_with(None)), {}, succeed_with("done"))

    assert outcome(result) == {"result": "done"}
    assert set(regions) == {"us-west-2"}
    assert len(client.requests("run_microvm")) == 1


def test_fails_with_the_microvm_error_and_still_terminates(
    run: Any, fake_client: Any
) -> None:
    client = fake_client()

    result = run(
        catching(config_with(client)), {}, fail_with("RuntimeError", "3 tests failed")
    )

    assert outcome(result) == {
        "error": "MicrovmJobFailedError",
        "message": 'MicroVM "build": the job failed (RuntimeError): 3 tests failed',
    }
    assert len(client.requests("terminate_microvm")) == 1


@pytest.mark.parametrize("error_type", [None, "Error"])
def test_leaves_out_an_error_type_that_names_nothing(
    run: Any, fake_client: Any, error_type: str | None
) -> None:
    result = run(
        catching(config_with(fake_client())), {}, fail_with(error_type, "broken")
    )

    assert outcome(result)["message"] == 'MicroVM "build": the job failed: broken'


def test_fails_with_a_timeout_error_when_no_result_arrives(
    run: Any, fake_client: Any
) -> None:
    client = fake_client()
    config = config_with(client, timeout=Duration.from_seconds(2))

    result = run(catching(config), {})

    error = outcome(result)
    assert error["error"] == "MicrovmTimeoutError"
    assert error["message"].startswith('MicroVM "build": the job timed out: ')
    assert len(client.requests("terminate_microvm")) == 1


def test_returns_the_result_when_terminate_fails(
    run: Any, fake_client: Any, make_error: Any
) -> None:
    client = fake_client(terminates=[make_error("AccessDeniedException", 403)])

    result = run(catching(config_with(client)), {}, succeed_with("done"))

    assert outcome(result) == {"result": "done"}
    assert len(client.requests("terminate_microvm")) == 1
    terminate = result.get_context("build").get_step("build.terminate")
    assert terminate.error is not None


def test_treats_resource_not_found_from_terminate_as_terminated(
    run: Any, fake_client: Any, make_error: Any
) -> None:
    client = fake_client(terminates=[make_error("ResourceNotFoundException", 404)])

    result = run(catching(config_with(client)), {}, succeed_with("done"))

    assert outcome(result) == {"result": "done"}
    terminate = result.get_context("build").get_step("build.terminate")
    assert terminate.error is None


def test_retries_a_throttled_launch_with_the_same_client_token(
    run: Any, fake_client: Any, make_error: Any
) -> None:
    client = fake_client(launches=[make_error("ThrottlingException", 400)])

    result = run(catching(config_with(client)), {}, succeed_with("done"))

    assert outcome(result) == {"result": "done"}
    first, second = client.requests("run_microvm")
    assert first["clientToken"] == second["clientToken"]
    assert first["runHookPayload"] == second["runHookPayload"]


def test_fails_with_the_launch_error_and_skips_terminate(
    run: Any, fake_client: Any, make_error: Any
) -> None:
    client = fake_client(launches=[make_error("ValidationException", 400)])

    result = run(catching(config_with(client)), {})

    error = outcome(result)
    assert error["error"] == "MicrovmLaunchError"
    assert error["message"].startswith('MicroVM "build": the launch failed: ')
    assert "ValidationException" in error["message"]
    assert client.requests("terminate_microvm") == []


def test_fails_a_launch_without_a_microvm_id(run: Any, fake_client: Any) -> None:
    class NoId(fake_client):  # type: ignore[misc, valid-type]
        def run_microvm(self, **request: Any) -> dict[str, Any]:
            super().run_microvm(**request)
            return {}

    result = run(catching(config_with(NoId())), {})

    error = outcome(result)
    assert error["error"] == "MicrovmLaunchError"
    assert "RunMicrovm returned no microvmId" in error["message"]


def test_fails_a_job_too_large_for_the_run_hook_before_the_launch(
    run: Any, fake_client: Any
) -> None:
    client = fake_client()

    result = run(catching(config_with(client)), "x" * MAX_RUN_HOOK_PAYLOAD_LENGTH)

    error = outcome(result)
    assert error["error"] == "MicrovmDeliveryError"
    assert "the run hook accepts at most 4096" in error["message"]
    assert client.calls == []


def test_raises_the_same_class_after_the_handler_stops_catching(
    run: Any, fake_client: Any
) -> None:
    config = config_with(fake_client())

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        return microvm(context, "build", event, config)

    result = run(handler, {}, fail_with("RuntimeError", "broken"))

    assert result.status is InvocationStatus.FAILED
    assert result.error is not None
    assert result.error.type == (
        "aws_durable_execution_sdk_python_extras.microvm.errors.MicrovmJobFailedError"
    )


def test_runs_in_the_context_that_the_caller_passes(run: Any, fake_client: Any) -> None:
    config = config_with(fake_client())

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        return context.run_in_child_context(
            lambda child: microvm(child, "build", event, config), name="outer"
        )

    result = run(handler, {}, succeed_with(7))

    assert outcome(result) == 7
    outer = result.get_context("outer")
    assert [operation.name for operation in outer.child_operations] == ["build"]


def test_runs_one_microvm_per_map_item(run: Any, fake_client: Any) -> None:
    client = fake_client()
    config = config_with(client)

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        return context.map(
            [1, 2, 3],
            lambda item_context, item, index, items: microvm(
                item_context, f"shard-{item}", item, config
            ),
            name="shards",
        ).get_results()

    runner = DurableFunctionTestRunner(handler=handler, poll_interval=0.05)
    try:
        arn = runner.run_async(input="{}", execution_timeout=WAIT)
        for item in [1, 2, 3]:
            callback_id = runner.wait_for_callback(
                arn, name=f"shard-{item}.callback", timeout=WAIT
            )
            runner.send_callback_success(callback_id, json.dumps(item * 10).encode())
        result = runner.wait_for_result(arn, timeout=WAIT)
    finally:
        runner.close()

    assert outcome(result) == [10, 20, 30]
    tokens = {request["clientToken"] for request in client.requests("run_microvm")}
    assert len(tokens) == 3
    assert len(client.requests("terminate_microvm")) == 3


def test_requests_match_the_lambda_microvms_api_model(run: Any) -> None:
    """botocore validates each request against the service model."""
    import datetime

    import boto3
    from botocore.stub import ANY, Stubber

    client = boto3.session.Session().client("lambda-microvms", region_name="us-west-2")
    config = config_with(
        client,
        image_version="7",
        heartbeat_timeout=Duration.from_seconds(60),
        logging={"cloudWatch": {"logGroup": "/microvm"}},
    )
    with Stubber(client) as stub:
        stub.add_response(
            "run_microvm",
            {
                "microvmId": "mvm-1",
                "state": "PENDING",
                "endpoint": "mvm-1.example",
                "imageArn": config.image_identifier,
                "imageVersion": "7",
                "maximumDurationInSeconds": 600,
                "startedAt": datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            },
            {
                "imageIdentifier": config.image_identifier,
                "imageVersion": "7",
                "executionRoleArn": config.execution_role_arn,
                "clientToken": ANY,
                "ingressNetworkConnectors": ANY,
                "egressNetworkConnectors": ANY,
                "maximumDurationInSeconds": 600,
                "logging": {"cloudWatch": {"logGroup": "/microvm"}},
                "runHookPayload": ANY,
            },
        )
        stub.add_response("terminate_microvm", {}, {"microvmIdentifier": "mvm-1"})

        result = run(catching(config), {"n": 1}, succeed_with("done"))

        stub.assert_no_pending_responses()
    client.close()
    assert outcome(result) == {"result": "done"}
