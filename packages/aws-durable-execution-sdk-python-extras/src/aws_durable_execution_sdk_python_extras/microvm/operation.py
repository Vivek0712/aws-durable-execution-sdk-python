# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The ``microvm`` operation: run one job in a new AWS Lambda MicroVM."""

from __future__ import annotations

import hashlib
import json
import os
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import boto3
from aws_durable_execution_sdk_python.config import (
    CallbackConfig,
    Duration,
    StepConfig,
)
from aws_durable_execution_sdk_python.exceptions import (
    CallbackError,
    CallbackTimeoutError,
    ChildContextError,
    StepError,
)
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python.serdes import JsonSerDes
from botocore.exceptions import ClientError

from aws_durable_execution_sdk_python_extras.microvm.config import (
    MAX_MICROVM_DURATION_SECONDS,
    LambdaMicrovmsClient,
    MicrovmConfig,
)
from aws_durable_execution_sdk_python_extras.microvm.errors import (
    MicrovmDeliveryError,
    MicrovmError,
    MicrovmJobFailedError,
    MicrovmLaunchError,
    MicrovmTimeoutError,
)
from aws_durable_execution_sdk_python_extras.microvm.job import (
    MAX_RUN_HOOK_PAYLOAD_LENGTH,
    RunHookPayload,
    fits_run_hook,
)
from aws_durable_execution_sdk_python_extras.microvm.retry import (
    default_microvm_retry_strategy,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.context import Callback, DurableContext


LAUNCH_MARGIN_SECONDS = 300
"""The time added to the operation timeout to get the MicroVM's maximum
duration.

The MicroVM must outlive the job's callback, or the platform terminates it
before the job can report. The margin covers the launch.
"""

RUN_HOOK_DELIVERY = "run-hook"
"""The delivery in which the job goes in the ``runHookPayload`` of the launch."""


@dataclass(frozen=True, kw_only=True)
class LaunchResult:
    """What the launch step checkpoints.

    Wire names: ``microvmId`` and ``delivery``.
    """

    microvm_id: str
    delivery: str
    """How the job reaches the MicroVM. Replay reads it from the checkpoint,
    so every replay follows the delivery that the launch prepared for."""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LaunchResult:
        return cls(microvm_id=data["microvmId"], delivery=data["delivery"])

    def to_dict(self) -> dict[str, Any]:
        return {"microvmId": self.microvm_id, "delivery": self.delivery}


def microvm(
    context: DurableContext,
    name: str,
    job_input: Any,
    config: MicrovmConfig,
) -> Any:
    """Run one job in a new AWS Lambda MicroVM, and return the job's result.

    The operation runs its durable operations in one child context named
    ``name``:

    1. ``<name>.callback`` creates a durable callback. The service generates
       the callback ID, and the SDK checkpoints it. So the ID is unique to this
       call and stays the same on every replay.
    2. ``<name>.launch`` calls ``RunMicrovm``. The job goes in the
       ``runHookPayload``, and Lambda passes it to the worker's ``run`` hook.
       The client token is the SHA-256 hex digest of the callback ID. So a
       retried or replayed launch returns the same MicroVM instead of starting
       a second one.
    3. The child context waits on the callback. The invocation ends during the
       wait, so no Lambda compute is billed. The worker in the MicroVM
       completes the callback with the job's result or failure.
    4. ``<name>.terminate`` calls ``TerminateMicrovm``. It runs after success,
       failure, and timeout alike.

    A terminate failure does not fail the operation. The job's outcome is
    already recorded when terminate runs, and the MicroVM's maximum duration
    bounds how long it keeps running. So the operation logs an error and
    returns the job's outcome.

    Pass the context that the call runs in. Inside a ``map`` item or a child
    context, pass that context, so the operation's steps belong to it.

    Args:
        context: The context to create the durable operations in.
        name: The child context name. It also prefixes the inner operations.
        job_input: The job's input. It must be JSON-serializable, and the same
            on every replay.
        config: The MicroVM and the timeouts.

    Returns:
        The value that the MicroVM passed to
        ``SendDurableExecutionCallbackSuccess``, parsed as JSON.

    Raises:
        ValueError: When ``name`` or ``config`` is not valid, or the Region
            cannot be determined. No durable operation starts.
        TypeError: When ``job_input`` is not JSON-serializable. No durable
            operation starts.
        MicrovmLaunchError: When ``RunMicrovm`` fails after all retries.
        MicrovmDeliveryError: When the job is too large for the ``run`` hook.
        MicrovmJobFailedError: When the MicroVM reports a failure.
        MicrovmTimeoutError: When no result or heartbeat arrives in time.
    """
    scope = _OperationScope.create(context, name, job_input, config)

    def run(child: DurableContext) -> Any:
        return scope.run_job(child, job_input)

    try:
        return context.run_in_child_context(run, name=name)
    except ChildContextError as error:
        raise MicrovmError.from_child_context_error(name, error) from error


@dataclass(frozen=True, kw_only=True)
class _OperationScope:
    """The validated values that every step of one operation uses."""

    name: str
    config: MicrovmConfig
    partition: str
    region: str
    timeout_seconds: int
    heartbeat_timeout_seconds: int | None
    retry_strategy: Callable[[Exception, int], RetryDecision]

    @classmethod
    def create(
        cls,
        context: DurableContext,
        name: str,
        job_input: Any,
        config: MicrovmConfig,
    ) -> _OperationScope:
        """Validate the call, before any durable operation starts.

        Raises:
            ValueError: When ``name`` or ``config`` is not valid, or the
                Region cannot be determined.
            TypeError: When ``job_input`` is not JSON-serializable.
        """
        if not isinstance(name, str) or not name:
            msg = "a MicroVM operation requires a non-empty name"
            raise ValueError(msg)
        if not config.image_identifier:
            msg = f'MicroVM "{name}": image_identifier is required'
            raise ValueError(msg)
        if not config.execution_role_arn:
            msg = f'MicroVM "{name}": execution_role_arn is required'
            raise ValueError(msg)
        timeout_seconds = config.timeout.to_seconds()
        if not 1 <= timeout_seconds <= MAX_MICROVM_DURATION_SECONDS:
            msg = (
                f'MicroVM "{name}": timeout must be between 1 second and '
                f"{MAX_MICROVM_DURATION_SECONDS} seconds (8 hours), because a "
                f"MicroVM lives at most 8 hours. Got {timeout_seconds} seconds."
            )
            raise ValueError(msg)
        heartbeat_timeout_seconds = (
            None
            if config.heartbeat_timeout is None
            else config.heartbeat_timeout.to_seconds()
        )
        if heartbeat_timeout_seconds is not None and heartbeat_timeout_seconds < 1:
            msg = f'MicroVM "{name}": heartbeat_timeout must be at least 1 second'
            raise ValueError(msg)
        try:
            json.dumps(job_input, allow_nan=False)
        except (TypeError, ValueError) as error:
            msg = f'MicroVM "{name}": the job input must be JSON-serializable: {error}'
            raise TypeError(msg) from error
        partition, region = _partition_and_region(
            context.execution_context.durable_execution_arn
        )
        if not region:
            # The worker rejects a payload without a Region. So the operation
            # fails before any durable operation instead.
            msg = (
                f'MicroVM "{name}": cannot determine the AWS Region. The durable '
                "execution ARN has none, and AWS_REGION is not set."
            )
            raise ValueError(msg)
        return cls(
            name=name,
            config=config,
            partition=partition,
            region=region,
            timeout_seconds=timeout_seconds,
            heartbeat_timeout_seconds=heartbeat_timeout_seconds,
            retry_strategy=config.retry_strategy or default_microvm_retry_strategy,
        )

    def run_job(self, child: DurableContext, job_input: Any) -> Any:
        """Run the operation's durable operations in its child context."""
        name = self.name
        callback: Callback = child.create_callback(
            name=f"{name}.callback",
            config=CallbackConfig(
                timeout=Duration.from_seconds(self.timeout_seconds),
                heartbeat_timeout=Duration.from_seconds(
                    self.heartbeat_timeout_seconds or 0
                ),
                # The worker sends the result as a JSON string. The default
                # callback serdes would return that string unparsed.
                serdes=JsonSerDes(),
            ),
        )
        payload = RunHookPayload(
            region=self.region,
            callback_id=callback.callback_id,
            job_input=job_input,
            heartbeat_timeout_seconds=self.heartbeat_timeout_seconds,
        ).to_json()
        # The payload contains the callback ID, so its size is known only
        # after the callback exists. The ID comes from the checkpoint on
        # replay, and the input must not change. So every replay reaches the
        # same decision.
        if not fits_run_hook(payload):
            msg = (
                f'MicroVM "{name}": the job is {len(payload)} code points in the '
                f"run hook payload, and the run hook accepts at most "
                f"{MAX_RUN_HOOK_PAYLOAD_LENGTH}. Delivery over HTTP for larger "
                "jobs is not available yet."
            )
            raise MicrovmDeliveryError(msg)

        try:
            launched = LaunchResult.from_dict(
                child.step(
                    lambda _: self._launch(callback.callback_id, payload).to_dict(),
                    name=f"{name}.launch",
                    config=StepConfig(retry_strategy=self.retry_strategy),
                )
            )
        except StepError as error:
            msg = f'MicroVM "{name}": the launch failed: {error.message}'
            raise MicrovmLaunchError(message=msg, data=error.data) from error
        microvm_id = launched.microvm_id

        try:
            result = callback.result()
        except Exception as error:
            # SuspendExecution is a BaseException, so a wait for the callback
            # passes through here without terminating the MicroVM.
            self._terminate(child, microvm_id)
            raise self._job_error(callback, error) from error
        self._terminate(child, microvm_id)
        return result

    def _launch(self, callback_id: str, payload: str) -> LaunchResult:
        """Call ``RunMicrovm``, and return what the launch step checkpoints."""
        config = self.config
        request: dict[str, Any] = {
            "imageIdentifier": config.image_identifier,
            "executionRoleArn": config.execution_role_arn,
            # A callback ID can be 1,024 characters, and the client token
            # allows 64. The SHA-256 hex digest is 64 characters.
            "clientToken": hashlib.sha256(callback_id.encode()).hexdigest(),
            "ingressNetworkConnectors": list(
                config.ingress_network_connectors
                or (self._connector_arn("NO_INGRESS"),)
            ),
            "egressNetworkConnectors": list(
                config.egress_network_connectors
                or (self._connector_arn("INTERNET_EGRESS"),)
            ),
            "maximumDurationInSeconds": min(
                self.timeout_seconds + LAUNCH_MARGIN_SECONDS,
                MAX_MICROVM_DURATION_SECONDS,
            ),
            "runHookPayload": payload,
        }
        if config.image_version is not None:
            request["imageVersion"] = config.image_version
        if config.logging is not None:
            request["logging"] = dict(config.logging)
        response = self._client().run_microvm(**request)
        microvm_id = response.get("microvmId")
        if not microvm_id:
            msg = "RunMicrovm returned no microvmId"
            raise RuntimeError(msg)
        return LaunchResult(microvm_id=microvm_id, delivery=RUN_HOOK_DELIVERY)

    def _terminate(self, child: DurableContext, microvm_id: str) -> None:
        """Run the terminate step, and log a final failure instead of raising.

        The job's outcome is already recorded when terminate runs, and the
        MicroVM's maximum duration bounds how long it can keep running. So a
        terminate failure must not discard the outcome. The step's failure is
        checkpointed. So on replay the step fails again at once, and this
        method logs again, with the same outcome.
        """
        try:
            child.step(
                lambda _: self._terminate_call(microvm_id),
                name=f"{self.name}.terminate",
                config=StepConfig(retry_strategy=self.retry_strategy),
            )
        except StepError as error:
            # The MicroVM keeps running, and is billed, until the platform
            # ends it. A missing permission fails every terminate this way,
            # and the default retry strategy does not retry
            # AccessDeniedException. This log is the only sign of it. So it
            # is an error, not a warning.
            child.logger.error(
                f'MicroVM "{self.name}": terminate failed for {microvm_id}. The '
                "MicroVM keeps running, and is billed, until the platform ends "
                "it at its maximum duration, the operation timeout plus 5 "
                "minutes. A missing permission needs lambda:TerminateMicrovm in "
                "the function's execution role.",
                extra={"microvmId": microvm_id, "error": error.message},
            )

    def _terminate_call(self, microvm_id: str) -> None:
        try:
            self._client().terminate_microvm(microvmIdentifier=microvm_id)
        except ClientError as error:
            # TerminateMicrovm succeeds for a terminated MicroVM. The service
            # answers ResourceNotFoundException for a MicroVM whose record it
            # has removed. Either way, the MicroVM is gone.
            if error.response.get("Error", {}).get("Code") != (
                "ResourceNotFoundException"
            ):
                raise

    def _job_error(self, callback: Callback, error: Exception) -> Exception:
        """Convert a failure of the callback wait into its MicroVM error."""
        name = self.name
        if isinstance(error, CallbackTimeoutError):
            return MicrovmTimeoutError(
                message=f'MicroVM "{name}": the job timed out: {error.message}',
                data=error.data,
            )
        if isinstance(error, CallbackError):
            # The SDK raises CallbackExternalError with the message and the
            # data. The error type that the MicroVM reported stays on the
            # callback's checkpoint, so the message carries it from there.
            reported = _reported_error_type(callback)
            detail = f" ({reported})" if reported else ""
            return MicrovmJobFailedError(
                message=f'MicroVM "{name}": the job failed{detail}: {error.message}',
                data=error.data,
            )
        # Any other failure, such as a result that is not valid JSON. The
        # operation contains no caller code, so it is a MicroVM failure.
        message = getattr(error, "message", None) or str(error)
        return MicrovmError(
            message=f'MicroVM "{name}": the job result could not be read: {message}',
            error_type=_qualified_type(error),
        )

    def _connector_arn(self, connector: str) -> str:
        return (
            f"arn:{self.partition}:lambda:{self.region}:aws:network-connector:"
            f"aws-network-connector:{connector}"
        )

    def _client(self) -> LambdaMicrovmsClient:
        if self.config.client is not None:
            return self.config.client
        return _DEFAULT_CLIENTS.get(self.region)


class _DefaultClients:
    """One ``lambda-microvms`` client per Region, created on first use.

    A boto3 client is safe to use from several threads. Creating one from a
    shared session is not. So creation runs under a lock, and each client gets
    its own session.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._clients: dict[str, LambdaMicrovmsClient] = {}

    def get(self, region: str) -> LambdaMicrovmsClient:
        with self._lock:
            client = self._clients.get(region)
            if client is None:
                client = boto3.session.Session().client(
                    "lambda-microvms", region_name=region
                )
                self._clients[region] = client
            return client


_DEFAULT_CLIENTS = _DefaultClients()


def _reported_error_type(callback: Callback) -> str | None:
    """The error type that the MicroVM reported for a failed callback.

    ``Error`` is the type that a worker reports for a value that is not an
    exception. It names nothing, so it is left out.
    """
    error = callback.state.get_checkpoint_result(callback.operation_id).error
    reported = None if error is None else error.type
    return None if reported in {None, "", "Error"} else reported


def _qualified_type(error: BaseException) -> str:
    """The fully-qualified class name, with builtins left unqualified, as the
    SDK writes an error type."""
    error_class = type(error)
    module = (
        "" if error_class.__module__ == "builtins" else f"{error_class.__module__}."
    )
    return f"{module}{error_class.__qualname__}"


def _partition_and_region(arn: str) -> tuple[str, str]:
    """Read the partition and Region from a durable execution ARN.

    The ARN has the form ``arn:<partition>:lambda:<region>:<account>:...``.
    Without that form, the partition is ``aws`` and the Region comes from
    ``AWS_REGION``.
    """
    parts = arn.split(":")
    if len(parts) > 3 and parts[0] == "arn" and parts[1] and parts[3]:
        return parts[1], parts[3]
    return "aws", os.environ.get("AWS_REGION", "")
