# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The configuration of :func:`microvm`."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.retries import RetryDecision


if TYPE_CHECKING:
    from mypy_boto3_lambda_microvms import LambdaMicroVMsClient as LambdaMicrovmsClient
else:
    LambdaMicrovmsClient = Any


MAX_MICROVM_DURATION_SECONDS = 28_800
"""A MicroVM lives at most 8 hours, so ``RunMicrovm`` accepts
``maximumDurationInSeconds`` of at most 28,800."""


@dataclass(frozen=True, kw_only=True)
class MicrovmConfig:
    """The MicroVM, and how long to wait for its result.

    :func:`microvm` validates the config before it starts any durable
    operation.
    """

    image_identifier: str
    """The ARN or ID of the MicroVM image to run.

    The image must enable the ``run`` lifecycle hook. The service then also
    requires the ``ready`` image hook. The worker package
    ``aws-durable-execution-sdk-python-microvm-worker`` answers both.
    """

    execution_role_arn: str
    """The IAM role that the MicroVM assumes.

    The MicroVM reports its result through the durable callback APIs. So the
    role needs ``lambda:SendDurableExecutionCallbackSuccess``,
    ``lambda:SendDurableExecutionCallbackFailure``, and
    ``lambda:SendDurableExecutionCallbackHeartbeat`` on the durable function's
    ARN.
    """

    timeout: Duration
    """The longest time to wait for the MicroVM to report a result.

    It must be between 1 second and 8 hours, because a MicroVM lives at most
    8 hours. The MicroVM's ``maximumDurationInSeconds`` is this timeout plus 5
    minutes, at most 8 hours. So a timeout above 7 hours 55 minutes leaves the
    MicroVM less than 5 minutes beyond the callback's timeout.
    """

    heartbeat_timeout: Duration | None = None
    """The longest time between two heartbeats from the MicroVM.

    When it is set, a MicroVM that fails to boot or stops responding fails the
    operation after this time, instead of after ``timeout``. The MicroVM
    receives the value in the job document.

    The heartbeat timeout starts when the callback is created, before the
    launch. So it must cover the launch, with its retries, and the boot. A
    throttled launch can take several minutes. A heartbeat timeout that is
    shorter fails the operation with ``MicrovmTimeoutError`` before the
    MicroVM can send its first heartbeat.
    """

    image_version: str | None = None
    """The image version to run. Defaults to the latest active version."""

    ingress_network_connectors: tuple[str, ...] | None = None
    """Ingress network connector ARNs.

    ``None`` gets the Lambda-provided ``NO_INGRESS`` connector, because the
    job arrives in the ``run`` hook and needs no inbound connection. An empty
    tuple sends no ingress connector.
    """

    egress_network_connectors: tuple[str, ...] | None = None
    """Egress network connector ARNs.

    ``None`` gets the Lambda-provided ``INTERNET_EGRESS`` connector. The
    MicroVM needs egress to reach the Lambda callback APIs. An empty tuple
    sends no egress connector.
    """

    logging: Mapping[str, Any] | None = None
    """The ``logging`` member of ``RunMicrovm``. Defaults to the service
    default."""

    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None
    """The step retry strategy of the launch and terminate steps.

    Defaults to :func:`default_microvm_retry_strategy`.
    """

    client: LambdaMicrovmsClient | None = None
    """The boto3 ``lambda-microvms`` client.

    Defaults to one client per Region, created on first use. The Region comes
    from the durable execution ARN. The default client has a 5-second connect
    timeout, a 30-second read timeout, and 3 attempts in botocore's standard
    retry mode. A client that you pass keeps its own settings. Keep each call
    shorter than the function's timeout, because a call that runs past it
    records no outcome for the step attempt.
    """
