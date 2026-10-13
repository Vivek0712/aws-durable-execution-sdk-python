# AWS Durable Execution SDK for Python: extras

> This package is experimental. Its API can change in any release.

Custom operations for the [AWS Durable Execution SDK for Python](../aws-durable-execution-sdk-python). Each operation composes the SDK's built-in operations and calls AWS APIs. The operations live in this package, so the core SDK does not depend on those APIs' clients.

The package is the Python counterpart of [`@aws/durable-execution-sdk-js-extras`](https://github.com/aws/aws-durable-execution-sdk-js/tree/main/packages/aws-durable-execution-sdk-js-extras).

## `microvm`: run a job in an AWS Lambda MicroVM

`microvm` launches a new [Lambda MicroVM](https://docs.aws.amazon.com/lambda/latest/dg/lambda-microvms-guide.html), delivers the job to it, waits for the job's result, and terminates the MicroVM. The function invocation ends while the MicroVM works, so no Lambda compute is billed during the wait.

```python
import os

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python_extras.microvm import MicrovmConfig, microvm

CONFIG = MicrovmConfig(
    image_identifier=os.environ["MICROVM_IMAGE_ARN"],
    execution_role_arn=os.environ["MICROVM_ROLE_ARN"],
    timeout=Duration.from_hours(1),
    heartbeat_timeout=Duration.from_minutes(5),
)


@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    return microvm(context, "build", job_input={"repo": event["repo"]}, config=CONFIG)
```

The first argument is the context that the call runs in. Inside a child context or a `map` item, pass that context, so the operation's steps belong to it. `job_input` and `config` are keyword-only. A job can be a string, and so is the name. So positional arguments in the wrong order would pass validation and run the wrong job.

The code in the MicroVM receives the job and reports its result. The worker package [`aws-durable-execution-sdk-python-microvm-worker`](../aws-durable-execution-sdk-python-microvm-worker) does this, and so does the JavaScript worker. The job document is the same in every SDK language.

### What it does

The operation runs its durable operations in one child context named after the second argument:

1. `<name>.callback` creates a durable callback. The service generates the callback ID, and the SDK checkpoints it. So the ID is unique to this call and stays the same on replay.
2. `<name>.launch` calls `RunMicrovm`. The job goes in the `runHookPayload`, and Lambda passes it to the worker's `run` lifecycle hook. The client token is the SHA-256 hex digest of the callback ID. So a retried or replayed launch returns the same MicroVM instead of starting a second one. The step checkpoints plain JSON, `{"microvmId": "...", "delivery": "run-hook"}`, as the JavaScript operation does. So a reader of the execution history finds the MicroVM ID there.
3. The child context waits for the MicroVM to complete the callback.
4. `<name>.terminate` calls `TerminateMicrovm`. It runs after success, failure, and timeout alike.

A terminate failure does not fail the operation. The job's outcome is already recorded at that point. So the operation logs an error instead of raising. The MicroVM then keeps running, and is billed, until the platform ends it at its maximum duration: the operation timeout plus 5 minutes, at most 8 hours.

The terminate step runs only when the operation sees the job's outcome. In these cases it never does, and the MicroVM also runs until its maximum duration:

- A `map` or `parallel` completes early, for example with `min_successful`, while an item still waits for its MicroVM. The SDK never runs that item again.
- The execution fails, times out, or is stopped while the operation waits.
- Every launch attempt timed out or lost its connection, but the service created the MicroVM anyway. The operation never learned the MicroVM's ID.

The `run` hook payload holds at most 4,096 Unicode code points. It includes the callback ID and the job's fields, so an input of about 3,600 code points fits. The operation fails a larger job with `MicrovmDeliveryError` before the launch. When the job does not fit even without the callback ID, it fails before the callback exists. Delivery over HTTP, for larger jobs, comes in a later release.

### Errors

Every MicroVM failure is a `MicrovmError`. Catch the base class for all of them, or a subclass for one kind:

| Error | When |
|---|---|
| `MicrovmLaunchError` | `RunMicrovm` failed after all retries. When the last attempt timed out or lost its connection, the service may have created the MicroVM anyway, and it then runs the job. So running the job again after this error can run it twice. |
| `MicrovmDeliveryError` | The job is too large for the `run` hook. |
| `MicrovmJobFailedError` | The job handler in the MicroVM failed, or the MicroVM sent a result that is not valid JSON. The message includes the error type that the MicroVM reported. |
| `MicrovmTimeoutError` | No result arrived within `timeout`, or no heartbeat within `heartbeat_timeout`. A MicroVM that crashed ends this way. |

```python
from aws_durable_execution_sdk_python_extras.microvm import (
    MicrovmError,
    MicrovmTimeoutError,
)

try:
    result = microvm(context, "build", job_input=job_input, config=CONFIG)
except MicrovmTimeoutError:
    ...  # The MicroVM crashed, or took too long.
except MicrovmError:
    ...  # Any other MicroVM failure.
```

The class is the same on the first run and on every replay, so code that branches on it stays deterministic. A `microvm` call inside your own child context, or inside a `map` or `parallel` item, reaches the code around that context as the SDK's `ChildContextError`. Its `__cause__` is the MicroVM error, so check `isinstance(error.__cause__, MicrovmTimeoutError)` there. An invalid config raises `ValueError`, and an input that is not JSON-serializable raises `TypeError`, before any durable operation starts.

When the error escapes the handler, the execution's error type is the class's fully-qualified name, such as `aws_durable_execution_sdk_python_extras.microvm.errors.MicrovmJobFailedError`. The SDK uses that name to rebuild the class on replay. The JavaScript operation reports the short name, `MicrovmJobFailedError`. So a rule that matches the error type of both SDKs, such as a Step Functions `Catch` or a metric filter, must match both forms. For example, match on the suffix.

### Retries

The launch and terminate steps retry with `default_microvm_retry_strategy`, unless the config sets `retry_strategy`. It retries throttling, server errors, connection failures, and timeouts, for at most 5 attempts, with a delay of 2, 4, 8, and 16 seconds. Every other error, such as `ValidationException`, `AccessDeniedException`, or `InsufficientCapacityException`, fails the step at once. The boto3 client also retries throttling and server errors inside each attempt.

### Defaults and limits

- `timeout` must be between 1 second and 8 hours, because a MicroVM lives at most 8 hours. The MicroVM's `maximumDurationInSeconds` is `timeout` plus 5 minutes, at most 8 hours. So a timeout above 7 hours 55 minutes leaves the MicroVM less than 5 minutes beyond the callback's timeout.
- `job_input` is sent as JSON. So the MicroVM receives what `json.dumps` writes: a tuple arrives as a list, and a dict key that is not a string arrives as a string.
- Without connectors in the config, the MicroVM gets the `NO_INGRESS` connector, because the job arrives in the `run` hook, and the `INTERNET_EGRESS` connector, because it needs egress to reach the Lambda callback APIs. An empty tuple sends no connector.
- `heartbeat_timeout` starts when the callback is created, before the launch. So it must cover the launch, with its retries, and the boot. A throttled launch can take several minutes. With a shorter heartbeat timeout, the operation fails with `MicrovmTimeoutError` before the MicroVM sends its first heartbeat.
- The Region comes from the durable execution ARN, or from `AWS_REGION`.
- Without a `client`, the operation creates one `lambda-microvms` client per Region, on first use. It has a 5-second connect timeout, a 30-second read timeout, and 3 attempts in botocore's standard retry mode. So one launch or terminate attempt takes at most about 2 minutes. A call that runs past the function's timeout records no outcome for its step attempt, and the step runs again. The launch is idempotent through its client token, so it is repeated, not doubled.

### Permissions

The function's execution role needs:

- `lambda:RunMicrovm` and `lambda:TerminateMicrovm` on the MicroVM image. Without `lambda:TerminateMicrovm`, every operation still returns its result, but every MicroVM runs until its timeout plus 5 minutes, at most 8 hours. The only sign is an error in the function's log.
- `lambda:PassNetworkConnector` on the connectors that the MicroVM uses. With the defaults, that is `arn:aws:lambda:<region>:aws:network-connector:aws-network-connector:*`.
- `iam:PassRole` on the MicroVM execution role.

The MicroVM execution role (`execution_role_arn`) needs `lambda:SendDurableExecutionCallbackSuccess`, `lambda:SendDurableExecutionCallbackFailure`, and `lambda:SendDurableExecutionCallbackHeartbeat` on the function ARN (`arn:aws:lambda:<region>:<account>:function:<name>:*`).

### Subtypes

The JavaScript operation records MicroVM subtypes, such as `Microvm` on the child context and `MicrovmLaunch` on the launch step. This package records the SDK's default subtypes for now, because the Python SDK does not accept custom subtypes yet.

On replay, the SDK compares the subtype of each operation with its checkpoint. A mismatch fails the execution with `NonDeterministicExecutionError`. So a release that changed the subtypes would fail every execution that waits on a MicroVM during the deploy. So this package is published only after the SDK accepts custom subtypes, and it records the MicroVM subtypes from its first release.
