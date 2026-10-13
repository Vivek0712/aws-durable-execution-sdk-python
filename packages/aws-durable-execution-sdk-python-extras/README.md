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
    heartbeat_timeout=Duration.from_minutes(1),
)


@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    return microvm(context, "build", {"repo": event["repo"]}, CONFIG)
```

The first argument is the context that the call runs in. Inside a child context or a `map` item, pass that context, so the operation's steps belong to it.

The code in the MicroVM receives the job and reports its result. The worker package [`aws-durable-execution-sdk-python-microvm-worker`](../aws-durable-execution-sdk-python-microvm-worker) does this, and so does the JavaScript worker. The job document is the same in every SDK language.

### What it does

The operation runs its durable operations in one child context named after the second argument:

1. `<name>.callback` creates a durable callback. The service generates the callback ID, and the SDK checkpoints it. So the ID is unique to this call and stays the same on replay.
2. `<name>.launch` calls `RunMicrovm`. The job goes in the `runHookPayload`, and Lambda passes it to the worker's `run` lifecycle hook. The client token is the SHA-256 hex digest of the callback ID. So a retried or replayed launch returns the same MicroVM instead of starting a second one.
3. The child context waits for the MicroVM to complete the callback.
4. `<name>.terminate` calls `TerminateMicrovm`. It runs after success, failure, and timeout alike.

A terminate failure does not fail the operation. The job's outcome is already recorded at that point. So the operation logs an error instead of raising. The MicroVM then keeps running, and is billed, until the platform ends it at its maximum duration: the operation timeout plus 5 minutes.

The `run` hook payload holds at most 4,096 Unicode code points. It includes the callback ID and the job's fields, so an input of about 3,600 code points fits. The operation fails a larger job with `MicrovmDeliveryError` before the launch. Delivery over HTTP, for larger jobs, comes in a later release.

### Errors

Every MicroVM failure is a `MicrovmError`. Catch the base class for all of them, or a subclass for one kind:

| Error | When |
|---|---|
| `MicrovmLaunchError` | `RunMicrovm` failed after all retries, so no MicroVM runs the job. |
| `MicrovmDeliveryError` | The job is too large for the `run` hook. |
| `MicrovmJobFailedError` | The job handler in the MicroVM failed. The message includes the error type that the MicroVM reported. |
| `MicrovmTimeoutError` | No result arrived within `timeout`, or no heartbeat within `heartbeat_timeout`. A MicroVM that crashed ends this way. |

```python
from aws_durable_execution_sdk_python_extras.microvm import (
    MicrovmError,
    MicrovmTimeoutError,
)

try:
    result = microvm(context, "build", job_input, CONFIG)
except MicrovmTimeoutError:
    ...  # The MicroVM crashed, or took too long.
except MicrovmError:
    ...  # Any other MicroVM failure.
```

The class is the same on the first run and on every replay, so code that branches on it stays deterministic. An invalid config raises `ValueError`, and an input that is not JSON-serializable raises `TypeError`, before any durable operation starts.

### Retries

The launch and terminate steps retry with `default_microvm_retry_strategy`, unless the config sets `retry_strategy`. It retries throttling, server errors, connection failures, and timeouts, for at most 5 attempts, with a delay of 2, 4, 8, and 16 seconds. Every other error, such as `ValidationException` or `AccessDeniedException`, fails the step at once. The boto3 client also retries throttling and server errors inside each attempt.

### Defaults and limits

- `timeout` must be between 1 second and 8 hours, because a MicroVM lives at most 8 hours. The MicroVM's `maximumDurationInSeconds` is `timeout` plus 5 minutes, at most 8 hours.
- The MicroVM gets the `NO_INGRESS` connector, because the job arrives in the `run` hook, and the `INTERNET_EGRESS` connector, because it needs egress to reach the Lambda callback APIs.
- The Region comes from the durable execution ARN, or from `AWS_REGION`.
- Without a `client`, the operation creates one `lambda-microvms` client per Region, on first use.

### Permissions

The function's execution role needs:

- `lambda:RunMicrovm` and `lambda:TerminateMicrovm` on the MicroVM image. Without `lambda:TerminateMicrovm`, every operation still returns its result, but every MicroVM runs until its timeout plus 5 minutes. The only sign is an error in the function's log.
- `lambda:PassNetworkConnector` on the connectors that the MicroVM uses. With the defaults, that is `arn:aws:lambda:<region>:aws:network-connector:aws-network-connector:*`.
- `iam:PassRole` on the MicroVM execution role.

The MicroVM execution role (`execution_role_arn`) needs `lambda:SendDurableExecutionCallbackSuccess`, `lambda:SendDurableExecutionCallbackFailure`, and `lambda:SendDurableExecutionCallbackHeartbeat` on the function ARN (`arn:aws:lambda:<region>:<account>:function:<name>:*`).

### Subtypes

The operation records the SDK's default subtypes for now. The child context, the steps, and the callback record the same subtypes as any other child context, step, and callback. The MicroVM subtypes of the JavaScript operation, such as `MicrovmLaunch`, come when the Python SDK accepts custom subtypes.
