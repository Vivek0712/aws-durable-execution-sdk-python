# End-to-end test for the microvm operation

`run_e2e.py` tests the `microvm` operation and the MicroVM worker against a real AWS account. It builds both from this repository, so a run tests the branch you are on.

The scenarios and the durable function's events are the same as those of the JavaScript SDK's `extras/e2e`. So the two SDKs are checked against one list of cases.

## What it does

1. Builds the core SDK, this package, and the MicroVM worker as wheels.
2. Creates or reuses an S3 bucket and three IAM roles: one for the image build, one for the MicroVM, and one for the function. Every resource is named with the `dex-microvm-e2e-py` prefix.
3. Builds a MicroVM image from `microvm/` with the worker, unless one with the same sources exists.
4. Deploys `function/index.py` as a durable function on Python 3.13, arm64, and publishes a version.
5. Starts one durable execution per scenario, and checks its outcome, its operation history, and that every MicroVM it launched was terminated.

## Running it

You need AWS credentials for the target account, boto3 1.43.35 or later, and `uv` or `pip`. From the repository root:

```
python packages/aws-durable-execution-sdk-python-extras/e2e/run_e2e.py
```

| Variable | Effect |
|---|---|
| `AWS_REGION` | The Region. Defaults to `us-east-1`. |
| `E2E_SCENARIOS` | A comma-separated list of scenario IDs. It also selects optional and pending scenarios. |
| `E2E_MAX_MICROVMS` | How many MicroVMs run at once. Defaults to 4: a new account may allocate 8 GB of MicroVM memory, and each MicroVM has 2 GB. A scenario starts only when its MicroVMs fit. |
| `E2E_IMAGE_ARN` | Use this MicroVM image instead of building one. For example, an image with the JavaScript worker checks the operation against both workers. |
| `E2E_FUNCTION_ARN` | Use this published function version instead of deploying one. |
| `E2E_SUB_TYPES` | `custom` expects the JavaScript operations' own subtypes, such as `MicrovmLaunch`. By default the runner expects the core subtypes that the Python operation records today. |

The runner prints one JSON report per scenario and exits with 1 when any scenario fails.

## Scenarios

| ID | Checks |
|---|---|
| `succeed` | A job in the `run` hook returns its result. |
| `fail` | A failing job raises `MicrovmJobFailedError` with the job's message. |
| `crash` | A MicroVM that exits mid-job fails on the 15-second heartbeat timeout, not the 900-second timeout. |
| `too-large-for-the-run-hook` | A job past the `run` hook limit fails with `MicrovmDeliveryError` before the callback exists. |
| `run-hook-cjk`, `run-hook-emoji` | 3,400 code points of multi-byte text fit the `run` hook, which counts code points, and arrive unchanged. |
| `parallel` | Two `microvm` calls in a `map` launch two MicroVMs. |
| `long-job` | Optional. A 10-minute job keeps its MicroVM running. |

The checks that apply to every scenario: the history has the `.callback`, `.launch`, and `.terminate` operations with their subtypes, and every launched MicroVM ends `TERMINATED`.

The JavaScript operations record their own subtypes, such as `Microvm` and `MicrovmLaunch`. The Python core SDK accepts only its own subtypes until issue #749 lands. So the Python operation records `RunInChildContext`, `Callback`, and `Step`, and the runner expects those unless `E2E_SUB_TYPES=custom`.

### Pending scenarios

These need changes that have not landed yet. The runner skips them unless `E2E_SCENARIOS` names them.

| ID | Needs |
|---|---|
| `terminated` | The worker's terminate action, which fails the callback when the MicroVM is terminated mid-job. |
| `request-succeed`, `request-fail`, `auto-http`, `auto-http-cjk` | Delivery over HTTP. When it lands, remove `too-large-for-the-run-hook`. |
| `session`, `suspend-session`, `idle-probe` | `microvm_session`, and the worker's suspend and resume. |

When a change lands, remove `pending` from its scenarios in `run_e2e.py`, and add the scenario's branch to `function/index.py`.

## Error types

The Python SDK reports the fully-qualified class name as the execution's error type, such as `aws_durable_execution_sdk_python_extras.microvm.errors.MicrovmJobFailedError`. The JavaScript SDK reports `MicrovmJobFailedError`. The runner compares the class name alone, so a scenario passes with either SDK's function.

## Cleaning up

The runner keeps its resources for the next run. A MicroVM image costs nothing while no MicroVM runs. To remove everything, delete the function `dex-microvm-e2e-py-fn`, the images named `dex-microvm-e2e-py-*`, the three roles named `dex-microvm-e2e-py-*`, and the bucket `dex-microvm-e2e-py-<account>-<region>`.
