# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""End-to-end test for the microvm operation and the MicroVM worker, against a
real AWS account.

It creates (or reuses) every resource it needs, all named with the
``dex-microvm-e2e-py`` prefix, runs its scenarios, and verifies the durable
execution outcome, the operation history, and that every MicroVM was
terminated. The scenarios are those of the JavaScript SDK's ``extras/e2e``.

Prerequisites: AWS credentials for the target account, boto3 1.43.35 or later,
and ``uv`` or ``pip`` to build the packages from this repository.

Usage, from the repository root::

    python packages/aws-durable-execution-sdk-python-extras/e2e/run_e2e.py

Environment:

- ``AWS_REGION``: the Region. Defaults to us-east-1.
- ``E2E_SCENARIOS``: a comma-separated list of scenario IDs. Without it, the
  runner runs every scenario that is neither optional nor pending.
- ``E2E_MAX_MICROVMS``: how many MicroVMs run at once. Defaults to 4: a new
  account may allocate 8 GB of MicroVM memory, and each MicroVM has 2 GB.
  A scenario starts only when its MicroVMs fit.
- ``E2E_IMAGE_ARN``: a MicroVM image to use instead of building one, for
  example an image with the JavaScript worker.
- ``E2E_FUNCTION_ARN``: a published function version to use instead of
  deploying one.
- ``E2E_SUB_TYPES``: ``custom`` expects the JavaScript operations' own
  subtypes, such as ``MicrovmLaunch``. By default the runner expects the
  core subtypes that the Python operation records today.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import zipfile
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError


HERE = Path(__file__).resolve().parent
PACKAGES = HERE.parent.parent
OUT = HERE / "build"
REGION = os.environ.get("AWS_REGION", "us-east-1")
PREFIX = "dex-microvm-e2e-py"
BASE_IMAGE_ARN = f"arn:aws:lambda:{REGION}:aws:microvm-image:al2023-1"
INTERNET_EGRESS = (
    f"arn:aws:lambda:{REGION}:aws:network-connector:aws-network-connector:"
    "INTERNET_EGRESS"
)
FUNCTION_RUNTIME = "python3.13"

CORE = PACKAGES / "aws-durable-execution-sdk-python"
EXTRAS = PACKAGES / "aws-durable-execution-sdk-python-extras"
WORKER = PACKAGES / "aws-durable-execution-sdk-python-microvm-worker"

# The clients are untyped. The runner reads the responses as plain dicts.
session = boto3.session.Session(region_name=REGION)
iam: Any = session.client("iam")
s3: Any = session.client("s3")
lambda_client: Any = session.client("lambda")
microvms: Any = session.client("lambda-microvms")


def log(message: str) -> None:
    print(f"[{datetime.now(UTC).isoformat(timespec='seconds')}] {message}", flush=True)


def not_found(error: ClientError) -> bool:
    code = error.response.get("Error", {}).get("Code", "")
    return code in {"NoSuchEntity", "ResourceNotFoundException", "404", "NotFound"}


# ---------------------------------------------------------------- bundles


def install(args: list[str]) -> None:
    """Run ``uv pip`` when uv is on the PATH, and ``python -m pip`` otherwise."""
    uv = shutil.which("uv")
    command = [uv, "pip", *args] if uv else [sys.executable, "-m", "pip", *args]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL)  # noqa: S603


def build_wheel(package: Path, out: Path) -> Path:
    uv = shutil.which("uv")
    command = (
        [uv, "build", "--wheel", "--out-dir", str(out), str(package)]
        if uv
        else [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--no-deps",
            "-w",
            str(out),
            str(package),
        ]
    )
    subprocess.run(command, check=True, capture_output=True)  # noqa: S603
    name = package.name.replace("-", "_")
    return max(out.glob(f"{name}-*.whl"), key=lambda path: path.stat().st_mtime)


def zip_files(target: Path, files: list[Path], root: Path | None = None) -> None:
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in files:
            archive.write(path, path.relative_to(root) if root else path.name)


def source_hash(*paths: Path) -> str:
    """Hash the sources, not the wheels, whose timestamps change every build."""
    digest = hashlib.sha256()
    for root in paths:
        files = [root] if root.is_file() else sorted(root.rglob("*"))
        for path in files:
            if path.is_file() and "__pycache__" not in path.parts:
                digest.update(str(path.relative_to(PACKAGES)).encode())
                digest.update(path.read_bytes())
    digest.update(json.dumps(IMAGE_HOOKS, sort_keys=True).encode())
    return digest.hexdigest()[:10]


def bundle() -> str:
    """Build the function and MicroVM bundles, and return the image hash."""
    shutil.rmtree(OUT, ignore_errors=True)
    wheels = OUT / "wheels"
    wheels.mkdir(parents=True)
    core = build_wheel(CORE, wheels)
    extras = build_wheel(EXTRAS, wheels)
    worker = build_wheel(WORKER, wheels)

    # The function bundle includes the SDK and a boto3 with the MicroVMs
    # client. The Lambda runtime's bundled boto3 may predate that client.
    function = OUT / "function"
    platform_args = (
        ["--python-platform", "aarch64-manylinux2014", "--python-version", "3.13"]
        if shutil.which("uv")
        else [
            "--platform",
            "manylinux2014_aarch64",
            "--implementation",
            "cp",
            "--python-version",
            "3.13",
            "--only-binary=:all:",
        ]
    )
    install(
        ["install", "--target", str(function), *platform_args, str(core), str(extras)]
    )
    shutil.copy(HERE / "function" / "index.py", function / "index.py")
    zip_files(
        OUT / "function.zip",
        [path for path in sorted(function.rglob("*")) if path.is_file()],
        root=function,
    )

    microvm = OUT / "microvm"
    microvm.mkdir()
    for path in [HERE / "microvm" / "app.py", HERE / "microvm" / "Dockerfile", worker]:
        shutil.copy(path, microvm / path.name)
    zip_files(OUT / "microvm.zip", sorted(microvm.iterdir()))
    return source_hash(HERE / "microvm", WORKER / "src", WORKER / "pyproject.toml")


# ---------------------------------------------------------------- IAM + S3

LAMBDA_TRUST = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": ["sts:AssumeRole", "sts:TagSession"],
            }
        ],
    }
)


def ensure_role(
    name: str,
    policy: list[dict[str, Any]],
    managed_policies: list[str] = (),  # type: ignore[assignment]
) -> tuple[str, bool]:
    """Create or update the role. Return its ARN and whether it is new."""
    created = False
    try:
        role = iam.get_role(RoleName=name)
    except ClientError as error:
        if not not_found(error):
            raise
        log(f"creating role {name}")
        role = iam.create_role(RoleName=name, AssumeRolePolicyDocument=LAMBDA_TRUST)
        created = True
    iam.put_role_policy(
        RoleName=name,
        PolicyName="e2e",
        PolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": policy}),
    )
    for arn in managed_policies:
        iam.attach_role_policy(RoleName=name, PolicyArn=arn)
    return role["Role"]["Arn"], created


@dataclass(frozen=True)
class Infrastructure:
    bucket: str
    build_role_arn: str
    microvm_role_arn: str
    function_role_arn: str
    roles_created: bool


def ensure_infrastructure(account: str) -> Infrastructure:
    bucket = f"{PREFIX}-{account}-{REGION}"
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as error:
        if not not_found(error):
            raise
        log(f"creating bucket {bucket}")
        if REGION == "us-east-1":
            s3.create_bucket(Bucket=bucket)
        else:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": REGION},
            )

    function_arn_pattern = f"arn:aws:lambda:{REGION}:{account}:function:{PREFIX}-fn:*"
    logs = {
        "Effect": "Allow",
        "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
        "Resource": "*",
    }
    build_role, build_created = ensure_role(
        f"{PREFIX}-build",
        [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": f"arn:aws:s3:::{bucket}/*",
            },
            logs,
        ],
    )
    # The role the MicroVM assumes: it completes the durable callback, and the
    # worker suspends its own idle session MicroVM. SuspendMicrovm authorizes
    # on the MicroVM image, so the statement names the test's images.
    microvm_role_name = f"{PREFIX}-microvm"
    microvm_role, microvm_created = ensure_role(
        microvm_role_name,
        [
            {
                "Effect": "Allow",
                "Action": [
                    "lambda:SendDurableExecutionCallbackSuccess",
                    "lambda:SendDurableExecutionCallbackFailure",
                    "lambda:SendDurableExecutionCallbackHeartbeat",
                ],
                "Resource": function_arn_pattern,
            },
            {
                "Effect": "Allow",
                "Action": ["lambda:SuspendMicrovm"],
                "Resource": f"arn:aws:lambda:{REGION}:{account}:microvm-image:{PREFIX}-*",
            },
            logs,
        ],
    )
    function_role, function_created = ensure_role(
        f"{PREFIX}-function",
        [
            {
                "Effect": "Allow",
                "Action": [
                    "lambda:RunMicrovm",
                    "lambda:TerminateMicrovm",
                    "lambda:GetMicrovm",
                    "lambda:CreateMicrovmAuthToken",
                    # A session resumes a MicroVM that suspended itself between
                    # jobs.
                    "lambda:ResumeMicrovm",
                ],
                "Resource": "*",
            },
            # RunMicrovm also authorizes each network connector it attaches.
            {
                "Effect": "Allow",
                "Action": ["lambda:PassNetworkConnector"],
                "Resource": f"arn:aws:lambda:{REGION}:aws:network-connector:aws-network-connector:*",
            },
            {
                "Effect": "Allow",
                "Action": ["iam:PassRole"],
                "Resource": f"arn:aws:iam::{account}:role/{microvm_role_name}",
            },
        ],
        [
            "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicDurableExecutionRolePolicy",
        ],
    )
    return Infrastructure(
        bucket=bucket,
        build_role_arn=build_role,
        microvm_role_arn=microvm_role,
        function_role_arn=function_role,
        roles_created=build_created or microvm_created or function_created,
    )


# ---------------------------------------------------------------- MicroVM image

IMAGE_HOOKS = {
    "port": 8080,
    "microvmHooks": {
        "run": "ENABLED",
        "runTimeoutInSeconds": 10,
        # The worker stops refusing jobs as soon as this hook arrives after
        # its own suspend.
        "resume": "ENABLED",
        "resumeTimeoutInSeconds": 10,
        # The worker refuses jobs from this hook until the resume hook.
        "suspend": "ENABLED",
        "suspendTimeoutInSeconds": 10,
        # The worker fails the callbacks of running jobs before it answers.
        "terminate": "ENABLED",
        "terminateTimeoutInSeconds": 10,
    },
    # The service requires the ready hook whenever a lifecycle hook is
    # enabled. The worker answers it with 200 once it is listening.
    "microvmImageHooks": {"ready": "ENABLED", "readyTimeoutInSeconds": 60},
}


def ensure_image(infra: Infrastructure, account: str, image_hash: str) -> str:
    name = f"{PREFIX}-{image_hash}"
    # GetMicrovmImage accepts only the ARN, not the name.
    arn = f"arn:aws:lambda:{REGION}:{account}:microvm-image:{name}"
    try:
        microvms.get_microvm_image(imageIdentifier=arn)
    except ClientError as error:
        if not not_found(error):
            raise
        key = f"microvm/{image_hash}.zip"
        s3.upload_file(str(OUT / "microvm.zip"), infra.bucket, key)
        log(f"creating MicroVM image {name}")
        microvms.create_microvm_image(
            name=name,
            codeArtifact={"uri": f"s3://{infra.bucket}/{key}"},
            baseImageArn=BASE_IMAGE_ARN,
            buildRoleArn=infra.build_role_arn,
            egressNetworkConnectors=[INTERNET_EGRESS],
            hooks=IMAGE_HOOKS,
        )
    for attempt in range(180):
        image = microvms.get_microvm_image(imageIdentifier=arn)
        state = image.get("state", "")
        if state in {"CREATED", "UPDATED"} and image.get("latestActiveImageVersion"):
            log(f"image {name} is {state}, version {image['latestActiveImageVersion']}")
            return image["imageArn"]
        if "FAILED" in state:
            msg = (
                f"image {name} is {state}: {image.get('stateReason', '')}. "
                f"Build logs: /aws/lambda-microvms/{name}"
            )
            raise RuntimeError(msg)
        if attempt % 6 == 0:
            log(f"waiting for image {name}: {state}")
        time.sleep(10)
    msg = f"image {name} was not ready after 30 minutes"
    raise RuntimeError(msg)


# ---------------------------------------------------------------- function


def ensure_function(infra: Infrastructure, image_arn: str) -> str:
    name = f"{PREFIX}-fn"
    environment = {
        "Variables": {
            "MICROVM_IMAGE_ARN": image_arn,
            "MICROVM_ROLE_ARN": infra.microvm_role_arn,
        }
    }
    code = (OUT / "function.zip").read_bytes()
    try:
        lambda_client.get_function(FunctionName=name)
    except ClientError as error:
        if not not_found(error):
            raise
        log(f"creating function {name}")
        lambda_client.create_function(
            FunctionName=name,
            Runtime=FUNCTION_RUNTIME,
            Architectures=["arm64"],
            Handler="index.handler",
            Role=infra.function_role_arn,
            Timeout=120,
            MemorySize=512,
            Environment=environment,
            DurableConfig={"ExecutionTimeout": 3600, "RetentionPeriodInDays": 1},
            Code={"ZipFile": code},
        )
        lambda_client.get_waiter("function_active_v2").wait(FunctionName=name)
    else:
        log(f"updating function {name}")
        lambda_client.update_function_code(FunctionName=name, ZipFile=code)
        lambda_client.get_waiter("function_updated_v2").wait(FunctionName=name)
        lambda_client.update_function_configuration(
            FunctionName=name, Environment=environment
        )
        lambda_client.get_waiter("function_updated_v2").wait(FunctionName=name)
    version = lambda_client.publish_version(FunctionName=name)
    log(f"published {name}:{version['Version']}")
    return version["FunctionArn"]


# ---------------------------------------------------------------- scenarios


@dataclass
class Outcome:
    execution: dict[str, Any]
    result: Any
    elapsed_seconds: int
    microvm_ids: list[str]
    microvm_states: list[dict[str, Any]]
    observed_states: list[str]
    terminated_after_seconds: int | None
    operation_names: list[str]
    sub_types: dict[str, str]
    execution_arn: str


@dataclass(frozen=True)
class Scenario:
    id: str
    event: dict[str, Any]
    expect: Callable[[Outcome], None]
    # Skipped unless E2E_SCENARIOS selects it: a probe that takes minutes.
    optional: bool = False
    # Skipped unless E2E_SCENARIOS selects it: it needs a change that has not
    # landed yet. The value names the change.
    pending: str | None = None
    # The runner terminates the MicroVM once it has been RUNNING this long.
    terminate_after_running_seconds: int | None = None
    # The runner records the MicroVM's states while the execution runs.
    observe_microvm: bool = False
    # Whether the scenario launches a MicroVM. One that fails before the
    # launch skips the checks of the launch, the callback, and the terminate.
    launches: bool = True
    # How many MicroVMs the scenario runs at once.
    microvms: int = 1


def check(condition: object, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def error_type(outcome: Outcome) -> str:
    """The short class name of the execution's error type.

    The Python SDK reports the fully-qualified name, such as
    ``aws_durable_execution_sdk_python_extras.microvm.errors.MicrovmJobFailedError``.
    The JavaScript SDK reports the class name alone.
    """
    return (
        (outcome.execution.get("Error") or {}).get("ErrorType", "").rsplit(".", 1)[-1]
    )


def error_text(outcome: Outcome) -> str:
    return json.dumps(outcome.execution.get("Error") or {}, ensure_ascii=False)


def status(outcome: Outcome) -> str:
    return outcome.execution.get("Status", "")


def expect_succeed(outcome: Outcome) -> None:
    check(status(outcome) == "SUCCEEDED", f"status {status(outcome)}")
    check((outcome.result or {}).get("label") == "single", f"result {outcome.result}")
    # A small input goes in the run hook. So no request step runs.
    check(
        "job.request" not in outcome.operation_names,
        "a small job was delivered over HTTP",
    )


def expect_job_failed(outcome: Outcome) -> None:
    check(status(outcome) == "FAILED", f"status {status(outcome)}")
    check("failed on purpose" in error_text(outcome), f"error {error_text(outcome)}")
    check(
        error_type(outcome) == "MicrovmJobFailedError",
        f"error type {error_type(outcome)}",
    )


def expect_crash(outcome: Outcome) -> None:
    check(status(outcome) == "FAILED", f"status {status(outcome)}")
    check(
        error_type(outcome) == "MicrovmTimeoutError",
        f"error type {error_type(outcome)}",
    )
    # The heartbeat timeout (15 s) must end the wait, not the 900 s callback
    # timeout.
    check(outcome.elapsed_seconds < 300, f"took {outcome.elapsed_seconds}s")


def expect_terminated(outcome: Outcome) -> None:
    check(status(outcome) == "FAILED", f"status {status(outcome)}")
    check(
        error_type(outcome) == "MicrovmJobFailedError",
        f"error type {error_type(outcome)}",
    )
    check(
        "MicrovmTerminatedError" in error_text(outcome), f"error {error_text(outcome)}"
    )
    terminated = outcome.terminated_after_seconds
    check(terminated is not None, "the runner did not terminate the MicroVM")
    # The poll interval is 5 seconds, and the durable function needs an
    # invocation to record the failure.
    after = outcome.elapsed_seconds - (terminated or 0)
    check(after < 60, f"failed {after} s after the terminate")


def expect_over_http(label: str, *, length: int = 10_000) -> Callable[[Outcome], None]:
    def expect(outcome: Outcome) -> None:
        check(status(outcome) == "SUCCEEDED", f"status {status(outcome)}")
        result = outcome.result or {}
        check(result.get("label") == label, f"result {result}")
        check(
            result.get("inputLength", 0) > length,
            f"inputLength {result.get('inputLength')}",
        )
        check(
            any(name.endswith(".request") for name in outcome.operation_names),
            "history has no *.request operation",
        )

    return expect


def expect_padding(
    char: str, length: int, *, over_http: bool
) -> Callable[[Outcome], None]:
    def expect(outcome: Outcome) -> None:
        check(status(outcome) == "SUCCEEDED", f"status {status(outcome)}")
        check(
            (outcome.result or {}).get("padding") == char * length,
            "the MicroVM did not receive the padding unchanged",
        )
        if over_http:
            check(
                "job.request" in outcome.operation_names,
                "history has no job.request operation",
            )
        else:
            check(
                "job.request" not in outcome.operation_names,
                "the job went over HTTP, not in the run hook",
            )

    return expect


def expect_too_large_for_the_run_hook(outcome: Outcome) -> None:
    check(status(outcome) == "FAILED", f"status {status(outcome)}")
    check(
        error_type(outcome) == "MicrovmDeliveryError",
        f"error type {error_type(outcome)}",
    )
    # The job does not fit even without the callback ID. So the operation
    # fails before the callback exists, and launches nothing.
    check(
        not any(name.endswith(".callback") for name in outcome.operation_names),
        "a callback was created for a job that cannot be delivered",
    )
    check(outcome.microvm_ids == [], f"launched {outcome.microvm_ids}")


def expect_session(outcome: Outcome) -> None:
    check(status(outcome) == "SUCCEEDED", f"status {status(outcome)}")
    result = outcome.result or {}
    check(
        (result.get("read") or {}).get("value") == "written-by-session-key",
        f"result {result}",
    )
    sub_types = outcome.sub_types
    check(
        sub_types.get("pipeline") == "MicrovmSession"
        and sub_types.get("write") == "MicrovmSessionJob"
        and sub_types.get("read") == "MicrovmSessionJob",
        f"session subtypes {sub_types}",
    )
    check(len(outcome.microvm_ids) == 1, f"launches {outcome.microvm_ids}")
    [microvm_id] = outcome.microvm_ids
    check(
        result["written"]["microvmId"] == microvm_id
        and result["read"]["microvmId"] == microvm_id
        and result.get("sessionMicrovmId") == microvm_id,
        "the two jobs did not run in the session's MicroVM",
    )
    for name in ["pipeline.session", "write.request", "read.request", "pause"]:
        check(name in outcome.operation_names, f"history has no {name}")


def expect_suspend_session(outcome: Outcome) -> None:
    check(status(outcome) == "SUCCEEDED", f"status {status(outcome)}")
    result = outcome.result or {}
    recalled = result.get("recalled") or {}
    # The value lives only in the worker's memory. The same value and process
    # ID after the waits show that the resume restored the memory.
    check(
        recalled.get("value") == "suspend-key"
        and recalled.get("pid") == (result.get("remembered") or {}).get("pid"),
        f"result {result}",
    )
    check(
        "SUSPENDED" in outcome.observed_states,
        f"observed states {outcome.observed_states}",
    )
    # The service called the resume hook after the self-suspend. So the worker
    # stopped refusing jobs without its fallback.
    check(
        recalled.get("resumeHooks", 0) >= 1,
        f"resume hooks {recalled.get('resumeHooks')}",
    )
    for name in ["long-pause", "second-pause", "recall.request"]:
        check(name in outcome.operation_names, f"history has no {name}")


def expect_long_job(outcome: Outcome) -> None:
    check(status(outcome) == "SUCCEEDED", f"status {status(outcome)}")
    check(outcome.elapsed_seconds >= 600, f"took {outcome.elapsed_seconds}s")


def expect_parallel(outcome: Outcome) -> None:
    check(status(outcome) == "SUCCEEDED", f"status {status(outcome)}")
    result = outcome.result
    check(isinstance(result, list) and len(result) == 2, f"result {result}")
    check(len(set(outcome.microvm_ids)) == 2, f"microvmIds {outcome.microvm_ids}")
    check(
        all(item["microvmId"] in outcome.microvm_ids for item in result),
        "result microvmIds do not match the launch steps",
    )


HTTP_DELIVERY = "delivery over HTTP"
SESSION = "microvm_session"
WORKER_TERMINATE = "the worker's terminate action"

SCENARIOS = [
    Scenario(
        id="succeed",
        event={
            "scenario": "single",
            "job": {"mode": "succeed", "sleepSeconds": 20, "label": "single"},
            "timeoutSeconds": 600,
            "heartbeatTimeoutSeconds": 15,
        },
        expect=expect_succeed,
    ),
    Scenario(
        id="fail",
        event={
            "scenario": "single",
            "job": {"mode": "fail", "sleepSeconds": 2, "label": "failing"},
            "timeoutSeconds": 300,
        },
        expect=expect_job_failed,
    ),
    Scenario(
        id="crash",
        event={
            "scenario": "single",
            "job": {"mode": "crash", "sleepSeconds": 0, "label": "crashing"},
            "timeoutSeconds": 900,
            "heartbeatTimeoutSeconds": 15,
        },
        expect=expect_crash,
    ),
    # The runner terminates the MicroVM while the job runs. The worker's
    # terminate hook then fails the job's callback. So the execution fails at
    # once, long before the 5-minute heartbeat timeout. The runner waits until
    # the MicroVM has been RUNNING for 5 seconds, so that the worker has
    # received the job before the terminate.
    Scenario(
        id="terminated",
        event={
            "scenario": "single",
            "job": {"mode": "succeed", "sleepSeconds": 600, "label": "terminated"},
            "timeoutSeconds": 900,
            "heartbeatTimeoutSeconds": 300,
        },
        terminate_after_running_seconds=5,
        expect=expect_terminated,
        pending=WORKER_TERMINATE,
    ),
    Scenario(
        id="request-succeed",
        event={
            "scenario": "single",
            "job": {"mode": "succeed", "sleepSeconds": 20, "label": "over-http"},
            "timeoutSeconds": 600,
            "heartbeatTimeoutSeconds": 15,
            "requestPath": "/job",
            # Larger than the 4,096-code-point run hook limit.
            "paddingLength": 10_000,
        },
        expect=expect_over_http("over-http"),
        pending=HTTP_DELIVERY,
    ),
    Scenario(
        id="auto-http",
        event={
            "scenario": "single",
            "job": {"mode": "succeed", "sleepSeconds": 5, "label": "auto-http"},
            "timeoutSeconds": 600,
            "heartbeatTimeoutSeconds": 15,
            # Larger than the run hook limit, and no requestPath. So microvm
            # must deliver the job over HTTP to the worker's handler.
            "paddingLength": 10_000,
        },
        expect=expect_over_http("auto-http"),
        pending=HTTP_DELIVERY,
    ),
    # Until delivery over HTTP lands, the same job fails before any launch.
    # Remove this scenario when auto-http stops being pending.
    Scenario(
        id="too-large-for-the-run-hook",
        event={
            "scenario": "single",
            "job": {"mode": "succeed", "sleepSeconds": 5, "label": "too-large"},
            "timeoutSeconds": 600,
            "heartbeatTimeoutSeconds": 15,
            "paddingLength": 10_000,
        },
        expect=expect_too_large_for_the_run_hook,
        launches=False,
        microvms=0,
    ),
    Scenario(
        id="run-hook-cjk",
        event={
            "scenario": "single",
            "job": {"mode": "succeed", "sleepSeconds": 2, "label": "run-hook-cjk"},
            "timeoutSeconds": 600,
            "heartbeatTimeoutSeconds": 15,
            # 3,400 code points and about 10,600 UTF-8 bytes. It fits the run
            # hook only because the service counts code points.
            "paddingLength": 3_400,
            "paddingChar": "日",
        },
        expect=expect_padding("日", 3_400, over_http=False),
    ),
    Scenario(
        id="run-hook-emoji",
        event={
            "scenario": "single",
            "job": {"mode": "succeed", "sleepSeconds": 2, "label": "run-hook-emoji"},
            "timeoutSeconds": 600,
            "heartbeatTimeoutSeconds": 15,
            # 3,400 code points, 6,800 UTF-16 code units, and about 13,900
            # bytes.
            "paddingLength": 3_400,
            "paddingChar": "😀",
        },
        expect=expect_padding("😀", 3_400, over_http=False),
    ),
    Scenario(
        id="auto-http-cjk",
        event={
            "scenario": "single",
            "job": {"mode": "succeed", "sleepSeconds": 2, "label": "auto-http-cjk"},
            "timeoutSeconds": 600,
            "heartbeatTimeoutSeconds": 15,
            # 4,200 code points: past the run hook limit.
            "paddingLength": 4_200,
            "paddingChar": "日",
        },
        expect=expect_padding("日", 4_200, over_http=True),
        pending=HTTP_DELIVERY,
    ),
    Scenario(
        id="request-fail",
        event={
            "scenario": "single",
            "job": {"mode": "fail", "sleepSeconds": 2, "label": "failing-http"},
            "timeoutSeconds": 300,
            "requestPath": "/job",
        },
        expect=expect_job_failed,
        pending=HTTP_DELIVERY,
    ),
    Scenario(
        id="session",
        event={
            "scenario": "session",
            "job": {"mode": "succeed", "sleepSeconds": 0, "label": "session-key"},
            "timeoutSeconds": 900,
        },
        expect=expect_session,
        pending=SESSION,
    ),
    Scenario(
        id="suspend-session",
        observe_microvm=True,
        event={
            "scenario": "suspend-session",
            "job": {"mode": "succeed", "sleepSeconds": 0, "label": "suspend-key"},
            "timeoutSeconds": 900,
            "pauseSeconds": 60,
            # The worker suspends the MicroVM 10 seconds after the first job
            # ends, during the 60-second wait.
            "autoSuspendIdleSeconds": 10,
        },
        expect=expect_suspend_session,
        pending=SESSION,
    ),
    # Opt-in probes. They take minutes and answer design questions, so the
    # default run skips them. Select them with E2E_SCENARIOS.
    Scenario(
        id="long-job",
        optional=True,
        event={
            "scenario": "single",
            "job": {"mode": "succeed", "sleepSeconds": 600, "label": "long"},
            "timeoutSeconds": 1_200,
            "heartbeatTimeoutSeconds": 60,
        },
        # No idle policy is set. A heartbeat timeout would show that the
        # MicroVM was suspended during the job.
        expect=expect_long_job,
    ),
    Scenario(
        id="idle-probe",
        optional=True,
        event={
            "scenario": "idle-session",
            "job": {"mode": "succeed", "sleepSeconds": 240, "label": "idle"},
            "timeoutSeconds": 900,
            "heartbeatTimeoutSeconds": 30,
            "idlePolicy": {
                "autoResumeEnabled": True,
                "maxIdleDurationSeconds": 60,
                "suspendedDurationSeconds": 600,
            },
        },
        # Both outcomes answer the question, so this probe only reports.
        # SUCCEEDED: outbound heartbeats kept the MicroVM running.
        # FAILED with a timeout: the MicroVM was suspended mid-job.
        expect=lambda outcome: None,
        pending=SESSION,
    ),
    Scenario(
        id="parallel",
        microvms=2,
        event={
            "scenario": "parallel",
            "job": {"mode": "succeed", "sleepSeconds": 5, "label": ""},
            "timeoutSeconds": 600,
        },
        expect=expect_parallel,
    ),
]


# ---------------------------------------------------------------- running


def history_events(execution_arn: str) -> Iterator[dict[str, Any]]:
    paginator = lambda_client.get_paginator("get_durable_execution_history")
    for page in paginator.paginate(
        DurableExecutionArn=execution_arn, IncludeExecutionData=True
    ):
        yield from page.get("Events", [])


def launch_ids(events: list[dict[str, Any]]) -> list[str]:
    """The MicroVM IDs that the launch steps checkpointed, as plain JSON."""
    ids = []
    for event in events:
        if event.get("EventType") == "StepSucceeded" and event.get("Name", "").endswith(
            ".launch"
        ):
            payload = (
                event.get("StepSucceededDetails", {}).get("Result", {}).get("Payload")
            )
            ids.append(json.loads(payload or "{}").get("microvmId"))
    return ids


def microvm_state(microvm_id: str) -> tuple[str, str | None]:
    try:
        vm = microvms.get_microvm(microvmIdentifier=microvm_id)
    except ClientError as error:
        if not_found(error):
            return "NOT_FOUND", None
        raise
    return vm.get("state", "NOT_FOUND"), vm.get("stateReason")


def find_execution_arn(function_arn: str, name: str) -> str:
    for _ in range(30):
        listed = lambda_client.list_durable_executions_by_function(
            FunctionName=function_arn, DurableExecutionName=name
        )
        for execution in listed.get("DurableExecutions", []):
            if execution.get("DurableExecutionName") == name:
                return execution["DurableExecutionArn"]
        time.sleep(2)
    msg = f"no durable execution named {name}"
    raise RuntimeError(msg)


def run_scenario(function_arn: str, scenario: Scenario, run_id: str) -> Outcome:
    name = f"{scenario.id}-{run_id}"
    started = time.monotonic()
    invoked = lambda_client.invoke(
        FunctionName=function_arn,
        InvocationType="Event",
        DurableExecutionName=name,
        Payload=json.dumps(scenario.event).encode(),
    )
    execution_arn = invoked.get("DurableExecutionArn") or find_execution_arn(
        function_arn, name
    )
    log(f"{scenario.id}: started {execution_arn}")

    # The distinct MicroVM states seen while the execution runs, in order.
    observed_states: list[str] = []
    # Seconds after the start at which the runner terminated the MicroVM.
    terminated_after_seconds: int | None = None
    # When the runner first saw the MicroVM RUNNING.
    running_since: float | None = None
    while True:
        execution = lambda_client.get_durable_execution(
            DurableExecutionArn=execution_arn, IncludeExecutionData=True
        )
        if execution.get("Status") != "RUNNING":
            break
        if scenario.terminate_after_running_seconds is not None and (
            terminated_after_seconds is None
        ):
            # The launch step records only that RunMicrovm returned. The
            # MicroVM can still be booting then, and a terminate before the
            # worker has the job would end in the heartbeat timeout instead.
            ids = launch_ids(list(history_events(execution_arn)))
            if ids:
                state, _ = microvm_state(ids[0])
                if state == "RUNNING" and running_since is None:
                    running_since = time.monotonic()
                if (
                    running_since is not None
                    and time.monotonic() - running_since
                    >= scenario.terminate_after_running_seconds
                ):
                    microvms.terminate_microvm(microvmIdentifier=ids[0])
                    terminated_after_seconds = round(time.monotonic() - started)
                    log(f"{scenario.id}: terminated {ids[0]}")
        if scenario.observe_microvm:
            ids = launch_ids(list(history_events(execution_arn)))
            if ids:
                state, _ = microvm_state(ids[0])
                if not observed_states or observed_states[-1] != state:
                    observed_states.append(state)
                    log(f"{scenario.id}: MicroVM is {state}")
        if time.monotonic() - started > 1_200:
            msg = f"{scenario.id}: still RUNNING after 20 minutes"
            raise RuntimeError(msg)
        time.sleep(5)

    elapsed_seconds = round(time.monotonic() - started)
    events = list(history_events(execution_arn))
    microvm_ids = launch_ids(events)
    raw_result = execution.get("Result")
    states = []
    for microvm_id in microvm_ids:
        state, reason = microvm_state(microvm_id)
        states.append({"id": microvm_id, "state": state, "reason": reason})
    return Outcome(
        execution=execution,
        result=None if raw_result is None else json.loads(raw_result),
        elapsed_seconds=elapsed_seconds,
        microvm_ids=microvm_ids,
        microvm_states=states,
        observed_states=observed_states,
        terminated_after_seconds=terminated_after_seconds,
        operation_names=list(dict.fromkeys(e["Name"] for e in events if e.get("Name"))),
        # Every event of one operation carries the same subtype.
        sub_types={e["Name"]: e.get("SubType", "") for e in events if e.get("Name")},
        execution_arn=execution_arn,
    )


# The operations label their parts. The names come from the scenario, so the
# runner matches the generated ones by suffix.
#
# The JavaScript operations set their own subtypes. The Python core SDK accepts
# only its own subtypes until issue #749 lands, so the Python operation records
# the subtypes of the core operations it uses. E2E_SUB_TYPES=custom expects the
# JavaScript ones, for a function built with the JavaScript SDK or after #749.
CUSTOM_SUB_TYPES = os.environ.get("E2E_SUB_TYPES") == "custom"
SUB_TYPES_BY_SUFFIX = (
    {
        ".callback": "MicrovmCallback",
        ".launch": "MicrovmLaunch",
        ".request": "MicrovmRequest",
        ".session": "MicrovmSessionId",
        ".terminate": "MicrovmTerminate",
    }
    if CUSTOM_SUB_TYPES
    else {
        ".callback": "Callback",
        ".launch": "Step",
        ".request": "Step",
        ".terminate": "Step",
    }
)
ROOT_SUB_TYPES = (
    {"Microvm", "MicrovmSession"} if CUSTOM_SUB_TYPES else {"RunInChildContext"}
)


def problems_of(scenario: Scenario, outcome: Outcome) -> list[str]:
    problems = []
    try:
        scenario.expect(outcome)
    except AssertionError as error:
        problems.append(str(error))
    if scenario.launches:
        for suffix in [".callback", ".launch", ".terminate"]:
            if not any(name.endswith(suffix) for name in outcome.operation_names):
                problems.append(f"history has no *{suffix} operation")
        if not outcome.microvm_ids:
            problems.append("no launch step result")
    for name, sub_type in outcome.sub_types.items():
        matched = next((s for s in SUB_TYPES_BY_SUFFIX if name.endswith(s)), None)
        if matched and sub_type != SUB_TYPES_BY_SUFFIX[matched]:
            problems.append(f"{name} has subtype {sub_type}")
    if not any(s in ROOT_SUB_TYPES for s in outcome.sub_types.values()):
        problems.append(f"history has no {' or '.join(sorted(ROOT_SUB_TYPES))} context")
    problems.extend(
        f"MicroVM {vm['id']} is {vm['state']}"
        for vm in outcome.microvm_states
        if vm["state"] not in {"TERMINATING", "TERMINATED", "NOT_FOUND"}
    )
    return problems


def select(only: list[str] | None) -> list[Scenario]:
    if only:
        unknown = set(only) - {scenario.id for scenario in SCENARIOS}
        if unknown:
            msg = f"unknown scenarios: {', '.join(sorted(unknown))}"
            raise SystemExit(msg)
        return [scenario for scenario in SCENARIOS if scenario.id in only]
    for scenario in SCENARIOS:
        if scenario.pending:
            log(f"{scenario.id}: skipped, needs {scenario.pending}")
    return [s for s in SCENARIOS if not s.optional and not s.pending]


class MicrovmBudget:
    """Lets scenarios start only while their MicroVMs fit the account's quota.

    A launch past the quota fails with ServiceQuotaExceededException. In a
    ``map``, the other item's MicroVM then keeps running, because its item
    never reaches the terminate step.
    """

    def __init__(self, size: int) -> None:
        self._size = max(size, 1)
        self._free = self._size
        self._changed = threading.Condition()

    @contextlib.contextmanager
    def reserve(self, count: int) -> Iterator[None]:
        # A scenario larger than the budget runs alone.
        count = min(count, self._size)
        with self._changed:
            self._changed.wait_for(lambda: self._free >= count)
            self._free -= count
        try:
            yield
        finally:
            with self._changed:
                self._free += count
                self._changed.notify_all()


def deploy(account: str) -> str:
    """Build and deploy the image and the function. Return the function ARN."""
    image_hash = bundle()
    infra = ensure_infrastructure(account)
    if infra.roles_created:
        log("waiting 15 s for new IAM roles to propagate")
        time.sleep(15)
    image_arn = os.environ.get("E2E_IMAGE_ARN") or ensure_image(
        infra, account, image_hash
    )
    return ensure_function(infra, image_arn)


def main() -> int:
    only = [s for s in os.environ.get("E2E_SCENARIOS", "").split(",") if s]
    selected = select(only or None)
    account = session.client("sts").get_caller_identity()["Account"]
    log(f"account {account}, region {REGION}")

    function_arn = os.environ.get("E2E_FUNCTION_ARN") or deploy(account)

    run_id = format(int(time.time()), "x")
    budget = MicrovmBudget(int(os.environ.get("E2E_MAX_MICROVMS", "4")))

    def run(scenario: Scenario) -> tuple[Scenario, Outcome | None, str | None]:
        with budget.reserve(scenario.microvms):
            try:
                return scenario, run_scenario(function_arn, scenario, run_id), None
            except Exception as error:  # noqa: BLE001
                return scenario, None, f"{type(error).__name__}: {error}"

    with ThreadPoolExecutor(max_workers=max(len(selected), 1)) as pool:
        results = list(pool.map(run, selected))

    failures = 0
    for scenario, outcome, error in results:
        problems = [error] if error else problems_of(scenario, outcome)  # type: ignore[arg-type]
        failures += 1 if problems else 0
        report: dict[str, Any] = {
            "scenario": scenario.id,
            "ok": not problems,
            "problems": problems,
        }
        if outcome is not None:
            report.update(
                status=status(outcome),
                elapsedSeconds=outcome.elapsed_seconds,
                result=outcome.result,
                error=outcome.execution.get("Error"),
                operations=outcome.operation_names,
                subTypes=outcome.sub_types,
                observedStates=outcome.observed_states,
                microvms=outcome.microvm_states,
                executionArn=outcome.execution_arn,
            )
        print(json.dumps(report, indent=2, ensure_ascii=False, default=str), flush=True)
    log(f"{len(selected) - failures}/{len(selected)} scenarios passed")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
