# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The errors that the MicroVM operations raise.

Every MicroVM failure is a :class:`MicrovmError`. Catch the base class to
handle every failure the same way, or a subclass to handle one kind.

The operation raises the same class on the first run and on every replay:

1. Inside its child context, the operation raises a subclass, such as
   :class:`MicrovmLaunchError`.
2. The SDK checkpoints the child context's failure with the class's
   fully-qualified name as the error type, and the message and data.
3. The SDK then raises ``ChildContextError`` with those fields, on the first
   run and on replay alike.
4. The operation rebuilds the MicroVM error from the error type, with
   :meth:`MicrovmError.from_child_context_error`.

So code that catches a subclass stays deterministic.
"""

from __future__ import annotations

from aws_durable_execution_sdk_python.exceptions import (
    ChildContextError,
    DurableOperationError,
)


class MicrovmError(DurableOperationError):
    """The base class of every MicroVM failure.

    ``message`` names the operation and keeps the underlying message.
    ``data`` is the error data of the failure, when it had any.
    """

    @classmethod
    def from_child_context_error(
        cls, name: str, error: ChildContextError
    ) -> MicrovmError:
        """Rebuild the MicroVM error that the operation's child context raised.

        1. A checkpointed MicroVM error type gets its own class back, with the
           checkpointed message and data.
        2. Any other error type becomes a plain :class:`MicrovmError`. It keeps
           the original error type in ``error_type``. The operation contains no
           caller code, so every failure in it is a MicroVM failure.

        Args:
            name: The operation name, for the message of a plain MicrovmError.
            error: The error that ``run_in_child_context`` raised.
        """
        error_class = _ERROR_CLASSES.get(error.error_type)
        if error_class is not None:
            return error_class(
                message=error.message,
                data=error.data,
                stack_trace=error.stack_trace,
            )
        return MicrovmError(
            message=f'MicroVM "{name}": {error.message}',
            error_type=error.error_type,
            data=error.data,
            stack_trace=error.stack_trace,
        )


class MicrovmLaunchError(MicrovmError):
    """``RunMicrovm`` failed after all retries, so no MicroVM runs the job."""


class MicrovmDeliveryError(MicrovmError):
    """The job could not be delivered to the MicroVM."""


class MicrovmJobFailedError(MicrovmError):
    """The job handler in the MicroVM failed.

    The MicroVM reported the failure with
    ``SendDurableExecutionCallbackFailure``. The message includes the error
    type that the MicroVM reported, such as the job handler's exception class.
    """


class MicrovmTimeoutError(MicrovmError):
    """No result arrived within the job's timeout, or no heartbeat arrived
    within its heartbeat timeout.

    A MicroVM that crashed or never booted ends this way. The message keeps
    the service's reason.
    """


_ERROR_CLASSES: dict[str, type[MicrovmError]] = {
    f"{error_class.__module__}.{error_class.__qualname__}": error_class
    for error_class in (
        MicrovmError,
        MicrovmLaunchError,
        MicrovmDeliveryError,
        MicrovmJobFailedError,
        MicrovmTimeoutError,
    )
}
"""The MicroVM error classes, keyed by the error type that the SDK
checkpoints for them: the fully-qualified class name."""
