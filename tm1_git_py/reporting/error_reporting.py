"""Shared error-reporting primitives for workflow coordinators.

Errors crossing thread or process boundaries must contain only serializable data.
In particular, exception instances, futures, callbacks, and service connections must
remain in the process that owns them.

This module is intentionally separate from ``progress_reporting``. Progress events
describe normal per-worker activity and are consumed through progress sinks at
DEBUG level. Workflow errors describe failed work and are collected and reported by
the coordinator process.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
import traceback as traceback_module
from typing import Callable, Iterable, Literal, MutableSequence, Optional


logger = logging.getLogger(__name__)


ErrorSeverity = Literal["recoverable", "fatal"]


@dataclass(frozen=True)
class WorkflowError:
    """A serializable diagnostic collected during a workflow.

    The fields deliberately contain strings only (with an optional traceback), so
    a worker can safely return this value through a process pool.  Coordinators
    are responsible for deciding what a recoverable or fatal error means for the
    final workflow result.
    """

    workflow: str
    phase: str
    subject: Optional[str]
    exception_type: str
    message: str
    severity: ErrorSeverity
    traceback: Optional[str] = None

    def __post_init__(self) -> None:
        if self.severity not in ("recoverable", "fatal"):
            raise ValueError(
                "WorkflowError severity must be 'recoverable' or 'fatal'."
            )

    def to_dict(self) -> dict[str, str | None]:
        """Return a stable, JSON-safe representation for callers and the CLI."""

        return {
            "workflow": self.workflow,
            "phase": self.phase,
            "subject": self.subject,
            "exception_type": self.exception_type,
            "message": self.message,
            "severity": self.severity,
            "traceback": self.traceback,
        }


ErrorCallback = Callable[[WorkflowError], None]


def workflow_error_from_exception(
    *,
    workflow: str,
    phase: str,
    subject: Optional[str],
    exception: BaseException,
    severity: ErrorSeverity,
    include_traceback: bool = False,
) -> WorkflowError:
    """Create a serializable workflow error from a caught exception.

    ``severity`` is deliberately required at each catch site. The helper does
    not infer recoverability from exception classes or message text.
    """

    formatted_traceback = None
    if include_traceback:
        formatted_traceback = "".join(
            traceback_module.format_exception(
                type(exception), exception, exception.__traceback__
            )
        )

    return WorkflowError(
        workflow=workflow,
        phase=phase,
        subject=subject,
        exception_type=type(exception).__name__,
        message=str(exception),
        severity=severity,
        traceback=formatted_traceback,
    )


def report_error(
    errors: MutableSequence[WorkflowError],
    error: WorkflowError,
    *,
    error_callback: Optional[ErrorCallback] = None,
) -> None:
    """Collect an error and notify the optional coordinator-side callback.

    Callback exceptions are intentionally contained: reporting must not cause an
    otherwise recoverable workflow error to become fatal.
    """

    errors.append(error)
    if error_callback is None:
        return

    try:
        error_callback(error)
    except Exception:
        logger.debug(
            "Error callback failed while reporting %s/%s for %s",
            error.workflow,
            error.phase,
            error.subject,
            exc_info=True,
        )


def collect_worker_errors(
    errors: MutableSequence[WorkflowError],
    worker_errors: Iterable[WorkflowError],
    *,
    error_callback: Optional[ErrorCallback] = None,
) -> None:
    """Merge worker diagnostics and invoke callbacks in the supplied order.

    Call this only in the thread or process coordinating the workflow. Workers
    return ``WorkflowError`` values as ordinary serializable result data; they
    must not receive or invoke an ``ErrorCallback``. The caller supplies worker
    results in submission/input order when deterministic reporting is required.
    """

    for error in worker_errors:
        report_error(errors, error, error_callback=error_callback)
