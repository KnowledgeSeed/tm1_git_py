import pickle

import pytest

from tm1_git_py.reporting.error_reporting import (
    ErrorSeverity,
    WorkflowError,
    collect_worker_errors,
    report_error,
    workflow_error_from_exception,
)


def _error(*, severity: ErrorSeverity = "recoverable") -> WorkflowError:
    return WorkflowError(
        workflow="export",
        phase="dimensions",
        subject="dimensions/Products.json",
        exception_type="ValueError",
        message="Invalid dimension",
        severity=severity,
    )


def test_workflow_error_is_immutable_and_pickle_safe():
    error = _error()

    assert pickle.loads(pickle.dumps(error)) == error
    with pytest.raises(AttributeError):
        error.message = "changed"  # type: ignore[misc]


def test_workflow_error_requires_known_severity():
    with pytest.raises(ValueError, match="severity"):
        WorkflowError(
            workflow="export",
            phase="dimensions",
            subject=None,
            exception_type="ValueError",
            message="Invalid dimension",
            severity="unknown",  # type: ignore[arg-type]
        )


def test_workflow_error_from_exception_uses_explicit_severity():
    try:
        raise ValueError("Invalid dimension")
    except ValueError as exception:
        error = workflow_error_from_exception(
            workflow="export",
            phase="dimensions",
            subject="dimensions/Products.json",
            exception=exception,
            severity="recoverable",
        )

    assert error.exception_type == "ValueError"
    assert error.message == "Invalid dimension"
    assert error.severity == "recoverable"
    assert error.traceback is None


def test_workflow_error_from_exception_includes_traceback_only_when_requested():
    try:
        raise ValueError("Invalid dimension")
    except ValueError as exception:
        error = workflow_error_from_exception(
            workflow="export",
            phase="dimensions",
            subject=None,
            exception=exception,
            severity="fatal",
            include_traceback=True,
        )

    assert error.traceback is not None
    assert "ValueError: Invalid dimension" in error.traceback


def test_report_error_collects_and_calls_callback_once():
    errors = []
    received = []
    error = _error()

    report_error(errors, error, error_callback=received.append)

    assert errors == [error]
    assert received == [error]


def test_report_error_contains_callback_failures(caplog):
    errors = []
    error = _error()

    def failing_callback(_: WorkflowError) -> None:
        raise RuntimeError("callback failure")

    with caplog.at_level("DEBUG", logger="tm1_git_py.reporting.error_reporting"):
        report_error(errors, error, error_callback=failing_callback)

    assert errors == [error]
    assert "Error callback failed" in caplog.text


def test_collect_worker_errors_preserves_supplied_order_and_dispatches_in_parent():
    first = _error()
    second = WorkflowError(
        workflow="serialize",
        phase="write_file",
        subject="cubes/Sales.json",
        exception_type="OSError",
        message="Disk full",
        severity="fatal",
    )
    errors = []
    received = []

    collect_worker_errors(
        errors,
        [first, second],
        error_callback=received.append,
    )

    assert errors == [first, second]
    assert received == [first, second]
