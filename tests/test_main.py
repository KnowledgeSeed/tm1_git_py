import logging
from pathlib import Path
import sys
import types
from unittest import mock

import pytest

import tm1_git_py.main as main_module
from tm1_git_py.main import (
    _CliDiagnosticCollector,
    _collect_deserialize_future,
    _create_cli_progress_sink,
)
from tm1_git_py.model import Model
from tm1_git_py.reporting.error_reporting import WorkflowError


def _error(*, severity: str = "recoverable") -> WorkflowError:
    return WorkflowError(
        workflow="compare",
        phase="objects",
        subject="cubes/Sales.json",
        exception_type="ValueError",
        message="Invalid cube",
        severity=severity,  # type: ignore[arg-type]
    )


def test_cli_diagnostic_collector_logs_structured_phase_summary(caplog):
    collector = _CliDiagnosticCollector()
    recoverable = _error()
    fatal = _error(severity="fatal")

    with caplog.at_level(logging.INFO, logger="tm1_git_py.main"):
        phase_errors = collector.extend([recoverable, fatal])
        collector.log_summary("compare", phase_errors)

    assert collector.errors == [recoverable, fatal]
    assert collector.has_errors
    assert "phase=compare total=2 recoverable=1 fatal=1" in caplog.text
    assert "'workflow': 'compare'" in caplog.text


def test_cli_diagnostic_collector_logs_empty_phase_at_info(caplog):
    collector = _CliDiagnosticCollector()

    with caplog.at_level(logging.INFO, logger="tm1_git_py.main"):
        collector.log_summary("export", [])

    assert not collector.has_errors
    assert "phase=export total=0" in caplog.text


def test_cli_diagnostic_collector_does_not_duplicate_callback_errors():
    collector = _CliDiagnosticCollector()
    error = _error()

    collector(error)
    collector.extend([error])

    assert collector.errors == [error]


def test_collect_deserialize_future_converts_pool_failure_to_fatal_error(tmp_path):
    class FailedFuture:
        def result(self):
            raise RuntimeError("worker exited")

    collector = _CliDiagnosticCollector()

    model = _collect_deserialize_future(
        FailedFuture(),
        model_path=Path(tmp_path / "source"),
        side="source",
        diagnostic_collector=collector,
    )

    assert model is None
    assert len(collector.errors) == 1
    error = collector.errors[0]
    assert error.workflow == "deserialize"
    assert error.phase == "process_pool"
    assert error.subject == str(tmp_path / "source")
    assert error.severity == "fatal"


@pytest.mark.parametrize(
    ("option_args", "expected_level"),
    [
        ([], "INFO"),
        (["--log-level", "info"], "INFO"),
        (["--log-level", "debug"], "DEBUG"),
        (["--debug"], "DEBUG"),
    ],
)
def test_main_normalizes_log_level_and_legacy_debug_alias(
    monkeypatch,
    option_args,
    expected_level,
):
    configured = {}
    received_args = []
    monkeypatch.setattr(
        sys,
        "argv",
        ["tm1gitpy", "export", "--server", "server", *option_args],
    )
    monkeypatch.setattr(
        main_module,
        "setup_logging",
        lambda level, **kwargs: configured.update(level=level, **kwargs),
    )
    monkeypatch.setattr(
        main_module,
        "_cmd_export",
        lambda args: (received_args.append(args), 0)[1],
    )

    assert main_module.main() == 0

    assert configured["level"] == expected_level
    assert configured["enable_console"] is (expected_level == "DEBUG")
    assert received_args[0].log_level == expected_level.lower()


def test_main_rejects_conflicting_log_level_and_debug(monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "tm1gitpy",
            "export",
            "--server",
            "server",
            "--log-level",
            "info",
            "--debug",
        ],
    )

    with pytest.raises(SystemExit) as exit_info:
        main_module.main()

    assert exit_info.value.code == 2
    assert "conflicts with --log-level info" in capsys.readouterr().err


def test_debug_progress_sink_includes_logging_sink_without_log_file(monkeypatch):
    tqdm_sink = mock.Mock()
    logging_sink = mock.Mock()
    composite_sink = mock.Mock()
    tqdm_factory = mock.Mock(return_value=tqdm_sink)
    logging_factory = mock.Mock(return_value=logging_sink)
    composite_factory = mock.Mock(return_value=composite_sink)
    monkeypatch.setattr(main_module, "TqdmProgressSink", tqdm_factory)
    monkeypatch.setattr(main_module, "LoggingProgressSink", logging_factory)
    monkeypatch.setattr(main_module, "CompositeProgressSink", composite_factory)

    returned_tqdm, progress_sink = _create_cli_progress_sink(
        types.SimpleNamespace(log_level="debug", debug=False, log_file=None),
        worker_count=3,
        leave=True,
    )

    assert returned_tqdm is tqdm_sink
    assert progress_sink is composite_sink
    tqdm_factory.assert_called_once_with(
        worker_count=3,
        base_position=0,
        leave=True,
        thread_tracing_enabled=True,
    )
    logging_factory.assert_called_once_with(main_module.logger)
    composite_factory.assert_called_once_with([tqdm_sink, logging_sink])


def test_info_progress_sink_suppresses_logging_progress_adapter(monkeypatch):
    tqdm_sink = mock.Mock()
    tqdm_factory = mock.Mock(return_value=tqdm_sink)
    logging_factory = mock.Mock()
    monkeypatch.setattr(main_module, "TqdmProgressSink", tqdm_factory)
    monkeypatch.setattr(main_module, "LoggingProgressSink", logging_factory)

    returned_tqdm, progress_sink = _create_cli_progress_sink(
        types.SimpleNamespace(log_level="info", debug=False, log_file="run.log"),
        worker_count=1,
        leave=False,
    )

    assert returned_tqdm is tqdm_sink
    assert progress_sink is tqdm_sink
    tqdm_factory.assert_called_once_with(
        worker_count=1,
        base_position=0,
        leave=False,
        thread_tracing_enabled=False,
    )
    logging_factory.assert_not_called()


def test_compare_diagnostics_prevent_changeset_export(monkeypatch, tmp_path):
    source = tmp_path / "source"
    target = tmp_path / "target"
    source.mkdir()
    target.mkdir()
    changeset = mock.Mock(changes=[])
    diagnostic = _error()

    class CompletedFuture:
        def __init__(self, model):
            self.model = model

        def result(self):
            return self.model, []

    class FakePool:
        def __init__(self, **_kwargs):
            model = Model(cubes=[], dimensions=[], processes=[], chores=[])
            self.futures = iter([CompletedFuture(model), CompletedFuture(model)])

        def submit(self, *_args):
            return next(self.futures)

    class FakeProgressManager:
        def __init__(self, sink):
            self.sink = sink

        def start(self):
            return None

        def get_multi_process_progress_queue_sink(self):
            return self.sink

        def close(self):
            return None

    class FakeComparator:
        def compare(self, *_args, **kwargs):
            kwargs["error_callback"](diagnostic)
            return changeset, [diagnostic]

    monkeypatch.setattr(main_module, "TqdmProgressSink", mock.Mock())
    monkeypatch.setattr(main_module, "MultiProcessProgressManager", FakeProgressManager)
    monkeypatch.setattr(main_module, "ProcessPoolExecutor", FakePool)
    monkeypatch.setattr(main_module, "process_pool_executor_kwargs", lambda **_kwargs: {})
    monkeypatch.setattr(main_module, "dispose_process_pool", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "Comparator", FakeComparator)

    args = types.SimpleNamespace(
        source=str(source),
        target=str(target),
        max_workers=1,
        debug=False,
        log_level="info",
        log_file=None,
        filter_rules=None,
        mode="full",
        output=str(tmp_path / "changeset.json"),
        format="json",
    )

    assert main_module._cmd_compare(args) == 1
    changeset.export.assert_not_called()
