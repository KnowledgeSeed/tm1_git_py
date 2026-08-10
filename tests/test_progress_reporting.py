"""Unit coverage for progress-only sinks and their DEBUG logging boundary."""

import logging
import os

import tm1_git_py.reporting.progress_reporting as progress_reporting_module
from tm1_git_py.reporting.progress_reporting import (
    CallbackProgressSink,
    CompositeProgressSink,
    LoggingProgressSink,
    ProgressEvent,
    ProgressKind,
    ProgressScope,
    ProgressUnit,
    TqdmProgressSink,
)


def test_callback_progress_sink_accepts_callable_and_forwards_event():
    events = []
    sink = CallbackProgressSink(lambda event: events.append(event))
    event = ProgressEvent.make(
        kind=ProgressKind.UPDATE,
        scope=ProgressScope.TOTAL,
        current=10,
        total=100,
        unit=ProgressUnit.BYTE,
    )
    sink.on_event(event)
    assert events == [event]


def test_callback_progress_sink_accepts_on_progress_object():
    class Handler:
        def __init__(self):
            self.events = []

        def on_progress(self, event):
            self.events.append(event)

    handler = Handler()
    sink = CallbackProgressSink(handler)
    event = ProgressEvent.make(
        kind=ProgressKind.UPDATE,
        scope=ProgressScope.WORKER,
        unit=ProgressUnit.LINE,
        current=0,
        total=1,
        message="reading",
    )
    sink.on_event(event)
    assert handler.events == [event]


def test_composite_and_logging_progress_sink_emit():
    records = []

    class CaptureHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    logger = logging.getLogger("tm1_git_py.tests.progress")
    logger.setLevel(logging.DEBUG)
    capture = CaptureHandler()
    logger.addHandler(capture)
    logger.propagate = False

    callback_events = []
    composite = CompositeProgressSink(
        [
            LoggingProgressSink(logger),
            CallbackProgressSink(lambda event: callback_events.append(event)),
        ]
    )
    event = ProgressEvent.make(
        kind=ProgressKind.START,
        scope=ProgressScope.WORKER,
        unit=ProgressUnit.LINE,
        current=0,
        total=1,
        path="x.json",
        message="reading",
    )
    composite.on_event(event)
    composite.close()

    assert callback_events == [event]
    assert any("progress | start WORKER" in record.getMessage() for record in records)
    assert all(record.levelno == logging.DEBUG for record in records)

    logger.removeHandler(capture)


def test_progress_event_worker_id_additive_field():
    event = ProgressEvent.make(
        kind=ProgressKind.UPDATE,
        scope=ProgressScope.WORKER,
        unit=ProgressUnit.LINE,
        current=0,
        total=1,
        worker_id="task-1",
    )
    assert event.worker_id == "task-1"
    legacy_event = ProgressEvent.make(
        kind=ProgressKind.UPDATE,
        scope=ProgressScope.WORKER,
        unit=ProgressUnit.LINE,
        current=0,
        total=1,
    )
    assert isinstance(legacy_event.worker_id, str)
    assert len(legacy_event.worker_id) == 64


def test_logging_progress_sink_includes_worker_id():
    records = []

    class CaptureHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    logger = logging.getLogger("tm1_git_py.tests.progress.worker_id")
    logger.setLevel(logging.DEBUG)
    capture = CaptureHandler()
    logger.addHandler(capture)
    logger.propagate = False
    sink = LoggingProgressSink(logger)
    sink.on_event(
        ProgressEvent.make(
            kind=ProgressKind.UPDATE,
            scope=ProgressScope.WORKER,
            unit=ProgressUnit.LINE,
            current=0,
            total=1,
            worker_id="wid-7",
        )
    )
    assert any("worker_id=wid-7" in record.getMessage() for record in records)
    assert all(record.levelno == logging.DEBUG for record in records)
    logger.removeHandler(capture)


def test_generic_tqdm_sink_worker_id_mapping_and_slot_height(monkeypatch):
    class _FakeBar:
        def __init__(self, *args, **kwargs):
            self.total = int(kwargs.get("total", 1))
            self.n = 0
            self.desc = str(kwargs.get("desc", ""))

        def reset(self, total=1):
            self.total = int(total)
            self.n = 0

        def set_description_str(self, value, refresh=False):
            _ = refresh
            self.desc = str(value)

        def refresh(self):
            return

        def close(self):
            return

    class _FakeTty:
        def isatty(self):
            return True

    monkeypatch.setattr(progress_reporting_module, "tqdm", _FakeBar)
    monkeypatch.setattr(progress_reporting_module.sys, "stderr", _FakeTty())
    monkeypatch.setattr(
        progress_reporting_module.shutil,
        "get_terminal_size",
        lambda fallback=(120, 24): os.terminal_size(fallback),
    )

    sink = TqdmProgressSink(worker_count=2, base_position=7, thread_tracing_enabled=True)
    try:
        assert sink.slot_height == 3
        assert sink.base_position == 7

        event_a_1 = ProgressEvent.make(kind=ProgressKind.UPDATE, scope=ProgressScope.WORKER, unit=ProgressUnit.LINE, current=1, total=10, worker_id="A")
        event_b = ProgressEvent.make(kind=ProgressKind.UPDATE, scope=ProgressScope.WORKER, unit=ProgressUnit.LINE, current=1, total=10, worker_id="B")
        event_a_2 = ProgressEvent.make(kind=ProgressKind.UPDATE, scope=ProgressScope.WORKER, unit=ProgressUnit.LINE, current=2, total=10, worker_id="A")
        sink.on_event(event_a_1)
        sink.on_event(event_b)
        first_assigned = sink.worker_bar_dict["A"]
        second_assigned = sink.worker_bar_dict["B"]
        sink.on_event(event_a_2)

        assert len(sink.worker_bar_dict) == 2
        assert sink.worker_bar_dict["A"] is first_assigned
        assert sink.worker_bar_dict["B"] is second_assigned
        assert first_assigned is sink._worker_bars[0]
        assert second_assigned is sink._worker_bars[1]
    finally:
        sink.close()


def test_generic_tqdm_sink_releases_worker_mapping_on_final_event(monkeypatch):
    class _FakeBar:
        def __init__(self, *args, **kwargs):
            self.total = int(kwargs.get("total", 1))
            self.n = 0
            self.desc = str(kwargs.get("desc", ""))

        def reset(self, total=1):
            self.total = int(total)
            self.n = 0

        def set_description_str(self, value, refresh=False):
            _ = refresh
            self.desc = str(value)

        def refresh(self):
            return

        def close(self):
            return

    class _FakeTty:
        def isatty(self):
            return True

    monkeypatch.setattr(progress_reporting_module, "tqdm", _FakeBar)
    monkeypatch.setattr(progress_reporting_module.sys, "stderr", _FakeTty())
    monkeypatch.setattr(
        progress_reporting_module.shutil,
        "get_terminal_size",
        lambda fallback=(120, 24): os.terminal_size(fallback),
    )

    sink = TqdmProgressSink(worker_count=1, base_position=0, leave=True, thread_tracing_enabled=True)
    try:
        sink.on_event(ProgressEvent.make(kind=ProgressKind.START, scope=ProgressScope.WORKER, unit=ProgressUnit.LINE, current=0, total=10, worker_id="A"))
        assert "A" in sink.worker_bar_dict
        sink.on_event(ProgressEvent.make(kind=ProgressKind.UPDATE, scope=ProgressScope.WORKER, unit=ProgressUnit.LINE, current=10, total=10, worker_id="A"))
        assert "A" in sink.worker_bar_dict
    finally:
        sink.close()

