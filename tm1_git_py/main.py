import argparse
import logging
import multiprocessing
import queue
import shutil
import sys
import threading
import tempfile
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import tracemalloc
from typing import Any, Iterable, Optional
from uuid import uuid4

from TM1py import TM1Service

from tm1_git_py.config import TM1ServersConfig
from tm1_git_py.config.logging_config import setup_logging
from tm1_git_py.db.model_store import ModelStore  # noqa: F401  # patched by tests
from tm1_git_py.internal.process_pool import (
    dispose_process_pool,
    ignore_sigint_in_worker,
    process_pool_executor_kwargs,
    shutdown_process_pool_now,
)
from tm1_git_py.internal.worker_config import resolve_worker_counts
from tm1_git_py.model import Model
from tm1_git_py.model.tm1_project_json import Tm1ProjectJson
from tm1_git_py.reporting.progress_reporting import (
    CallbackProgressSink,
    CompositeProgressSink,
    LoggingProgressSink,
    MultiProcessProgressManager,
    ProgressEvent,
    ProgressSink,
    TqdmProgressSink,
)
from tm1_git_py.reporting.error_reporting import (
    WorkflowError,
    workflow_error_from_exception,
)
from tm1_git_py.services.changeset import import_changeset
from tm1_git_py.services.comparator import Comparator
from tm1_git_py.services.deserializer import deserialize_model
from tm1_git_py.services.exporter import export
from tm1_git_py.services.filter import FilterRules, import_filter
from tm1_git_py.services.serializer import serialize_model
from tm1_git_py import __version__

logger = logging.getLogger(__name__)


class _CliDiagnosticCollector:
    """Collect and format workflow diagnostics in the CLI coordinator.

    Service workers return diagnostics as ordinary result data.  This collector
    deliberately stays in the command process and serves as the external error
    callback for coordinator-local workflows.
    """

    def __init__(self) -> None:
        self.errors: list[WorkflowError] = []
        self._reported_error_ids: set[int] = set()

    def __call__(self, error: WorkflowError) -> None:
        self.errors.append(error)
        self._reported_error_ids.add(id(error))

    def extend(self, errors: Iterable[WorkflowError]) -> list[WorkflowError]:
        added = list(errors)
        for error in added:
            if id(error) not in self._reported_error_ids:
                self.errors.append(error)
                self._reported_error_ids.add(id(error))
        return added

    def log_summary(
        self,
        phase: str,
        errors: Iterable[WorkflowError],
    ) -> None:
        phase_errors = list(errors)
        if not phase_errors:
            logger.info("Workflow diagnostic summary | phase=%s total=0", phase)
            return

        recoverable_count = sum(
            error.severity == "recoverable" for error in phase_errors
        )
        fatal_count = len(phase_errors) - recoverable_count
        logger.warning(
            "Workflow diagnostic summary | phase=%s total=%d recoverable=%d fatal=%d",
            phase,
            len(phase_errors),
            recoverable_count,
            fatal_count,
        )
        for error in phase_errors:
            logger.warning("Workflow diagnostic | %s", error.to_dict())

    @property
    def has_errors(self) -> bool:
        return bool(self.errors)


def _normalize_max_workers(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return None


def _resolve_cli_log_level(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser | None = None,
) -> str:
    """Normalize the level option and its deprecated compatibility alias."""

    requested_level = getattr(args, "log_level", None)
    if bool(getattr(args, "debug", False)):
        if requested_level not in (None, "debug"):
            message = "--debug conflicts with --log-level info"
            if parser is not None:
                parser.error(message)
            raise ValueError(message)
        return "debug"
    return requested_level or "info"


def _debug_progress_enabled(args: argparse.Namespace) -> bool:
    return _resolve_cli_log_level(args) == "debug"


def _create_cli_progress_sink(
    args: argparse.Namespace,
    *,
    worker_count: int | None,
    leave: bool,
) -> tuple[TqdmProgressSink, ProgressSink]:
    """Create the total progress UI and optional debug progress log adapter."""

    debug_progress = _debug_progress_enabled(args)
    tqdm_sink = TqdmProgressSink(
        worker_count=worker_count,
        base_position=0,
        leave=leave,
        thread_tracing_enabled=debug_progress,
    )
    sinks: list[ProgressSink] = [tqdm_sink]
    if debug_progress:
        sinks.append(LoggingProgressSink(logger))
    return tqdm_sink, sinks[0] if len(sinks) == 1 else CompositeProgressSink(sinks)


def _split_compare_workers(max_workers: int) -> tuple[int, int]:
    total = max(1, int(max_workers))
    source_workers = max(1, total // 2)
    target_workers = max(1, total - source_workers)
    return source_workers, target_workers


def _tm1_connection(server_name: str) -> TM1Service:
    config = TM1ServersConfig()
    config.load()
    return _tm1_connection_from_config(config, server_name)


def _tm1_connection_from_config(config: TM1ServersConfig, server_name: str) -> TM1Service:
    server_config = config.get(server_name)
    logger.debug(
        "Creating TM1 connection for server='%s' base_url='%s' user='%s'",
        server_name,
        server_config.base_url,
        server_config.user,
    )

    tm1 = TM1Service(
        base_url=server_config.base_url,
        user=server_config.user,
        password=server_config.password or ""
    )
    return tm1


def _deserialize_model_worker(
    model_dir: str,
    progress_sink: ProgressSink,
    max_workers: int,
) -> tuple[Model, list[WorkflowError]]:

    model, errors = deserialize_model(
        model_dir,
        progress_sink=progress_sink,
        max_workers=max_workers,
    )
    return model, errors


def _collect_deserialize_future(
    future: Any,
    *,
    model_path: Path,
    side: str,
    diagnostic_collector: _CliDiagnosticCollector,
) -> Model | None:
    """Resolve one deserialization future in the parent process.

    Child processes return only models and serializable diagnostics.  A failed
    future is converted here so pool exceptions remain contextual diagnostics
    rather than bypassing the CLI's output policy.
    """

    try:
        model, worker_errors = future.result()
    except KeyboardInterrupt:
        raise
    except Exception as exc:
        error = workflow_error_from_exception(
            workflow="deserialize",
            phase="process_pool",
            subject=str(model_path),
            exception=exc,
            severity="fatal",
        )
        diagnostic_collector(error)
        diagnostic_collector.log_summary(f"{side}-deserialize", [error])
        return None

    phase_errors = diagnostic_collector.extend(worker_errors)
    diagnostic_collector.log_summary(f"{side}-deserialize", phase_errors)
    return model


def _consume_compare_progress_events(
    progress_queue: Any,
    source_sink: ProgressSink,
    target_sink: ProgressSink,
    stop_event: threading.Event,
) -> None:
    while True:
        try:
            if stop_event.is_set():
                try:
                    item = progress_queue.get_nowait()
                except queue.Empty:
                    break
            else:
                try:
                    item = progress_queue.get(timeout=0.1)
                except queue.Empty:
                    continue
        except (BrokenPipeError, EOFError, OSError):
            # Manager queue can disappear during shutdown; exit silently.
            break
        if item is None:
            if stop_event.is_set():
                break
            continue
        tqdm_group_index, event = item
        if int(tqdm_group_index) == 0:
            source_sink.on_event(event)
        else:
            target_sink.on_event(event)


def _validate_export_destination(output_path: Path, overwrite: bool) -> bool:
    """Confirm publication is permitted without touching existing output."""

    if not output_path.exists():
        return True
    if not output_path.is_dir():
        logger.error("Model output path is not a directory: %s", output_path)
        return False
    if not overwrite:
        logger.error(
            "Model folder '%s' already exists. Use --overwrite to replace it after a successful export.",
            output_path,
        )
        return False
    return True


def _create_export_staging_directory(output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return Path(
        tempfile.mkdtemp(
            prefix=f".{output_path.name}.tm1gitpy-staging-",
            dir=output_path.parent,
        )
    )


def _promote_export_staging_directory(
    staging_path: Path,
    output_path: Path,
    *,
    overwrite: bool,
) -> None:
    """Publish a staged export while preserving existing output on failure."""

    if not output_path.exists():
        staging_path.replace(output_path)
        return

    if not overwrite:
        raise FileExistsError(f"Model folder already exists: {output_path}")
    if not output_path.is_dir():
        raise NotADirectoryError(f"Model output path is not a directory: {output_path}")

    backup_path = output_path.with_name(
        f".{output_path.name}.tm1gitpy-backup-{uuid4().hex}"
    )
    output_path.replace(backup_path)
    try:
        staging_path.replace(output_path)
    except Exception:
        backup_path.replace(output_path)
        raise

    try:
        shutil.rmtree(backup_path)
    except OSError:
        logger.warning("Could not remove previous export backup: %s", backup_path)


def _filter_path_from_arg(filter_arg: str) -> Path:
    raw = str(filter_arg).strip()
    if raw.startswith("file://"):
        return Path(raw[len("file://"):].strip()).expanduser().resolve()
    return Path(raw).expanduser().resolve()


def _load_filter_rules_from_path(filter_path: Path) -> list[str]:
    if not filter_path.exists():
        logger.error("Filter file '%s' not found.", filter_path)
        sys.exit(1)
    try:
        if Tm1ProjectJson.is_tm1project_path(filter_path):
            project = Tm1ProjectJson.from_path(filter_path)
            rules = project.ignore_rules()
            logger.info(
                "Loaded %d ignore rule(s) from tm1project: %s",
                len(rules),
                filter_path,
            )
            return rules
        filter_rules = import_filter(str(filter_path))
        logger.info("Loaded %d filter rule(s) from: %s", len(filter_rules), filter_path)
        return filter_rules
    except Exception:
        logger.exception("Error loading filter from: %s", filter_path)
        sys.exit(1)


def _load_filter_rules(filter_file: str | None) -> list[str]:
    if not filter_file:
        return []
    raw = str(filter_file).strip()

    if raw.startswith("file://"):
        return _load_filter_rules_from_path(_filter_path_from_arg(raw))

    if "," in raw:
        rules = [part.strip() for part in raw.split(",") if part.strip()]
        logger.info("Loaded %d inline filter rule(s)", len(rules))
        return rules

    return _load_filter_rules_from_path(Path(raw).expanduser().resolve())


def _resolve_filter_rules(
    filter_arg: str | None,
) -> Optional[FilterRules]:
    """Resolve export/compare filter argument to FilterRules (tm1project includes defaults)."""
    if not filter_arg:
        return None
    raw = str(filter_arg).strip()

    def _from_path(filter_path: Path) -> Optional[FilterRules]:
        if not filter_path.exists():
            logger.error("Filter file '%s' not found.", filter_path)
            sys.exit(1)
        try:
            if Tm1ProjectJson.is_tm1project_path(filter_path):
                project = Tm1ProjectJson.from_path(filter_path)
                rules = project.to_filter_rules()
                logger.info(
                    "Loaded %d ignore rule(s) from tm1project: %s",
                    len(project.ignore),
                    filter_path,
                )
                return rules
            lines = import_filter(str(filter_path))
            logger.info("Loaded %d filter rule(s) from: %s", len(lines), filter_path)
            return FilterRules(lines) if lines else None
        except Exception:
            logger.exception("Error loading filter from: %s", filter_path)
            sys.exit(1)

    if raw.startswith("file://"):
        return _from_path(_filter_path_from_arg(raw))

    if "," in raw:
        rules = [part.strip() for part in raw.split(",") if part.strip()]
        logger.info("Loaded %d inline filter rule(s)", len(rules))
        return FilterRules(rules) if rules else None

    return _from_path(Path(raw).expanduser().resolve())


def _add_common_cli_options(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Optional log file path or directory for timestamped execution logs",
    )
    p.add_argument(
        "--console-logs",
        action="store_true",
        help="Enable console log output in addition to progress UI",
    )
    p.add_argument(
        "--log-level",
        choices=("info", "debug"),
        default=None,
        help="Application log level (default: info)",
    )
    p.add_argument(
        "--debug",
        action="store_true",
        help="Deprecated alias for --log-level debug; also enables worker tracing progress rows",
    )


def _cmd_export(args: argparse.Namespace) -> int:
    config = TM1ServersConfig()
    config.load()
    tm1_service = _tm1_connection_from_config(config, args.server)
    model_output_folder = args.model_output_folder or "export"
    model_output_path = Path(model_output_folder).expanduser().resolve()
    diagnostic_collector = _CliDiagnosticCollector()
    if not _validate_export_destination(model_output_path, args.overwrite):
        return 1

    filter_rules = _resolve_filter_rules(args.filter)

    logger.info("Exporting model to folder: %s", model_output_folder)
    model_id = model_output_path.name.strip()
    if not model_id:
        raise ValueError("model_id must not be empty")
    requested_max_workers = resolve_worker_counts(args.max_workers).max_workers

    tqdm_sink, main_sink = _create_cli_progress_sink(
        args,
        worker_count=requested_max_workers,
        leave=True,
    )
    staging_path: Path | None = None
    try:
        try:
            staging_path = _create_export_staging_directory(model_output_path)
        except Exception as exc:
            error = workflow_error_from_exception(
                workflow="export",
                phase="staging",
                subject=str(model_output_path),
                exception=exc,
                severity="fatal",
            )
            diagnostic_collector(error)
            diagnostic_collector.log_summary("staging", [error])
            return 1

        exported_model: Model | None = None
        try:
            exported_model, export_errors = export(
                tm1_service,
                model_id=model_id,
                filter_rules=filter_rules,
                progress_sink=main_sink,
                max_workers=requested_max_workers,
                error_callback=diagnostic_collector,
            )
            phase_errors = diagnostic_collector.extend(export_errors)
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            error = workflow_error_from_exception(
                workflow="export",
                phase="export",
                subject=model_id,
                exception=exc,
                severity="fatal",
            )
            diagnostic_collector(error)
            phase_errors = [error]
        finally:
            tqdm_sink.reset_bars()
        diagnostic_collector.log_summary("export", phase_errors)

        if exported_model is not None:
            try:
                serialization_errors = serialize_model(
                    exported_model,
                    str(staging_path),
                    progress_sink=main_sink,
                    max_workers=requested_max_workers,
                    error_callback=diagnostic_collector,
                )
                phase_errors = diagnostic_collector.extend(serialization_errors)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                error = workflow_error_from_exception(
                    workflow="serialize",
                    phase="serialize",
                    subject=str(staging_path),
                    exception=exc,
                    severity="fatal",
                )
                diagnostic_collector(error)
                phase_errors = [error]
            diagnostic_collector.log_summary("serialize", phase_errors)

        if diagnostic_collector.has_errors:
            logger.warning("Model output was not published because diagnostics were collected")
            return 1

        try:
            _promote_export_staging_directory(
                staging_path,
                model_output_path,
                overwrite=args.overwrite,
            )
        except Exception as exc:
            error = workflow_error_from_exception(
                workflow="export",
                phase="publish",
                subject=str(model_output_path),
                exception=exc,
                severity="fatal",
            )
            diagnostic_collector(error)
            diagnostic_collector.log_summary("publish", [error])
            return 1

        logger.info("Model serialized to: %s", model_output_path)
        return 0
    finally:
        try:
            main_sink.close()
        finally:
            if staging_path is not None and staging_path.exists():
                shutil.rmtree(staging_path, ignore_errors=True)


def _cmd_compare(args: argparse.Namespace) -> int:
    source = Path(args.source).expanduser().resolve()
    target = Path(args.target).expanduser().resolve()
    if not source.is_dir():
        logger.error("Source model path is not a directory: %s", source)
        return 1
    if not target.is_dir():
        logger.error("Target model path is not a directory: %s", target)
        return 1

    changeset = None
    tqdm_sink: TqdmProgressSink | None = None
    diagnostic_collector = _CliDiagnosticCollector()
    try:
        requested_max_workers = resolve_worker_counts(args.max_workers).max_workers
        
        tqdm_sink, queuing_progress_sink = _create_cli_progress_sink(
            args,
            worker_count=requested_max_workers,
            leave=False,
        )
        source_workers, target_workers = _split_compare_workers(requested_max_workers)

        multi_process_progress_manager: Optional[MultiProcessProgressManager] = None
        multi_process_progress_manager = MultiProcessProgressManager(queuing_progress_sink)
        multi_process_progress_manager.start()
        queuing_progress_sink = multi_process_progress_manager.get_multi_process_progress_queue_sink()

        logger.info("Loading source model from %s", source)
        pool: ProcessPoolExecutor | None = None
        try:
            multiprocessing.freeze_support()
            pool = ProcessPoolExecutor(
                **process_pool_executor_kwargs(max_workers=2, initializer=ignore_sigint_in_worker),
            )
            source_future = pool.submit(_deserialize_model_worker, str(source), queuing_progress_sink, source_workers)
            target_future = pool.submit(_deserialize_model_worker, str(target), queuing_progress_sink, target_workers)
            # ``Model`` (and the SQLite-backed sequences inside its hierarchies)
            # is picklable: ``StoreBackedSequence.__getstate__`` drops the live
            # ``ModelStore`` and the receiving process re-acquires it through
            # ``ModelStore.for_db_path`` lazily on first access.
            model_source = _collect_deserialize_future(
                source_future,
                model_path=source,
                side="source",
                diagnostic_collector=diagnostic_collector,
            )
            model_target = _collect_deserialize_future(
                target_future,
                model_path=target,
                side="target",
                diagnostic_collector=diagnostic_collector,
            )

            tqdm_sink.reset_bars()
            logger.info("Loading target model from %s", target)

            if model_source is None or model_target is None:
                logger.warning("Comparison was skipped because a model could not be deserialized")
                return 1

            extra_filter = _resolve_filter_rules(args.filter_rules)

            comparator = Comparator()

            changeset, compare_errors = comparator.compare(
                model_source,
                model_target,
                mode=args.mode,
                filter_rules=extra_filter,
                progress_sink=queuing_progress_sink,
                error_callback=diagnostic_collector,
            )
            phase_errors = diagnostic_collector.extend(compare_errors)
            diagnostic_collector.log_summary("compare", phase_errors)
        
        except KeyboardInterrupt:
            if pool is not None:
                shutdown_process_pool_now(pool)
                pool = None
            raise
        finally:
            if pool is not None:
                dispose_process_pool(pool, mode="aggressive", log=True)
                pool = None
            multi_process_progress_manager.close()

        if diagnostic_collector.has_errors:
            logger.warning("Changeset output was not written because diagnostics were collected")
            return 1

        out = args.output
        if not out:
            out = "changeset.yaml" if args.format == "yaml" else "changeset.json"
        output_path = Path(out).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)

        changeset.export(output_path, format=args.format, progress_sink=tqdm_sink)
        if args.format == "json":
            logger.info("Wrote JSON changeset (%d change(s)) to %s", len(changeset.changes), output_path)
        else:
            logger.info("Wrote YAML changeset (%d change(s)) to %s", len(changeset.changes), output_path)
        return 0
    finally:
        if changeset is not None:
            changeset.close()
        if tqdm_sink is not None:
            tqdm_sink.close()


def _cmd_apply(args: argparse.Namespace) -> None:
    changeset_path = Path(args.changeset).expanduser().resolve()
    if not changeset_path.is_file():
        logger.error("Changeset file not found: %s", changeset_path)
        sys.exit(1)

    tm1_service = _tm1_connection(args.server)
    changeset = import_changeset(changeset_path)

    status_dir = Path(args.status_dir).expanduser().resolve() if args.status_dir else None
    _, apply_progress_sink = _create_cli_progress_sink(
        args,
        worker_count=1,
        leave=False,
    )
    try:
        ok, errors = changeset.apply(
            tm1_service,
            status_dir=status_dir,
            execution_id=args.execution_id,
            fail_fast=not args.no_fail_fast,
            progress_sink=apply_progress_sink,
        )
    finally:
        apply_progress_sink.close()
        changeset.close()
    if ok:
        logger.info("Apply finished successfully")
    else:
        logger.error("Apply finished with failures: %s", errors)
        sys.exit(1)


def _cmd_changeset_filter(args: argparse.Namespace) -> None:
    changeset_path = Path(args.changeset_path).expanduser().resolve()
    if not changeset_path.is_file():
        logger.error("Changeset file not found: %s", changeset_path)
        sys.exit(1)

    filter_rules = _load_filter_rules(args.filter_rules)
    changeset = import_changeset(changeset_path)
    tqdm_sink, progress_sink = _create_cli_progress_sink(
        args,
        worker_count=1,
        leave=False,
    )
    try:
        toggled_count = changeset.filter(filter_rules)
        changeset.export(changeset_path, progress_sink=progress_sink)
    finally:
        progress_sink.close()
        changeset.close()
    logger.info(
        "Applied changeset filter rules and toggled apply for %d change(s): %s",
        toggled_count,
        changeset_path,
    )


def main():
    # Must run before argparse in frozen binaries so multiprocessing helper
    # processes (e.g. resource_tracker) do not get parsed as CLI commands.
    multiprocessing.freeze_support()
    tracemalloc.start()
    parser = argparse.ArgumentParser(description="TM1 Git Py - TM1 model export, compare, apply, and changeset filtering")
    parser.add_argument(
        "--version",
        action="version",
        version=f"tm1gitpy {__version__}",
        help="Show version and exit",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_export = sub.add_parser("export", help="Export model from TM1 to a folder")
    _add_common_cli_options(p_export)
    p_export.add_argument("-s", "--server", type=str, required=True, help="TM1 server name from tm1servers config")
    p_export.add_argument(
        "-mo", "--model-output-folder",
        type=str,
        default="export",
        help="Folder to write the serialized model",
    )
    p_export.add_argument("-o", "--overwrite", action="store_true", help="Clear output folder if it already exists")
    p_export.add_argument(
        "-f",
        "--filter",
        type=str,
        help="Filter rules: file:// path to filter.txt or tm1project.json, or comma-separated rules",
    )
    p_export.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help=(
            "Total CPU + IO worker count. If defined, workers are split near a 1:3 CPU/IO ratio. "
            "If omitted, CPU workers default to cpu_count/2 + 1 and IO workers to 3x that value."
        ),
    )
    p_export.set_defaults(handler=_cmd_export)

    p_compare = sub.add_parser("compare", help="Compare two model folders and write a changeset file")
    _add_common_cli_options(p_compare)
    p_compare.add_argument(
        "--source",
        type=str,
        required=True,
        help="Base / old model directory (e.g. Git branch A)",
    )
    p_compare.add_argument(
        "--target",
        type=str,
        required=True,
        help="New model directory (e.g. Git branch B)",
    )
    p_compare.add_argument(
        "-o", "--output",
        type=str,
        default=None,
        help="Output changeset path (default: changeset.yaml or changeset.json by --format)",
    )
    p_compare.add_argument(
        "--mode",
        type=str,
        choices=["full", "add_only"],
        default="full",
        help="full: add/remove/modify; add_only: add/modify only",
    )
    p_compare.add_argument(
        "-f",
        "--filter-rules",
        type=str,
        help="Filter rules: file:// path to filter.txt or tm1project.json, or comma-separated rules",
    )
    p_compare.add_argument(
        "--format",
        type=str,
        choices=["yaml", "json"],
        default="yaml",
        help="Changeset output format",
    )
    p_compare.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help=(
            "Total CPU + IO worker count. Compare uses the resolved CPU worker count for deserialization. "
            "CPU workers are split between source and target; odd values assign one extra to target."
        ),
    )
    p_compare.set_defaults(handler=_cmd_compare)

    p_apply = sub.add_parser("apply", help="Apply a changeset file to a TM1 server")
    _add_common_cli_options(p_apply)
    p_apply.add_argument("-s", "--server", type=str, required=True, help="TM1 server name from tm1servers config")
    p_apply.add_argument(
        "-c", "--changeset",
        type=str,
        required=True,
        help="Path to changeset YAML or JSON file",
    )
    p_apply.add_argument(
        "--status-dir",
        type=str,
        default=None,
        help="Directory for execution status files (optional)",
    )
    p_apply.add_argument("--execution-id", type=str, default=None, help="Execution id for status tracking")
    p_apply.add_argument(
        "--no-fail-fast",
        action="store_true",
        help="Continue applying after a failed change",
    )
    p_apply.set_defaults(handler=_cmd_apply)

    p_changeset_filter = sub.add_parser(
        "changset-filter",
        aliases=["changeset-filter"],
        help="Toggle apply flags in a changeset using filter rules",
    )
    _add_common_cli_options(p_changeset_filter)
    p_changeset_filter.add_argument(
        "--changeset-path",
        type=str,
        required=True,
        help="Path to changeset YAML or JSON file",
    )
    p_changeset_filter.add_argument(
        "--filter-rules",
        type=str,
        required=True,
        help="Filter rules as file path, file:// URI, or comma-separated rules",
    )
    p_changeset_filter.set_defaults(handler=_cmd_changeset_filter)

    args = parser.parse_args()
    args.log_level = _resolve_cli_log_level(args, parser)
    setup_logging(
        args.log_level.upper(),
        enable_console=(
            bool(getattr(args, "console_logs", False))
            or args.log_level == "debug"
        ),
        log_file=getattr(args, "log_file", None),
        command_name=getattr(args, "command", None),
    )
    if bool(getattr(args, "debug", False)):
        logger.warning("--debug is deprecated; use --log-level debug")
    logger.info("Command started: %s", args.command)
    try:
        exit_code = args.handler(args)
    except KeyboardInterrupt:
        logger.warning("Command interrupted by user")
        return 130
    logger.info("Command finished: %s", args.command)
    return 0 if exit_code is None else exit_code


if __name__ == "__main__":
    sys.exit(main())
