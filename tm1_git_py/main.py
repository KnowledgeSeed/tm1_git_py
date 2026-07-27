import argparse
import logging
import multiprocessing
import queue
import shutil
import sys
import threading
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import tracemalloc
from typing import Any, Optional

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
from tm1_git_py.services.changeset import import_changeset
from tm1_git_py.services.apply import (
    apply_post_pull_operations,
    apply_post_push_operations,
    apply_pre_pull_operations,
    apply_pre_push_operations,
)
from tm1_git_py.services.comparator import Comparator, TqdmComparatorProgressSink
from tm1_git_py.services.deserializer import deserialize_model
from tm1_git_py.services.exporter import export
from tm1_git_py.services.filter import FilterRules, import_filter
from tm1_git_py.services.serializer import serialize_model
from tm1_git_py import __version__

logger = logging.getLogger(__name__)


def _normalize_max_workers(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return None


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
) -> tuple[Model, dict[str, str]]:

    model, errors = deserialize_model(
        model_dir,
        progress_sink=progress_sink,
        max_workers=max_workers,
    )
    return model, errors


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


def _prepare_model_folder(model_folder: str, overwrite: bool = False):
    model_path = Path(model_folder)
    if model_path.exists() and model_path.is_dir():
        if not overwrite:
            logger.error(
                "Model folder '%s' already exists. Use --overwrite flag to clear and overwrite.",
                model_folder,
            )
            sys.exit(1)
        logger.info("Clearing existing model folder: %s", model_folder)
        shutil.rmtree(model_folder)


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
        "--debug",
        action="store_true",
        help="Enable detailed worker/thread progress bars in the terminal progress UI",
    )


def _cmd_export(args: argparse.Namespace) -> None:
    config = TM1ServersConfig()
    config.load()
    tm1_service = _tm1_connection_from_config(config, args.server)
    model_output_folder = args.model_output_folder or "export"
    model_output_path = Path(model_output_folder).expanduser().resolve()

    _prepare_model_folder(model_output_folder, args.overwrite)

    filter_rules = _resolve_filter_rules(args.filter)

    logger.info("Exporting model to folder: %s", model_output_folder)
    model_id = model_output_path.name.strip()
    if not model_id:
        raise ValueError("model_id must not be empty")
    requested_max_workers = _normalize_max_workers(args.max_workers)

    debug_progress = bool(getattr(args, "debug", False))
    tqdm_sink = TqdmProgressSink(worker_count=requested_max_workers, base_position=0, leave=True, thread_tracing_enabled=debug_progress)
    sink_list: list[ProgressSink] = [tqdm_sink]
    if bool(args.log_file):
        sink_list.append(LoggingProgressSink(logger))
    main_sink: ProgressSink = (
        sink_list[0] if len(sink_list) == 1 else CompositeProgressSink(sink_list)
    )
    exported_model, export_errors = export(
        tm1_service,
        model_id=model_id,
        filter_rules=filter_rules,
        progress_sink=main_sink,
        max_workers=requested_max_workers,
    )
    tqdm_sink.reset_bars()

    if export_errors and any(export_errors.values()):
        logger.warning("Export errors encountered")
        for error_type, errors in export_errors.items():
            if errors:
                logger.warning("Export error category=%s details=%s", error_type, errors)
    else:
        logger.info("Export completed successfully with no errors")

    serialize_model(exported_model, model_output_folder, progress_sink=main_sink, max_workers=requested_max_workers)
    
    logger.info("Model serialized to: %s", model_output_folder)


def _cmd_compare(args: argparse.Namespace) -> None:
    source = Path(args.source).expanduser().resolve()
    target = Path(args.target).expanduser().resolve()
    if not source.is_dir():
        logger.error("Source model path is not a directory: %s", source)
        sys.exit(1)
    if not target.is_dir():
        logger.error("Target model path is not a directory: %s", target)
        sys.exit(1)

    changeset = None
    try:
        requested_max_workers = _normalize_max_workers(args.max_workers)
        
        tqdm_sink = TqdmProgressSink(
            worker_count=requested_max_workers,
            base_position=0,
            leave=False,
            thread_tracing_enabled=bool(getattr(args, "debug", False)),
        )
        source_workers, target_workers = _split_compare_workers(requested_max_workers)
        progress_sinks: list[ProgressSink] = [tqdm_sink]
        if bool(args.log_file):
            progress_sinks.append(LoggingProgressSink(logger))
        queuing_progress_sink: ProgressSink = (
            progress_sinks[0] if len(progress_sinks) == 1 else CompositeProgressSink(progress_sinks)
        )

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
            model_source, err_source = source_future.result()
            model_target, err_target = target_future.result()

            tqdm_sink.reset_bars()

            if err_source:
                logger.warning("Source deserialization reported %d error(s)", len(err_source))
            logger.info("Loading target model from %s", target)
            if err_target:
                logger.warning("Target deserialization reported %d error(s)", len(err_target))

            extra_filter = _resolve_filter_rules(args.filter_rules)

            comparator = Comparator()

            changeset = comparator.compare(
                model_source,
                model_target,
                mode=args.mode,
                filter_rules=extra_filter,
                progress_sink=queuing_progress_sink,
            )
        
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
    finally:
        if changeset is not None:
            changeset.close()


def _cmd_apply(args: argparse.Namespace) -> None:
    changeset_path = Path(args.changeset).expanduser().resolve()
    if not changeset_path.is_file():
        logger.error("Changeset file not found: %s", changeset_path)
        sys.exit(1)

    tm1_service = _tm1_connection(args.server)
    changeset = import_changeset(changeset_path)

    status_dir = Path(args.status_dir).expanduser().resolve() if args.status_dir else None
    apply_sinks: list[ProgressSink] = [
        TqdmProgressSink(
            base_position=0,
            worker_count=1,
            leave=False,
            thread_tracing_enabled=bool(getattr(args, "debug", False)),
        )
    ]
    if bool(args.log_file):
        apply_sinks.append(LoggingProgressSink(logger))
    apply_progress_sink: ProgressSink = (
        apply_sinks[0] if len(apply_sinks) == 1 else CompositeProgressSink(apply_sinks)
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
    tqdm_sink = TqdmProgressSink(
        worker_count=1,
        base_position=0,
        leave=False,
        thread_tracing_enabled=bool(getattr(args, "debug", False)),
    )
    try:
        toggled_count = changeset.filter(filter_rules)
        changeset.export(changeset_path, progress_sink=tqdm_sink)
    finally:
        tqdm_sink.close()
        changeset.close()
    logger.info(
        "Applied changeset filter rules and toggled apply for %d change(s): %s",
        toggled_count,
        changeset_path,
    )


def _cmd_run_hook(args: argparse.Namespace) -> None:
    project_path = _filter_path_from_arg(args.tm1project)
    if not project_path.is_file():
        logger.error("tm1project file not found: %s", project_path)
        sys.exit(1)

    if args.deployment:
        phase = args.deployment
        operation = "deployment"
        task_runner = {
            "pre": apply_pre_pull_operations,
            "post": apply_post_pull_operations,
        }[phase]
    elif args.source_control:
        phase = args.source_control
        operation = "source-control"
        task_runner = {
            "pre": apply_pre_push_operations,
            "post": apply_post_push_operations,
        }[phase]
    else:
        # argparse requires exactly one phase option. Keep this guard for callers
        # that invoke the handler directly.
        raise ValueError("Select a deployment or source-control phase")

    logger.info(
        "Running %s project tasks for %s phase in environment '%s'",
        operation,
        phase,
        args.environment,
    )
    response = task_runner(
        tm1_service=_tm1_connection(args.server),
        environment=args.environment,
        project_file_path=project_path,
    )
    if response is None:
        logger.info(
            "No %s project tasks configured for %s phase in environment '%s'",
            operation,
            phase,
            args.environment,
        )
    else:
        logger.info(
            "%s project tasks completed for %s phase in environment '%s'",
            operation.capitalize(),
            phase,
            args.environment,
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

    p_run_hook = sub.add_parser(
        "run-hook",
        help="Run configured pre/post deployment or source-control tasks",
    )
    _add_common_cli_options(p_run_hook)
    p_run_hook.add_argument("-s", "--server", type=str, required=True, help="TM1 server name from tm1servers config")
    p_run_hook.add_argument(
        "--tm1project",
        type=str,
        required=True,
        help="Path or file:// URI to tm1project.json",
    )
    p_run_hook.add_argument(
        "--environment",
        type=str,
        required=True,
        help="Deployment environment name in tm1project.json",
    )
    task_phase_group = p_run_hook.add_mutually_exclusive_group(required=True)
    task_phase_group.add_argument(
        "--deployment",
        choices=["pre", "post"],
        help="Run tasks before or after deployment",
    )
    task_phase_group.add_argument(
        "--source-control",
        choices=["pre", "post"],
        help="Run tasks before or after source-control model tracking",
    )
    p_run_hook.set_defaults(handler=_cmd_run_hook)

    args = parser.parse_args()
    setup_logging(
        "DEBUG" if bool(getattr(args, "debug", False)) else None,
        enable_console=bool(getattr(args, "console_logs", False)),
        log_file=getattr(args, "log_file", None),
        command_name=getattr(args, "command", None),
    )
    logger.info("Command started: %s", args.command)
    try:
        args.handler(args)
    except KeyboardInterrupt:
        logger.warning("Command interrupted by user")
        sys.exit(130)
    logger.info("Command finished: %s", args.command)


if __name__ == "__main__":
    main()
