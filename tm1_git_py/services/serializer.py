import json
import logging
import multiprocessing
import os
import shutil
import sqlite3
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional
from urllib.parse import quote

from tm1_git_py.internal.process_pool import (
    dispose_process_pool,
    ignore_sigint_in_worker,
    process_pool_executor_kwargs,
)
from tm1_git_py.internal.worker_config import resolve_worker_counts
from tm1_git_py.model.chore import Chore
from tm1_git_py.model.tm1git_json import dumps_tm1git
from tm1_git_py.model.cube import Cube
from tm1_git_py.model.dimension import Dimension
from tm1_git_py.model.hierarchy import (
    Hierarchy,
    _HierarchyStagedWriter,
    ordered_hierarchy_items,
)
from tm1_git_py.model.model import Model
from tm1_git_py.model.process import Process
from tm1_git_py.reporting.error_reporting import (
    ErrorCallback,
    WorkflowError,
    collect_worker_errors,
    workflow_error_from_exception,
)
from tm1_git_py.reporting.progress_reporting import MultiProcessProgressManager, NoopProgressSink, ProgressEvent, \
    ProgressSink

logger = logging.getLogger(__name__)

# Normal-exit pool shutdown: shorter than generic calculators so serializer does not
# appear stuck at the end of a run when workers are already idle.
SERIALIZER_POOL_GRACEFUL_SHUTDOWN_SEC = 45.0

_PROCESS_POOL_UNAVAILABLE = object()


@dataclass(frozen=True)
class SerializationJobMetadata:
    """Coordinator-side context for a serializable model artifact.

    The metadata contains primitives only and remains beside a submitted job or
    future.  In particular, it deliberately does not contain a callback or a
    model object, so it can also be used when the job runs in a process pool.
    """

    phase: str
    object_type: str
    object_name: str
    target_path: str
    dimension_name: Optional[str] = None
    hierarchy_name: Optional[str] = None


@dataclass(frozen=True)
class _SubmittedSerializationJob:
    """Coordinator-only metadata kept beside a submitted future."""

    submission_index: int
    metadata: SerializationJobMetadata
    dimension_id: Optional[int] = None


_STORE_OBJECT_CONFIG = {
    "elements": {
        "table": "element_objects",
        "columns": ("Name", "Type"),
        "fallback_order": "Name, Type",
        "indexed_order": "ElementIndex IS NULL, ElementIndex, Name",
    },
    "edges": {
        "table": "edge_objects",
        "columns": ("ParentName", "ComponentName", "Weight"),
        "fallback_order": "ComponentName, ParentName",
        "indexed_order": "ComponentIndex IS NULL, ComponentIndex, ParentName, ComponentName",
    },
    "subsets": {
        "table": "subset_objects",
        "columns": ("Name", "Expression", "Elements"),
        "fallback_order": "Name",
        "indexed_order": "Name",
    },
}


def _handle_long_path(file_path) -> str:
    file_path = os.path.abspath(file_path)

    if os.name == 'nt' and not file_path.startswith("\\\\?\\"):
        if file_path.startswith("\\\\"):
            file_path = Path(file_path[2:])
            file_path = "\\\\?\\UNC\\" /file_path
            return str(file_path)
        else:
            file_path = Path(file_path)
            file_path = "\\\\?\\" / file_path
            return str(file_path)
    return file_path


def serialize_model(
    model: Model,
    dir,
    *,
    progress_sink: Optional[ProgressSink] = None,
    max_workers: Optional[int] = None,
    error_callback: Optional[ErrorCallback] = None,
) -> list[WorkflowError]:
    """Serialize a model and return coordinator-collected diagnostics.

    Serialization workers receive only their existing pickle-safe job data and
    progress sink.  Error callbacks are invoked only after the coordinator has
    collected all phase results and released process/progress resources.
    """
    logger.info(
        "Serializing model to '%s' (dimensions=%d cubes=%d processes=%d chores=%d)",
        dir,
        len(model.dimensions),
        len(model.cubes),
        len(model.processes),
        len(model.chores),
    )
    progress_sink = progress_sink if progress_sink is not None else NoopProgressSink()
    multi_process_progress_manager: Optional[MultiProcessProgressManager] = None
    process_pool = None
    progress_manager_started = False
    active_progress_sink: ProgressSink = progress_sink
    setup_errors: list[WorkflowError] = []
    dimension_errors: list[WorkflowError] = []
    cube_errors: list[WorkflowError] = []
    process_errors: list[WorkflowError] = []
    chore_errors: list[WorkflowError] = []
    cleanup_errors: list[WorkflowError] = []
    can_serialize = True
    cpu_workers = 1

    try:
        os.makedirs(dir, exist_ok=True)
        dir = _handle_long_path(dir)
        cpu_workers = max_workers or resolve_worker_counts(max_workers).cpu_workers
    except Exception as exc:
        setup_errors.append(
            workflow_error_from_exception(
                workflow="serialize",
                phase="setup",
                subject=str(dir),
                exception=exc,
                severity="fatal",
            )
        )
        can_serialize = False

    if can_serialize and cpu_workers > 1 and not isinstance(progress_sink, NoopProgressSink):
        try:
            multi_process_progress_manager = MultiProcessProgressManager(progress_sink)
            multi_process_progress_manager.start()
            progress_manager_started = True
            active_progress_sink = (
                multi_process_progress_manager.get_multi_process_progress_queue_sink()
            )
        except Exception as exc:
            setup_errors.append(
                workflow_error_from_exception(
                    workflow="serialize",
                    phase="progress_setup",
                    subject=str(dir),
                    exception=exc,
                    severity="fatal",
                )
            )
            active_progress_sink = NoopProgressSink()

    if can_serialize:
        try:
            active_progress_sink.on_event(ProgressEvent.total_line(message="Serializing"))
            total_count = Model.recalculate_total_object_count(model)
            active_progress_sink.on_event(ProgressEvent.total_line(total=total_count))
        except Exception as exc:
            setup_errors.append(
                workflow_error_from_exception(
                    workflow="serialize",
                    phase="progress",
                    subject=str(dir),
                    exception=exc,
                    severity="fatal",
                )
            )
            active_progress_sink = NoopProgressSink()

        if cpu_workers > 1:
            try:
                multiprocessing.freeze_support()
                process_pool = ProcessPoolExecutor(
                    **process_pool_executor_kwargs(
                        max_workers=int(cpu_workers),
                        initializer=ignore_sigint_in_worker,
                    ),
                )
            except (OSError, NotImplementedError):
                process_pool = _PROCESS_POOL_UNAVAILABLE
                logger.warning("ProcessPoolExecutor unavailable for serializer; using serial mode", exc_info=True)

    pool_shutdown_aggressive = False
    try:
        if can_serialize and model.dimensions:
            dim_dir = dir + '/dimensions'
            try:
                logger.info("Serializing %d dimension(s) to '%s'", len(model.dimensions), dim_dir)
                os.makedirs(dim_dir, exist_ok=True)
                dimension_errors = serialize_dimensions(
                    model.dimensions,
                    dim_dir,
                    process_pool=process_pool,
                    progress_sink=active_progress_sink,
                ) or []
            except Exception as exc:
                dimension_errors.append(
                    workflow_error_from_exception(
                        workflow="serialize",
                        phase="dimensions",
                        subject=dim_dir,
                        exception=exc,
                        severity="fatal",
                    )
                )
            logger.info(
                "Dimension serialization finished (diagnostics=%d)",
                len(dimension_errors),
            )

        cubes_dir = dir + '/cubes'
        if can_serialize and model.cubes:
            try:
                logger.info("Serializing %d cube(s) to '%s'", len(model.cubes), cubes_dir)
                os.makedirs(cubes_dir, exist_ok=True)
                cube_errors = serialize_cubes(
                    model.cubes,
                    cubes_dir,
                    process_pool=process_pool,
                    progress_sink=active_progress_sink,
                ) or []
            except Exception as exc:
                cube_errors.append(
                    workflow_error_from_exception(
                        workflow="serialize",
                        phase="cubes",
                        subject=cubes_dir,
                        exception=exc,
                        severity="fatal",
                    )
                )
            logger.info("Cube serialization finished (diagnostics=%d)", len(cube_errors))

        processes_dir = dir + '/processes'
        if can_serialize and model.processes:
            try:
                logger.info("Serializing %d process(es) to '%s'", len(model.processes), processes_dir)
                os.makedirs(processes_dir, exist_ok=True)
                process_errors = serialize_processes(
                    model.processes,
                    processes_dir,
                    process_pool=process_pool,
                    progress_sink=active_progress_sink,
                ) or []
            except Exception as exc:
                process_errors.append(
                    workflow_error_from_exception(
                        workflow="serialize",
                        phase="processes",
                        subject=processes_dir,
                        exception=exc,
                        severity="fatal",
                    )
                )
            logger.info("Process serialization finished (diagnostics=%d)", len(process_errors))

        chores_dir = dir + '/chores'
        if can_serialize and model.chores:
            try:
                logger.info("Serializing %d chore(s) to '%s'", len(model.chores), chores_dir)
                os.makedirs(chores_dir, exist_ok=True)
                chore_errors = serialize_chores(
                    model.chores,
                    chores_dir,
                    process_pool=process_pool,
                    progress_sink=active_progress_sink,
                ) or []
            except Exception as exc:
                chore_errors.append(
                    workflow_error_from_exception(
                        workflow="serialize",
                        phase="chores",
                        subject=chores_dir,
                        exception=exc,
                        severity="fatal",
                    )
                )
            logger.info("Chore serialization finished (diagnostics=%d)", len(chore_errors))
    except KeyboardInterrupt:
        pool_shutdown_aggressive = True
        raise
    finally:
        if isinstance(process_pool, ProcessPoolExecutor):
            try:
                if pool_shutdown_aggressive:
                    dispose_process_pool(process_pool, mode="aggressive", log=True)
                else:
                    dispose_process_pool(
                        process_pool,
                        mode="graceful_bounded",
                        graceful_timeout_sec=SERIALIZER_POOL_GRACEFUL_SHUTDOWN_SEC,
                        log=True,
                    )
            except Exception as exc:
                cleanup_errors.append(
                    workflow_error_from_exception(
                        workflow="serialize",
                        phase="process_pool_cleanup",
                        subject=str(dir),
                        exception=exc,
                        severity="fatal",
                    )
                )
        try:
            active_progress_sink.close()
        except Exception as exc:
            cleanup_errors.append(
                workflow_error_from_exception(
                    workflow="serialize",
                    phase="progress_cleanup",
                    subject=str(dir),
                    exception=exc,
                    severity="fatal",
                )
            )
        if multi_process_progress_manager is not None and progress_manager_started:
            try:
                multi_process_progress_manager.close()
            except Exception as exc:
                cleanup_errors.append(
                    workflow_error_from_exception(
                        workflow="serialize",
                        phase="progress_manager_cleanup",
                        subject=str(dir),
                        exception=exc,
                        severity="fatal",
                    )
                )

    errors: list[WorkflowError] = []
    for phase_errors in (
        setup_errors,
        dimension_errors,
        cube_errors,
        process_errors,
        chore_errors,
        cleanup_errors,
    ):
        collect_worker_errors(
            errors,
            phase_errors,
            error_callback=error_callback,
        )
    for error in errors:
        log = logger.warning if error.severity == "recoverable" else logger.error
        log(
            "Serialization diagnostic workflow=%s phase=%s subject=%s exception=%s: %s",
            error.workflow,
            error.phase,
            error.subject,
            error.exception_type,
            error.message,
        )
    logger.info(
        "Model serialization finished for '%s' (diagnostics=%d recoverable=%d fatal=%d)",
        dir,
        len(errors),
        sum(error.severity == "recoverable" for error in errors),
        sum(error.severity == "fatal" for error in errors),
    )
    return errors


def serialize_dimensions(
    dimensions: List[Dimension],
    dim_dir,
    process_pool: Optional[ProcessPoolExecutor],
    progress_sink: ProgressSink,
) -> list[WorkflowError]:
    """Serialize dimensions and return hierarchy diagnostics to the coordinator."""
    errors: list[WorkflowError] = []
    dimension_jobs = [
        (dim, _build_dimension_serialize_job(dim, dim_dir))
        for dim in dimensions
    ]
    if process_pool is None or process_pool is _PROCESS_POOL_UNAVAILABLE:
        for dim, job in dimension_jobs:
            hierarchy_results: list[dict[str, str]] = []
            for hierarchy_job in job["hierarchies"]:
                metadata = _hierarchy_serialization_metadata(hierarchy_job)
                try:
                    hierarchy_results.append(
                        _serialize_hierarchy_job(hierarchy_job, progress_sink)
                    )
                except Exception as exc:
                    errors.append(
                        workflow_error_from_exception(
                            workflow="serialize",
                            phase=metadata.phase,
                            subject=metadata.target_path,
                            exception=exc,
                            severity="recoverable",
                        )
                    )
            errors.extend(_finish_dimension_serialization(dim, job, hierarchy_results))
        return errors

    pending_by_dimension: dict[int, int] = {}
    hierarchy_results_by_dimension: dict[int, list[dict[str, str]]] = {}
    jobs_by_dimension_id: dict[int, tuple[Dimension, dict[str, Any]]] = {}
    future_to_job: dict[Any, _SubmittedSerializationJob] = {}
    finalization_errors_by_dimension: dict[int, list[WorkflowError]] = {}
    finished_dimensions: set[int] = set()
    hierarchy_failures: list[tuple[int, WorkflowError]] = []
    process_pool_failures: list[tuple[int, WorkflowError]] = []
    submission_index = 0

    for dimension_id, (dim, job) in enumerate(dimension_jobs):
        jobs_by_dimension_id[dimension_id] = (dim, job)
        hierarchy_results_by_dimension[dimension_id] = []
        pending_by_dimension[dimension_id] = len(job["hierarchies"])
        finalization_errors_by_dimension[dimension_id] = []
        if not job["hierarchies"]:
            finalization_errors_by_dimension[dimension_id].extend(
                _finish_dimension_serialization(dim, job, [])
            )
            finished_dimensions.add(dimension_id)
            continue
        progress_sink.on_event(ProgressEvent.worker_line(current=0, total=len(job["hierarchies"]), message=f"Serializing dimension {dim.name}"))
        for hierarchy_job in job["hierarchies"]:
            submitted_job = _SubmittedSerializationJob(
                submission_index=submission_index,
                dimension_id=dimension_id,
                metadata=_hierarchy_serialization_metadata(hierarchy_job),
            )
            submission_index += 1
            try:
                future = process_pool.submit(
                    _serialize_hierarchy_job,
                    hierarchy_job,
                    progress_sink,
                )
            except Exception as exc:
                hierarchy_failures.append(
                    (
                        submitted_job.submission_index,
                        _serialization_job_error(submitted_job.metadata, exc),
                    )
                )
                if isinstance(exc, BrokenProcessPool):
                    process_pool_failures.append(
                        (
                            submitted_job.submission_index,
                            workflow_error_from_exception(
                                workflow="serialize",
                                phase="process_pool",
                                subject=str(dim_dir),
                                exception=exc,
                                severity="fatal",
                            ),
                        )
                    )
                pending_by_dimension[dimension_id] -= 1
            else:
                future_to_job[future] = submitted_job

    for future in as_completed(future_to_job):
        submitted_job = future_to_job[future]
        dimension_id = submitted_job.dimension_id
        assert dimension_id is not None
        dim, job = jobs_by_dimension_id[dimension_id]
        try:
            hierarchy_result = future.result()
        except Exception as exc:
            hierarchy_failures.append(
                (
                    submitted_job.submission_index,
                    workflow_error_from_exception(
                        workflow="serialize",
                        phase=submitted_job.metadata.phase,
                        subject=submitted_job.metadata.target_path,
                        exception=exc,
                        severity="recoverable",
                    ),
                )
            )
            if isinstance(exc, BrokenProcessPool):
                process_pool_failures.append(
                    (
                        submitted_job.submission_index,
                        workflow_error_from_exception(
                            workflow="serialize",
                            phase="process_pool",
                            subject=str(dim_dir),
                            exception=exc,
                            severity="fatal",
                        ),
                    )
                )
        else:
            hierarchy_results_by_dimension[dimension_id].append(hierarchy_result)
        pending_by_dimension[dimension_id] -= 1
        completed = len(job["hierarchies"]) - pending_by_dimension[dimension_id]
        progress_sink.on_event(ProgressEvent.worker_line(current=completed, total=len(job["hierarchies"]), message=f"Serializing dimension {dim.name}"))
        if pending_by_dimension[dimension_id] == 0:
            finalization_errors_by_dimension[dimension_id].extend(
                _finish_dimension_serialization(
                    dim,
                    job,
                    hierarchy_results_by_dimension[dimension_id],
                )
            )
            finished_dimensions.add(dimension_id)
    errors.extend(error for _, error in sorted(hierarchy_failures))
    if process_pool_failures:
        errors.append(sorted(process_pool_failures)[0][1])
    for dimension_id in range(len(dimension_jobs)):
        if pending_by_dimension[dimension_id] == 0 and dimension_id not in finished_dimensions:
            dim, job = jobs_by_dimension_id[dimension_id]
            finalization_errors_by_dimension[dimension_id].extend(
                _finish_dimension_serialization(
                    dim,
                    job,
                    hierarchy_results_by_dimension[dimension_id],
                )
            )
            finished_dimensions.add(dimension_id)
        errors.extend(finalization_errors_by_dimension[dimension_id])
    return errors


def _finish_dimension_serialization(
    dim: Dimension,
    job: dict[str, Any],
    hierarchy_results: list[dict[str, str]],
) -> list[WorkflowError]:
    _write_text_staged(job["dimension_path"], job["dimension_json"])
    return _finalize_dimension_serialize_result(dim, hierarchy_results)


def _finalize_dimension_serialize_result(
    dim: Dimension,
    hierarchy_results: list[dict[str, str]],
) -> list[WorkflowError]:
    errors: list[WorkflowError] = []
    for hierarchy_result in hierarchy_results:
        hierarchy = _hierarchy_by_name(dim, hierarchy_result["name"])
        if hierarchy is None:
            continue
        errors.extend(
            _set_source_json_mtime_from_path(
                hierarchy,
                hierarchy_result["source_mtime_path"],
            )
        )
    return errors


def _hierarchy_by_name(dim: Dimension, hierarchy_name: str) -> Optional[Hierarchy]:
    return next((hierarchy for hierarchy in dim.hierarchies if hierarchy.name == hierarchy_name), None)


def _hierarchy_serialization_metadata(
    hierarchy_job: dict[str, Any],
) -> SerializationJobMetadata:
    dimension_name = str(hierarchy_job["dimension_name"])
    hierarchy_name = str(hierarchy_job["name"])
    return SerializationJobMetadata(
        phase="hierarchy",
        object_type="hierarchy",
        object_name=f"{dimension_name}/{hierarchy_name}",
        target_path=str(hierarchy_job["target_path"]),
        dimension_name=dimension_name,
        hierarchy_name=hierarchy_name,
    )


def _build_hierarchy_serialize_job(dim: Dimension, hierarchy: Hierarchy, dim_dir: str) -> dict[str, Any]:
    hierarchy_dir = os.path.join(dim_dir, dim.name + '.hierarchies')
    target_path = os.path.join(hierarchy_dir, hierarchy.name + '.json')
    writer_model_output_dir, staging_root = _writer_model_output_dir_for_target(
        dim_dir=dim_dir,
        dimension_name=dim.name,
    )
    hierarchy_job: dict[str, Any] = {
        "name": hierarchy.name,
        "dimension_name": dim.name,
        "target_path": target_path,
        "writer_model_output_dir": writer_model_output_dir,
        "staging_root": staging_root,
        "subset_dir": os.path.join(hierarchy_dir, hierarchy.name + '.subsets'),
        "store": _store_backed_hierarchy_spec(hierarchy),
        "elements_sort_type": hierarchy.elements_sort_type,
        "elements_sort_sense": hierarchy.elements_sort_sense,
        "components_sort_type": hierarchy.components_sort_type,
        "components_sort_sense": hierarchy.components_sort_sense,
    }
    if hierarchy_job["store"] is None:
        hierarchy_job["collections"] = _in_memory_hierarchy_collections(hierarchy)
        hierarchy_job["subsets"] = [
            {
                "name": subset.name,
                "json": subset.as_json(),
            }
            for subset in hierarchy.subsets
        ]
    return hierarchy_job

def _build_dimension_serialize_job(dim: Dimension, dim_dir: str) -> dict[str, Any]:
    return {
        "dimension_path": os.path.join(dim_dir, dim.name + '.json'),
        "dimension_json": dim.as_json(),
        "hierarchies": [
            _build_hierarchy_serialize_job(dim, hierarchy, dim_dir)
            for hierarchy in dim.hierarchies
        ],
    }


def _writer_model_output_dir_for_target(
    *,
    dim_dir: str,
    dimension_name: str,
) -> tuple[str, Optional[str]]:
    normalized_dim_dir = os.path.abspath(dim_dir)
    if os.path.basename(normalized_dim_dir) == "dimensions":
        return os.path.dirname(normalized_dim_dir), None
    staging_root = os.path.join(
        normalized_dim_dir,
        f".staged-writer-{dimension_name}.{uuid.uuid4().hex}",
    )
    return staging_root, staging_root


def _store_backed_hierarchy_spec(hierarchy: Hierarchy) -> Optional[dict[str, Any]]:
    sequence_specs: dict[str, dict[str, Any]] = {}
    db_path: Optional[str] = None
    model_store = None
    for object_type, sequence in (
        ("elements", hierarchy.elements),
        ("edges", hierarchy.edges),
        ("subsets", hierarchy.subsets),
    ):
        group_id = getattr(sequence, "group_id", None)
        store = getattr(sequence, "_store", None)
        store_db_path = getattr(store, "db_path", None)
        if group_id is None or store_db_path is None:
            return None
        if model_store is None:
            model_store = store
        elif model_store is not store:
            return None
        if db_path is None:
            db_path = str(store_db_path)
        elif db_path != str(store_db_path):
            return None
        sequence_specs[object_type] = {"group_id": int(group_id)}
    if db_path is None:
        return None
    worker_conn = getattr(model_store, "_conn", None)
    if worker_conn is not None and hasattr(worker_conn, "commit"):
        worker_conn.commit()
    return {
        "db_path": db_path,
        "sequences": sequence_specs,
    }


def _serialize_hierarchy_job(job: dict[str, Any], progress_sink: ProgressSink) -> dict[str, str]:
    os.makedirs(os.path.dirname(job["target_path"]), exist_ok=True)
    store = job.get("store")
    if store is not None:
        _write_store_backed_subsets(job["subset_dir"], store)
        staged_path = _serialize_store_backed_hierarchy_with_staged_writer(
            model_output_dir=job["writer_model_output_dir"],
            dimension_name=job["dimension_name"],
            hierarchy_name=job["name"],
            store=store,
            elements_sort_type=job.get("elements_sort_type"),
            elements_sort_sense=job.get("elements_sort_sense"),
            components_sort_type=job.get("components_sort_type"),
            components_sort_sense=job.get("components_sort_sense"),
            progress_sink=progress_sink,
        )
    else:
        for subset in job.get("subsets", []):
            os.makedirs(job["subset_dir"], exist_ok=True)
            _write_text_staged(
                os.path.join(job["subset_dir"], subset["name"] + '.json'),
                subset["json"],
            )
        staged_path = _serialize_payload_hierarchy_with_staged_writer(
            model_output_dir=job["writer_model_output_dir"],
            dimension_name=job["dimension_name"],
            hierarchy_name=job["name"],
            collections=job["collections"],
            elements_sort_type=job.get("elements_sort_type"),
            elements_sort_sense=job.get("elements_sort_sense"),
            components_sort_type=job.get("components_sort_type"),
            components_sort_sense=job.get("components_sort_sense"),
            progress_sink=progress_sink
        )

    if os.path.abspath(staged_path) != os.path.abspath(job["target_path"]):
        _copy_file_staged(staged_path, job["target_path"])
    staging_root = job.get("staging_root")
    if staging_root:
        shutil.rmtree(staging_root, ignore_errors=True)

    return {
        "name": job["name"],
        "source_mtime_path": job["target_path"],
    }


def _write_text_staged(path: str, content: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    inprogress_path = os.path.join(
        os.path.dirname(path),
        f".{os.path.basename(path)}.{uuid.uuid4().hex}.inprogress",
    )
    with open(inprogress_path, 'w', encoding='utf-8') as fh:
        fh.write(content)
    os.replace(inprogress_path, path)


def _copy_file_staged(source_path: str, target_path: str) -> None:
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    inprogress_path = os.path.join(
        os.path.dirname(target_path),
        f".{os.path.basename(target_path)}.{uuid.uuid4().hex}.inprogress",
    )
    shutil.copyfile(source_path, inprogress_path)
    os.replace(inprogress_path, target_path)


def _open_readonly_connection(db_path: str, *, busy_timeout_ms: int = 30_000) -> sqlite3.Connection:
    quoted_path = quote(os.path.abspath(db_path))
    conn = sqlite3.connect(
        f"file:{quoted_path}?mode=ro",
        uri=True,
        isolation_level=None,
    )
    conn.execute("PRAGMA query_only=ON")
    conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
    return conn


def _store_group_id(store: dict[str, Any], object_type: str) -> int:
    return int(store["sequences"][object_type]["group_id"])


def _store_row_count(conn: sqlite3.Connection, object_type: str, group_id: int) -> int:
    table = _STORE_OBJECT_CONFIG[object_type]["table"]
    row = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE group_id=?", (group_id,)).fetchone()
    return int(row[0]) if row is not None else 0


def _iter_store_payload_rows(
    conn: sqlite3.Connection,
    object_type: str,
    group_id: int,
    *,
    order_by_internal_index: bool = False,
    fetch_size: int = 10_000,
):
    config = _STORE_OBJECT_CONFIG[object_type]
    columns = ", ".join(config["columns"])
    order_key = "indexed_order" if order_by_internal_index else "fallback_order"
    cursor = conn.execute(
        f"SELECT {columns} FROM {config['table']} WHERE group_id=? ORDER BY {config[order_key]}",
        (group_id,),
    )
    while True:
        rows = cursor.fetchmany(fetch_size)
        if not rows:
            break
        yield from rows


def _json_value(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _subset_elements_from_store(raw_elements: Any) -> list[dict[str, str]]:
    if raw_elements in (None, ""):
        return []
    return [{"@id": str(element_id)} for element_id in json.loads(str(raw_elements))]


def _subset_row_payload(name: Any, expression: Any, raw_elements: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {"name": name}
    if expression not in (None, ""):
        payload["expression"] = expression
    else:
        payload["Elements"] = _subset_elements_from_store(raw_elements)
    return payload


def _subset_file_payload(name: Any, expression: Any, raw_elements: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "@type": "Subset",
        "Name": name,
    }
    if expression not in (None, ""):
        payload["Expression"] = expression
    else:
        payload["Elements"] = _subset_elements_from_store(raw_elements)
    return payload


def _payload_json_from_row(object_type: str, row: tuple[Any, ...]) -> str:
    if object_type == "elements":
        return (
            "{\n"
            f"\t\t\t\"Name\":{_json_value(row[0])},\n"
            f"\t\t\t\"Type\":{_json_value(row[1])}\n"
            "\t\t}"
        )
    if object_type == "edges":
        return (
            "{\n"
            f"\t\t\t\"ParentName\":{_json_value(row[0])},\n"
            f"\t\t\t\"ComponentName\":{_json_value(row[1])},\n"
            f"\t\t\t\"Weight\":{_json_value(row[2])}\n"
            "\t\t}"
        )
    if object_type == "subsets":
        payload = _subset_row_payload(row[0], row[1], row[2])
        fields = [
            f"\t\t\t{_json_value(key)}:{_json_value(value)}"
            for key, value in payload.items()
        ]
        return "{\n" + ",\n".join(fields) + "\n\t\t}"
    raise ValueError(f"Unsupported store object type: {object_type}")


def _payload_json_from_payload(object_type: str, payload: dict[str, Any]) -> str:
    if object_type == "elements":
        return _payload_json_from_row(
            "elements",
            (payload.get("Name"), payload.get("Type")),
        )
    if object_type == "edges":
        return _payload_json_from_row(
            "edges",
            (
                payload.get("ParentName"),
                payload.get("ComponentName"),
                payload.get("Weight"),
            ),
        )
    if object_type == "subsets":
        elements = payload.get("Elements")
        if elements is None:
            elements = payload.get("elements")
        if elements is None:
            elements = payload.get("element_ids")
        return _payload_json_from_row(
            "subsets",
            (
                payload.get("Name"),
                payload.get("Expression"),
                json.dumps(
                    [
                        item if isinstance(item, str) else item.get("@id") or item.get("@odata.id")
                        for item in elements or []
                    ],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            ),
        )
    raise ValueError(f"Unsupported payload object type: {object_type}")


class _RawSqliteStoreBackedSequence:
    """Read-only sequence adapter used by process workers for staged writing."""

    def __init__(
        self,
        *,
        db_path: str,
        object_type: str,
        group_id: int,
        order_by_internal_index: bool = False,
    ) -> None:
        self.db_path = db_path
        self.object_type = object_type
        self.group_id = int(group_id)
        self.order_by_internal_index = bool(order_by_internal_index)

    def __len__(self) -> int:
        conn = _open_readonly_connection(self.db_path)
        try:
            return _store_row_count(conn, self.object_type, self.group_id)
        finally:
            conn.close()

    def iter_payload_json_strings(
        self,
        *,
        ordered_by_identity: bool = False,
        order_by_internal_index: bool = False,
        progress_label: Optional[str] = None,
        progress_every: int = 10_000,
    ):
        order_by_index = self.order_by_internal_index or order_by_internal_index
        conn = _open_readonly_connection(self.db_path)
        try:
            for row in _iter_store_payload_rows(
                conn,
                self.object_type,
                self.group_id,
                order_by_internal_index=order_by_index,
            ):
                yield _payload_json_from_row(self.object_type, tuple(row))
        finally:
            conn.close()


class _JsonPayloadSequence:
    def __init__(self, payload_json_strings: list[str]) -> None:
        self._payload_json_strings = payload_json_strings

    def __len__(self) -> int:
        return len(self._payload_json_strings)

    def iter_payload_json_strings(
        self,
        *,
        ordered_by_identity: bool = False,
        order_by_internal_index: bool = False,
        progress_label: Optional[str] = None,
        progress_every: int = 10_000,
    ):
        _ = ordered_by_identity, order_by_internal_index, progress_label, progress_every
        yield from self._payload_json_strings


class _StagedHierarchyView:
    def __init__(
        self,
        name: str,
        *,
        elements: Any,
        edges: Any,
        subsets: Any,
        elements_sort_type: Optional[str] = None,
        elements_sort_sense: Optional[str] = None,
        components_sort_type: Optional[str] = None,
        components_sort_sense: Optional[str] = None,
    ) -> None:
        self.name = name
        self.elements = elements
        self.edges = edges
        self.subsets = subsets
        self.elements_sort_type = elements_sort_type
        self.elements_sort_sense = elements_sort_sense
        self.components_sort_type = components_sort_type
        self.components_sort_sense = components_sort_sense


def _in_memory_hierarchy_collections(hierarchy: Hierarchy) -> dict[str, list[str]]:
    elements = [item.to_dict() for item in ordered_hierarchy_items(hierarchy, "Elements", hierarchy.elements)]
    edges = [item.to_dict() for item in ordered_hierarchy_items(hierarchy, "Edges", hierarchy.edges)]
    subsets = [item.to_dict() for item in hierarchy.subsets]
    return {
        "elements": [_payload_json_from_payload("elements", payload) for payload in elements],
        "edges": [_payload_json_from_payload("edges", payload) for payload in edges],
        "subsets": [_payload_json_from_payload("subsets", payload) for payload in subsets],
    }


def _serialize_store_backed_hierarchy_with_staged_writer(
    *,
    model_output_dir: str,
    dimension_name: str,
    hierarchy_name: str,
    store: dict[str, Any],
    progress_sink: ProgressSink,
    elements_sort_type: Optional[str] = None,
    elements_sort_sense: Optional[str] = None,
    components_sort_type: Optional[str] = None,
    components_sort_sense: Optional[str] = None,
) -> str:
    sort_metadata_exists = any(
        value is not None
        for value in (
            elements_sort_type,
            elements_sort_sense,
            components_sort_type,
            components_sort_sense,
        )
    )
    hierarchy = _StagedHierarchyView(
        hierarchy_name,
        elements=_raw_sequence(store, "elements", order_by_internal_index=sort_metadata_exists),
        edges=_raw_sequence(store, "edges", order_by_internal_index=sort_metadata_exists),
        subsets=_raw_sequence(store, "subsets"),
        elements_sort_type=elements_sort_type,
        elements_sort_sense=elements_sort_sense,
        components_sort_type=components_sort_type,
        components_sort_sense=components_sort_sense,
    )
    writer = _HierarchyStagedWriter(
        model_output_dir=model_output_dir,
        dimension_name=dimension_name,
        hierarchy=hierarchy,
    )
    writer.bind_collections()
    return writer.serialize_hierarchy_json(progress_sink=progress_sink)


def _serialize_payload_hierarchy_with_staged_writer(
    *,
    model_output_dir: str,
    dimension_name: str,
    hierarchy_name: str,
    collections: dict[str, list[str]],
    progress_sink: ProgressSink,
    elements_sort_type: Optional[str] = None,
    elements_sort_sense: Optional[str] = None,
    components_sort_type: Optional[str] = None,
    components_sort_sense: Optional[str] = None,
) -> str:
    hierarchy = _StagedHierarchyView(
        hierarchy_name,
        elements=_JsonPayloadSequence(collections["elements"]),
        edges=_JsonPayloadSequence(collections["edges"]),
        subsets=_JsonPayloadSequence(collections["subsets"]),
        elements_sort_type=elements_sort_type,
        elements_sort_sense=elements_sort_sense,
        components_sort_type=components_sort_type,
        components_sort_sense=components_sort_sense,
    )
    writer = _HierarchyStagedWriter(
        model_output_dir=model_output_dir,
        dimension_name=dimension_name,
        hierarchy=hierarchy,
    )
    writer.bind_collections()
    return writer.serialize_hierarchy_json(progress_sink=progress_sink)


def _raw_sequence(
    store: dict[str, Any],
    object_type: str,
    *,
    order_by_internal_index: bool = False,
) -> _RawSqliteStoreBackedSequence:
    return _RawSqliteStoreBackedSequence(
        db_path=store["db_path"],
        object_type=object_type,
        group_id=_store_group_id(store, object_type),
        order_by_internal_index=order_by_internal_index,
    )


def _write_store_backed_subsets(subsets_dir: str, store: dict[str, Any]) -> None:
    conn = _open_readonly_connection(store["db_path"])
    try:
        subsets_group_id = _store_group_id(store, "subsets")
        if _store_row_count(conn, "subsets", subsets_group_id) == 0:
            return
        os.makedirs(subsets_dir, exist_ok=True)
        for name, expression, raw_elements in _iter_store_payload_rows(conn, "subsets", subsets_group_id):
            subset_json = dumps_tm1git(
                _subset_file_payload(name, expression, raw_elements),
            )
            _write_text_staged(os.path.join(subsets_dir, str(name) + '.json'), subset_json)
    finally:
        conn.close()


def _set_source_json_mtime_from_path(
    hierarchy,
    hierarchy_json_path: str,
) -> list[WorkflowError]:
    try:
        source_json_mtime_ns = int(os.stat(hierarchy_json_path).st_mtime_ns)
    except OSError as exc:
        return [
            workflow_error_from_exception(
                workflow="serialize",
                phase="hierarchy_metadata",
                subject=hierarchy_json_path,
                exception=exc,
                severity="recoverable",
            )
        ]
    for sequence in (hierarchy.elements, hierarchy.edges, hierarchy.subsets):
        if hasattr(sequence, "set_source_json_mtime_ns"):
            sequence.set_source_json_mtime_ns(source_json_mtime_ns)
    return []


def serialize_cubes(
    cubes: List[Cube],
    cubes_dir,
    process_pool: Optional[ProcessPoolExecutor],
    progress_sink: ProgressSink,
) -> list[WorkflowError]:
    return _run_io_jobs(
        (
            (
                _serialize_cube,
                (_build_cube_serialize_job(cube), cubes_dir, progress_sink),
                SerializationJobMetadata(
                    phase="cube",
                    object_type="cube",
                    object_name=cube.name,
                    target_path=os.path.join(cubes_dir, cube.name + ".json"),
                ),
            )
            for cube in cubes
        ),
        process_pool=process_pool,
    )


def _build_cube_serialize_job(cube: Cube) -> dict[str, Any]:
    rule_text = cube.get_rule_text() if cube.rules else None
    drillthrough_rule_text = (
        cube.get_drillthrough_rule_text()
        if getattr(cube, "drillthrough_rules", None)
        else None
    )
    return {
        "name": cube.name,
        "cube_json": cube.as_json(),
        "rule_text": rule_text,
        "drillthrough_rule_text": drillthrough_rule_text,
        "views": [
            {
                "name": view.name,
                "type": getattr(view, 'type', '').lower(),
                "json": view.as_json(),
                "mdx": getattr(view, "mdx", None),
            }
            for view in cube.views
        ],
    }


def _serialize_cube(cube_job: dict[str, Any], cubes_dir: str, progress_sink: ProgressSink) -> None:
    cube_name = str(cube_job["name"])
    progress_sink.on_event(ProgressEvent.worker_line(current=0, total=1, message=f"Serializing cube {cube_name}"))
    rule_text = cube_job.get("rule_text")
    if rule_text:
        with open(os.path.join(cubes_dir, cube_name + '.rules'), 'w', encoding='utf-8') as rule_file:
            rule_file.write(str(rule_text))

    drillthrough_rule_text = cube_job.get("drillthrough_rule_text")
    if drillthrough_rule_text:
        with open(os.path.join(cubes_dir, cube_name + '.drillthrough.rules'), 'w', encoding='utf-8') as rule_file:
            rule_file.write(str(drillthrough_rule_text))

    with open(os.path.join(cubes_dir, cube_name + '.json'), 'w', encoding='utf-8') as cube_file:
        cube_file.write(str(cube_job["cube_json"]))

    views = cube_job.get("views", [])
    if views:
        views_dir = os.path.join(cubes_dir, cube_name + '.views')
        os.makedirs(views_dir, exist_ok=True)
        for view in views:
            view_name = str(view["name"])
            view_type = str(view.get("type", ""))
            with open(os.path.join(views_dir, view_name + '.json'), 'w', encoding='utf-8') as view_json_file:
                view_json_file.write(str(view["json"]))
            if view_type == 'mdxview':
                with open(os.path.join(views_dir, view_name + '.mdx'), 'w', encoding='utf-8') as mdx_file:
                    mdx_file.write(str(view.get("mdx", "")))
    progress_sink.on_event(ProgressEvent.worker_line(current=1, total=1, message=f"Serializing cube {cube_name}"))


def serialize_processes(
    processes: List[Process],
    process_dir,
    process_pool: Optional[ProcessPoolExecutor],
    progress_sink: ProgressSink,
) -> list[WorkflowError]:
    return _run_io_jobs(
        (
            (
                _serialize_process,
                (process, process_dir, progress_sink),
                SerializationJobMetadata(
                    phase="process",
                    object_type="process",
                    object_name=process.name,
                    target_path=os.path.join(process_dir, process.name + ".json"),
                ),
            )
            for process in processes
        ),
        process_pool=process_pool,
    )


def _serialize_process(process: Process, process_dir: str, progress_sink: ProgressSink) -> None:
    progress_sink.on_event(ProgressEvent.worker_line(current=0, total=1, message=f"Serializing process {process.name}"))
    with open(process_dir + '/' + process.name + '.ti', 'w', encoding='utf-8', newline='\n') as processti_file:
        processti_file.write(process.ti.ti_as_string())

    with open(process_dir + '/' + process.name + '.json', 'w', encoding='utf-8') as processjson_file:
        processjson_file.write(process.as_json())
    progress_sink.on_event(ProgressEvent.worker_line(current=1, total=1, message=f"Serializing process {process.name}"))


def serialize_chores(
    chores: List[Chore],
    chores_dir,
    process_pool: Optional[ProcessPoolExecutor],
    progress_sink: ProgressSink,
) -> list[WorkflowError]:
    return _run_io_jobs(
        (
            (
                _serialize_chore,
                (chore, chores_dir, progress_sink),
                SerializationJobMetadata(
                    phase="chore",
                    object_type="chore",
                    object_name=chore.name,
                    target_path=os.path.join(chores_dir, chore.name + ".json"),
                ),
            )
            for chore in chores
        ),
        process_pool=process_pool,
    )


def _serialize_chore(chore: Chore, chores_dir: str, progress_sink: ProgressSink) -> None:
    progress_sink.on_event(ProgressEvent.worker_line(current=0, total=1, message=f"Serializing chore {chore.name}"))
    with open(chores_dir + '/' + chore.name + '.json', 'w', encoding='utf-8') as chore_file:
        chore_file.write(chore.as_json())
    progress_sink.on_event(ProgressEvent.worker_line(current=1, total=1, message=f"Serializing chore {chore.name}"))


def _run_io_jobs(
    jobs,
    *,
    process_pool: Optional[ProcessPoolExecutor] = None,
) -> list[WorkflowError]:
    """Run independent file jobs and collect failures in submission order."""
    errors: list[WorkflowError] = []
    if process_pool is None or process_pool is _PROCESS_POOL_UNAVAILABLE:
        for fn, args, metadata in jobs:
            try:
                fn(*args)
            except Exception as exc:
                errors.append(_serialization_job_error(metadata, exc))
        return errors

    future_to_job: dict[Any, _SubmittedSerializationJob] = {}
    job_failures: list[tuple[int, WorkflowError]] = []
    process_pool_failures: list[tuple[int, WorkflowError]] = []
    for submission_index, (fn, args, metadata) in enumerate(jobs):
        submitted_job = _SubmittedSerializationJob(
            submission_index=submission_index,
            metadata=metadata,
        )
        try:
            future = process_pool.submit(fn, *args)
        except Exception as exc:
            job_failures.append(
                (submission_index, _serialization_job_error(metadata, exc))
            )
            if isinstance(exc, BrokenProcessPool):
                process_pool_failures.append(
                    (
                        submission_index,
                        workflow_error_from_exception(
                            workflow="serialize",
                            phase="process_pool",
                            subject=metadata.target_path,
                            exception=exc,
                            severity="fatal",
                        ),
                    )
                )
        else:
            future_to_job[future] = submitted_job

    for future in as_completed(future_to_job):
        submitted_job = future_to_job[future]
        try:
            future.result()
        except Exception as exc:
            job_failures.append(
                (
                    submitted_job.submission_index,
                    _serialization_job_error(submitted_job.metadata, exc),
                )
            )
            if isinstance(exc, BrokenProcessPool):
                process_pool_failures.append(
                    (
                        submitted_job.submission_index,
                        workflow_error_from_exception(
                            workflow="serialize",
                            phase="process_pool",
                            subject=submitted_job.metadata.target_path,
                            exception=exc,
                            severity="fatal",
                        ),
                    )
                )
    errors.extend(error for _, error in sorted(job_failures))
    if process_pool_failures:
        errors.append(sorted(process_pool_failures)[0][1])
    return errors


def _serialization_job_error(
    metadata: SerializationJobMetadata,
    exception: BaseException,
) -> WorkflowError:
    return workflow_error_from_exception(
        workflow="serialize",
        phase=metadata.phase,
        subject=metadata.target_path,
        exception=exception,
        severity="recoverable",
    )
