import concurrent
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
import logging
import os
import re
from pathlib import Path
import signal
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional
from urllib.parse import unquote
import ijson
import orjson
from tm1_git_py.model import Edge
from tm1_git_py.model.chore import Chore
from tm1_git_py.model.cube import Cube
from tm1_git_py.model.dimension import Dimension
from tm1_git_py.model.element import Element
from tm1_git_py.model.hierarchy import Hierarchy, hierarchy_sort_metadata_json
from tm1_git_py.model.mdxview import MDXView
from tm1_git_py.model.nativeview import NativeView
from tm1_git_py.model.model import Model
from tm1_git_py.db.model_store import ModelStore
from tm1_git_py.model.store_backed_sequence import StoreBackedSequence
from tm1_git_py.model.process import Process
from tm1_git_py.model.rule import Rule
from tm1_git_py.model.subset import Subset
from tm1_git_py.model.task import Task
from tm1_git_py.model.ti import TI
from tm1_git_py.internal.content_hash_calculator import ContentHashCalculator
from tm1_git_py.internal.worker_config import resolve_worker_counts
from tm1_git_py.reporting.progress_reporting import (
    MultiProcessProgressManager,
    MultiProcessProgressQueueSink,
    NoopProgressSink,
    ProgressKind,
    ProgressEvent,
    ProgressScope,
    ProgressSink,
    ProgressUnit,
)
from tm1_git_py.reporting.error_reporting import (
    ErrorCallback,
    WorkflowError,
    collect_worker_errors,
    workflow_error_from_exception,
)


logger = logging.getLogger(__name__)
DESERIALIZE_PROGRESS_EVERY = 100_000

_HIERARCHY_SORT_JSON_FIELDS = {
    "ElementsSortType",
    "ElementsSortSense",
    "ComponentsSortType",
    "ComponentsSortSense",
}


@dataclass(frozen=True)
class HierarchyFutureMetadata:
    """Coordinator-side context for one submitted hierarchy parse."""

    submission_index: int
    dimension_name: str
    hierarchy_name: str
    hierarchy_json_path: str
    future: Future


def _json_load_text(raw: str) -> Any:
    return orjson.loads(raw)


def _json_load_file(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as src:
        return _json_load_text(src.read())


def _progress_start(
    progress: ProgressSink,
    file_path: str,
    activity: str
) -> None:
    total = 0
    try:
        total = int(os.path.getsize(file_path))
    except OSError:
        total = 0
    progress.on_event(
        ProgressEvent.make(
            kind=ProgressKind.START,
            scope=ProgressScope.WORKER,
            current=0,
            total=max(1, total),
            unit=ProgressUnit.BYTE,
            message=activity,
            path=file_path
        )
    )


def _progress_mark(
    progress: ProgressSink,
    file_path: str,
    *,
    include_total: bool = True,
) -> None:
    total = 0
    try:
        total = int(os.path.getsize(file_path))
    except OSError:
        total = 0
    progress.on_event(
        ProgressEvent.make(
            kind=ProgressKind.UPDATE,
            scope=ProgressScope.WORKER,
            current=max(1, total),
            total=max(1, total),
            unit=ProgressUnit.BYTE,
            message="completed",
            path=file_path,
            update_total=True,
        )
    )
    if include_total:
        progress.on_event(
            ProgressEvent.make(
                kind=ProgressKind.UPDATE,
                scope=ProgressScope.TOTAL,
                current_delta=max(0, total),
                total=None,
                unit=ProgressUnit.BYTE,
                message="Building internal model",
            )
        )


def _directory_total_bytes(model_dir: str) -> int:
    total_bytes = 0
    for folder_name in ("cubes", "chores", "dimensions", "processes"):
        folder_path = os.path.join(model_dir, folder_name)
        if not os.path.isdir(folder_path):
            continue
        for root, _, files in os.walk(folder_path):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                try:
                    total_bytes += int(os.path.getsize(file_path))
                except OSError:
                    continue
    return total_bytes


def _recalculate_group_signature_task(
    *,
    model_id: str,
    group_id: int,
    progress: ProgressSink,
    progress_scope: str,
    content_hash_calculator: ContentHashCalculator,
    count: int,
) -> tuple[int, str]:
    store = ModelStore.for_model_id(model_id)
    normalized, _, total_lines = store.resolve_parallel_hash_inputs(group_id)
    progress.on_event(
        ProgressEvent.make(
            kind=ProgressKind.START,
            scope=ProgressScope.WORKER,
            current=0,
            total=max(1, total_lines),
            unit=ProgressUnit.LINE,
            message="recalculating hash",
            path=progress_scope,
        )
    )

    # since content_hash_calculator is not thread safe, we need to ensure the consistency of the group before calculating the hash
    content_hash_calculator.await_consistency(
        group_id=group_id,
        object_type=normalized,
        expected_count=count,
    )
    row_count, content_hash = content_hash_calculator.calculate_group_content_signature(
        group_id=group_id,
        object_type=normalized,
    )
    if row_count == count:
        store.commit_group_content_signature(
            group_id,
            row_count=row_count,
            content_hash=content_hash,
        )
    else:
        raise ValueError(f"Row count {row_count} does not match count {count}")

    progress.on_event(
        ProgressEvent.make(
            kind=ProgressKind.UPDATE,
            scope=ProgressScope.WORKER,
            current=max(0, int(row_count)),
            total=max(1, total_lines),
            unit=ProgressUnit.LINE,
            message="recalculating hash",
            path=progress_scope,
        )
    )
    return row_count, content_hash


def _hierarchy_has_top_level_key(hierarchy_json_path: str, key: str) -> bool:
    with open(hierarchy_json_path, "rb") as src:
        for prefix, event, value in ijson.parse(src):
            if prefix == "" and event == "map_key" and value == key:
                return True
    return False


def _read_hierarchy_sort_metadata(hierarchy_json_path: str) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    pending_key: Optional[str] = None
    scalar_events = {"string", "number", "boolean", "null"}
    with open(hierarchy_json_path, "rb") as src:
        for prefix, event, value in ijson.parse(src):
            if prefix == "" and event == "map_key":
                pending_key = str(value)
                continue
            if pending_key in _HIERARCHY_SORT_JSON_FIELDS:
                if event in scalar_events:
                    metadata[pending_key] = value
                    pending_key = None
                elif event in {"start_array", "start_map"}:
                    pending_key = None
            elif event != "map_key":
                pending_key = None
    return metadata


def _iter_hierarchy_array_payloads(
    hierarchy_json_path: str,
    key: str,
    *,
    progress: ProgressSink,
    progress_range: tuple[float, float] = (0.0, 1.0),
) -> Iterator[dict]:
    item_prefix = f"{key}.item"
    emitted = False
    reported_at = 0
    try:
        file_size = int(os.path.getsize(hierarchy_json_path))
    except OSError:
        file_size = 0
    start_fraction = max(0.0, min(1.0, float(progress_range[0])))
    end_fraction = max(0.0, min(1.0, float(progress_range[1])))
    if end_fraction < start_fraction:
        start_fraction, end_fraction = end_fraction, start_fraction

    def _scaled_position(raw_position: int) -> int:
        if file_size <= 0:
            return max(0, int(raw_position))
        ratio = max(0.0, min(1.0, float(raw_position) / float(file_size)))
        scaled_ratio = start_fraction + ((end_fraction - start_fraction) * ratio)
        return int(scaled_ratio * file_size)

    last_total_position = int(start_fraction * file_size) if file_size > 0 else 0

    with open(hierarchy_json_path, "rb") as src:
        for index, payload in enumerate(ijson.items(src, item_prefix), start=1):
            if not isinstance(payload, dict):
                raise ValueError(f"Malformed hierarchy json: non-object payload in array '{key}'")
            emitted = True
            if index % 1_000 == 0:
                position = int(src.tell())
                if position > reported_at:
                    scaled_position = max(0, int(_scaled_position(position)))
                    progress.on_event(
                        ProgressEvent.make(
                            kind=ProgressKind.UPDATE,
                            scope=ProgressScope.WORKER,
                            current=scaled_position,
                            total=max(1, file_size),
                            unit=ProgressUnit.BYTE,
                            message="reading hierarchy",
                            path=hierarchy_json_path,
                        )
                    )
                    delta = max(0, scaled_position - last_total_position)
                    if delta > 0:
                        progress.on_event(
                            ProgressEvent.make(
                                kind=ProgressKind.UPDATE,
                                scope=ProgressScope.TOTAL,
                                current_delta=delta,
                                total=None,
                                unit=ProgressUnit.BYTE,
                                message="Building internal model",
                            )
                        )
                        last_total_position = scaled_position
                    reported_at = position
            yield payload
        final_position = max(0, int(_scaled_position(int(src.tell()))))
        progress.on_event(
            ProgressEvent.make(
                kind=ProgressKind.UPDATE,
                scope=ProgressScope.WORKER,
                current=final_position,
                total=max(1, file_size),
                unit=ProgressUnit.BYTE,
                message="reading hierarchy",
                path=hierarchy_json_path,
            )
        )
        final_delta = max(0, final_position - last_total_position)
        if final_delta > 0:
            progress.on_event(
                ProgressEvent.make(
                    kind=ProgressKind.UPDATE,
                    scope=ProgressScope.TOTAL,
                    current_delta=final_delta,
                    total=None,
                    unit=ProgressUnit.BYTE,
                    message="Building internal model",
                )
            )
    if not emitted and not _hierarchy_has_top_level_key(hierarchy_json_path, key):
        if key == "Edges":
            return
        raise ValueError(f"Malformed hierarchy json: key '{key}' not found")


def _hierarchy_array_has_items(hierarchy_json_path: str, key: str) -> bool:
    item_prefix = f"{key}.item"
    with open(hierarchy_json_path, "rb") as src:
        for payload in ijson.items(src, item_prefix):
            if isinstance(payload, dict):
                return True
    return False


def _append_payloads_in_batches(
    *,
    store: ModelStore,
    group_id: int,
    payloads: Iterable[dict],
    start_index: Optional[int] = None,
    batch_size: int = 100_000,
    progress_label: Optional[str] = None,
    progress_every: int = DESERIALIZE_PROGRESS_EVERY,
) -> int:
    return store.append_payloads(
        group_id=group_id,
        payloads=payloads,
        start_index=start_index,
        batch_size=batch_size,
        progress_label=progress_label,
        progress_every=progress_every,
    )


def _subset_source_mtime_ns(subset_dir_path: str) -> int:
    if not os.path.isdir(subset_dir_path):
        return 0
    latest = 0
    for subset_file_name in os.listdir(subset_dir_path):
        if not subset_file_name.endswith(".json"):
            continue
        subset_path = os.path.join(subset_dir_path, subset_file_name)
        try:
            mtime_ns = int(os.stat(subset_path).st_mtime_ns)
        except OSError:
            continue
        if mtime_ns > latest:
            latest = mtime_ns
    return latest


def _ensure_hierarchy_store_groups(
    *,
    hierarchy_json_path: str,
    model_id: str,
    dimension_name: str,
    hierarchy_name: str,
    subset_dir_path: str,
    progress: ProgressSink,
    thread_pool_executor: ThreadPoolExecutor,
    content_hash_calculator: ContentHashCalculator,
) -> tuple[StoreBackedSequence[Element], StoreBackedSequence[Edge], StoreBackedSequence[Subset]]:
    store = ModelStore.for_model_id(model_id)
    elements = StoreBackedSequence.for_elements_sink(
        store=store,
        model_id=model_id,
        dimension_name=dimension_name,
        hierarchy_name=hierarchy_name,
    )
    edges = StoreBackedSequence.for_edges_sink(
        store=store,
        model_id=model_id,
        dimension_name=dimension_name,
        hierarchy_name=hierarchy_name,
    )
    subsets = StoreBackedSequence.for_subsets_sink(
        store=store,
        model_id=model_id,
        dimension_name=dimension_name,
        hierarchy_name=hierarchy_name,
    )
    source_mtime_ns = int(os.stat(hierarchy_json_path).st_mtime_ns) if os.path.exists(hierarchy_json_path) else None
    
    needs_elements_rebuild = source_mtime_ns is None or elements.source_json_mtime_ns() != source_mtime_ns or elements.content_signature()[1] == ModelStore.EMPTY_CONTENT_HASH
    needs_edges_rebuild = source_mtime_ns is None or edges.source_json_mtime_ns() != source_mtime_ns or edges.content_signature()[1] == ModelStore.EMPTY_CONTENT_HASH
    subset_source_mtime_ns = _subset_source_mtime_ns(subset_dir_path) 
    needs_subsets_rebuild = subsets.source_json_mtime_ns() != subset_source_mtime_ns or subsets.content_signature()[1] == ModelStore.EMPTY_CONTENT_HASH

    if not needs_elements_rebuild:
        source_has_elements = _hierarchy_array_has_items(hierarchy_json_path, "Elements")
        cached_has_elements = len(elements) > 0
        if source_has_elements != cached_has_elements:
            needs_elements_rebuild = True
    if not needs_edges_rebuild:
        source_has_edges = _hierarchy_array_has_items(hierarchy_json_path, "Edges")
        cached_has_edges = len(edges) > 0
        if source_has_edges != cached_has_edges:
            needs_edges_rebuild = True
    if needs_elements_rebuild and needs_edges_rebuild:
        elements_progress_range = (0.0, 0.5)
        edges_progress_range = (0.5, 1.0)
    elif needs_elements_rebuild:
        elements_progress_range = (0.0, 1.0)
        edges_progress_range = (0.0, 1.0)
    elif needs_edges_rebuild:
        elements_progress_range = (0.0, 1.0)
        edges_progress_range = (0.0, 1.0)
    else:
        elements_progress_range = (0.0, 1.0)
        edges_progress_range = (0.0, 1.0)

    def _enqueue_hash_recalc(group_id: int, scope: str, count: int) -> None:
        _recalculate_group_signature_task(
            model_id=model_id,
            group_id=group_id,
            progress=progress,
            progress_scope=scope,
            content_hash_calculator=content_hash_calculator,
            count=count,
        )

    if needs_elements_rebuild:
        _progress_start(progress, hierarchy_json_path, "inserting elements")
        elements.replace_with_payloads(())
        _append_payloads_in_batches(
            store=store,
            group_id=elements.group_id,
            payloads=_iter_hierarchy_array_payloads(
                hierarchy_json_path,
                "Elements",
                progress=progress,
                progress_range=elements_progress_range,
            ),
            start_index=0,
        )
        if source_mtime_ns is not None:
            elements.set_source_json_mtime_ns(source_mtime_ns)
        _enqueue_hash_recalc(elements.group_id, f"{dimension_name}/{hierarchy_name} elements", len(elements))
    if needs_edges_rebuild:
        _progress_start(progress, hierarchy_json_path, "inserting edges")
        edges.replace_with_payloads(())
        _append_payloads_in_batches(
            store=store,
            group_id=edges.group_id,
            payloads=_iter_hierarchy_array_payloads(
                hierarchy_json_path,
                "Edges",
                progress=progress,
                progress_range=edges_progress_range,
            ),
            start_index=0,
        )
        if source_mtime_ns is not None:
            edges.set_source_json_mtime_ns(source_mtime_ns)
        _enqueue_hash_recalc(edges.group_id, f"{dimension_name}/{hierarchy_name} edges", len(edges))

    if needs_subsets_rebuild:
        subsets.replace_with_payloads(())
        if os.path.isdir(subset_dir_path):
            def _subset_payload_iter() -> Iterator[dict]:
                for subset_file_name in sorted(os.listdir(subset_dir_path)):
                    if not subset_file_name.endswith(".json"):
                        subset_artifact_path = os.path.join(subset_dir_path, subset_file_name)
                        _progress_start(progress, subset_artifact_path, "skipping non-json subset artifact")
                        _progress_mark(progress, subset_artifact_path)
                        continue
                    subset_path = os.path.join(subset_dir_path, subset_file_name)
                    _progress_start(progress, subset_path, "inserting subsets")
                    subset_json = _json_load_file(subset_path)
                    _progress_mark(progress, subset_path)
                    subset_payload = {
                        "name": subset_json.get("Name") or subset_json.get("name"),
                        "expression": subset_json.get("Expression") or subset_json.get("expression"),
                    }
                    if "Elements" in subset_json:
                        subset_payload["Elements"] = subset_json["Elements"]
                    elif "elements" in subset_json:
                        subset_payload["elements"] = subset_json["elements"]
                    yield subset_payload

            _append_payloads_in_batches(
                store=store,
                group_id=subsets.group_id,
                payloads=_subset_payload_iter(),
            )
        subsets.set_source_json_mtime_ns(subset_source_mtime_ns)
        _enqueue_hash_recalc(subsets.group_id, f"{dimension_name}/{hierarchy_name} subsets", len(subsets))
    elif os.path.isdir(subset_dir_path):
        for subset_file_name in sorted(os.listdir(subset_dir_path)):
            _progress_mark(progress, os.path.join(subset_dir_path, subset_file_name))
    if needs_elements_rebuild or needs_edges_rebuild:
        _progress_mark(progress, hierarchy_json_path, include_total=False)
    else:
        _progress_mark(progress, hierarchy_json_path)
    return elements, edges, subsets



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

def deserialize_model(
    dir: str,
    model_id: Optional[str] = None,
    *,
    progress_sink: Optional[ProgressSink] = None,
    max_workers: Optional[int] = None,
    error_callback: Optional[ErrorCallback] = None,
    _resolved_cpu_workers: Optional[int] = None,
) -> tuple[Model, list[WorkflowError]]:

    dir = _handle_long_path(dir)
    resolved_model_id = (model_id or Path(dir).resolve().name).strip()
    if not resolved_model_id:
        raise ValueError("model_id must not be empty")

    progress_sink = progress_sink if progress_sink is not None else NoopProgressSink()
    worker_counts = resolve_worker_counts(max_workers=max_workers, io_ratio=1)
    multi_process_progress_manager: Optional[MultiProcessProgressManager] = None
    progress_manager_started = False
    dimensions_dir = dir + '/dimensions'
    cubes_dir = dir + '/cubes'
    processes_dir = dir + '/processes'
    chores_dir = dir + '/chores'
    total_object_count = 0
    active_progress_sink = progress_sink
    _setup_errors: list[WorkflowError] = []
    _processes: Dict[str, Process] = {}
    _chores: Dict[str, Chore] = {}
    _dimensions: Dict[str, Dimension] = {}
    _cubes: Dict[str, Cube] = {}
    _process_errors: list[WorkflowError] = []
    _chore_errors: list[WorkflowError] = []
    _dim_errors: list[WorkflowError] = []
    _cube_errors: list[WorkflowError] = []

    logger.info(
        "Starting model deserialization path=%s model_id=%s max_workers=%s",
        dir,
        resolved_model_id,
        worker_counts.max_workers,
    )
    
    def _add_object_count(delta: int) -> None:
        nonlocal total_object_count
        if delta > 0:
            total_object_count += int(delta)

    try:
        if (
            worker_counts.cpu_workers > 1
            and not isinstance(progress_sink, NoopProgressSink)
            and not isinstance(progress_sink, MultiProcessProgressQueueSink)
        ):
            try:
                multi_process_progress_manager = MultiProcessProgressManager(progress_sink)
                multi_process_progress_manager.start()
                progress_manager_started = True
                active_progress_sink = (
                    multi_process_progress_manager.get_multi_process_progress_queue_sink()
                )
            except Exception as exc:
                _setup_errors.append(
                    workflow_error_from_exception(
                        workflow="deserialize",
                        phase="progress_setup",
                        subject=dir,
                        exception=exc,
                        severity="fatal",
                    )
                )

        try:
            active_progress_sink.on_event(
                ProgressEvent.total_line(
                    message="Deserializing",
                    total_delta=max(1, _directory_total_bytes(dir)),
                    unit=ProgressUnit.BYTE,
                )
            )
        except Exception as exc:
            _setup_errors.append(
                workflow_error_from_exception(
                    workflow="deserialize",
                    phase="progress",
                    subject=dir,
                    exception=exc,
                    severity="fatal",
                )
            )

        try:
            _processes, _process_errors = deserialize_processes(
                processes_dir,
                progress=active_progress_sink,
                count_callback=_add_object_count,
            )
        except Exception as exc:
            _process_errors.append(
                workflow_error_from_exception(
                    workflow="deserialize",
                    phase="processes",
                    subject=processes_dir,
                    exception=exc,
                    severity="fatal",
                )
            )
        logger.info(
            "Process deserialization finished kept=%d errors=%d",
            len(_processes),
            len(_process_errors),
        )

        try:
            _chores, _chore_errors = deserialize_chores(
                chores_dir,
                progress=active_progress_sink,
                count_callback=_add_object_count,
            )
        except Exception as exc:
            _chore_errors.append(
                workflow_error_from_exception(
                    workflow="deserialize",
                    phase="chores",
                    subject=chores_dir,
                    exception=exc,
                    severity="fatal",
                )
            )
        logger.info(
            "Chore deserialization finished kept=%d errors=%d",
            len(_chores),
            len(_chore_errors),
        )

        try:
            model_store = ModelStore.for_model_id(resolved_model_id)
            with ThreadPoolExecutor(
                max_workers=max(1, worker_counts.io_workers)
            ) as thread_pool_executor, ContentHashCalculator(
                db_path=model_store.db_path,
                max_workers=worker_counts.cpu_workers,
                progress_sink=active_progress_sink,
            ) as content_hash_calculator:
                try:
                    _dimensions, _dim_errors = deserialize_dimensions(
                        dimensions_dir,
                        resolved_model_id,
                        progress=active_progress_sink,
                        count_callback=_add_object_count,
                        thread_pool_executor=thread_pool_executor,
                        content_hash_calculator=content_hash_calculator,
                    )
                except Exception as exc:
                    _dim_errors.append(
                        workflow_error_from_exception(
                            workflow="deserialize",
                            phase="dimensions",
                            subject=dimensions_dir,
                            exception=exc,
                            severity="fatal",
                        )
                    )
        except Exception as exc:
            _dim_errors.append(
                workflow_error_from_exception(
                    workflow="deserialize",
                    phase="dimensions_setup",
                    subject=dimensions_dir,
                    exception=exc,
                    severity="fatal",
                )
            )
        logger.info(
            "Dimension deserialization finished kept=%d errors=%d",
            len(_dimensions),
            len(_dim_errors),
        )

        try:
            _cubes, _cube_errors = deserialize_cubes(
                cubes_dir,
                _dimensions,
                progress=active_progress_sink,
                count_callback=_add_object_count,
            )
        except Exception as exc:
            _cube_errors.append(
                workflow_error_from_exception(
                    workflow="deserialize",
                    phase="cubes",
                    subject=cubes_dir,
                    exception=exc,
                    severity="fatal",
                )
            )
        logger.info(
            "Cube deserialization finished kept=%d errors=%d",
            len(_cubes),
            len(_cube_errors),
        )
    finally:
        if multi_process_progress_manager is not None and progress_manager_started:
            multi_process_progress_manager.close()

    _assembly_errors: list[WorkflowError] = []
    try:
        _model = Model(
            cubes=list(_cubes.values()),
            dimensions=list(_dimensions.values()),
            processes=list(_processes.values()),
            chores=list(_chores.values()),
            model_id=resolved_model_id,
            total_object_count=total_object_count,
        )
    except Exception as exc:
        _assembly_errors.append(
            workflow_error_from_exception(
                workflow="deserialize",
                phase="model_assembly",
                subject=dir,
                exception=exc,
                severity="fatal",
            )
        )
        _model = Model(
            cubes=[],
            dimensions=[],
            processes=[],
            chores=[],
            model_id=resolved_model_id,
            total_object_count=0,
        )
    _errors: list[WorkflowError] = []
    for helper_errors in (
        _setup_errors,
        _process_errors,
        _chore_errors,
        _dim_errors,
        _cube_errors,
        _assembly_errors,
    ):
        collect_worker_errors(
            _errors,
            helper_errors,
            error_callback=error_callback,
        )
    logger.info(
        "Deserialized model path=%s dimensions=%d cubes=%d processes=%d chores=%d",
        dir,
        len(_dimensions),
        len(_cubes),
        len(_processes),
        len(_chores),
    )
    if _errors:
        recoverable_errors = sum(error.severity == "recoverable" for error in _errors)
        for error in _errors:
            logger.log(
                logging.ERROR if error.severity == "fatal" else logging.WARNING,
                "Model deserialization error workflow=%s phase=%s subject=%s type=%s message=%s",
                error.workflow,
                error.phase,
                error.subject,
                error.exception_type,
                error.message,
            )
        logger.warning(
            "Model deserialization completed with errors total=%d recoverable=%d fatal=%d",
            len(_errors),
            recoverable_errors,
            len(_errors) - recoverable_errors,
        )
    else:
        logger.info("Model deserialization completed without errors")
    return _model, _errors


def deserialize_chores(
    chore_dir,
    *,
    progress: Optional[ProgressSink] = None,
    count_callback: Optional[Callable[[int], None]] = None,
) -> tuple[Dict[str, Chore], list[WorkflowError]]:
    progress = progress if progress is not None else NoopProgressSink()
    chores: Dict[str, Chore] = {}
    chores_errors: list[WorkflowError] = []
    if not os.path.exists(chore_dir):
        return chores, chores_errors

    for file_name in os.listdir(chore_dir):
        file_path = os.path.join(chore_dir, file_name)
        _progress_start(progress, file_path, "reading chore")
        if not file_name.endswith('.json'):
            chores_errors.append(
                WorkflowError(
                    workflow="deserialize",
                    phase="chore_artifact",
                    subject=file_path,
                    exception_type="UnsupportedArtifact",
                    message="not a chore json file",
                    severity="recoverable",
                )
            )
            _progress_mark(progress, file_path)
            continue
        file_name_base, _, _ = file_name.rpartition('.')
        try:
            chore_json = _json_load_file(file_path)
            _progress_mark(progress, file_path)

            tasks = []
            for task_data in chore_json.get('Tasks', []):
                process_bind = task_data.get("Process@odata.bind", "")
                match = re.search(r"Processes\('([^']*)'\)", process_bind)
                if match:
                    tasks.append(Task(process_name=match.group(1), parameters=task_data.get('Parameters', [])))

            chores[chore_json['Name']] = Chore(name=chore_json['Name'], start_time=chore_json['StartTime'],
                                               dst_sensitive=chore_json['DSTSensitive'], active=chore_json['Active'],
                                               execution_mode=chore_json['ExecutionMode'],
                                               frequency=chore_json['Frequency'], tasks=tasks)
            if count_callback is not None:
                count_callback(1)
        except Exception as e:
            chores_errors.append(
                workflow_error_from_exception(
                    workflow="deserialize",
                    phase="chore",
                    subject=file_path,
                    exception=e,
                    severity="recoverable",
                )
            )
            _progress_mark(progress, file_path)
    return chores, chores_errors


def deserialize_processes(
    process_dir,
    *,
    progress: Optional[ProgressSink] = None,
    count_callback: Optional[Callable[[int], None]] = None,
) -> tuple[Dict[str, Process], list[WorkflowError]]:
    progress = progress if progress is not None else NoopProgressSink()
    processes: Dict[str, Process] = {}
    process_errors: list[WorkflowError] = []
    files = directory_to_dict(process_dir)
    for file_name in list(files.keys()):

        file_name_base, dot, file_name_ext = file_name.rpartition('.')
        process_file_path = os.path.join(process_dir, file_name)

        if file_name_ext != 'json' and file_name_ext != 'ti':
            process_errors.append(
                WorkflowError(
                    workflow="deserialize",
                    phase="process_artifact",
                    subject=process_file_path,
                    exception_type="UnsupportedArtifact",
                    message="not a process json or ti file",
                    severity="recoverable",
                )
            )
            _progress_mark(progress, os.path.join(process_dir, file_name))
            continue
        if file_name_ext != 'json':
            continue

        files.pop(file_name, None)
        process_json = None
        process_ti = None

        _progress_start(progress, process_file_path, "reading process json")
        with open(process_file_path, 'r', encoding='utf-8') as file:
            try:
                data = file.read()
                process_json = _json_load_text(data)
                _progress_mark(progress, process_file_path)
            except Exception as e:
                process_errors.append(
                    workflow_error_from_exception(
                        workflow="deserialize",
                        phase="process_json",
                        subject=process_file_path,
                        exception=e,
                        severity="recoverable",
                    )
                )
                _progress_mark(progress, process_file_path)
                continue

        ti_file_name = file_name_base + '.ti'
        if ti_file_name not in files:
            process_errors.append(
                WorkflowError(
                    workflow="deserialize",
                    phase="process_ti",
                    subject=os.path.join(process_dir, ti_file_name),
                    exception_type="FileNotFoundError",
                    message="related TI file not found",
                    severity="recoverable",
                )
            )
            continue

        ti_file_path = os.path.join(process_dir, ti_file_name)
        _progress_start(progress, ti_file_path, "reading process ti")
        with open(ti_file_path, 'r', encoding='utf-8') as file:
            try:
                data = file.read()
                process_ti = TI.from_string(data)
                _progress_mark(progress, ti_file_path)
            except Exception as e:
                process_errors.append(
                    workflow_error_from_exception(
                        workflow="deserialize",
                        phase="process_ti",
                        subject=ti_file_path,
                        exception=e,
                        severity="recoverable",
                    )
                )
                _progress_mark(progress, ti_file_path)
            finally:
                files.pop(ti_file_name, None)

        try:
            _process = Process(
                name=process_json['Name'],
                hasSecurityAccess=process_json['HasSecurityAccess'],
                code_link=process_json['Code@Code.link'],
                datasource=process_json.get('DataSource'),
                parameters=process_json['Parameters'],
                variables=process_json['Variables'],
                ti=process_ti,
                variables_ui_data=process_json.get('VariablesUIData'),
                ui_data=process_json.get('UIData'),
            )
            processes[process_json['Name']] = _process
            if count_callback is not None:
                count_callback(1)
        except Exception as e:
            process_errors.append(
                workflow_error_from_exception(
                    workflow="deserialize",
                    phase="process",
                    subject=process_file_path,
                    exception=e,
                    severity="recoverable",
                )
            )
    return processes, process_errors


def _deserialize_single_hierarchy(
    *,
    hierarchy_json_path: str,
    model_id: str,
    dimension_name: str,
    hierarchy_name: str,
    subset_dir_path: str,
    progress: ProgressSink,
    thread_pool_executor: ThreadPoolExecutor,
    content_hash_calculator: ContentHashCalculator,
) -> Hierarchy:
    sort_metadata = _read_hierarchy_sort_metadata(hierarchy_json_path)
    elements, edges, subsets = _ensure_hierarchy_store_groups(
        hierarchy_json_path=hierarchy_json_path,
        model_id=model_id,
        dimension_name=dimension_name,
        hierarchy_name=hierarchy_name,
        subset_dir_path=subset_dir_path,
        progress=progress,
        thread_pool_executor=thread_pool_executor,
        content_hash_calculator=content_hash_calculator,
    )
    hierarchy = Hierarchy(
        name=hierarchy_name,
        elements=elements,
        edges=edges,
        subsets=subsets,
        elements_sort_type=sort_metadata.get("ElementsSortType"),
        elements_sort_sense=sort_metadata.get("ElementsSortSense"),
        components_sort_type=sort_metadata.get("ComponentsSortType"),
        components_sort_sense=sort_metadata.get("ComponentsSortSense"),
    )
    normalized_sort_metadata = hierarchy_sort_metadata_json(hierarchy)
    for sequence in (elements, edges, subsets):
        if hasattr(sequence, "set_sort_metadata"):
            sequence.set_sort_metadata(normalized_sort_metadata)
    return hierarchy


def deserialize_dimensions(
    dimension_dir,
    model_id: str,
    *,
    progress: ProgressSink,
    count_callback: Optional[Callable[[int], None]] = None,
    thread_pool_executor: ThreadPoolExecutor,
    content_hash_calculator: ContentHashCalculator,
) -> tuple[Dict[str, Dimension], list[WorkflowError]]:
    model_id = model_id.strip()
    if not model_id:
        raise ValueError("model_id must not be empty")
    dimensions: Dict[str, Dimension] = {}
    dimension_errors: list[WorkflowError] = []
    files = directory_to_dict(dimension_dir)
    for file_name in sorted(list(files.keys())):
        file_name_base, dot, file_name_ext = file_name.rpartition('.')
        dim_file_path = os.path.join(dimension_dir, file_name)

        if file_name_ext not in ['json', 'hierarchies']:
            dimension_errors.append(
                WorkflowError(
                    workflow="deserialize",
                    phase="dimension_artifact",
                    subject=dim_file_path,
                    exception_type="UnsupportedArtifact",
                    message="not a dimension json or .hierarchies folder",
                    severity="recoverable",
                )
            )
            _progress_mark(progress, os.path.join(dimension_dir, file_name))
            continue
        if file_name_ext != 'json':
            continue

        files.pop(file_name, None)
        dim_json = None

        try:
            _progress_start(progress, dim_file_path, "reading dimension")
            with open(dim_file_path, 'r', encoding='utf-8') as file:
                data = file.read()
                dim_json = _json_load_text(data)
                _progress_mark(progress, dim_file_path)
        except Exception as e:
            dimension_errors.append(
                workflow_error_from_exception(
                    workflow="deserialize",
                    phase="dimension_json",
                    subject=dim_file_path,
                    exception=e,
                    severity="recoverable",
                )
            )
            _progress_mark(progress, dim_file_path)
            continue

        try:
            dim_name = dim_json['Name']
            _dimension = Dimension(name=dim_name, hierarchies=[], defaultHierarchy=None)
            dimension_object_count = 1
        except Exception as e:
            dimension_errors.append(
                workflow_error_from_exception(
                    workflow="deserialize",
                    phase="dimension",
                    subject=dim_file_path,
                    exception=e,
                    severity="recoverable",
                )
            )
            continue

        hier_dir_name = file_name_base + '.hierarchies'
        hier_dir_path = os.path.join(dimension_dir, hier_dir_name)

        if hier_dir_name not in files and not os.path.isdir(hier_dir_path):
            dimension_errors.append(
                WorkflowError(
                    workflow="deserialize",
                    phase="dimension_hierarchies",
                    subject=hier_dir_path,
                    exception_type="FileNotFoundError",
                    message="no hierarchies found",
                    severity="recoverable",
                )
            )
            continue

        hiers = files.get(hier_dir_name)
        parsed_hierarchies: dict[str, Hierarchy] = {}
        
        hierarchy_futures: list[HierarchyFutureMetadata] = []
        for submission_index, hier_file_name in enumerate(sorted(list(hiers.keys()))):
            hierarchy_file_path = os.path.join(hier_dir_path, hier_file_name)
            _progress_start(progress, hierarchy_file_path, "reading hierarchy")
            # Ignore temporary/in-progress hierarchy artifacts.
            if hier_file_name.endswith(".json.inprogress") or hier_file_name.endswith(".tmp.json.meta.json"):
                _progress_mark(progress, hierarchy_file_path)
                continue
            hier_file_name_base, dot, file_name_ext = hier_file_name.rpartition('.')
            if file_name_ext not in ['json', 'subsets']:
                dimension_errors.append(
                    WorkflowError(
                        workflow="deserialize",
                        phase="hierarchy_artifact",
                        subject=hierarchy_file_path,
                        exception_type="UnsupportedArtifact",
                        message="not a hierarchy json or .subset folder",
                        severity="recoverable",
                    )
                )
                _progress_mark(progress, hierarchy_file_path)
                continue
            if file_name_ext != 'json':
                continue

            hiers.pop(hier_file_name, None)
            subset_dir_name = hier_file_name_base + '.subsets'
            subset_dir_path = os.path.join(hier_dir_path, subset_dir_name)

            future = thread_pool_executor.submit(_deserialize_single_hierarchy,
                hierarchy_json_path=hierarchy_file_path,
                model_id=model_id,
                dimension_name=file_name_base,
                hierarchy_name=hier_file_name_base,
                subset_dir_path=subset_dir_path,
                progress=progress,
                thread_pool_executor=thread_pool_executor,
                content_hash_calculator=content_hash_calculator,
            )
            hierarchy_futures.append(
                HierarchyFutureMetadata(
                    submission_index=submission_index,
                    dimension_name=file_name_base,
                    hierarchy_name=hier_file_name_base,
                    hierarchy_json_path=hierarchy_file_path,
                    future=future,
                )
            )

        hierarchy_futures_by_future = {
            metadata.future: metadata
            for metadata in hierarchy_futures
        }
        hierarchy_future_errors: list[tuple[int, WorkflowError]] = []
        for future in concurrent.futures.as_completed(hierarchy_futures_by_future):
            metadata = hierarchy_futures_by_future[future]
            try:
                _hierarchy = future.result()
                parsed_hierarchies[_hierarchy.name] = _hierarchy
                _dimension.hierarchies.append(_hierarchy)
                hierarchy_object_count = 1 + len(_hierarchy.subsets) + len(_hierarchy.edges)
                if _hierarchy.name.strip().lower() != "leaves":
                    hierarchy_object_count += len(_hierarchy.elements)
                dimension_object_count += hierarchy_object_count
            except Exception as e:
                hierarchy_future_errors.append(
                    (
                        metadata.submission_index,
                        workflow_error_from_exception(
                            workflow="deserialize",
                            phase="hierarchy",
                            subject=Hierarchy.uri_for(
                                metadata.dimension_name,
                                metadata.hierarchy_name,
                            ),
                            exception=e,
                            severity="recoverable",
                        ),
                    )
                )
        dimension_errors.extend(
            error
            for _, error in sorted(hierarchy_future_errors, key=lambda item: item[0])
        )

        pattern = r"Dimensions\('([^']*)'\)/Hierarchies\('([^']*)'\)"
        default_hierarchy_payload = dim_json.get("DefaultHierarchy")
        default_hierarchy_ref = default_hierarchy_payload
        if isinstance(default_hierarchy_payload, dict):
            default_hierarchy_ref = (
                default_hierarchy_payload.get("@id")
                or default_hierarchy_payload.get("id")
            )
        match = re.search(pattern, default_hierarchy_ref or "")
        configured_default_hierarchy: Optional[Hierarchy] = None
        default_hierarchy_error: Optional[WorkflowError] = None
        if match:
            _, default_hierarchy_name = match.groups()
            resolved_default_hierarchy_name = unquote(default_hierarchy_name)
            configured_default_hierarchy = (
                parsed_hierarchies.get(default_hierarchy_name)
                or parsed_hierarchies.get(resolved_default_hierarchy_name)
            )
            if configured_default_hierarchy is None:
                default_hierarchy_error = WorkflowError(
                    workflow="deserialize",
                    phase="default_hierarchy",
                    subject=dim_file_path,
                    exception_type="ValueError",
                    message="configured default hierarchy could not be resolved",
                    severity="recoverable",
                )
        else:
            default_hierarchy_error = WorkflowError(
                workflow="deserialize",
                phase="default_hierarchy",
                subject=dim_file_path,
                exception_type="ValueError",
                message="no default hierarchy configured",
                severity="recoverable",
            )

        if not _dimension.hierarchies:
            if default_hierarchy_error is not None:
                dimension_errors.append(default_hierarchy_error)
            continue

        _dimension.defaultHierarchy = Dimension._select_default_hierarchy(
            dimension_name=_dimension.name,
            hierarchies=_dimension.hierarchies,
            default_hierarchy=configured_default_hierarchy,
        )
        if default_hierarchy_error is not None:
            dimension_errors.append(default_hierarchy_error)
        dimensions[_dimension.name] = _dimension
        if count_callback is not None:
            count_callback(dimension_object_count)
    return dimensions, dimension_errors


def deserialize_cubes(
    cubes_dir,
    _dimensions: Dict[str, Dimension],
    *,
    progress: Optional[ProgressSink] = None,
    count_callback: Optional[Callable[[int], None]] = None,
) -> tuple[Dict[str, Cube], list[WorkflowError]]:
    progress = progress if progress is not None else NoopProgressSink()
    cubes: Dict[str, Cube] = {}
    cube_errors: list[WorkflowError] = []
    files = directory_to_dict(cubes_dir)
    for file_name in list(files.keys()):
        file_name_base, dot, file_name_ext = file_name.rpartition('.')
        cube_artifact_path = os.path.join(cubes_dir, file_name)

        if file_name_ext not in ['json', 'rules', 'views']:
            cube_errors.append(
                WorkflowError(
                    workflow="deserialize",
                    phase="cube_artifact",
                    subject=cube_artifact_path,
                    exception_type="UnsupportedArtifact",
                    message="not a cube json or .rules or .views folder",
                    severity="recoverable",
                )
            )
            _progress_start(progress, cube_artifact_path, "skipping non-cube artifact")
            _progress_mark(progress, cube_artifact_path)
            continue
        if file_name_ext != 'json':
            continue

        files.pop(file_name, None)
        cube_json = None

        cube_file_path = os.path.join(cubes_dir, file_name)
        _progress_start(progress, cube_file_path, "reading cube")
        try:
            with open(cube_file_path, 'r', encoding='utf-8') as file:
                cube_json = _json_load_text(file.read())
        except Exception as e:
            cube_errors.append(
                workflow_error_from_exception(
                    workflow="deserialize",
                    phase="cube_json",
                    subject=cube_file_path,
                    exception=e,
                    severity="recoverable",
                )
            )
            _progress_mark(progress, cube_file_path)
            continue

        try:
            rules_list = []
            rule_file_path = os.path.join(cubes_dir, file_name_base + '.rules')
            if os.path.exists(rule_file_path):
                _progress_start(progress, rule_file_path, "reading cube rules")
                with open(rule_file_path, 'r', encoding='utf-8') as file:
                    rule_text = file.read()
                    _progress_mark(progress, rule_file_path)
                    # rules_list = _parse_rules(rule_text, cube_name=file_name_base)
                    if rule_text:
                        rules_list = [Rule(area="[default]", full_statement=rule_text, comment="", name="default")]

            drillthrough_rules_list = []
            drillthrough_rule_file_path = os.path.join(cubes_dir, f"}}CubeDrill_{file_name_base}.rules")
            if os.path.exists(drillthrough_rule_file_path):
                _progress_start(progress, drillthrough_rule_file_path, "reading cube drillthrough rules")
                with open(drillthrough_rule_file_path, 'r', encoding='utf-8') as file:
                    drillthrough_rule_text = file.read()
                    _progress_mark(progress, drillthrough_rule_file_path)
                    if drillthrough_rule_text:
                        drillthrough_rules_list = [
                            Rule(
                                area="[default]",
                                full_statement=drillthrough_rule_text,
                                comment="",
                                name="default",
                            )
                        ]
            _cube = Cube(
                name=cube_json['Name'],
                dimensions=[],
                rules=rules_list,
                views=[],
                drillthrough_rules=drillthrough_rules_list,
            )
        except Exception as e:
            cube_errors.append(
                workflow_error_from_exception(
                    workflow="deserialize",
                    phase="cube",
                    subject=cube_file_path,
                    exception=e,
                    severity="recoverable",
                )
            )
            _progress_mark(progress, cube_file_path)
            continue

        try:
            for dim in cube_json['Dimensions']:
                pattern = r"Dimensions\('([^']*)'\)"
                match = re.search(pattern, dim['@id'])
                if match:
                    dimension_name = match.group(1)
                    _cube.dimensions.append(dimension_name)
        except Exception as e:
            cube_errors.append(
                workflow_error_from_exception(
                    workflow="deserialize",
                    phase="cube_dimensions",
                    subject=cube_file_path,
                    exception=e,
                    severity="recoverable",
                )
            )
            continue

        view_dir_name = file_name_base + '.views'
        view_dir_path = os.path.join(cubes_dir, view_dir_name)
        if view_dir_name in files and os.path.isdir(view_dir_path):
            views = files.get(view_dir_name)
            for view_file_name in list(views.keys()):
                view_file_name_base, dot, file_name_ext = view_file_name.rpartition('.')

                view = None
                mdx = None
                if file_name_ext == 'json':
                    _progress_start(progress, os.path.join(view_dir_path, view_file_name), "reading cube view")
                    with open(os.path.join(view_dir_path, view_file_name), 'r', encoding='utf-8') as file:
                        try:
                            data = file.read()
                            view = _json_load_text(data)
                            _progress_mark(progress, os.path.join(view_dir_path, view_file_name))
                        except Exception as e:
                            cube_errors.append(
                                workflow_error_from_exception(
                                    workflow="deserialize",
                                    phase="cube_view",
                                    subject=os.path.join(view_dir_path, view_file_name),
                                    exception=e,
                                    severity="recoverable",
                                )
                            )
                            continue
                else:
                    continue

                view_type = (view.get('@type') or '').lower()

                if view_type == 'mdxview':
                    mdx_file_name = view_file_name_base + '.mdx'
                    if mdx_file_name in views:
                        _progress_start(progress, os.path.join(view_dir_path, mdx_file_name), "reading cube view mdx")
                        with open(os.path.join(view_dir_path, mdx_file_name), 'r', encoding='utf-8') as file:
                            try:
                                mdx = file.read()
                                _progress_mark(progress, os.path.join(view_dir_path, mdx_file_name))
                            except Exception as e:
                                cube_errors.append(
                                    workflow_error_from_exception(
                                        workflow="deserialize",
                                        phase="cube_view_mdx",
                                        subject=os.path.join(view_dir_path, mdx_file_name),
                                        exception=e,
                                        severity="recoverable",
                                    )
                                )
                        files.pop(mdx_file_name, None)
                    else:
                        cube_errors.append(
                            WorkflowError(
                                workflow="deserialize",
                                phase="cube_view_mdx",
                                subject=os.path.join(view_dir_path, mdx_file_name),
                                exception_type="FileNotFoundError",
                                message="mdx not found",
                                severity="recoverable",
                            )
                        )
                        continue

                    if not mdx:
                        cube_errors.append(
                            WorkflowError(
                                workflow="deserialize",
                                phase="cube_view_mdx",
                                subject=os.path.join(view_dir_path, mdx_file_name),
                                exception_type="ValueError",
                                message="mdx cannot be parsed",
                                severity="recoverable",
                            )
                        )
                        continue

                    meta_raw = view.get("Meta")
                    meta = dict(meta_raw) if isinstance(meta_raw, dict) else None
                    _cube.views.append(
                        MDXView(
                            name=view["Name"],
                            mdx=mdx,
                            format_string=view.get("FormatString", "0.#########"),
                            meta=meta,
                        )
                    )
                elif view_type == 'nativeview':
                    _cube.views.append(
                        NativeView(
                            name=view['Name'],
                            columns=view.get('Columns', []),
                            rows=view.get('Rows', []),
                            titles=view.get('Titles', []),
                            suppress_empty_columns=view.get('SuppressEmptyColumns', False),
                            suppress_empty_rows=view.get('SuppressEmptyRows', False),
                            format_string=view.get('FormatString', '0.#########'),
                        )
                    )
                else:
                    cube_errors.append(
                        WorkflowError(
                            workflow="deserialize",
                            phase="cube_view",
                            subject=os.path.join(view_dir_path, view_file_name),
                            exception_type="UnsupportedArtifact",
                            message="unsupported view type",
                            severity="recoverable",
                        )
                    )
        cubes[_cube.name] = _cube
        if count_callback is not None:
            count_callback(1 + len(_cube.rules) + len(_cube.drillthrough_rules) + len(_cube.views))
    return cubes, cube_errors


def _parse_rules(rule_text: str, cube_name: str) -> List[Rule]:
    if not rule_text: return []
    rules = []
    seen_names: dict[str, int] = {}

    def _unique_rule_name(area: str) -> str:
        base = Rule.name_from_area(area)
        seen_names[base] = seen_names.get(base, 0) + 1
        if seen_names[base] == 1:
            return base
        return f"{base}_{seen_names[base]}"

    pattern = re.compile(r"(?P<comment>(?:#.*(?:\r\n|\n|$)\s*)*)?(?P<statement>\[.*?\][^;]*;)", re.DOTALL)
    header_match = re.match(r'^(.*?)(?=\[|#|$)', rule_text, re.DOTALL)
    last_pos = 0
    if header_match:
        header_text = header_match.group(1).strip()
        if header_text:
            rules.append(
                Rule(
                    name=_unique_rule_name("[HEADER]"),
                    area="[HEADER]",
                    full_statement=header_text,
                    comment="",
                )
            )
        last_pos = header_match.end()
    for match in pattern.finditer(rule_text, last_pos):
        comment = (match.group('comment') or "").strip()
        statement_text = match.group('statement').strip()
        area_match = re.search(r'(\[.*?\])', statement_text)
        area = area_match.group(1) if area_match else "[UNKNOWN]"
        rules.append(
            Rule(
                name=_unique_rule_name(area),
                area=area,
                full_statement=statement_text,
                comment=comment,
            )
        )
    return rules


def directory_to_dict(path):
    """Converts a directory structure to a nested dictionary."""
    if not os.path.isdir(path):
        return {}
    directory_dict = {}
    for item in os.listdir(path):
        item_path = os.path.join(path, item)
        if os.path.isdir(item_path):
            # If the item is a directory, recursively populate its contents
            directory_dict[item] = directory_to_dict(item_path)
        else:
            # If the item is a file, set it to None or any specific value if needed
            directory_dict[item] = None
    return directory_dict
