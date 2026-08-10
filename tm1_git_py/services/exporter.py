import json
import logging
import re
from concurrent.futures import Future, wait
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, MutableSequence, Optional

import TM1py
from TM1py import TM1Service

from tm1_git_py.db.model_store import ModelStore
from tm1_git_py.internal.content_hash_calculator import ContentHashCalculator
from tm1_git_py.internal.priority_thread_pool_executor import PriorityThreadPoolExecutor
from tm1_git_py.internal.worker_config import WorkerCounts, resolve_worker_counts
from tm1_git_py.model.chore import Chore
from tm1_git_py.model.cube import Cube
from tm1_git_py.model.dimension import Dimension
from tm1_git_py.model.hierarchy import (
    Hierarchy,
    hierarchy_sort_metadata_json,
)
from tm1_git_py.model.mdxview import MDXView
from tm1_git_py.model.model import Model
from tm1_git_py.model.nativeview import NativeView
from tm1_git_py.model.process import Process
from tm1_git_py.model.rule import Rule
from tm1_git_py.model.task import Task
from tm1_git_py.model.ti import TI
from tm1_git_py.reporting.progress_reporting import (
    MultiProcessProgressManager,
    NoopProgressSink,
    ProgressEvent,
    ProgressSink,
)
from tm1_git_py.reporting.error_reporting import (
    ErrorCallback,
    WorkflowError,
    collect_worker_errors,
    workflow_error_from_exception,
)
from tm1_git_py.services.filter import (
    EntityType,
    FilterRules,
    apply_default_filter_rules,
    normalize_for_path,
)
from tm1_git_py.services.sort_metadata import get_hierarchy_sort_metadata
from tm1_git_py.tm1_api import (
    get_cube_names,
    get_edges_count,
    get_elements_count,
    _get_elements_page,
    _get_edges_page,
    _get_subsets_page,
    get_process_names,
    get_subsets_identity_etag,
    get_subsets_count,
    get_views,
)
from tm1_git_py.tm1_api.dimension_service import get_names as get_dimension_names
from tm1_git_py.tm1_api.hierarchy_service import get_all_names as get_hierarchy_names

logger = logging.getLogger(__name__)


def _can_reuse_content_signature(content_signature: Optional[tuple[int, str]]) -> bool:
    if content_signature is None:
        return False
    row_count, content_hash = content_signature
    if int(row_count) == 0:
        return True
    return content_hash != ModelStore.EMPTY_CONTENT_HASH


class _InlineExecutor:
    def __enter__(self) -> "_InlineExecutor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def submit(self, fn, *args, **kwargs) -> Future:
        kwargs.pop("priority", None)
        future: Future = Future()
        try:
            future.set_result(fn(*args, **kwargs))
        except BaseException as exc:
            future.set_exception(exc)
        return future


@dataclass
class PageFuture:
    skip: int
    top: int
    future: Future


@dataclass
class CountFuture:
    hierarchy_future: "HierarchyFuture"
    page_kind: str
    future: Future


@dataclass
class HierarchyFuture:
    dimension_name: str
    hierarchy: Hierarchy
    tm1_object_counts: dict[str, int] = field(default_factory=dict)
    element_pages: list[PageFuture] = field(default_factory=list)
    edge_pages: list[PageFuture] = field(default_factory=list)
    subset_pages: list[PageFuture] = field(default_factory=list)
    _elements_done_callbacks: list[Callable[["HierarchyFuture"], None]] = field(default_factory=list)
    _edges_done_callbacks: list[Callable[["HierarchyFuture"], None]] = field(default_factory=list)
    _subset_done_callbacks: list[Callable[["HierarchyFuture"], None]] = field(default_factory=list)

    @property
    def all_futures(self) -> list[Future]:
        return [
            page.future
            for pages in (self.element_pages, self.edge_pages, self.subset_pages)
            for page in pages
        ]

    def page_futures_with_context(self) -> list[tuple[str, PageFuture]]:
        return [
            (page_kind, page)
            for page_kind, pages in (
                ("elements", self.element_pages),
                ("edges", self.edge_pages),
                ("subsets", self.subset_pages),
            )
            for page in pages
        ]

    def pages_for(self, page_kind: str) -> list[PageFuture]:
        if page_kind == "elements":
            return self.element_pages
        if page_kind == "edges":
            return self.edge_pages
        if page_kind == "subsets":
            return self.subset_pages
        raise ValueError(f"Unsupported page kind: {page_kind}")

    @property
    def elements_done(self) -> bool:
        return all(page.future.done() for page in self.element_pages)

    @property
    def edges_done(self) -> bool:
        return all(page.future.done() for page in self.edge_pages)

    @property
    def subset_done(self) -> bool:
        return all(page.future.done() for page in self.subset_pages)

    def add_page(self, page_kind: str, *, skip: int, top: int, future: Future) -> None:
        self.pages_for(page_kind).append(
            PageFuture(skip=skip, top=top, future=future)
        )

    def add_elements_done_callback(self, fn: Callable[["HierarchyFuture"], None]) -> None:
        self._elements_done_callbacks.append(fn)

    def add_edges_done_callback(self, fn: Callable[["HierarchyFuture"], None]) -> None:
        self._edges_done_callbacks.append(fn)

    def add_subset_done_callback(self, fn: Callable[["HierarchyFuture"], None]) -> None:
        self._subset_done_callbacks.append(fn)

    def _notify_callbacks(self, callbacks: list[Callable[["HierarchyFuture"], None]]) -> None:
        for callback in callbacks:
            callback(self)

    def notify_successful_page_kind(self, page_kind: str) -> None:
        """Run dependent completion work from the export coordinator.

        The caller must first resolve every future for the page kind and call
        this only if they all succeeded. This prevents completion callbacks
        from persisting etags or hashes for an incomplete hierarchy.
        """

        if page_kind == "elements" and self._elements_done_callbacks:
            callbacks = self._elements_done_callbacks
            self._elements_done_callbacks = []
            self._notify_callbacks(callbacks)
        elif page_kind == "edges" and self._edges_done_callbacks:
            callbacks = self._edges_done_callbacks
            self._edges_done_callbacks = []
            self._notify_callbacks(callbacks)
        elif page_kind == "subsets" and self._subset_done_callbacks:
            callbacks = self._subset_done_callbacks
            self._subset_done_callbacks = []
            self._notify_callbacks(callbacks)


def export(
    tm1_conn: TM1Service,
    model_id: str,
    filter_rules: Optional[FilterRules] = None,
    *,
    progress_sink: Optional[ProgressSink] = None,
    max_workers: Optional[int] = None,
    error_callback: Optional[ErrorCallback] = None,
) -> tuple[Model, list[WorkflowError]]:
    progress_sink = progress_sink if progress_sink is not None else NoopProgressSink()
    worker_counts = resolve_worker_counts(max_workers)
    filter_rules = apply_default_filter_rules(filter_rules)

    logger.info(
        "Starting TM1 export model_id=%s max_workers=%s",
        model_id,
        worker_counts.max_workers,
    )

    multi_process_progress_manager: Optional[MultiProcessProgressManager] = None
    _dimensions: Dict[str, Dimension] = {}
    _cubes: Dict[str, Cube] = {}
    _processes: Dict[str, Process] = {}
    _chores: Dict[str, Chore] = {}
    _dim_errors: list[WorkflowError] = []
    _cube_errors: list[WorkflowError] = []
    _process_errors: list[WorkflowError] = []
    _chore_errors: list[WorkflowError] = []
    try:
        if worker_counts.cpu_workers > 1 and not isinstance(progress_sink, NoopProgressSink):
            multi_process_progress_manager = MultiProcessProgressManager(progress_sink)
            multi_process_progress_manager.start()
            active_progress_sink = multi_process_progress_manager.get_multi_process_progress_queue_sink()
        else:
            active_progress_sink = progress_sink

        progress_sink.on_event(ProgressEvent.total_line(message="Exporting"))
        try:
            _dimensions, _dim_errors = dimensions_to_model(
                tm1_conn,
                model_id=model_id,
                filter_rules=filter_rules,
                progress_sink=active_progress_sink,
                worker_counts=worker_counts,
            )
        except Exception as exc:
            _dim_errors.append(
                workflow_error_from_exception(
                    workflow="export",
                    phase="dimensions",
                    subject=model_id,
                    exception=exc,
                    severity="fatal",
                )
            )
        try:
            _cubes, _cube_errors = cubes_to_model(
                tm1_conn,
                _dimensions,
                filter_rules=filter_rules,
                progress_sink=active_progress_sink,
            )
        except Exception as exc:
            _cube_errors.append(
                workflow_error_from_exception(
                    workflow="export",
                    phase="cubes",
                    subject=model_id,
                    exception=exc,
                    severity="fatal",
                )
            )
        try:
            _processes, _process_errors = procs_to_model(
                tm1_conn,
                filter_rules=filter_rules,
                progress_sink=active_progress_sink,
            )
        except Exception as exc:
            _process_errors.append(
                workflow_error_from_exception(
                    workflow="export",
                    phase="processes",
                    subject=model_id,
                    exception=exc,
                    severity="fatal",
                )
            )
        try:
            _chores, _chore_errors = chores_to_model(
                tm1_conn,
                filter_rules=filter_rules,
                progress_sink=active_progress_sink,
            )
        except Exception as exc:
            _chore_errors.append(
                workflow_error_from_exception(
                    workflow="export",
                    phase="chores",
                    subject=model_id,
                    exception=exc,
                    severity="fatal",
                )
            )
    finally:
        if multi_process_progress_manager is not None:
            multi_process_progress_manager.close()

    _model = Model(
        cubes=list(_cubes.values()),
        dimensions=list(_dimensions.values()),
        processes=list(_processes.values()),
        chores=list(_chores.values()),
    )
    logger.info(
        "TM1 export model assembled dimensions=%d cubes=%d processes=%d chores=%d",
        len(_model.dimensions),
        len(_model.cubes),
        len(_model.processes),
        len(_model.chores),
    )
    errors: list[WorkflowError] = []
    for helper_errors in (
        _dim_errors,
        _cube_errors,
        _process_errors,
        _chore_errors,
    ):
        collect_worker_errors(
            errors,
            helper_errors,
            error_callback=error_callback,
        )

    if errors:
        recoverable_errors = sum(error.severity == "recoverable" for error in errors)
        fatal_errors = len(errors) - recoverable_errors
        for error in errors:
            logger.log(
                logging.ERROR if error.severity == "fatal" else logging.WARNING,
                "TM1 export error workflow=%s phase=%s subject=%s type=%s message=%s",
                error.workflow,
                error.phase,
                error.subject,
                error.exception_type,
                error.message,
            )
        logger.warning(
            "TM1 export completed with errors total=%d recoverable=%d fatal=%d",
            len(errors),
            recoverable_errors,
            fatal_errors,
        )
    else:
        logger.info("TM1 export completed without errors")

    return _model, errors


def chores_to_model(
    tm1_conn,
    filter_rules: FilterRules,
    progress_sink: Optional[ProgressSink] = None
) -> tuple[Dict[str, Chore], list[WorkflowError]]:
    progress_sink = progress_sink if progress_sink is not None else NoopProgressSink()
    all_chores = tm1_conn.chores.get_all_names()
    _chores: Dict[str, Chore] = {}
    _errors: list[WorkflowError] = []
    skipped_chores = 0
    skipped_tasks = 0
    logger.info("Exporting %d chores", len(all_chores))
    progress_sink.on_event(ProgressEvent.total_line(total_delta=len(all_chores)))

    for idx, chore_name in enumerate(all_chores, start=1):
        progress_sink.on_event(ProgressEvent.worker_line(current=0, total=1, message=f"Fetching chore {chore_name}"))
        try:
            chore_url = Chore.uri_for(chore_name)
            if filter_rules.should_exclude(chore_url):
                skipped_chores += 1
                continue

            chore = tm1_conn.chores.get(chore_name=chore_name)

            tasks_for_model = []
            for tm1py_task in chore.tasks:
                task_dict = tm1py_task.body_as_dict
                process_name = ""
                process_bind = task_dict.get("Process@odata.bind", "")
                match = re.search(r"Processes\('([^']*)'\)", process_bind)
                if match:
                    process_name = match.group(1)
                task_path = f"{chore_url}/Tasks('{process_name}')"
                if filter_rules.should_exclude(task_path):
                    skipped_tasks += 1
                    continue

                task_obj = Task(
                    process_name=process_name,
                    parameters=task_dict.get('Parameters', [])
                )
                tasks_for_model.append(task_obj)

            _chore = Chore(
                name=chore.name,
                start_time=chore.start_time.start_time_string,
                dst_sensitive=chore.dst_sensitivity,
                active=chore.active,
                execution_mode=chore.execution_mode,
                frequency=chore.frequency.frequency_string,
                tasks=tasks_for_model,
            )
            _chores[chore.name] = _chore
        except Exception as exc:
            _errors.append(
                workflow_error_from_exception(
                    workflow="export",
                    phase="chores",
                    subject=Chore.uri_for(chore_name),
                    exception=exc,
                    severity="recoverable",
                )
            )
        finally:
            progress_sink.on_event(ProgressEvent.total_line(current_delta=1))
            progress_sink.on_event(ProgressEvent.worker_line(current=1, total=1, message=f"Fetching chore {chore_name}"))

    logger.info(
        "Chore export assembly finished total=%d kept=%d skipped_chores=%d skipped_tasks=%d",
        len(all_chores),
        len(_chores),
        skipped_chores,
        skipped_tasks,
    )
    return _chores, _errors


def procs_to_model(
    tm1_conn :TM1Service,
    filter_rules: FilterRules,
    progress_sink: Optional[ProgressSink] = None
) -> tuple[Dict[str, Process], list[WorkflowError]]:
    progress_sink = progress_sink if progress_sink is not None else NoopProgressSink()
    processes_tm1_filter = filter_rules.to_tm1_name_filter(EntityType.PROCESS)
    filtered_process_names = [] if processes_tm1_filter.skip_all else get_process_names(
            tm1_conn,
            filter=processes_tm1_filter.filter_expr
    )

    _processes: Dict[str, Process] = {}
    _errors: list[WorkflowError] = []
    logger.info("Exporting %d processes", len(filtered_process_names))

    progress_sink.on_event(ProgressEvent.total_line(total_delta=len(filtered_process_names)))
    for idx, process_name in enumerate(filtered_process_names, start=1):
        progress_sink.on_event(ProgressEvent.worker_line(current=0, total=1, message=f"Fetching process {process_name}"))
        try:
            process = tm1_conn.processes.get(name_process=process_name)
            process_body = {}
            raw_body = getattr(process, "body", None)
            if raw_body:
                try:
                    process_body = json.loads(raw_body)
                except (TypeError, json.JSONDecodeError) as exc:
                    _errors.append(
                        workflow_error_from_exception(
                            workflow="export",
                            phase="process_body",
                            subject=Process.uri_for(process_name),
                            exception=exc,
                            severity="recoverable",
                        )
                    )

            _ti = TI(prolog_procedure=process.prolog_procedure,
                     metadata_procedure=process.metadata_procedure,
                     data_procedure=process.data_procedure,
                     epilog_procedure=process.epilog_procedure)
            _process = Process(name=process.name, hasSecurityAccess=process.has_security_access,
                               code_link=process_name + '.ti',
                               datasource=process_body.get("DataSource"),
                               parameters=process.parameters, variables=process.variables, ti=_ti,
                               variables_ui_data=process_body.get("VariablesUIData"),
                               ui_data=process_body.get("UIData"))
            _processes[process.name] = _process
        except Exception as exc:
            _errors.append(
                workflow_error_from_exception(
                    workflow="export",
                    phase="processes",
                    subject=Process.uri_for(process_name),
                    exception=exc,
                    severity="recoverable",
                )
            )
        finally:
            progress_sink.on_event(ProgressEvent.total_line(current_delta=1))
            progress_sink.on_event(ProgressEvent.worker_line(current=1, total=1, message=f"Fetching process {process_name}"))
    logger.info(
        "Process export assembly finished total=%d kept=%d",
        len(filtered_process_names),
        len(_processes)
    )
    return _processes, _errors


def cubes_to_model(
    tm1_conn: TM1Service,
    _dimensions: Dict[str, Dimension],
    filter_rules: FilterRules,
    progress_sink: Optional[ProgressSink] = None
) -> tuple[Dict[str, Cube], list[WorkflowError]]:
    progress_sink = progress_sink if progress_sink is not None else NoopProgressSink()
    cubes_tm1_filter = filter_rules.to_tm1_name_filter(EntityType.CUBE)
    filtered_cube_names = [] if cubes_tm1_filter.skip_all else get_cube_names(
        tm1_conn,
        filter=cubes_tm1_filter.filter_expr,
    )

    _cubes: Dict[str, Cube] = {}
    _errors: list[WorkflowError] = []
    skipped_rules = 0
    skipped_views = 0

    logger.info("Exporting %d cubes", len(filtered_cube_names))
    progress_sink.on_event(ProgressEvent.total_line(total_delta=len(filtered_cube_names)))
    progress_sink.on_event(ProgressEvent.worker_line(current=0, total=len(filtered_cube_names), message=f"Fetching cubes"))
   
    for idx, cube_name in enumerate(filtered_cube_names, start=1):
        progress_sink.on_event(ProgressEvent.worker_line(current=0, total=1, message=f"Fetching Cube {cube_name}"))
        try:

            cube = tm1_conn.cubes.get(cube_name=cube_name)

            rule_text = _extract_cube_rule_text(cube)

            # rules_list = _parse_rules(rule_text)
            rules_list = []
            if rule_text:
                rules_list = [Rule(area="[default]", full_statement=rule_text, comment="", name="default")]
            filtered_rules_list = []
            for rule in rules_list:
                rule_path = f"{Rule.uri_for(cube_name)}|{normalize_for_path(rule.area)}"
                if filter_rules.should_exclude(rule_path):
                    skipped_rules += 1
                    continue
                filtered_rules_list.append(rule)
            _cube = Cube(
                name=cube_name,
                dimensions=[],
                rules=filtered_rules_list,
                views=[],
                drillthrough_rules=_extract_drillthrough_rules(
                    tm1_conn,
                    cube_name,
                    filter_rules,
                ),
            )

            if cube.dimensions:
                for dimension in cube.dimensions:
                    _cube.dimensions.append(dimension)

            _cubes[cube_name] = _cube

            views_tm1_filter = filter_rules.to_tm1_child_name_filter(
                parent_chain=[(EntityType.CUBE, cube_name)],
                child_entity_type=EntityType.VIEW,
            )
            filtered_view_tuples = get_views(
                tm1_conn,
                cube_name=cube_name,
                filter=views_tm1_filter.filter_expr,
            )
            if filtered_view_tuples:
                private_views, public_views = filtered_view_tuples
                for view in private_views + public_views:
                    if isinstance(view, TM1py.Objects.MDXView):
                        _view = MDXView(
                            name=view.name,
                            mdx=view.mdx,
                        )
                    elif isinstance(view, TM1py.Objects.NativeView):
                        _view = NativeView.from_tm1py(view)
                    else:
                        continue
                    _cube.views.append(_view)


        except Exception as e:
            _errors.append(
                workflow_error_from_exception(
                    workflow="export",
                    phase="cubes",
                    subject=Cube.uri_for(cube_name),
                    exception=e,
                    severity="recoverable",
                )
            )
        finally:
            progress_sink.on_event(ProgressEvent.total_line(current_delta=1))
            progress_sink.on_event(ProgressEvent.worker_line(current=1, total=1, message=f"Fetching Cube {cube_name}"))
            

    logger.info(
        "Cube export assembly finished total=%d kept=%d skipped_rules=%d skipped_views=%d",
        len(filtered_cube_names),
        len(_cubes),
        skipped_rules,
        skipped_views,
    )
   
    return _cubes, _errors


def _extract_cube_rule_text(cube: Any) -> str:
    if not getattr(cube, "has_rules", False):
        return ""
    raw_body = getattr(getattr(cube, "rules", None), "body", "")
    try:
        rule_data = json.loads(raw_body)
        return rule_data.get("Rules", "")
    except (json.JSONDecodeError, AttributeError, TypeError):
        return raw_body if isinstance(raw_body, str) else ""


def _drillthrough_rule_uri(cube_name: str) -> str:
    return f"Cubes('{cube_name}')/DrillthroughRules('default')"


def _extract_drillthrough_rules(
    tm1_conn: TM1Service,
    cube_name: str,
    filter_rules: FilterRules,
) -> list[Rule]:
    drill_cube_name = f"}}CubeDrill_{cube_name}"
    try:
        if not tm1_conn.cubes.exists(drill_cube_name):
            return []
        drill_cube = tm1_conn.cubes.get(cube_name=drill_cube_name)
    except Exception:
        return []

    drillthrough_rule_text = _extract_cube_rule_text(drill_cube)
    if not drillthrough_rule_text:
        return []

    rule = Rule(
        area="[default]",
        full_statement=drillthrough_rule_text,
        comment="",
        name="default",
    )
    rule_path = f"{_drillthrough_rule_uri(cube_name)}|{normalize_for_path(rule.area)}"
    if filter_rules.should_exclude(rule_path):
        return []
    return [rule]


def dimensions_to_model(
    tm1_conn: TM1Service,
    model_id: str,
    filter_rules: FilterRules,
    *,
    progress_sink: ProgressSink,
    worker_counts: WorkerCounts,
) -> tuple[Dict[str, Dimension], list[WorkflowError]]:

    dimensions_tm1_filter = filter_rules.to_tm1_name_filter(EntityType.DIMENSION)
    all_dims = [] if dimensions_tm1_filter.skip_all else get_dimension_names(
        tm1_conn,
        filter=dimensions_tm1_filter.filter_expr,
    )
    _errors: list[WorkflowError] = []
    _dimensions: Dict[str, Dimension] = {}
    model_store = ModelStore.for_model_id(model_id)

    logger.info("Exporting %d dimensions", len(all_dims))

    cpu_workers = worker_counts.cpu_workers
    io_workers = worker_counts.io_workers

    io_executor = (
        PriorityThreadPoolExecutor(max_workers=io_workers, thread_name_prefix="dim_executor")
        if io_workers > 0
        else _InlineExecutor()
    )
    with ContentHashCalculator(db_path=model_store.db_path, max_workers=cpu_workers, progress_sink=progress_sink) as content_hash_calculator, \
            io_executor as executor:
        hierarchy_futures: list[HierarchyFuture] = []
        count_futures: list[CountFuture] = []

        def _compute_and_commit_hash(group_id: int, page_kind: str, expected_tm1_count: int) -> tuple[int, str]:
            progress_sink.on_event(ProgressEvent.worker_line(current=0, total=1, message=f"Calculating hash {group_id}"))
            
            # since content_hash_calculator is not thread safe, we need to ensure the consistency of the group before calculating the hash
            content_hash_calculator.await_consistency(
                group_id=group_id,
                object_type=page_kind,
                expected_count=expected_tm1_count,
            )
            row_count, content_hash = content_hash_calculator.calculate_group_content_signature(
                group_id=group_id,
                object_type=page_kind,
            )
            progress_sink.on_event(ProgressEvent.worker_line(current=1, total=1))
            if row_count == expected_tm1_count:
                model_store.commit_group_content_signature(
                    group_id,
                    row_count=row_count,
                    content_hash=content_hash,
                )
            else:
                raise ValueError(f"Row count {row_count} does not match TM1 count {expected_tm1_count} for {page_kind}")
            return row_count, content_hash

        def _make_kind_done_callback(
            page_kind: str,
            etag_persister: Callable[[Hierarchy], None],
            group_id: Optional[int],
            sequence: MutableSequence,
            filter_rules_for_sequence: list[str],
            sort_metadata_for_sequence: dict[str, str],
        ) -> Callable[[HierarchyFuture], None]:
            def _on_done(done_hf: HierarchyFuture) -> None:
                pages = done_hf.pages_for(page_kind)
                if any(page.future.exception() is not None for page in pages):
                    return
                etag_persister(done_hf.hierarchy)
                if hasattr(sequence, "set_filter_rules"):
                    sequence.set_filter_rules(filter_rules_for_sequence)
                if hasattr(sequence, "set_sort_metadata"):
                    sequence.set_sort_metadata(sort_metadata_for_sequence)
                if group_id is None:
                    return
                count = done_hf.tm1_object_counts[page_kind]
                _compute_and_commit_hash(group_id, page_kind, count)
                if (
                    page_kind == "edges"
                    and hasattr(sequence, "set_cardinality")
                    and hasattr(sequence, "calculate_edge_cardinality")
                ):
                    sequence.set_cardinality(sequence.calculate_edge_cardinality())

            return _on_done

        def _start_page(fn, tm1_conn, dim_name, hierarchy_name, filter_expr, skip, top, total, mutable_list: MutableSequence) -> None:
            hierarchy_uri = Hierarchy.uri_for(dimension_name=dim_name, hierarchy_name=hierarchy_name)
            progress_sink.on_event(ProgressEvent.worker_line(current=0, total=1, message=f"Fetching {hierarchy_uri} page {int(skip / top)}"))
            page = fn(tm1_conn=tm1_conn, dimension_name=dim_name, hierarchy_name=hierarchy_name, filter=filter_expr, skip=skip, top=top, count=True)
            progress_sink.on_event(ProgressEvent.worker_line(current=1, total=1))
            if hasattr(mutable_list, "extend_payloads"):
                mutable_list.extend_payloads(page.raw_rows, start_index=skip)
            else:
                mutable_list.extend(page.objects)
            progress_sink.on_event(ProgressEvent.total_line(current_delta=len(page.objects)))

        def _count_and_submit_pages(
            hierarchy_future: HierarchyFuture,
            page_kind: str,
            count_fn,
            page_fn,
            dim_name: str,
            hierarchy_name: str,
            filter_expr: Optional[str],
            skip_all: bool,
            can_reuse: bool,
            mutable_list: MutableSequence,
            done_callback: Callable[[HierarchyFuture], None],
        ) -> int:
            hierarchy_uri = Hierarchy.uri_for(dimension_name=dim_name, hierarchy_name=hierarchy_name)
            progress_sink.on_event(ProgressEvent.worker_line(current=0, total=1, message=f"Counting {hierarchy_uri} total objects"))
            count = count_fn(tm1_conn, dim_name, hierarchy_name, filter=filter_expr)
            hierarchy_future.tm1_object_counts[page_kind] = count
            progress_sink.on_event(ProgressEvent.worker_line(current=1, total=1, message=f"Counting {hierarchy_uri} total objects"))
            progress_sink.on_event(ProgressEvent.total_line(total_delta=count))
            if not can_reuse and not skip_all:
                for i in range(0, count, 100_000):
                    future = executor.submit(
                        _start_page,
                        page_fn,
                        tm1_conn,
                        dim_name,
                        hierarchy_name,
                        filter_expr,
                        i,
                        100_000,
                        count,
                        mutable_list,
                        priority=100,
                    )
                    hierarchy_future.add_page(
                        page_kind,
                        skip=i,
                        top=100_000,
                        future=future,
                    )
                done_callback(hierarchy_future)
            else:
                progress_sink.on_event(ProgressEvent.total_line(current_delta=count))
            return count

        for dim_index, dim_name in enumerate(all_dims, start=1):
            try:
                hierarchies_tm1_filter = filter_rules.to_tm1_hierarchy_name_filter(dim_name)
                hierarchy_identities = [] if hierarchies_tm1_filter.skip_all else get_hierarchy_names(tm1_conn, dim_name, filter=hierarchies_tm1_filter.filter_expr)
                sort_metadata_by_hierarchy = get_hierarchy_sort_metadata(
                    tm1_conn,
                    dim_name,
                    [hierarchy_identity.name for hierarchy_identity in hierarchy_identities],
                )
                hierarchy_list: List[Hierarchy] = []

                for idx, hierarchy_identity in enumerate(hierarchy_identities):
                    hierarchy_name = hierarchy_identity.name
                    sort_metadata = sort_metadata_by_hierarchy.get((dim_name, hierarchy_name), {})
                    incoming_hierarchy_etag = hierarchy_identity.etag
                    incoming_cardinality = hierarchy_identity.cardinality
                    elements_tm1_filter = filter_rules.to_tm1_element_name_filter(dim_name, hierarchy_name)
                    subsets_tm1_filter = filter_rules.to_tm1_subset_name_filter(dim_name, hierarchy_name)
                    edges_tm1_filter = filter_rules.to_tm1_edge_name_filter(dim_name, hierarchy_name)
                    incoming_subsets_etag = incoming_hierarchy_etag

                    can_reuse_elements = False
                    can_reuse_subsets = False
                    can_reuse_edges = False
                    if incoming_hierarchy_etag is not None:
                        subsets_id_etag = get_subsets_identity_etag(tm1_conn, dim_name, hierarchy_name, filter=subsets_tm1_filter.filter_expr)
                        incoming_subsets_etag = f"{incoming_hierarchy_etag}:subsets:{subsets_id_etag}"
                        e_elements_etag, e_elements_rules, e_elements_content_hash, _ = model_store.get_group_reuse_metadata(model_id=model_id, dimension_name=dim_name, hierarchy_name=hierarchy_name, object_type="elements")
                        e_subsets_etag, e_subsets_rules, e_subsets_content_hash, _ = model_store.get_group_reuse_metadata(model_id=model_id, dimension_name=dim_name, hierarchy_name=hierarchy_name, object_type="subsets")
                        e_edges_etag, e_edges_rules, e_edges_content_hash, e_edges_cardinality = model_store.get_group_reuse_metadata(model_id=model_id, dimension_name=dim_name, hierarchy_name=hierarchy_name, object_type="edges")
                       
                        total_hierarchy_count = e_elements_content_hash[0] if e_elements_content_hash is not None else 0
                        if e_edges_cardinality is not None:
                            total_hierarchy_count += e_edges_cardinality

                        can_reuse_elements = (
                            e_elements_etag == incoming_hierarchy_etag
                            and e_elements_rules == elements_tm1_filter.applicable_rules
                            and incoming_cardinality == total_hierarchy_count
                            and _can_reuse_content_signature(e_elements_content_hash)
                        )
                        can_reuse_subsets = (
                            e_subsets_etag == incoming_subsets_etag
                            and e_subsets_rules == subsets_tm1_filter.applicable_rules
                            and _can_reuse_content_signature(e_subsets_content_hash)
                        )
                        can_reuse_edges = (
                            e_edges_etag == incoming_hierarchy_etag
                            and e_edges_rules == edges_tm1_filter.applicable_rules
                            and incoming_cardinality == total_hierarchy_count
                            and _can_reuse_content_signature(e_edges_content_hash)
                        )

                    hierarchy = Hierarchy(
                        name=hierarchy_name,
                        dimension_name=dim_name,
                        model_id=model_id,
                        hierarchy_etag=incoming_hierarchy_etag,
                        reuse_existing_store=True,
                        elements_filter_rules=elements_tm1_filter.applicable_rules,
                        edges_filter_rules=edges_tm1_filter.applicable_rules,
                        subsets_filter_rules=subsets_tm1_filter.applicable_rules,
                        elements_sort_type=sort_metadata.get("ElementsSortType"),
                        elements_sort_sense=sort_metadata.get("ElementsSortSense"),
                        components_sort_type=sort_metadata.get("ComponentsSortType"),
                        components_sort_sense=sort_metadata.get("ComponentsSortSense"),
                    )
                    current_sort_metadata = hierarchy_sort_metadata_json(hierarchy)
                    if incoming_hierarchy_etag is not None:
                        if can_reuse_elements and hasattr(hierarchy.elements, "sort_metadata"):
                            can_reuse_elements = hierarchy.elements.sort_metadata() == current_sort_metadata
                        if can_reuse_subsets and hasattr(hierarchy.subsets, "sort_metadata"):
                            can_reuse_subsets = hierarchy.subsets.sort_metadata() == current_sort_metadata
                        if can_reuse_edges and hasattr(hierarchy.edges, "sort_metadata"):
                            can_reuse_edges = hierarchy.edges.sort_metadata() == current_sort_metadata
                    hierarchy_list.append(hierarchy)

                    if not can_reuse_elements and hasattr(hierarchy.elements, "replace_with_payloads"):
                        hierarchy.elements.replace_with_payloads(())
                    if not can_reuse_subsets and hasattr(hierarchy.subsets, "replace_with_payloads"):
                        hierarchy.subsets.replace_with_payloads(())
                    if not can_reuse_edges and hasattr(hierarchy.edges, "replace_with_payloads"):
                        hierarchy.edges.replace_with_payloads(())

                    hierarchy_future = HierarchyFuture(dimension_name=dim_name, hierarchy=hierarchy)
                    hierarchy_futures.append(hierarchy_future)

                    collection_jobs = (
                        (
                            "elements",
                            get_elements_count,
                            _get_elements_page,
                            elements_tm1_filter,
                            can_reuse_elements,
                            hierarchy.elements,
                            lambda h: h.persist_elements_etag(),
                            HierarchyFuture.add_elements_done_callback,
                        ),
                        (
                            "edges",
                            get_edges_count,
                            _get_edges_page,
                            edges_tm1_filter,
                            can_reuse_edges,
                            hierarchy.edges,
                            lambda h: h.persist_edges_etag(),
                            HierarchyFuture.add_edges_done_callback,
                        ),
                        (
                            "subsets",
                            get_subsets_count,
                            _get_subsets_page,
                            subsets_tm1_filter,
                            can_reuse_subsets,
                            hierarchy.subsets,
                            lambda h, _etag=incoming_subsets_etag: h.subsets.set_etag(_etag) if hasattr(h.subsets, "set_etag") else None,
                            HierarchyFuture.add_subset_done_callback,
                        ),
                    )
                    for (
                        page_kind,
                        count_fn,
                        page_fn,
                        tm1_filter,
                        can_reuse,
                        sequence,
                        etag_persister,
                        add_done_callback,
                    ) in collection_jobs:
                        kind_callback = _make_kind_done_callback(
                            page_kind,
                            etag_persister,
                            getattr(sequence, "group_id", None),
                            sequence,
                            tm1_filter.applicable_rules,
                            current_sort_metadata,
                        )
                        count_future = executor.submit(
                            _count_and_submit_pages,
                            hierarchy_future,
                            page_kind,
                            count_fn,
                            page_fn,
                            dim_name,
                            hierarchy_name,
                            tm1_filter.filter_expr,
                            tm1_filter.skip_all,
                            can_reuse,
                            sequence,
                            lambda hf, _cb=kind_callback, _add_done=add_done_callback: _add_done(hf, _cb),
                            priority=0,
                        )
                        count_futures.append(
                            CountFuture(
                                hierarchy_future=hierarchy_future,
                                page_kind=page_kind,
                                future=count_future,
                            )
                        )

                if hierarchy_list:
                    _dimension = Dimension(
                        name=dim_name,
                        hierarchies=hierarchy_list,
                        defaultHierarchy=hierarchy_list[0],
                    )
                else:
                    empty_hierarchy = Hierarchy(name=dim_name, elements=[], edges=[], subsets=[])
                    _dimension = Dimension(
                        name=dim_name,
                        hierarchies=[empty_hierarchy],
                        defaultHierarchy=empty_hierarchy,   
                    )
                _dimensions[dim_name] = _dimension
            except Exception as e:
                _errors.append(
                    workflow_error_from_exception(
                        workflow="export",
                        phase="dimensions",
                        subject=Dimension.uri_for(dim_name),
                        exception=e,
                        severity="recoverable",
                    )
                )

        page_kind_order = {"elements": 0, "edges": 1, "subsets": 2}
        hierarchy_order = {
            id(hierarchy_future): index
            for index, hierarchy_future in enumerate(hierarchy_futures)
        }
        future_errors: list[tuple[tuple[int, int, int, int], WorkflowError]] = []
        failed_hierarchy_ids: set[int] = set()
        successful_count_kinds: set[tuple[int, str]] = set()
        if count_futures:
            wait([count_future.future for count_future in count_futures])
            for count_future in count_futures:
                hierarchy_future = count_future.hierarchy_future
                page_kind = count_future.page_kind
                try:
                    count_future.future.result()
                    successful_count_kinds.add((id(hierarchy_future), page_kind))
                except Exception as e:
                    failed_hierarchy_ids.add(id(hierarchy_future.hierarchy))
                    hierarchy_uri = Hierarchy.uri_for(
                        dimension_name=hierarchy_future.dimension_name,
                        hierarchy_name=hierarchy_future.hierarchy.name,
                    )
                    future_errors.append(
                        (
                            (
                                hierarchy_order[id(hierarchy_future)],
                                page_kind_order[page_kind],
                                0,
                                0,
                            ),
                            workflow_error_from_exception(
                                workflow="export",
                                phase="hierarchy_count",
                                subject=hierarchy_uri,
                                exception=e,
                                severity="recoverable",
                            ),
                        )
                    )

        page_futures = [
            (hierarchy_future, page_kind, page_future)
            for hierarchy_future in hierarchy_futures
            for page_kind, page_future in hierarchy_future.page_futures_with_context()
        ]
        if page_futures:
            wait([page_future.future for _, _, page_future in page_futures])
            for hierarchy_future, page_kind, page_future in page_futures:
                try:
                    page_future.future.result()
                except Exception as e:
                    failed_hierarchy_ids.add(id(hierarchy_future.hierarchy))
                    hierarchy_uri = Hierarchy.uri_for(
                        dimension_name=hierarchy_future.dimension_name,
                        hierarchy_name=hierarchy_future.hierarchy.name,
                    )
                    future_errors.append(
                        (
                            (
                                hierarchy_order[id(hierarchy_future)],
                                page_kind_order[page_kind],
                                1,
                                page_future.skip,
                            ),
                            workflow_error_from_exception(
                                workflow="export",
                                phase="hierarchy_page",
                                subject=(
                                    f"{hierarchy_uri}?$skip={page_future.skip}"
                                    f"&$top={page_future.top}"
                                ),
                                exception=e,
                                severity="recoverable",
                            ),
                        )
                    )

        for hierarchy_future in hierarchy_futures:
            if id(hierarchy_future.hierarchy) in failed_hierarchy_ids:
                continue
            for page_kind in page_kind_order:
                if (id(hierarchy_future), page_kind) not in successful_count_kinds:
                    continue
                try:
                    hierarchy_future.notify_successful_page_kind(page_kind)
                except Exception as e:
                    hierarchy_uri = Hierarchy.uri_for(
                        dimension_name=hierarchy_future.dimension_name,
                        hierarchy_name=hierarchy_future.hierarchy.name,
                    )
                    future_errors.append(
                        (
                            (
                                hierarchy_order[id(hierarchy_future)],
                                page_kind_order[page_kind],
                                2,
                                0,
                            ),
                            workflow_error_from_exception(
                                workflow="export",
                                phase="content_hash",
                                subject=hierarchy_uri,
                                exception=e,
                                severity="recoverable",
                            ),
                        )
                    )

        _errors.extend(
            error for _, error in sorted(future_errors, key=lambda item: item[0])
        )

        if failed_hierarchy_ids:
            for dimension_name, dimension in list(_dimensions.items()):
                valid_hierarchies = [
                    hierarchy
                    for hierarchy in dimension.hierarchies
                    if id(hierarchy) not in failed_hierarchy_ids
                ]
                if not valid_hierarchies:
                    logger.info(
                        "Discarding incomplete dimension '%s' after hierarchy export failure",
                        dimension_name,
                    )
                    del _dimensions[dimension_name]
                    continue
                if len(valid_hierarchies) != len(dimension.hierarchies):
                    dimension.hierarchies = valid_hierarchies
                    dimension.defaultHierarchy = dimension._select_default_hierarchy(
                        dimension_name=dimension.name,
                        hierarchies=valid_hierarchies,
                        default_hierarchy=dimension.defaultHierarchy,
                    )

    logger.info(
        "Dimension export assembly finished total=%d kept=%d errors=%d",
        len(all_dims),
        len(_dimensions),
        len(_errors),
    )
    return _dimensions, _errors


def _parse_rules(rule_text: str) -> List[Rule]:
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


def server_configs_to_model(tm1_conn: TM1Service) -> Dict:
    configs = tm1_conn.configuration.get_active()
    return configs
