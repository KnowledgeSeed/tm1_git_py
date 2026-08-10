import logging
import shutil
import sys
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Optional, Literal
from tqdm import tqdm

from tm1_git_py.services.changeset import Changeset, Change, ChangeType, ObjectType
from tm1_git_py.model import Hierarchy, MDXView, NativeView, Subset, Element, Edge, Rule
from tm1_git_py.model.store_backed_sequence import StoreBackedSequence
from tm1_git_py.model.hierarchy import hierarchy_has_sort_metadata, hierarchy_sort_metadata_json
from tm1_git_py.model.chore import Chore
from tm1_git_py.model.cube import Cube
from tm1_git_py.model.dimension import Dimension
from tm1_git_py.model.model import Model
from tm1_git_py.db.model_store import ModelStore
from tm1_git_py.model.process import Process
from tm1_git_py.services.filter import FilterRules, apply_default_filter_rules
from tm1_git_py.reporting.progress_reporting import (
    NoopProgressSink,
    ProgressEvent,
    ProgressKind,
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


@dataclass
class _CompareObjectListsResult:
    matched_pairs: dict[str, tuple[Any, Any]]
    added_items: list[Any]
    removed_items: list[Any]


class ComparisonResult(tuple):
    """Tuple result with a temporary Changeset-compatible attribute bridge.

    The public contract is ``(changeset, errors)``.  Attribute delegation keeps
    existing integrations functional during their explicit result migration.
    """

    def __new__(cls, changeset: Changeset, errors: list[WorkflowError]):
        return super().__new__(cls, (changeset, errors))

    @property
    def changeset(self) -> Changeset:
        return self[0]

    @property
    def errors(self) -> list[WorkflowError]:
        return self[1]

    def __getattr__(self, name: str) -> Any:
        return getattr(self.changeset, name)


class TqdmComparatorProgressSink:
    _SLOT_HEIGHT = 5
    _slot_lock = threading.Lock()
    _active_slots: set[int] = set()

    def __init__(
        self,
        *,
        enable_fallback_logs: bool = True,
        preferred_slot_index: Optional[int] = None,
    ):
        self._enable_fallback_logs = enable_fallback_logs
        self._slot_index = self._acquire_slot(preferred_slot_index)
        self._base_position = self._slot_index * self._SLOT_HEIGHT
        self._lock = threading.Lock()
        self._overall_bar = None
        self._collection_bar = None
        self._last_overall_desc = ""
        self._last_collection_desc = ""
        if sys.stderr.isatty() and tqdm is not None:
            terminal_width = shutil.get_terminal_size(fallback=(120, 24)).columns
            self._overall_bar = tqdm(
                total=1,
                unit="object",
                unit_scale=False,
                desc="Comparing..",
                leave=True,
                dynamic_ncols=True,
                ncols=terminal_width,
                position=self._base_position,
            )
            self._collection_bar = tqdm(
                total=1,
                unit="object",
                unit_scale=False,
                desc="Collection",
                leave=True,
                dynamic_ncols=True,
                ncols=terminal_width,
                position=self._base_position + 1,
            )

    @classmethod
    def _acquire_slot(cls, preferred_slot: Optional[int] = None) -> int:
        with cls._slot_lock:
            if preferred_slot is not None:
                preferred = max(0, int(preferred_slot))
                if preferred not in cls._active_slots:
                    cls._active_slots.add(preferred)
                    return preferred
            slot = 0
            while slot in cls._active_slots:
                slot += 1
            cls._active_slots.add(slot)
            return slot

    @classmethod
    def _release_slot(cls, slot_index: int) -> None:
        with cls._slot_lock:
            cls._active_slots.discard(slot_index)

    def on_event(self, event: ProgressEvent) -> None:
        with self._lock:
            if self._overall_bar is None or self._collection_bar is None:
                return
            if event.scope == ProgressScope.TOTAL:
                bar = self._overall_bar
                fallback_desc = "Comparing.."
            elif event.scope == ProgressScope.WORKER:
                bar = self._collection_bar
                fallback_desc = "Collection"
            else:
                return
            bar.unit = event.unit.value
            bar.unit_scale = event.unit == ProgressUnit.BYTE
            target_total = int(event.total) if event.total is not None else int(bar.total or 1)
            target_total = max(1, target_total)
            if int(bar.total or 0) != target_total:
                bar.reset(total=target_total)
            if event.current is not None:
                bar.n = min(max(0, int(event.current)), target_total)
            else:
                bar.n = min(max(0, int(bar.n) + int(event.current_delta or 0)), target_total)
            desc = event.message or fallback_desc
            if event.path:
                desc = f"{desc}: {event.path}"
            if bar is self._overall_bar:
                if desc != self._last_overall_desc:
                    bar.set_description_str(desc, refresh=False)
                    self._last_overall_desc = desc
            else:
                if desc != self._last_collection_desc:
                    bar.set_description_str(desc, refresh=False)
                    self._last_collection_desc = desc
            bar.refresh()

    def close(self) -> None:
        with self._lock:
            if self._collection_bar is not None:
                self._collection_bar.close()
                self._collection_bar = None
            if self._overall_bar is not None:
                self._overall_bar.close()
                self._overall_bar = None
        self._release_slot(self._slot_index)


def _dimensions_equal_shallow(old_dimension: Dimension, new_dimension: Dimension) -> bool:
    if old_dimension.name != new_dimension.name:
        return False

    old_default = getattr(old_dimension.defaultHierarchy, "name", None)
    new_default = getattr(new_dimension.defaultHierarchy, "name", None)
    if old_default != new_default:
        return False

    return True


def _hierarchies_equal_shallow(old_hierarchy: Hierarchy, new_hierarchy: Hierarchy) -> bool:
    if old_hierarchy.name != new_hierarchy.name:
        return False
    if hierarchy_sort_metadata_json(old_hierarchy) != hierarchy_sort_metadata_json(new_hierarchy):
        return False
    if hierarchy_has_sort_metadata(old_hierarchy) or hierarchy_has_sort_metadata(new_hierarchy):
        if _hierarchy_element_order_signature(old_hierarchy) != _hierarchy_element_order_signature(new_hierarchy):
            return False
        if _hierarchy_edge_order_signature(old_hierarchy) != _hierarchy_edge_order_signature(new_hierarchy):
            return False
    return True


def _hierarchy_element_order_signature(hierarchy: Hierarchy) -> dict[str, Optional[int]]:
    return {
        str(name): index
        for name, index in _iter_hierarchy_order_signature(
            getattr(hierarchy, "elements", []),
            object_type=Element,
        )
    }


def _hierarchy_edge_order_signature(hierarchy: Hierarchy) -> dict[tuple[str, str], Optional[int]]:
    return {
        (str(parent), str(component)): index
        for (parent, component), index in _iter_hierarchy_order_signature(
            getattr(hierarchy, "edges", []),
            object_type=Edge,
        )
    }


def _iter_hierarchy_order_signature(
    collection: Iterable[Any],
    *,
    object_type: type,
) -> Iterable[tuple[Any, Optional[int]]]:
    if isinstance(collection, StoreBackedSequence):
        for payload in collection.iter_payloads(
            ordered_by_identity=True,
            include_internal_indexes=True,
        ):
            if object_type is Element:
                yield payload.get("Name"), _optional_int(payload.get("ElementIndex"))
            elif object_type is Edge:
                yield (
                    payload.get("ParentName"),
                    payload.get("ComponentName"),
                ), _optional_int(payload.get("ComponentIndex"))
        return

    for item in collection:
        if object_type is Element and isinstance(item, Element):
            yield getattr(item, "name", ""), _optional_int(getattr(item, "element_index", None))
        elif object_type is Edge and isinstance(item, Edge):
            yield (
                getattr(item, "parent", ""),
                getattr(item, "component_name", ""),
            ), _optional_int(getattr(item, "component_index", None))


def _optional_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    return int(value)


def _cubes_equal_shallow(old_cube: Cube, new_cube: Cube) -> bool:
    if old_cube.name != new_cube.name:
        return False

    old_dim_names = {str(getattr(dim, "name", dim)) for dim in old_cube.dimensions}
    new_dim_names = {str(getattr(dim, "name", dim)) for dim in new_cube.dimensions}
    if old_dim_names != new_dim_names:
        return False

    return True


def _uri_from_object(obj: Any, context: Optional[dict[str, str]] = None) -> str:
    context = context or {}
    try:
        if isinstance(obj, Rule):
            cube_name = context.get("cube_name")
            if cube_name and context.get("rule_collection") == "drillthrough_rules":
                return f"Cubes('{cube_name}')/DrillthroughRules('default')"
            return obj.uri(cube_name) if cube_name else ""
        if isinstance(obj, (MDXView, NativeView)):
            cube_name = context.get("cube_name")
            return obj.uri(cube_name) if cube_name else ""
        if isinstance(obj, Hierarchy):
            dimension_name = context.get("dimension_name")
            return obj.uri(dimension_name) if dimension_name else ""
        if isinstance(obj, (Subset, Element, Edge)):
            dimension_name = context.get("dimension_name")
            hierarchy_name = context.get("hierarchy_name")
            return obj.uri(dimension_name, hierarchy_name) if dimension_name and hierarchy_name else ""
        return obj.uri()
    except Exception:
        return ""


def _resolve_change_uri(obj: Any, context: Optional[dict[str, str]] = None) -> str:
    object_uri = _uri_from_object(obj, context=context)
    return object_uri or ""


def _object_identity(obj: Any, context: Optional[dict[str, str]] = None) -> str:
    obj_type = obj.__class__.__name__
    if isinstance(obj, Rule):
        object_uri = _uri_from_object(obj, context=context)
        area = getattr(obj, "area", "")
        if object_uri:
            return f"{obj_type}:{object_uri}|{area}"
        return f"{obj_type}:{getattr(obj, 'name', '')}:{area}"

    if isinstance(obj, Element):
        object_uri = _uri_from_object(obj, context=context)
        if object_uri:
            return f"{obj_type}:{object_uri}"
        return f"{obj_type}:{getattr(obj, 'name', '')}"

    object_uri = _uri_from_object(obj, context=context)
    if object_uri:
        return f"{obj_type}:{object_uri}"

    if isinstance(obj, Edge):
        return f"{obj_type}:{getattr(obj, 'parent', '')}:{getattr(obj, 'name', '')}"

    name = getattr(obj, "name", None)
    if name is not None:
        return f"{obj_type}:{name}"

    uri_fn = getattr(obj, "uri", None)
    if callable(uri_fn):
        try:
            uri = uri_fn()
        except Exception:
            uri = None
        if uri:
            return f"{obj_type}:{uri}"
    raise AttributeError(f"Object '{obj}' has neither uri nor name.")


class Comparator:
    DISK_BACKED_PROGRESS_EVERY: int = 100_000
    COMPARE_CHANGE_BATCH_SIZE: int = 100_000
    LOCAL_SORT_CHUNK_SIZE: int = 100_000

    _CHILD_RELATIONS: Mapping[type, list[tuple[str, type]]] = {
        Dimension: [("hierarchies", Hierarchy)],
        Hierarchy: [("subsets", Subset), ("elements", Element), ("edges", Edge)],
        Cube: [("views", MDXView), ("views", NativeView), ("rules", Rule), ("drillthrough_rules", Rule)],
    }

    _EQUALITY_OVERRIDES: Mapping[type, Callable[[Any, Any], bool]] = {
        Dimension: _dimensions_equal_shallow,
        Hierarchy: _hierarchies_equal_shallow,
        Cube: _cubes_equal_shallow
    }

    def __init__(self) -> None:
        self._progress_sink: Optional[ProgressSink] = None
        self._active_filter_rules: Optional[FilterRules] = None
        self._compare_progress_total = 0
        self._compare_progress_current = 0
        self._collection_progress_total = 0
        self._collection_progress_current = 0
        self._collection_progress_label = ""
        self._comparison_errors: list[WorkflowError] = []

    def _record_comparison_error(
            self,
            *,
            phase: str,
            subject: str,
            exception: Exception,
            severity: Literal["recoverable", "fatal"],
    ) -> None:
        """Collect a coordinator-side comparison diagnostic.

        Comparator work is currently performed in the owning process.  Keeping
        records on the comparator until ``compare`` completes means the public
        callback has one owner and can never be invoked from an implementation
        detail such as a child collection traversal.
        """

        error = workflow_error_from_exception(
            workflow="compare",
            phase=phase,
            subject=subject or None,
            exception=exception,
            severity=severity,
        )
        # ``Cube.views`` is traversed once for each supported view class.  If
        # the shared relation itself cannot be read, it is one failed relation,
        # not two independent diagnostics.
        if phase == "child_relation" and error in self._comparison_errors:
            return
        self._comparison_errors.append(error)

    @staticmethod
    def _error_sort_key(error: WorkflowError) -> tuple[str, str, str, str, str]:
        """Use a stable coordinator order for returned records and callbacks."""
        return (
            error.phase,
            error.subject or "",
            error.exception_type,
            error.message,
            error.severity,
        )

    @staticmethod
    def _log_comparison_errors(errors: Iterable[WorkflowError]) -> None:
        """Emit each collected diagnostic once, from the comparison coordinator."""
        for error in errors:
            log = logger.warning if error.severity == "recoverable" else logger.error
            log(
                "Comparison diagnostic workflow=%s phase=%s subject=%s type=%s message=%s",
                error.workflow,
                error.phase,
                error.subject or "<none>",
                error.exception_type,
                error.message,
            )

    @staticmethod
    def _comparison_subject(obj: Any, context: Optional[dict[str, str]] = None) -> str:
        return _resolve_change_uri(obj, context=context) or getattr(obj, "name", "") or type(obj).__name__

    def _is_uri_in_scope(self, object_uri: str) -> bool:
        if not object_uri:
            return True
        if self._active_filter_rules is None:
            return True
        return not self._active_filter_rules.should_exclude(object_uri)

    def _is_object_in_scope(self, obj: Any, context: Optional[dict[str, str]] = None) -> bool:
        object_uri = _resolve_change_uri(obj, context=context)
        return self._is_uri_in_scope(object_uri)

    def _emit_progress_event(
        self,
        *,
        kind: ProgressKind,
        scope: ProgressScope,
        current: Optional[int] = None,
        current_delta: Optional[int] = None,
        total: Optional[int] = None,
        unit: ProgressUnit = ProgressUnit.LINE,
        message: Optional[str] = None,
        path: Optional[str] = None,
    ) -> None:
        assert self._progress_sink is not None
        self._progress_sink.on_event(
            ProgressEvent.make(
                kind=kind,
                scope=scope,
                unit=unit,
                current=current,
                current_delta=current_delta,
                total=total,
                message=message,
                path=path,
            )
        )

    def _begin_compare_progress(self, total_units: int) -> None:
        self._compare_progress_total = max(1, int(total_units))
        self._compare_progress_current = 0
        self._emit_progress_event(
            kind=ProgressKind.START,
            scope=ProgressScope.TOTAL,
            current=0,
            total=self._compare_progress_total,
            unit=ProgressUnit.LINE,
            message="comparing models",
        )

    def _advance_compare_progress(self, delta: int, *, message: Optional[str] = None) -> None:
        if delta <= 0:
            return
        self._compare_progress_current = min(
            self._compare_progress_total,
            self._compare_progress_current + int(delta),
        )
        self._emit_progress_event(
            kind=ProgressKind.UPDATE,
            scope=ProgressScope.TOTAL,
            current=self._compare_progress_current,
            total=self._compare_progress_total,
            unit=ProgressUnit.LINE,
            message=message or "comparing models",
        )

    def _begin_collection_progress(self, *, label: str, total_units: int) -> None:
        self._collection_progress_label = str(label)
        self._collection_progress_total = max(1, int(total_units))
        self._collection_progress_current = 0
        self._emit_progress_event(
            kind=ProgressKind.START,
            scope=ProgressScope.WORKER,
            current=0,
            total=self._collection_progress_total,
            unit=ProgressUnit.LINE,
            message="comparing collection",
            path=self._collection_progress_label,
        )

    def _advance_collection_progress(self, delta: int, *, message: Optional[str] = None) -> None:
        if delta <= 0:
            return
        self._collection_progress_current = min(
            self._collection_progress_total,
            self._collection_progress_current + int(delta),
        )
        self._emit_progress_event(
            kind=ProgressKind.UPDATE,
            scope=ProgressScope.WORKER,
            current=self._collection_progress_current,
            total=self._collection_progress_total,
            unit=ProgressUnit.LINE,
            message=message or "comparing collection",
            path=self._collection_progress_label,
        )

    @staticmethod
    def _resolved_model_object_count(model: Model) -> int:
        cached = getattr(model, "total_object_count", None)
        if cached is not None:
            return max(0, int(cached))
        return Model.recalculate_total_object_count(model)

    def compare(
            self,
            model1: Model,
            model2: Model,
            progress_sink: Optional[ProgressSink] = None,
            mode: Literal['full', 'add_only'] = 'full',
            filter_rules: Optional[FilterRules] = None,
            error_callback: Optional[ErrorCallback] = None,
    ) -> tuple[Changeset, list[WorkflowError]]:
        """
        Compare two models and build a Changeset of Change entries.
        mode='full' emits add/remove/modify changes.
        mode='add_only' emits add/modify changes.
        """

        changeset = Changeset()
        errors: list[WorkflowError] = []
        self._comparison_errors = errors
        self._progress_sink = progress_sink if progress_sink is not None else NoopProgressSink()
        self._compare_progress_total = 0
        self._compare_progress_current = 0
        try:
            logger.info("Starting model compare mode=%s", mode)
            try:
                self._active_filter_rules = apply_default_filter_rules(filter_rules)
                phase_rows = [
                    ("Cube", model1.cubes, model2.cubes, Cube),
                    ("Dimension", model1.dimensions, model2.dimensions, Dimension),
                    ("Process", model1.processes, model2.processes, Process),
                    ("Chore", model1.chores, model2.chores, Chore),
                ]
                overall_object_total = (
                    self._resolved_model_object_count(model1)
                    + self._resolved_model_object_count(model2)
                )
                self._begin_compare_progress(overall_object_total)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                self._record_comparison_error(
                    phase="setup",
                    subject="models",
                    exception=exc,
                    severity="fatal",
                )
                return ComparisonResult(changeset, errors)

            for object_type_name, old_rows, new_rows, parent_cls in phase_rows:
                phase_error_count = len(errors)
                phase_change_count = len(changeset.changes)
                logger.info("Starting compare phase=%s", object_type_name)
                try:
                    self._compare_with_children(old_rows, new_rows, parent_cls, changeset, mode)
                except (KeyboardInterrupt, SystemExit):
                    raise
                except Exception as exc:
                    self._record_comparison_error(
                        phase="object_phase",
                        subject=object_type_name,
                        exception=exc,
                        severity="fatal",
                    )
                finally:
                    logger.info(
                        "Completed compare phase=%s changes=%d errors=%d",
                        object_type_name,
                        len(changeset.changes) - phase_change_count,
                        len(errors) - phase_error_count,
                    )

            # cube_rule_texts = {cube.name: cube.get_rule_text() for cube in model2.cubes}
            # changeset.unify_rule_changes(cube_rule_texts=cube_rule_texts)
            # summary = {"add": 0, "remove": 0, "modify": 0}
            # for change in changeset.changes:
            #     key = change.change_type.value if hasattr(change.change_type, "value") else str(change.change_type)
            #     summary[key] = summary.get(key, 0) + 1
            # logger.info(
            #     "Completed model compare mode=%s total=%d add=%d remove=%d modify=%d",
            #     mode,
            #     len(changeset.changes),
            #     summary.get("add", 0),
            #     summary.get("remove", 0),
            #     summary.get("modify", 0),
            # )
            try:
                self._emit_progress_event(
                    kind=ProgressKind.UPDATE,
                    scope=ProgressScope.TOTAL,
                    unit=ProgressUnit.LINE,
                    current=self._compare_progress_total,
                    total=self._compare_progress_total,
                    message="compare complete",
                )
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                self._record_comparison_error(
                    phase="finalization",
                    subject="changeset",
                    exception=exc,
                    severity="fatal",
                )
            return ComparisonResult(changeset, errors)
        finally:
            errors.sort(key=self._error_sort_key)
            self._log_comparison_errors(errors)
            logger.info(
                "Completed model compare mode=%s changes=%d errors=%d recoverable=%d fatal=%d",
                mode,
                len(changeset.changes),
                len(errors),
                sum(error.severity == "recoverable" for error in errors),
                sum(error.severity == "fatal" for error in errors),
            )
            # A callback belongs to this coordinator, not to an object or
            # relation traversal.  It sees the exact collected result once.
            if error_callback is not None:
                collect_worker_errors([], errors, error_callback=error_callback)
            self._progress_sink = None
            self._active_filter_rules = None
            self._compare_progress_total = 0
            self._compare_progress_current = 0
            self._collection_progress_total = 0
            self._collection_progress_current = 0
            self._collection_progress_label = ""
            self._comparison_errors = []

    def _enqueue_batched_change(
            self,
            changeset: Changeset,
            pending: list[Change],
            *,
            change_type: ChangeType,
            obj: Any,
            uri: str = "",
    ) -> None:
        """Buffer one change; flush to ChangesetStore in bulk when batch is full."""
        pending.append(
            Change(
                change_type=change_type,
                object_type=ObjectType.from_object(obj),
                uri=uri,
                body=obj,
            )
        )
        limit = max(1, int(self.COMPARE_CHANGE_BATCH_SIZE))
        if len(pending) >= limit:
            changeset._append_changes(pending)
            pending.clear()

    @staticmethod
    def _flush_batched_changes(changeset: Changeset, pending: list[Change]) -> None:
        if pending:
            changeset._append_changes(pending)
            pending.clear()

    @staticmethod
    def _coerce_empty_collection_to_store_backed(
            collection: Iterable[Any],
            peer: Iterable[Any],
            *,
            child_cls: type,
    ) -> Iterable[Any]:
        """Convert ``[]`` to an empty StoreBackedSequence when the peer is disk-backed."""
        if not isinstance(collection, list) or len(collection) != 0:
            return collection
        if not isinstance(peer, StoreBackedSequence):
            return collection
        if child_cls not in (Element, Edge, Subset):
            return collection

        store = peer._active_store()
        model_id = getattr(peer, "_model_id", None)
        dimension_name = getattr(peer, "_dimension_name", "")
        hierarchy_name = f"{getattr(peer, '_hierarchy_name', 'Hierarchy')}__cmp_empty_{uuid.uuid4().hex}"

        if child_cls is Element:
            return StoreBackedSequence.for_elements_sink(
                store=store,
                model_id=model_id,
                dimension_name=dimension_name,
                hierarchy_name=hierarchy_name,
            )
        if child_cls is Edge:
            return StoreBackedSequence.for_edges_sink(
                store=store,
                model_id=model_id,
                dimension_name=dimension_name,
                hierarchy_name=hierarchy_name,
            )
        return StoreBackedSequence.for_subsets_sink(
            store=store,
            model_id=model_id,
            dimension_name=dimension_name,
            hierarchy_name=hierarchy_name,
        )
    def _compare_with_children(
            self,
            old_list: Iterable[Any],
            new_list: Iterable[Any],
            parent_cls: type,
            changeset: Changeset,
            mode: Literal['full', 'add_only'],
            context: Optional[dict[str, str]] = None,
    ) -> _CompareObjectListsResult:

        old_list = self._coerce_empty_collection_to_store_backed(old_list, new_list, child_cls=parent_cls)
        new_list = self._coerce_empty_collection_to_store_backed(new_list, old_list, child_cls=parent_cls)

        equals_fn = self._EQUALITY_OVERRIDES.get(parent_cls)
        object_type_name = getattr(parent_cls, "__name__", str(parent_cls))

        def _ensure_elements_of_type(slot: Iterable[Any], child_cls: type) -> Iterable[Any]:
            new_children = [
                child for child in slot
                if isinstance(child, child_cls)
            ]
            return new_children

        compare_result = self._compare_object_lists(
            old_list,
            new_list,
            changeset,
            object_type_name=object_type_name,
            mode=mode,
            equals_fn=equals_fn,
            context=context,
            parent_cls=parent_cls,
        )

        child_relations = self._CHILD_RELATIONS.get(parent_cls, [])
        parent_pairs = compare_result.matched_pairs
        if child_relations and parent_pairs:
            for old_obj, new_obj in parent_pairs.values():
                for child_attr, child_cls in child_relations:
                    try:
                        slot_old = getattr(old_obj, child_attr, None) or []
                        slot_new = getattr(new_obj, child_attr, None) or []
                        if isinstance(slot_old, StoreBackedSequence) and isinstance(slot_new, StoreBackedSequence):
                            old_children = slot_old
                            new_children = slot_new
                        else:
                            old_children = _ensure_elements_of_type(slot_old, child_cls)
                            new_children = _ensure_elements_of_type(slot_new, child_cls)
                        child_context = dict(context or {})
                        if parent_cls is Cube:
                            child_context["cube_name"] = getattr(new_obj, "name", "")
                            if child_cls is Rule:
                                child_context["rule_collection"] = child_attr
                        if parent_cls is Dimension:
                            child_context["dimension_name"] = getattr(new_obj, "name", "")
                        if parent_cls is Hierarchy:
                            child_context["hierarchy_name"] = getattr(new_obj, "name", "")
                        self._compare_with_children(old_children, new_children, child_cls, changeset, mode, context=child_context)
                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except Exception as exc:
                        self._record_comparison_error(
                            phase="child_relation",
                            subject=self._comparison_subject(new_obj, context),
                            exception=exc,
                            severity="recoverable",
                        )

        if child_relations and compare_result.added_items:
            for new_obj in compare_result.added_items:
                for child_attr, child_cls in child_relations:
                    try:
                        slot_new = getattr(new_obj, child_attr, None) or []
                        if isinstance(slot_new, StoreBackedSequence):
                            new_children = slot_new
                        else:
                            new_children = _ensure_elements_of_type(slot_new, child_cls)
                        child_context = dict(context or {})
                        if parent_cls is Cube:
                            child_context["cube_name"] = getattr(new_obj, "name", "")
                            if child_cls is Rule:
                                child_context["rule_collection"] = child_attr
                        if parent_cls is Dimension:
                            child_context["dimension_name"] = getattr(new_obj, "name", "")
                        if parent_cls is Hierarchy:
                            child_context["hierarchy_name"] = getattr(new_obj, "name", "")
                        self._compare_with_children([], new_children, child_cls, changeset, mode, context=child_context)
                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except Exception as exc:
                        self._record_comparison_error(
                            phase="child_relation",
                            subject=self._comparison_subject(new_obj, context),
                            exception=exc,
                            severity="recoverable",
                        )

        if mode == "full" and child_relations and compare_result.removed_items:
            for old_obj in compare_result.removed_items:
                for child_attr, child_cls in child_relations:
                    try:
                        slot_old = getattr(old_obj, child_attr, None) or []
                        if isinstance(slot_old, StoreBackedSequence):
                            old_children = slot_old
                        else:
                            old_children = _ensure_elements_of_type(slot_old, child_cls)
                        child_context = dict(context or {})
                        if parent_cls is Cube:
                            child_context["cube_name"] = getattr(old_obj, "name", "")
                            if child_cls is Rule:
                                child_context["rule_collection"] = child_attr
                        if parent_cls is Dimension:
                            child_context["dimension_name"] = getattr(old_obj, "name", "")
                        if parent_cls is Hierarchy:
                            child_context["hierarchy_name"] = getattr(old_obj, "name", "")
                        self._compare_with_children(slot_old, [], child_cls, changeset, mode, context=child_context)
                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except Exception as exc:
                        self._record_comparison_error(
                            phase="child_relation",
                            subject=self._comparison_subject(old_obj, context),
                            exception=exc,
                            severity="recoverable",
                        )
        return compare_result

    def _compare_disk_backed_sorted_merge(
            self,
            old_db: Any,
            new_db: Any,
            changeset: Changeset,
            *,
            object_type_name: str,
            mode: Literal['full', 'add_only'],
            equals_fn: Optional[Callable[[Any, Any], bool]],
            context: Optional[dict[str, str]],
    ) -> _CompareObjectListsResult:
        """
        Compare two on-disk JSONL collections in merge order.

        Callers must ensure both sides iterate in ascending order by
        ``_object_identity(..., context)`` (same order as TM1/export queries).
        """
        old_total = len(old_db)
        new_total = len(new_db)
        old_signature = old_db.content_signature()
        new_signature = new_db.content_signature()

        if (
            old_signature
            and new_signature
            and old_signature == new_signature
            and (old_signature[0] == 0 or old_signature[1] != ModelStore.EMPTY_CONTENT_HASH)
        ):
            skipped_total = max(1, old_total + new_total)
            self._begin_collection_progress(label=object_type_name, total_units=skipped_total)
            self._advance_collection_progress(skipped_total, message=f"skipping {object_type_name}")
            self._advance_compare_progress(skipped_total, message=f"skipping {object_type_name}")
            logger.info(
                "Skipping %s streaming compare: count+hash match count=%d hash_algo=%s",
                object_type_name,
                old_signature[0],
                getattr(old_db, "HASH_ALGO", "sha256-tree-v1"),
            )
            return _CompareObjectListsResult(matched_pairs={}, added_items=[], removed_items=[])

        old_it = (old_db.item_from_payload(payload) for payload in old_db.iter_payloads(ordered_by_identity=True))
        new_it = (new_db.item_from_payload(payload) for payload in new_db.iter_payloads(ordered_by_identity=True))
        old_item: Any = next(old_it, None)
        new_item: Any = next(new_it, None)
        added_c = removed_c = common_c = 0
        old_seen = 0
        new_seen = 0
        progress_every = max(1, self.DISK_BACKED_PROGRESS_EVERY)
        next_log_at = progress_every
        reported_processed = 0

        logger.info(
            "Starting compare phase=%s streaming old_size=%d new_size=%d",
            object_type_name,
            old_total,
            new_total,
        )
        stream_total = max(1, old_total + new_total)
        self._begin_collection_progress(label=object_type_name, total_units=stream_total)
        pending_changes: list[Change] = []

        def _enqueue_streaming_change(change_type: ChangeType, obj: Any) -> bool:
            """Keep an invalid streamed item from aborting unrelated items."""
            try:
                self._enqueue_batched_change(
                    changeset,
                    pending_changes,
                    change_type=change_type,
                    obj=obj,
                    uri=_resolve_change_uri(obj, context),
                )
                return True
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                self._record_comparison_error(
                    phase="object_change",
                    subject=self._comparison_subject(obj, context),
                    exception=exc,
                    severity="recoverable",
                )
                return False

        def _is_streaming_object_in_scope(obj: Any) -> bool:
            try:
                return self._is_object_in_scope(obj, context=context)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                self._record_comparison_error(
                    phase="object_filter",
                    subject=self._comparison_subject(obj, context),
                    exception=exc,
                    severity="recoverable",
                )
                return False

        def _log_progress(force: bool = False) -> None:
            nonlocal next_log_at, reported_processed
            current = old_seen + new_seen
            should_log = force or current >= next_log_at
            if not should_log:
                return
            delta = max(0, current - reported_processed)
            if delta > 0:
                self._advance_collection_progress(delta, message=f"streaming {object_type_name}")
                self._advance_compare_progress(delta, message=f"streaming {object_type_name}")
                reported_processed = current
            while current >= next_log_at:
                next_log_at += progress_every

        while old_item is not None or new_item is not None:
            if old_item is None:
                if _is_streaming_object_in_scope(new_item):
                    added_c += _enqueue_streaming_change(ChangeType.ADD, new_item)
                new_item = next(new_it, None)
                new_seen += 1
                _log_progress()
                continue
            if new_item is None:
                if mode == 'full' and _is_streaming_object_in_scope(old_item):
                    removed_c += _enqueue_streaming_change(ChangeType.REMOVE, old_item)
                old_item = next(old_it, None)
                old_seen += 1
                _log_progress()
                continue

            try:
                key_old = _object_identity(old_item, context=context)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                self._record_comparison_error(
                    phase="object_identity",
                    subject=self._comparison_subject(old_item, context),
                    exception=exc,
                    severity="recoverable",
                )
                old_item = next(old_it, None)
                old_seen += 1
                _log_progress()
                continue
            try:
                key_new = _object_identity(new_item, context=context)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                self._record_comparison_error(
                    phase="object_identity",
                    subject=self._comparison_subject(new_item, context),
                    exception=exc,
                    severity="recoverable",
                )
                new_item = next(new_it, None)
                new_seen += 1
                _log_progress()
                continue

            if key_old < key_new:
                if mode == 'full' and _is_streaming_object_in_scope(old_item):
                    removed_c += _enqueue_streaming_change(ChangeType.REMOVE, old_item)
                old_item = next(old_it, None)
                old_seen += 1
                _log_progress()
            elif key_old > key_new:
                if _is_streaming_object_in_scope(new_item):
                    added_c += _enqueue_streaming_change(ChangeType.ADD, new_item)
                new_item = next(new_it, None)
                new_seen += 1
                _log_progress()
            else:
                common_c += 1
                try:
                    if self._is_object_in_scope(new_item, context=context):
                        objects_equal = equals_fn(old_item, new_item) if equals_fn else old_item == new_item
                        if not objects_equal:
                            _enqueue_streaming_change(ChangeType.MODIFY, new_item)
                except (KeyboardInterrupt, SystemExit):
                    raise
                except Exception as exc:
                    self._record_comparison_error(
                        phase="object_comparison",
                        subject=self._comparison_subject(new_item, context),
                        exception=exc,
                        severity="recoverable",
                    )
                old_item = next(old_it, None)
                new_item = next(new_it, None)
                old_seen += 1
                new_seen += 1
                _log_progress()

        self._flush_batched_changes(changeset, pending_changes)
        _log_progress(force=True)
        logger.info(
            "Completed compare phase=%s streaming added=%d removed=%d common=%d",
            object_type_name,
            added_c,
            removed_c,
            common_c,
        )
        return _CompareObjectListsResult(matched_pairs={}, added_items=[], removed_items=[])

    def _compare_object_lists(self,
                              old_list: Iterable[Any],
                              new_list: Iterable[Any],
                              changeset: Changeset,
                              object_type_name: str,
                              mode: Literal['full', 'add_only'],
                              equals_fn: Optional[Callable[[Any, Any], bool]] = None,
                              context: Optional[dict[str, str]] = None,
                              parent_cls: Optional[type] = None) -> _CompareObjectListsResult:

        if (
            parent_cls in (Element, Edge)
            and isinstance(old_list, StoreBackedSequence)
            and isinstance(new_list, StoreBackedSequence)
        ):
            return self._compare_disk_backed_sorted_merge(
                old_list,
                new_list,
                changeset,
                object_type_name=object_type_name,
                mode=mode,
                equals_fn=equals_fn,
                context=context,
            )

        try:
            old_list_m = list(old_list)
            new_list_m = list(new_list)
            local_total_units = max(1, len(old_list_m) + len(new_list_m))
            self._begin_collection_progress(label=object_type_name, total_units=local_total_units)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            # Materialising a collection is a phase-level operation.  The
            # caller records it as fatal because no safe item boundary exists.
            raise

        def _build_object_map(items: Iterable[Any]) -> dict[str, Any]:
            object_map: dict[str, Any] = {}
            for item in items:
                try:
                    if not self._is_object_in_scope(item, context=context):
                        continue
                    object_map[_object_identity(item, context=context)] = item
                except (KeyboardInterrupt, SystemExit):
                    raise
                except Exception as exc:
                    self._record_comparison_error(
                        phase="object_identity",
                        subject=self._comparison_subject(item, context),
                        exception=exc,
                        severity="recoverable",
                    )
            return object_map

        old_map = _build_object_map(old_list_m)
        new_map = _build_object_map(new_list_m)

        new_names = set(new_map.keys())
        old_names = set(old_map.keys())

        added_names = new_names - old_names
        removed_names = old_names - new_names
        common_names = new_names & old_names
        pending_changes: list[Change] = []

        added_items: list[Any] = []
        for name in added_names:
            new_obj = new_map[name]
            try:
                self._enqueue_batched_change(
                    changeset,
                    pending_changes,
                    change_type=ChangeType.ADD,
                    obj=new_obj,
                    uri=_resolve_change_uri(new_obj, context),
                )
                added_items.append(new_obj)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                self._record_comparison_error(
                    phase="object_change",
                    subject=self._comparison_subject(new_obj, context),
                    exception=exc,
                    severity="recoverable",
                )

        removed_items: list[Any] = []
        if mode == 'full':
            for name in removed_names:
                old_obj = old_map[name]
                try:
                    self._enqueue_batched_change(
                        changeset,
                        pending_changes,
                        change_type=ChangeType.REMOVE,
                        obj=old_obj,
                        uri=_resolve_change_uri(old_obj, context),
                    )
                    removed_items.append(old_obj)
                except (KeyboardInterrupt, SystemExit):
                    raise
                except Exception as exc:
                    self._record_comparison_error(
                        phase="object_change",
                        subject=self._comparison_subject(old_obj, context),
                        exception=exc,
                        severity="recoverable",
                    )

        matched_pairs: dict[str, tuple[Any, Any]] = {}
        for name in common_names:
            try:
                old_obj = old_map[name]
                new_obj = new_map[name]
                objects_equal = equals_fn(old_obj, new_obj) if equals_fn else old_obj == new_obj
                if not objects_equal:
                    self._enqueue_batched_change(
                        changeset,
                        pending_changes,
                        change_type=ChangeType.MODIFY,
                        obj=new_obj,
                        uri=_resolve_change_uri(new_obj, context),
                    )
                matched_pairs[name] = (old_obj, new_obj)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                self._record_comparison_error(
                    phase="object_comparison",
                    subject=self._comparison_subject(new_map[name], context),
                    exception=exc,
                    severity="recoverable",
                )

        self._flush_batched_changes(changeset, pending_changes)

        processed_units = len(old_list_m) + len(new_list_m)
        self._advance_collection_progress(processed_units, message=f"comparing {object_type_name}")
        self._advance_compare_progress(processed_units, message=f"comparing {object_type_name}")

        return _CompareObjectListsResult(
            matched_pairs=matched_pairs,
            added_items=added_items,
            removed_items=removed_items,
        )
