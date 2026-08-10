import threading

from tm1_git_py.db.sqlite_worker import SqliteWorker
from tm1_git_py.db.changeset_store import ChangesetStore
from tm1_git_py.services.apply import delete_object, update_object
from tests.unit_common import *


class TestChangeset:
    def test_sqlite_worker_await_query_result_times_out(self):
        w = object.__new__(SqliteWorker)
        w._file_name = "dummy.sqlite"
        w._result_timeout_seconds = 0.01
        w._lock = threading.Lock()
        w._select_events = {}
        w._results = {}

        with pytest.raises(TimeoutError, match="Timed out waiting"):
            w.fetch_or_wait_for_result("missing-token")

    def test_changeset_store_for_changeset_id_initializes_outside_instances_lock(self, monkeypatch, tmp_path):
        lock = threading.Lock()
        initialized_without_lock = False

        def fake_init(self, *, changeset_id, base_dir=None, require_exists=False):
            nonlocal initialized_without_lock
            initialized_without_lock = lock.acquire(blocking=False)
            if initialized_without_lock:
                lock.release()
            self.db_path = ChangesetStore.path_for(changeset_id=changeset_id, base_dir=base_dir)
            self._closed = False

        monkeypatch.setattr(ChangesetStore, "_instances", {})
        monkeypatch.setattr(ChangesetStore, "_instances_lock", lock)
        monkeypatch.setattr(ChangesetStore, "__init__", fake_init)

        store = ChangesetStore.for_changeset_id(
            changeset_id="lock-check",
            base_dir=str(tmp_path),
        )

        assert store is ChangesetStore._instances[os.path.abspath(store.db_path)]
        assert not initialized_without_lock

    def test_changeset_get_store_concurrent_first_open(self, tmp_path):
        """Multiple threads opening the same new Changeset must not race first-open clear."""
        cs = Changeset(changeset_id="concurrent-store-open", base_dir=str(tmp_path))
        errors: list[BaseException] = []

        def touch_store() -> None:
            try:
                _ = cs._active_store()
            except BaseException as exc:  # pragma: no cover - diagnostic
                errors.append(exc)

        threads = [threading.Thread(target=touch_store) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
            assert not t.is_alive(), "thread hung in _get_store"
        assert not errors
        assert len(cs.changes) == 0

    def test_process_change_round_trip_preserves_ui_metadata(self, tmp_path):
        proc = make_process(name="ProcWithUI")
        proc.ui_data = "CubeAction=1511\fDataAction=1503\fCubeLogChanges=0\f"
        proc.variables_ui_data = [
            "VarType=32\fColType=827\f",
            "VarType=33\fColType=827\f",
        ]
        cs = Changeset(changeset_id="process-ui-round-trip", base_dir=str(tmp_path))

        cs.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.PROCESS,
                uri=proc.uri(),
                body=proc,
            )
        ]

        round_tripped = cs.changes[0].body
        assert round_tripped.ui_data == proc.ui_data
        assert round_tripped.variables_ui_data == proc.variables_ui_data

    def test_create_object_ignores_duplicate_create_for_technical_object(self, mocker, caplog):
        process = make_process(name="}Stats")
        duplicate_error = RuntimeError(
            "Text: "
            "'{\"error\":{\"code\":\"278\",\"message\":\"A process with Name \\\"}Stats\\\" already exists.\"}}' "
            "- Status Code: 400 - Reason: 'Bad Request'"
        )
        duplicate_error.status_code = 400
        mock_create = mocker.patch("tm1_git_py.model.process.create_process", side_effect=duplicate_error)

        with caplog.at_level(logging.WARNING):
            response = create_object(
                tm1_service=mocker.Mock(),
                object_instance=process,
                object_type=ObjectType.PROCESS.value,
                uri=process.uri(),
            )

        assert response.ok
        assert response.status_code == 208
        assert "Ignoring duplicate create failure for technical object" in caplog.text
        assert mock_create.call_count == 1

    def test_create_object_raises_duplicate_create_for_non_technical_object(self, mocker):
        process = make_process(name="RegularProcess")
        duplicate_error = RuntimeError(
            "Text: "
            "'{\"error\":{\"code\":\"278\",\"message\":\"A process with Name \\\"RegularProcess\\\" already exists.\"}}' "
            "- Status Code: 400 - Reason: 'Bad Request'"
        )
        duplicate_error.status_code = 400
        mocker.patch("tm1_git_py.model.process.create_process", side_effect=duplicate_error)

        with pytest.raises(RuntimeError, match="already exists"):
            create_object(
                tm1_service=mocker.Mock(),
                object_instance=process,
                object_type=ObjectType.PROCESS.value,
                uri=process.uri(),
            )

    def test_create_object_routes_drillthrough_rule_to_technical_cube(self, mocker):
        tm1_service = mocker.Mock()
        rule = make_rule(area="[default]", full_statement="[]=s:'simple_drillthrough';")

        create_object(
            tm1_service=tm1_service,
            object_instance=rule,
            object_type=ObjectType.RULE.value,
            uri="Cubes('Sales')/DrillthroughRules('default')",
        )

        tm1_service.cubes.update_or_create_rules.assert_called_once_with(
            cube_name="}CubeDrill_Sales",
            rules="[]=s:'simple_drillthrough';",
        )

    def test_update_object_routes_drillthrough_rule_to_technical_cube(self, mocker):
        tm1_service = mocker.Mock()
        rule = make_rule(area="[default]", full_statement="[]=s:'updated_drill';")

        update_object(
            tm1_service=tm1_service,
            object_instance=rule,
            object_type=ObjectType.RULE.value,
            uri="Cubes('Sales')/DrillthroughRules('default')",
        )

        tm1_service.cubes.update_or_create_rules.assert_called_once_with(
            cube_name="}CubeDrill_Sales",
            rules="[]=s:'updated_drill';",
        )

    def test_delete_object_routes_drillthrough_rule_to_technical_cube(self, mocker):
        tm1_service = mocker.Mock()
        rule = make_rule(area="[default]", full_statement="[]=s:'old_drill';")

        delete_object(
            tm1_service=tm1_service,
            object_instance=rule,
            object_type=ObjectType.RULE.value,
            uri="Cubes('Sales')/DrillthroughRules('default')",
        )

        tm1_service.cubes.update_or_create_rules.assert_called_once_with(
            cube_name="}CubeDrill_Sales",
            rules="",
        )

    def test_update_object_keeps_regular_rule_on_source_cube(self, mocker):
        tm1_service = mocker.Mock()
        rule = make_rule(area="[default]", full_statement="[] = N: 1;")

        update_object(
            tm1_service=tm1_service,
            object_instance=rule,
            object_type=ObjectType.RULE.value,
            uri="Cubes('Sales')/Rules('default')",
        )

        tm1_service.cubes.update_or_create_rules.assert_called_once_with(
            cube_name="Sales",
            rules="[] = N: 1;",
        )

    def test_static_subset_apply_dispatch_uses_subset_crud_helpers(self, mocker):
        tm1_service = mocker.Mock()
        uri = Subset.uri_for("Dim_A", "Hier_A", "Bikes")
        static_subset = Subset(
            name="Bikes",
            element_ids=[Element.uri_for("Dim_A", "Hier_A", "Bike")],
        )
        mock_create = mocker.patch("tm1_git_py.model.subset.create_subset", return_value="created")
        mock_update = mocker.patch("tm1_git_py.model.subset.update_subset", return_value="updated")

        create_result = create_object(
            tm1_service=tm1_service,
            object_instance=static_subset,
            object_type=ObjectType.SUBSET.value,
            uri=uri,
        )
        update_result = update_object(
            tm1_service=tm1_service,
            object_instance=static_subset,
            object_type=ObjectType.SUBSET.value,
            uri=uri,
        )

        mock_create.assert_called_once_with(tm1_service, static_subset, uri=uri)
        mock_update.assert_called_once_with(tm1_service, static_subset, uri=uri)
        assert create_result == "created"
        assert update_result == "updated"

    def test_apply_continues_after_duplicate_create_for_technical_object(self, mocker, caplog):
        technical_process = make_process(name="}Stats")
        regular_process = make_process(name="RegularProcess")
        changeset = Changeset(changeset_id="20260420000001")
        changeset.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.PROCESS,
                uri=technical_process.uri(),
                body=technical_process,
                apply=True,
            ),
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.PROCESS,
                uri=regular_process.uri(),
                body=regular_process,
                apply=True,
            ),
        ]

        def create_process_side_effect(_tm1_service, process, **_kwargs):
            if process.name == "}Stats":
                duplicate_error = RuntimeError(
                    "Text: "
                    "'{\"error\":{\"code\":\"278\",\"message\":\"A process with Name \\\"}Stats\\\" already exists.\"}}' "
                    "- Status Code: 400 - Reason: 'Bad Request'"
                )
                duplicate_error.status_code = 400
                raise duplicate_error
            return types.SimpleNamespace(url=process.uri(), status_code=201, ok=True)

        mocker.patch("tm1_git_py.model.process.create_process", side_effect=create_process_side_effect)
        mocker.patch("tm1_git_py.services.apply.delete_object")
        mocker.patch("tm1_git_py.services.apply.update_object")

        with caplog.at_level(logging.WARNING):
            success, changes = apply(tm1_service=mocker.Mock(), changeset=changeset, fail_fast=True)

        assert success
        assert changes == [regular_process.uri(), technical_process.uri()]
        assert "Ignoring duplicate create failure for technical object" in caplog.text

    def test_apply_uses_sorted_order_for_delete(self, mocker):
        model_old, errors_old = deserialize_model(str(test_model_dir_base))
        model_new, errors_new = deserialize_model(str(test_model_dir_diff))
        comparator = Comparator()

        changeset, errors = comparator.compare(model_old, model_new)
        assert not errors, errors

        # Patch deletes so we can inspect call order
        mock_delete = mocker.patch("tm1_git_py.services.apply.delete_object")
        mock_create = mocker.patch("tm1_git_py.services.apply.create_object")
        mock_update = mocker.patch("tm1_git_py.services.apply.update_object")

        # Give delete something with a .url so apply() doesn't fail
        def delete_side_effect(**kwargs):
            obj = kwargs["object_instance"]
            return types.SimpleNamespace(url=f"DELETE:{obj.__class__}:{obj.name}", status_code=200, ok=True)

        mock_delete.side_effect = delete_side_effect

        tm1_service = mocker.Mock()

        success, _ = apply(tm1_service=tm1_service, changeset=changeset, fail_fast=False)
        assert success

        # --- Assert delete order ---
        deleted_types = [
            type(call.kwargs["object_instance"])
            for call in mock_delete.call_args_list
        ]

        # For deletes, precedence is:
        # mdx_views -> rules -> cubes -> edges -> subsets -> elements -> hierarchies -> dimensions -> chore -> process
        # Default compare filters technical cubes (e.g. }testbenchCube), so Cube is not deleted here.
        assert deleted_types == [MDXView, Edge, Element, Chore, Process]


    def test_apply_uses_sorted_order_for_create(self, mocker):
        model_old, errors_old = deserialize_model(str(test_model_dir_base))
        model_new, errors_new = deserialize_model(str(test_model_dir_diff))
        comparator = Comparator()

        changeset, errors = comparator.compare(model_old, model_new)
        assert not errors, errors

        # Patch creates so we can inspect call order
        mock_delete = mocker.patch("tm1_git_py.services.apply.delete_object")
        mock_create = mocker.patch("tm1_git_py.services.apply.create_object")
        mock_update = mocker.patch("tm1_git_py.services.apply.update_object")

        # Give create something with a .url so apply() doesn't fail
        def create_side_effect(**kwargs):
            obj = kwargs["object_instance"]
            return types.SimpleNamespace(url=f"CREATE:{obj.__class__}:{obj.name}", status_code=200, ok=True)

        mock_create.side_effect = create_side_effect

        tm1_service = mocker.Mock()

        success, _ = apply(tm1_service=tm1_service, changeset=changeset, fail_fast=False)
        assert success

        # --- Assert create order ---
        created_types = [
            type(call.kwargs["object_instance"])
            for call in mock_create.call_args_list
        ]

        # For creates, precedence is:
        # dimensions -> hierarchies -> elements -> subsets -> edges -> cubes -> mdx_views -> rules -> processes -> chores
        assert created_types == [Element, Element, Subset, Edge, Edge, MDXView]


    def test_apply_uses_sorted_order_for_update(self, mocker):
        model_old, errors_old = deserialize_model(str(test_model_dir_base))
        model_new, errors_new = deserialize_model(str(test_model_dir_diff))
        comparator = Comparator()

        changeset, errors = comparator.compare(model_old, model_new)
        assert not errors, errors

        # Patch update so we can inspect call order
        mock_delete = mocker.patch("tm1_git_py.services.apply.delete_object")
        mock_create = mocker.patch("tm1_git_py.services.apply.create_object")
        mock_update = mocker.patch("tm1_git_py.services.apply.update_object")

        # Give update something with a .url so apply() doesn't fail
        def update_side_effect(**kwargs):
            obj = kwargs["object_instance"]
            return types.SimpleNamespace(url=f"UPDATE:{obj.__class__}:{obj.name}", status_code=200, ok=True)

        mock_update.side_effect = update_side_effect

        tm1_service = mocker.Mock()

        success, _ = apply(tm1_service=tm1_service, changeset=changeset, fail_fast=False)
        assert success

        # --- Assert update order ---
        updated_types = [
            type(call.kwargs["object_instance"])
            for call in mock_update.call_args_list
        ]

        # For updates, precedence is:
        # subsets -> mdx_views -> unified rules -> processes -> chores
        assert updated_types == [Subset, MDXView, Rule, Process, Chore]

    def test_apply_orders_static_subset_add_after_referenced_element_add(self, mocker):
        element_uri = Element.uri_for("Dim_A", "Hier_A", "Bike")
        static_subset = Subset(name="Bikes", element_ids=[element_uri])
        element = Element(name="Bike", type="Numeric")
        changeset = Changeset(changeset_id="20260521000001")
        changeset.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.SUBSET,
                uri=Subset.uri_for("Dim_A", "Hier_A", "Bikes"),
                body=static_subset,
            ),
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.ELEMENT,
                uri=element_uri,
                body=element,
            ),
        ]

        mock_create = mocker.patch(
            "tm1_git_py.services.apply.create_object",
            side_effect=lambda **kwargs: types.SimpleNamespace(
                url=kwargs["uri"],
                status_code=200,
                ok=True,
            ),
        )

        success, _ = apply(tm1_service=mocker.Mock(), changeset=changeset, fail_fast=False)

        assert success
        assert [
            call.kwargs["object_type"]
            for call in mock_create.call_args_list
        ] == ["Element", "Subset"]

    def test_apply_orders_subset_modify_before_element_remove_in_same_hierarchy(self, mocker):
        element_uri = Element.uri_for("Dim_A", "Hier_A", "Bike")
        static_subset = Subset(
            name="Bikes",
            element_ids=[Element.uri_for("Dim_A", "Hier_A", "Helmet")],
        )
        changeset = Changeset(changeset_id="20260521000002")
        changeset.changes = [
            Change(
                change_type=ChangeType.REMOVE,
                object_type=ObjectType.ELEMENT,
                uri=element_uri,
                body=Element(name="Bike", type="Numeric"),
            ),
            Change(
                change_type=ChangeType.MODIFY,
                object_type=ObjectType.SUBSET,
                uri=Subset.uri_for("Dim_A", "Hier_A", "Bikes"),
                body=static_subset,
            ),
        ]

        operations = []
        mocker.patch(
            "tm1_git_py.services.apply.update_object",
            side_effect=lambda **kwargs: operations.append(("modify", kwargs["object_type"]))
            or types.SimpleNamespace(url=kwargs["uri"], status_code=200, ok=True),
        )
        mocker.patch(
            "tm1_git_py.services.apply.delete_object",
            side_effect=lambda **kwargs: operations.append(("remove", kwargs["object_type"]))
            or types.SimpleNamespace(url=kwargs["uri"], status_code=200, ok=True),
        )

        success, _ = apply(tm1_service=mocker.Mock(), changeset=changeset, fail_fast=False)

        assert success
        assert operations == [("modify", "Subset"), ("remove", "Element")]

    def test_apply_orders_subset_remove_before_element_remove_in_same_hierarchy(self, mocker):
        changeset = Changeset(changeset_id="20260521000004")
        changeset.changes = [
            Change(
                change_type=ChangeType.REMOVE,
                object_type=ObjectType.ELEMENT,
                uri=Element.uri_for("Dim_A", "Hier_A", "Bike"),
                body=Element(name="Bike", type="Numeric"),
            ),
            Change(
                change_type=ChangeType.REMOVE,
                object_type=ObjectType.SUBSET,
                uri=Subset.uri_for("Dim_A", "Hier_A", "Bikes"),
                body=Subset(name="Bikes", element_ids=[]),
            ),
        ]

        mock_delete = mocker.patch(
            "tm1_git_py.services.apply.delete_object",
            side_effect=lambda **kwargs: types.SimpleNamespace(
                url=kwargs["uri"],
                status_code=200,
                ok=True,
            ),
        )

        success, _ = apply(tm1_service=mocker.Mock(), changeset=changeset, fail_fast=False)

        assert success
        assert [
            call.kwargs["object_type"]
            for call in mock_delete.call_args_list
        ] == ["Subset", "Element"]

    def test_apply_skips_changes_marked_apply_false(self, mocker):
        changeset = Changeset(changeset_id="20260413000001")
        process_a = make_process(name="ProcApply")
        process_b = make_process(name="ProcSkip")
        changeset.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.PROCESS,
                uri=process_a.uri(),
                body=process_a,
                apply=True,
            ),
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.PROCESS,
                uri=process_b.uri(),
                body=process_b,
                apply=False,
            ),
        ]

        mock_create = mocker.patch("tm1_git_py.services.apply.create_object")
        mocker.patch("tm1_git_py.services.apply.delete_object")
        mocker.patch("tm1_git_py.services.apply.update_object")
        mock_create.return_value = types.SimpleNamespace(url="ok", status_code=200, ok=True)

        success, _ = apply(tm1_service=mocker.Mock(), changeset=changeset, fail_fast=False)

        assert success
        assert mock_create.call_count == 1
        assert mock_create.call_args.kwargs["object_instance"].name == "ProcApply"

    def test_changeset_persist_creates_sqlite_with_expected_name(self):
        changeset = Changeset(changeset_id="20260413000002")
        process_obj = make_process(name="ProcPersist")
        changeset.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.PROCESS,
                uri=process_obj.uri(),
                body=process_obj,
            )
        ]

        sqlite_path = changeset.sqlite_path
        assert sqlite_path.name == "changeset-20260413000002.sqlite"
        assert sqlite_path.exists()

        assert changeset.has_changes()
        assert len(changeset.changes) == 1


    def test_changeset_filter_uses_readme_rules_and_preserves_parent_exclude(self):
        changeset = Changeset(changeset_id="20260413000003")
        dim = make_dimension(name="Sales", hierarchy_names=["Main"])
        subset_obj = make_subset(
            name="SubsetA",
            expression="{TM1SUBSETALL([Sales].[Main])}",
            dimension_name="Sales",
            hierarchy_name="Main",
        )
        process_obj = make_process(name="KeepProcess")

        changeset.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.DIMENSION,
                uri=dim.uri(),
                body=dim,
            ),
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.SUBSET,
                uri=subset_obj.uri("Sales", "Main"),
                body=subset_obj,
            ),
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.PROCESS,
                uri=process_obj.uri(),
                body=process_obj,
            ),
        ]

        toggled = changeset.filter(["Dimensions('Sales')"])
        assert toggled == 2
        changes = changeset.query(from_=0, to=10)
        by_uri = {change.uri: change.apply for change in changes}
        assert by_uri[dim.uri()] is False
        assert by_uri[subset_obj.uri("Sales", "Main")] is False
        assert by_uri[process_obj.uri()] is True

    def test_changeset_query_supports_filter_and_paging(self):
        changeset = Changeset(changeset_id="20260413000004")
        processes = [make_process(name=f"Proc{i}") for i in range(6)]
        changeset.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.PROCESS,
                uri=process_obj.uri(),
                body=process_obj,
            )
            for process_obj in processes
        ]

        page = changeset.query(
            rules=["Processes('Proc0')", "Processes('Proc4')"],
            offset=1,
            limit=2,
        )
        assert [change.body.name for change in page] == ["Proc2", "Proc3"]

    def test_changeset_filter_preserves_apply_when_rule_does_not_match(self):
        changeset = Changeset(changeset_id="20260413000006")
        p0 = make_process(name="Proc0")
        p1 = make_process(name="Proc1")
        changeset.changes = [
            Change(change_type=ChangeType.ADD, object_type=ObjectType.PROCESS, uri=p0.uri(), body=p0, apply=False),
            Change(change_type=ChangeType.ADD, object_type=ObjectType.PROCESS, uri=p1.uri(), body=p1, apply=True),
        ]

        updated = changeset.filter(["Processes('Proc1')"])

        assert updated == 1
        queried = changeset.query(from_=0, to=10)
        by_name = {change.body.name: change.apply for change in queried}
        assert by_name["Proc0"] is False
        assert by_name["Proc1"] is False

    def test_changeset_filter_unignores_only_matching_rules(self):
        changeset = Changeset(changeset_id="20260413000008")
        p0 = make_process(name="Proc0")
        p1 = make_process(name="Proc1")
        changeset.changes = [
            Change(change_type=ChangeType.ADD, object_type=ObjectType.PROCESS, uri=p0.uri(), body=p0, apply=False),
            Change(change_type=ChangeType.ADD, object_type=ObjectType.PROCESS, uri=p1.uri(), body=p1, apply=False),
        ]

        updated = changeset.filter(["!Processes('Proc1')"])

        assert updated == 1
        queried = changeset.query(from_=0, to=10)
        by_name = {change.body.name: change.apply for change in queried}
        assert by_name["Proc0"] is False
        assert by_name["Proc1"] is True

    def test_changeset_filter_with_no_rules_preserves_existing_apply_state(self):
        changeset = Changeset(changeset_id="20260413000009")
        p0 = make_process(name="Proc0")
        p1 = make_process(name="Proc1")
        changeset.changes = [
            Change(change_type=ChangeType.ADD, object_type=ObjectType.PROCESS, uri=p0.uri(), body=p0, apply=False),
            Change(change_type=ChangeType.ADD, object_type=ObjectType.PROCESS, uri=p1.uri(), body=p1, apply=True),
        ]

        updated = changeset.filter([])

        assert updated == 0
        queried = changeset.query(from_=0, to=10)
        by_name = {change.body.name: change.apply for change in queried}
        assert by_name["Proc0"] is False
        assert by_name["Proc1"] is True

    def test_changeset_filter_force_include_dimension_cascades_to_descendants(self):
        changeset = Changeset(changeset_id="20260413000010")
        hierarchy_obj = Hierarchy(name="Main", elements=[], edges=[], subsets=[])
        dimension_obj = Dimension(
            name="Sales",
            hierarchies=[hierarchy_obj],
            defaultHierarchy=hierarchy_obj,
        )
        subset_obj = make_subset(
            name="SubsetA",
            expression="{TM1SUBSETALL([Sales].[Main])}",
            dimension_name="Sales",
            hierarchy_name="Main",
        )
        element_obj = make_element("Leaf1")
        edge_obj = Edge(parent="Total", component_name="Leaf1", weight=1)

        changeset.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.DIMENSION,
                uri=dimension_obj.uri(),
                body=dimension_obj,
                apply=False,
            ),
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.HIERARCHY,
                uri=hierarchy_obj.uri("Sales"),
                body=hierarchy_obj,
                apply=False,
            ),
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.SUBSET,
                uri=subset_obj.uri("Sales", "Main"),
                body=subset_obj,
                apply=False,
            ),
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.ELEMENT,
                uri=element_obj.uri("Sales", "Main"),
                body=element_obj,
                apply=False,
            ),
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.EDGE,
                uri=edge_obj.uri("Sales", "Main"),
                body=edge_obj,
                apply=False,
            ),
        ]

        updated = changeset.filter(["!Dimensions('*')"])

        assert updated == 5
        queried = changeset.query(from_=0, to=20)
        by_uri = {change.uri: change.apply for change in queried}
        assert by_uri[dimension_obj.uri()] is True
        assert by_uri[hierarchy_obj.uri("Sales")] is True
        assert by_uri[subset_obj.uri("Sales", "Main")] is True
        assert by_uri[element_obj.uri("Sales", "Main")] is True
        assert by_uri[edge_obj.uri("Sales", "Main")] is True

    def test_changeset_can_be_loaded_by_changeset_id(self):
        changeset = Changeset(changeset_id="20260413000007")
        changeset._store.clear()
        process_obj = make_process(name="ProcLoad")
        changeset.changes.append(
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.PROCESS,
                uri=process_obj.uri(),
                body=process_obj,
            )
        )
        loaded = Changeset.from_changeset_id("20260413000007")
        assert len(loaded.changes) == 1
        assert loaded.changes[0].body.name == "ProcLoad"

    def test_changeset_from_changeset_id_raises_when_missing(self):
        with pytest.raises(FileNotFoundError):
            Changeset.from_changeset_id("20990101000000")


    def test_models_expose_canonical_urls(self):
        hierarchy_obj = make_hierarchy(dimension_name="TestDim", hierarchy_name="TestHier")
        dimension_obj = Dimension(
            name="TestDim",
            hierarchies=[hierarchy_obj],
            defaultHierarchy=hierarchy_obj,
        )
        subset_obj = make_subset(
            name="SubsetA",
            expression="{TM1SUBSETALL([TestDim].[TestHier])}",
            dimension_name="TestDim",
            hierarchy_name="TestHier",
        )
        process_obj = make_process(name="ProcURL")
        chore_obj = make_chore(name="ChoreURL")
        view_obj = MDXView(name="ViewURL", mdx="SELECT FROM [MockCube]")
        rule_obj = Rule(area="[default]", full_statement="[default]=N:1;")
        cube_obj = Cube(
            name="MockCube",
            dimensions=[dimension_obj.name],
            rules=[rule_obj],
            views=[view_obj],
        )

        assert cube_obj.uri() == "Cubes('MockCube')"
        assert rule_obj.uri("MockCube") == "Cubes('MockCube')/Rules('default')"
        assert view_obj.uri("MockCube") == "Cubes('MockCube')/Views('ViewURL')"
        assert dimension_obj.uri() == "Dimensions('TestDim')"
        assert hierarchy_obj.uri("TestDim") == "Dimensions('TestDim')/Hierarchies('TestHier')"
        assert subset_obj.uri("TestDim", "TestHier") == "Dimensions('TestDim')/Hierarchies('TestHier')/Subsets('SubsetA')"
        assert process_obj.uri() == "Processes('ProcURL')"
        assert chore_obj.uri() == "Chores('ChoreURL')"

    def test_filter_rules_exclude_rule_and_cube_urls(self):
        rule_a = Rule(area="[A]", full_statement="[A]=N:1;")
        rule_b = Rule(area="[B]", full_statement="[B]=N:2;")
        view_obj = MDXView(name="KeepView", mdx="SELECT FROM [MockCube]")
        dim_obj = make_dimension(name="MockDim", hierarchy_names=[])
        cube_obj = Cube(
            name="MockCube",
            dimensions=[dim_obj.name],
            rules=[rule_a, rule_b],
            views=[view_obj],
        )
        model = Model(cubes=[cube_obj], dimensions=[dim_obj], processes=[make_process("ProcA")], chores=[make_chore("ChoreA")])

        rules_exclude_default_rule = FilterRules(
            with_default_leaves_ignore(["Cubes('MockCube')/Rules('default')"])
        )
        for r in model.cubes[0].rules:
            url = f"{r.uri('MockCube')}|{normalize_for_path(r.area)}"
            assert rules_exclude_default_rule.should_exclude(url)

        rules_exclude_cube = FilterRules(with_default_leaves_ignore(["Cubes('MockCube')"]))
        assert rules_exclude_cube.should_exclude(model.cubes[0].uri())

    def test_default_leaves_hierarchy_excluded_by_filter_rules(self):
        main_hierarchy = Hierarchy(
            name="Main",
            elements=[],
            edges=[],
            subsets=[],
        )
        leaves_hierarchy = Hierarchy(
            name="Leaves",
            elements=[],
            edges=[],
            subsets=[],
        )
        dimension = Dimension(
            name="MockDim",
            hierarchies=[main_hierarchy, leaves_hierarchy],
            defaultHierarchy=main_hierarchy,
        )

        rules = FilterRules(with_default_leaves_ignore([]))
        leaves_uri = leaves_hierarchy.uri("MockDim")
        assert rules.should_exclude(leaves_uri)
        assert not rules.should_exclude(main_hierarchy.uri("MockDim"))

    def test_force_include_leaves_hierarchy_overrides_default_exclude(self):
        main_hierarchy = Hierarchy(
            name="Main",
            elements=[],
            edges=[],
            subsets=[],
        )
        leaves_hierarchy = Hierarchy(
            name="Leaves",
            elements=[],
            edges=[],
            subsets=[],
        )
        dimension = Dimension(
            name="MockDim",
            hierarchies=[main_hierarchy, leaves_hierarchy],
            defaultHierarchy=main_hierarchy,
        )

        rules = FilterRules(
            with_default_leaves_ignore(["!Dimensions('*')/Hierarchies('Leaves')"])
        )
        leaves_uri = leaves_hierarchy.uri("MockDim")
        assert not rules.should_exclude(leaves_uri)


    def test_export_persists_expected_payload(self, tmp_path):
        changes = Changeset(changeset_id="20260413000000")

        created_subset = make_subset(
            name="Subset_Create",
            expression="{[Dim_New].[Hier_New].Members}",
            dimension_name="Dim_New",
            hierarchy_name="Hier_New",
        )
        removed_view = make_mdx_view(
            name="View_To_Delete",
            mdx="SELECT FROM [Cube_One]",
            source_path="cubes/Cube_One.views/View_To_Delete.json",
        )
        new_dimension = make_dimension(
            name="Dim_Update",
            hierarchy_names=["Base", "Added"],
            source_path="/dimensions/Dim_Update",
        )

        changes.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.SUBSET,
                uri=Subset.uri_for("Dim_New", "Hier_New", "Subset_Create"),
                body=created_subset,
            ),
            Change(
                change_type=ChangeType.MODIFY,
                object_type=ObjectType.DIMENSION,
                uri=Dimension.uri_for("Dim_Update"),
                body=new_dimension,
            ),
            Change(
                change_type=ChangeType.REMOVE,
                object_type=ObjectType.MDX_VIEW,
                uri=MDXView.uri_for("Cube_One", "View_To_Delete"),
                body=removed_view,
            ),
        ]

        export_path = tmp_path / "changes.yml"
        changes.export(export_path)

        exported_payload = yaml.safe_load(export_path.read_text(encoding="utf-8"))

        expected_payload = {
            "changeset_id": "20260413000000",
            "summary": {
                "add": 1,
                "remove": 1,
                "modify": 1,
            },
            "changes": [
                {
                    "change_type": "add",
                    "object_type": "Subset",
                    "uri": Subset.uri_for("Dim_New", "Hier_New", "Subset_Create"),
                    "apply": True,
                    "body": {
                        "Name": "Subset_Create",
                        "Expression": "{[Dim_New].[Hier_New].Members}",
                    },
                },
                {
                    "change_type": "modify",
                    "object_type": "Dimension",
                    "uri": Dimension.uri_for("Dim_Update"),
                    "apply": True,
                    "body": {
                        "Name": "Dim_Update",
                        "Hierarchies": [
                            "dimensions/Dim_Update.hierarchies/Base.json",
                            "dimensions/Dim_Update.hierarchies/Added.json",
                        ],
                        "DefaultHierarchy": "dimensions/Dim_Update.hierarchies/Base.json",
                    },
                },
                {
                    "change_type": "remove",
                    "object_type": "MDXView",
                    "uri": MDXView.uri_for("Cube_One", "View_To_Delete"),
                    "apply": True,
                    "body": {
                        "Name": "View_To_Delete",
                    },
                },
            ],
        }

        exported_payload_pretty = json.dumps(exported_payload, sort_keys=True, indent=2)
        expected_payload_pretty = json.dumps(expected_payload, sort_keys=True, indent=2)
        assert exported_payload_pretty == expected_payload_pretty

    def test_export_cube_change_serializes_dimension_names_as_reference_paths(self, tmp_path):
        changes = Changeset(changeset_id="20260413000012")
        changes.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.CUBE,
                uri=Cube.uri_for("Organization Units Settings"),
                body=Cube(
                    name="Organization Units Settings",
                    dimensions=["Versions", "Organization Units"],
                    rules=[],
                    views=[],
                ),
            )
        ]

        export_path = tmp_path / "cube_changes.yml"
        changes.export(export_path)

        exported_payload = yaml.safe_load(export_path.read_text(encoding="utf-8"))
        assert exported_payload["changes"][0]["body"] == {
            "Name": "Organization Units Settings",
            "Dimensions": [
                "dimensions/Versions.json",
                "dimensions/Organization Units.json",
            ],
        }

    def test_export_import_static_subset_change_preserves_element_reference_ids(self, tmp_path):
        element_ids = [
            Element.uri_for("Product", "Product", "Bike"),
            Element.uri_for("Product", "Product", "Helmet"),
        ]
        changes = Changeset(changeset_id="20260521000003")
        changes.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.SUBSET,
                uri=Subset.uri_for("Product", "Product", "Export"),
                body=Subset(name="Export", element_ids=element_ids),
            )
        ]

        export_path = tmp_path / "static_subset_changes.yml"
        changes.export(export_path)

        exported_payload = yaml.safe_load(export_path.read_text(encoding="utf-8"))
        assert exported_payload["changes"][0]["body"] == {
            "Name": "Export",
            "Elements": [{"@id": element_id} for element_id in element_ids],
        }

        imported = import_changeset(export_path)
        imported_subset = imported.changes[0].body
        assert isinstance(imported_subset, Subset)
        assert imported_subset.element_ids == element_ids
        assert imported_subset.is_static is True

    def test_import_cube_change_normalizes_dimension_reference_paths_to_names(self, tmp_path):
        import_path = tmp_path / "cube_changes.yml"
        import_path.write_text(
            yaml.safe_dump({
                "changeset_id": "20260413000013",
                "changes": [
                    {
                        "change_type": "add",
                        "object_type": "Cube",
                        "uri": Cube.uri_for("Organization Units Settings"),
                        "apply": True,
                        "body": {
                            "Name": "Organization Units Settings",
                            "Dimensions": [
                                "dimensions/Versions.json",
                                "dimensions/Organization Units.json",
                            ],
                        },
                    }
                ],
            }),
            encoding="utf-8",
        )

        imported = import_changeset(import_path)

        cube = imported.changes[0].body
        assert isinstance(cube, Cube)
        assert cube.dimensions == ["Versions", "Organization Units"]


    def test_import_changeset(self, tmp_path):
        model_old, errors_old = deserialize_model(str(test_model_dir_base))
        model_new, errors_new = deserialize_model(str(test_model_dir_diff))
        comparator = tm1_git_py.Comparator()

        changeset_compared, errors = comparator.compare(model_old, model_new)
        assert not errors, errors
        export_path = tmp_path / "changes_exported.yaml"
        changeset_compared.export(file_path=export_path)

        changeset_imported = import_changeset(
            changeset_file=str(export_path)
        )

        for expected, actual in zip(changeset_compared.changes, changeset_imported.changes):
            assert expected.change_type == actual.change_type
            assert expected.object_type == actual.object_type
            assert expected.uri == actual.uri
            assert expected.body.__class__ == actual.body.__class__

    def test_changeset_class_import_alias_json_stream(self, tmp_path):
        changeset = Changeset(changeset_id="20260413000008")
        changeset._store.clear()
        process_obj = make_process(name="ProcAlias")
        changeset.changes.append(
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.PROCESS,
                uri=process_obj.uri(),
                body=process_obj,
                apply=False,
            )
        )
        export_path = tmp_path / "alias_import.json"
        changeset.export(export_path)

        imported = import_changeset(export_path)
        assert imported._changeset_id == "20260413000008"
        assert len(imported.changes) == 1
        assert imported.changes[0].apply is False

    def test_export_format_json_without_json_suffix_imports(self, tmp_path):
        changeset = Changeset(changeset_id="20260413000009")
        changeset._store.clear()
        process_obj = make_process(name="ProcNoSuffix")
        changeset.changes.append(
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.PROCESS,
                uri=process_obj.uri(),
                body=process_obj,
                apply=True,
            )
        )
        export_path = tmp_path / "changeset_no_ext"
        changeset.export(export_path, format="json")

        imported = import_changeset(export_path)
        assert imported._changeset_id == "20260413000009"
        assert len(imported.changes) == 1
        assert imported.changes[0].apply is True

    def test_export_reports_progress_to_sink(self, tmp_path):
        changeset = Changeset(changeset_id="20260413000010")
        changeset._store.clear()
        p1 = make_process(name="P1")
        p2 = make_process(name="P2")
        changeset.changes.extend(
            [
                Change(
                    change_type=ChangeType.ADD,
                    object_type=ObjectType.PROCESS,
                    uri=p1.uri(),
                    body=p1,
                ),
                Change(
                    change_type=ChangeType.ADD,
                    object_type=ObjectType.PROCESS,
                    uri=p2.uri(),
                    body=p2,
                ),
            ]
        )
        events = []
        sink = CallbackProgressSink(lambda e: events.append(e))
        export_path = tmp_path / "progress_export.json"
        changeset.export(export_path, format="json", progress_sink=sink)
        currents = [e.current for e in events]
        assert 0 in currents
        assert 2 in currents
        assert all(e.message == "Writing changeset" for e in events)

    def test_export_empty_changeset_reports_progress_to_sink(self, tmp_path):
        changeset = Changeset(changeset_id="20260413000011")
        changeset._store.clear()
        events = []
        sink = CallbackProgressSink(lambda e: events.append(e))
        export_path = tmp_path / "empty_progress.json"
        changeset.export(export_path, format="json", progress_sink=sink)
        assert len(events) >= 2
        assert events[0].current == 0
        assert events[-1].current == 1
        assert events[-1].total == 1

    def test_export_import_roundtrip_preserves_changeset_id_and_apply(self, tmp_path):
        changeset = Changeset(changeset_id="20260413000005")
        process_obj = make_process(name="ProcRoundtrip")
        changeset.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.PROCESS,
                uri=process_obj.uri(),
                body=process_obj,
                apply=False,
            )
        ]

        export_path = tmp_path / "roundtrip_changeset.yaml"
        changeset.export(export_path)

        imported = import_changeset(str(export_path))

        assert imported._changeset_id == "20260413000005"
        assert len(imported.changes) == 1
        assert imported.changes[0].apply is False

    def test_export_remove_edge_body_uses_parent_component_weight(self, tmp_path):
        changeset = Changeset(changeset_id="20260416000003")
        edge = Edge(parent="DimElemC", component_name="DimElem1", weight=1)
        changeset.changes = [
            Change(
                change_type=ChangeType.REMOVE,
                object_type=ObjectType.EDGE,
                uri="Dimensions('TestDimMultiHier')/Hierarchies('TestDimMultiHier')/Edges('DimElemC'/'DimElem1')",
                body=edge,
            )
        ]

        export_path = tmp_path / "remove_edge_payload.yml"
        changeset.export(export_path)
        payload = yaml.safe_load(export_path.read_text(encoding="utf-8"))
        body = payload["changes"][0]["body"]
        assert body == {
            "ParentName": "DimElemC",
            "ComponentName": "DimElem1",
            "Weight": 1,
        }

    @pytest.mark.skip(reason="Legacy name format is no longer supported")
    def test_import_rejects_edge_body_legacy_name_format(self, tmp_path):
        changeset = Changeset(changeset_id="20260416000001")
        edge = Edge(parent="DimElemC", component_name="DimElem1", weight=1)
        changeset.changes = [
            Change(
                change_type=ChangeType.REMOVE,
                object_type=ObjectType.EDGE,
                uri="Dimensions('TestDimMultiHier')/Hierarchies('TestDimMultiHier')/Edges('DimElemC'/'DimElem1')",
                body=edge,
            )
        ]
        path = tmp_path / "edge_remove_legacy_name.yml"
        changeset.export(path)
        raw = path.read_text(encoding="utf-8")
        raw = raw.replace("ParentName: DimElemC\n", "")
        raw = raw.replace("ComponentName: DimElem1\n", "name: DimElemC:DimElem1\n")
        path.write_text(raw, encoding="utf-8")

        imported = import_changeset(path)
        assert len(imported.changes) == 0

    def test_export_changeset_preserves_unicode_characters(self, tmp_path):
        changes = Changeset()
        subset = make_subset(
            name="Subset_Día",
            expression="{[Dim].[Hier].[café]}",
            dimension_name="Dim",
            hierarchy_name="Hier",
        )
        changes.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.SUBSET,
                uri=Subset.uri_for("Dim", "Hier", "Subset_Día"),
                body=subset,
            )
        ]

        export_path = tmp_path / "unicode_changes.yml"
        changes.export(export_path)
        raw_yaml = export_path.read_text(encoding="utf-8")

        assert "Subset_Día" in raw_yaml
        assert "café" in raw_yaml
        assert "\\x" not in raw_yaml
