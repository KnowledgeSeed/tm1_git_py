from tests.unit_common import *


class TestExporter:

    def test_worker_count_resolution(self, monkeypatch):
        from tm1_git_py.internal import worker_config
        from tm1_git_py.internal.worker_config import resolve_worker_counts

        explicit = resolve_worker_counts(4)
        assert explicit.cpu_workers == 1
        assert explicit.io_workers == 3

        monkeypatch.setattr(worker_config.os, "cpu_count", lambda: 16)
        defaulted = resolve_worker_counts(None)
        assert defaulted.cpu_workers == 9
        assert defaulted.io_workers == 27

    def test_export_forwards_max_workers_to_dimensions(self):
        tm1_conn = mock.Mock()
        with mock.patch.object(exporter_module, "dimensions_to_model", return_value=({}, [])) as mock_dims, \
             mock.patch.object(exporter_module, "cubes_to_model", return_value=({}, [])), \
             mock.patch.object(exporter_module, "procs_to_model", return_value=({}, [])), \
             mock.patch.object(exporter_module, "chores_to_model", return_value=({}, [])):
            exporter_module.export(tm1_conn, model_id="unit-export", max_workers=9)
        worker_counts = mock_dims.call_args.kwargs.get("worker_counts")
        assert worker_counts is not None
        assert worker_counts.max_workers == 9

    def test_dimensions_to_model_uses_cpu_workers_for_hash_and_io_workers_for_fetch(self, monkeypatch):
        seen: dict[str, int] = {}

        class FakeContentHashCalculator:
            def __init__(self, *, db_path, max_workers, progress_sink=None):
                seen["cpu_workers"] = max_workers

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return None

        class FakePriorityThreadPoolExecutor:
            def __init__(self, *, max_workers, thread_name_prefix):
                seen["io_workers"] = max_workers

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return None

        monkeypatch.setattr(exporter_module, "ContentHashCalculator", FakeContentHashCalculator)
        monkeypatch.setattr(exporter_module, "PriorityThreadPoolExecutor", FakePriorityThreadPoolExecutor)
        monkeypatch.setattr(exporter_module, "get_dimension_names", lambda *args, **kwargs: [])
        monkeypatch.setattr(exporter_module.ModelStore, "for_model_id", classmethod(lambda cls, model_id: mock.Mock()))

        from tm1_git_py.internal.worker_config import resolve_worker_counts

        exporter_module.dimensions_to_model(
            mock.Mock(),
            model_id="unit-worker-split",
            filter_rules=FilterRules([]),
            progress_sink=exporter_module.NoopProgressSink(),
            worker_counts=resolve_worker_counts(8),
        )

        assert seen == {"cpu_workers": 2, "io_workers": 6}

    def test_get_hierarchy_sort_metadata_uses_default_member_for_single_hierarchy(self):
        tm1_conn = mock.Mock()
        tm1_conn.cells.execute_mdx_elements_value_dict.return_value = {
            "SORTELEMENTSTYPE|Products": "BYNAME",
            "SORTELEMENTSSENSE|Products": "DESCENDING",
            "SORTCOMPONENTSTYPE|Products": "BYHIERARCHY",
            "SORTCOMPONENTSSENSE|Products": "ASCENDING",
        }

        result = exporter_module.get_hierarchy_sort_metadata(
            tm1_conn,
            "Products",
            ["Products"],
        )

        mdx = tm1_conn.cells.execute_mdx_elements_value_dict.call_args.args[0]
        assert "[}Dimensions].[}Dimensions].[Products]" in mdx
        assert "[}Dimensions].[}Dimensions].[Products:Products]" not in mdx
        assert result == {
            ("Products", "Products"): {
                "ElementsSortType": "ByName",
                "ElementsSortSense": "Descending",
                "ComponentsSortType": "ByHierarchy",
                "ComponentsSortSense": "Ascending",
            }
        }

    def test_get_hierarchy_sort_metadata_uses_default_member_for_case_variant_default_hierarchy(self):
        tm1_conn = mock.Mock()
        tm1_conn.cells.execute_mdx_elements_value_dict.return_value = {
            "SORTELEMENTSTYPE|}Views_TestCube3WithView": "BYNAME",
        }

        exporter_module.get_hierarchy_sort_metadata(
            tm1_conn,
            "}Views_TestCube3WithView",
            ["}Views_testcube3withview"],
        )

        mdx = tm1_conn.cells.execute_mdx_elements_value_dict.call_args.args[0]
        assert "[}Dimensions].[}Dimensions].[}Views_TestCube3WithView]" in mdx
        assert "}Views_TestCube3WithView:}Views_testcube3withview" not in mdx

    def test_get_hierarchy_sort_metadata_uses_qualified_member_for_alternate_hierarchy(self):
        tm1_conn = mock.Mock()
        tm1_conn.cells.execute_mdx_elements_value_dict.side_effect = [
            {"SORTELEMENTSTYPE|Products": "BYINPUT"},
            {"SORTCOMPONENTSSENSE|Products:Leaves": "DESCENDING"},
        ]

        result = exporter_module.get_hierarchy_sort_metadata(
            tm1_conn,
            "Products",
            ["Products", "Leaves"],
        )

        first_mdx = tm1_conn.cells.execute_mdx_elements_value_dict.call_args_list[0].args[0]
        second_mdx = tm1_conn.cells.execute_mdx_elements_value_dict.call_args_list[1].args[0]
        assert "[}Dimensions].[}Dimensions].[Products]" in first_mdx
        assert "[}Dimensions].[}Dimensions].[Products:Products]" not in first_mdx
        assert "[}Dimensions].[}Dimensions].[Products:Leaves]" in second_mdx
        assert result == {
            ("Products", "Products"): {"ElementsSortType": "ByInput"},
            ("Products", "Leaves"): {"ComponentsSortSense": "Descending"},
        }

    def test_get_hierarchy_sort_metadata_omits_missing_or_empty_cells(self):
        tm1_conn = mock.Mock()
        tm1_conn.cells.execute_mdx_elements_value_dict.return_value = {
            "SORTELEMENTSTYPE|Products": None,
            "SORTELEMENTSSENSE|Products": "",
            "SORTCOMPONENTSTYPE|Products": "BYLEVEL",
            "IGNORED|Products": "DESCENDING",
        }

        result = exporter_module.get_hierarchy_sort_metadata(
            tm1_conn,
            "Products",
            ["Products"],
        )

        assert result == {
            ("Products", "Products"): {
                "ComponentsSortType": "ByLevel",
            }
        }

    def test_dimensions_to_model_applies_sort_metadata_to_hierarchy(self, monkeypatch):
        import uuid

        model_id = f"export_sort_metadata_{uuid.uuid4().hex}"
        tm1_conn = mock.Mock()
        hierarchy_identity = types.SimpleNamespace(
            name="Products",
            etag=None,
            cardinality=0,
        )

        monkeypatch.setattr(exporter_module, "get_dimension_names", lambda *args, **kwargs: ["Products"])
        monkeypatch.setattr(exporter_module, "get_hierarchy_names", lambda *args, **kwargs: [hierarchy_identity])
        monkeypatch.setattr(
            exporter_module,
            "get_hierarchy_sort_metadata",
            lambda *args, **kwargs: {
                ("Products", "Products"): {
                    "ElementsSortType": "ByInput",
                    "ElementsSortSense": "Descending",
                    "ComponentsSortType": "ByHierarchy",
                    "ComponentsSortSense": "Descending",
                }
            },
        )
        monkeypatch.setattr(exporter_module, "get_elements_count", lambda *args, **kwargs: 0)
        monkeypatch.setattr(exporter_module, "get_edges_count", lambda *args, **kwargs: 0)
        monkeypatch.setattr(exporter_module, "get_subsets_count", lambda *args, **kwargs: 0)

        dimensions, errors = exporter_module.dimensions_to_model(
            tm1_conn,
            model_id=model_id,
            filter_rules=FilterRules([]),
            progress_sink=exporter_module.NoopProgressSink(),
            worker_counts=exporter_module.resolve_worker_counts(1),
        )

        assert errors == []
        hierarchy = dimensions["Products"].hierarchies[0]
        assert hierarchy.elements_sort_type == "ByInput"
        assert hierarchy.elements_sort_sense == "Descending"
        assert hierarchy.components_sort_type == "ByHierarchy"
        assert hierarchy.components_sort_sense == "Descending"
        assert hierarchy.elements.sort_metadata() == {
            "ElementsSortType": "ByInput",
            "ElementsSortSense": "Descending",
            "ComponentsSortType": "ByHierarchy",
            "ComponentsSortSense": "Descending",
        }

    def test_dimensions_to_model_stores_only_serialized_sort_metadata_for_reuse(self, monkeypatch):
        import uuid

        model_id = f"export_sort_defaults_{uuid.uuid4().hex}"
        tm1_conn = mock.Mock()
        hierarchy_identity = types.SimpleNamespace(
            name="Products",
            etag=None,
            cardinality=0,
        )

        monkeypatch.setattr(exporter_module, "get_dimension_names", lambda *args, **kwargs: ["Products"])
        monkeypatch.setattr(exporter_module, "get_hierarchy_names", lambda *args, **kwargs: [hierarchy_identity])
        monkeypatch.setattr(
            exporter_module,
            "get_hierarchy_sort_metadata",
            lambda *args, **kwargs: {
                ("Products", "Products"): {
                    "ElementsSortType": "ByHierarchy",
                    "ElementsSortSense": "Ascending",
                    "ComponentsSortType": "ByName",
                    "ComponentsSortSense": "Ascending",
                }
            },
        )
        monkeypatch.setattr(exporter_module, "get_elements_count", lambda *args, **kwargs: 0)
        monkeypatch.setattr(exporter_module, "get_edges_count", lambda *args, **kwargs: 0)
        monkeypatch.setattr(exporter_module, "get_subsets_count", lambda *args, **kwargs: 0)

        dimensions, errors = exporter_module.dimensions_to_model(
            tm1_conn,
            model_id=model_id,
            filter_rules=FilterRules([]),
            progress_sink=exporter_module.NoopProgressSink(),
            worker_counts=exporter_module.resolve_worker_counts(1),
        )

        assert errors == []
        hierarchy = dimensions["Products"].hierarchies[0]
        assert hierarchy.elements.sort_metadata() == {
            "ElementsSortType": "ByHierarchy",
            "ComponentsSortType": "ByName",
        }

    def test_hierarchy_completion_callbacks_are_deferred_to_the_coordinator(self):
        from concurrent.futures import Future

        hierarchy_future = exporter_module.HierarchyFuture(
            dimension_name="Products",
            hierarchy=mock.Mock(),
        )
        page_future = Future()
        completed = []
        hierarchy_future.add_page(
            "elements",
            skip=0,
            top=100_000,
            future=page_future,
        )
        hierarchy_future.add_elements_done_callback(completed.append)

        page_future.set_result(None)

        assert completed == []
        hierarchy_future.notify_successful_page_kind("elements")
        assert completed == [hierarchy_future]

    def test_dimensions_discards_hierarchy_after_page_failure_and_skips_finalization(
        self, monkeypatch
    ):
        import uuid

        class FakeContentHashCalculator:
            def __init__(self, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return None

        model_id = f"export_failed_page_{uuid.uuid4().hex}"
        hierarchy_identity = types.SimpleNamespace(
            name="Products",
            etag=None,
            cardinality=1,
        )
        monkeypatch.setattr(exporter_module, "ContentHashCalculator", FakeContentHashCalculator)
        monkeypatch.setattr(exporter_module, "get_dimension_names", lambda *args, **kwargs: ["Products"])
        monkeypatch.setattr(exporter_module, "get_hierarchy_names", lambda *args, **kwargs: [hierarchy_identity])
        monkeypatch.setattr(exporter_module, "get_hierarchy_sort_metadata", lambda *args, **kwargs: {})
        monkeypatch.setattr(exporter_module, "get_elements_count", lambda *args, **kwargs: 1)
        monkeypatch.setattr(exporter_module, "get_edges_count", lambda *args, **kwargs: 0)
        monkeypatch.setattr(exporter_module, "get_subsets_count", lambda *args, **kwargs: 0)

        def fail_page(*args, **kwargs):
            raise RuntimeError("elements page unavailable")

        monkeypatch.setattr(exporter_module, "_get_elements_page", fail_page)
        with mock.patch.object(
            exporter_module.Hierarchy,
            "persist_elements_etag",
        ) as persist_elements_etag:
            dimensions, errors = exporter_module.dimensions_to_model(
                mock.Mock(),
                model_id=model_id,
                filter_rules=FilterRules([]),
                progress_sink=exporter_module.NoopProgressSink(),
                worker_counts=exporter_module.resolve_worker_counts(1),
            )

            assert dimensions == {}
            assert [(error.phase, error.subject) for error in errors] == [
                (
                    "hierarchy_page",
                    "Dimensions('Products')/Hierarchies('Products')?$skip=0&$top=100000",
                )
            ]
            persist_elements_etag.assert_not_called()

    def test_dimensions_orders_count_future_errors_by_hierarchy_work_item(self, monkeypatch):
        import uuid

        class FakeContentHashCalculator:
            def __init__(self, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return None

        model_id = f"export_failed_counts_{uuid.uuid4().hex}"
        hierarchy_identity = types.SimpleNamespace(
            name="Products",
            etag=None,
            cardinality=0,
        )
        monkeypatch.setattr(exporter_module, "ContentHashCalculator", FakeContentHashCalculator)
        monkeypatch.setattr(exporter_module, "get_dimension_names", lambda *args, **kwargs: ["Products"])
        monkeypatch.setattr(exporter_module, "get_hierarchy_names", lambda *args, **kwargs: [hierarchy_identity])
        monkeypatch.setattr(exporter_module, "get_hierarchy_sort_metadata", lambda *args, **kwargs: {})

        def failing_count(label):
            def _count(*args, **kwargs):
                raise RuntimeError(f"{label} count unavailable")

            return _count

        monkeypatch.setattr(exporter_module, "get_elements_count", failing_count("elements"))
        monkeypatch.setattr(exporter_module, "get_edges_count", failing_count("edges"))
        monkeypatch.setattr(exporter_module, "get_subsets_count", failing_count("subsets"))

        dimensions, errors = exporter_module.dimensions_to_model(
            mock.Mock(),
            model_id=model_id,
            filter_rules=FilterRules([]),
            progress_sink=exporter_module.NoopProgressSink(),
            worker_counts=exporter_module.resolve_worker_counts(4),
        )

        assert dimensions == {}
        assert [error.message for error in errors] == [
            "elements count unavailable",
            "edges count unavailable",
            "subsets count unavailable",
        ]
        assert all(error.phase == "hierarchy_count" for error in errors)

    def test_dimensions_to_model_reuse_includes_cached_edge_cardinality(self, monkeypatch):
        import uuid

        model_id = f"export_edge_cardinality_{uuid.uuid4().hex}"
        tm1_conn = mock.Mock()
        hierarchy_identity = types.SimpleNamespace(
            name="Products",
            etag="W/etag-products",
            cardinality=3,
        )
        calls = {"elements_page": 0, "edges_page": 0}

        def _elements_page(*args, **kwargs):
            calls["elements_page"] += 1
            return types.SimpleNamespace(
                objects=[object(), object()],
                raw_rows=[
                    {"Name": "Leaf", "Type": "Numeric"},
                    {"Name": "Single", "Type": "Numeric"},
                ],
            )

        def _edges_page(*args, **kwargs):
            calls["edges_page"] += 1
            return types.SimpleNamespace(
                objects=[object(), object(), object()],
                raw_rows=[
                    {"ParentName": "A", "ComponentName": "Leaf", "Weight": 1},
                    {"ParentName": "B", "ComponentName": "Leaf", "Weight": 1},
                    {"ParentName": "A", "ComponentName": "Single", "Weight": 1},
                ],
            )

        monkeypatch.setattr(exporter_module, "get_dimension_names", lambda *args, **kwargs: ["Products"])
        monkeypatch.setattr(exporter_module, "get_hierarchy_names", lambda *args, **kwargs: [hierarchy_identity])
        monkeypatch.setattr(exporter_module, "get_hierarchy_sort_metadata", lambda *args, **kwargs: {})
        monkeypatch.setattr(exporter_module, "get_subsets_identity_etag", lambda *args, **kwargs: "subsets-id")
        monkeypatch.setattr(exporter_module, "get_elements_count", lambda *args, **kwargs: 2)
        monkeypatch.setattr(exporter_module, "get_edges_count", lambda *args, **kwargs: 3)
        monkeypatch.setattr(exporter_module, "get_subsets_count", lambda *args, **kwargs: 0)
        monkeypatch.setattr(exporter_module, "_get_elements_page", _elements_page)
        monkeypatch.setattr(exporter_module, "_get_edges_page", _edges_page)

        first_dimensions, first_errors = exporter_module.dimensions_to_model(
            tm1_conn,
            model_id=model_id,
            filter_rules=FilterRules([]),
            progress_sink=exporter_module.NoopProgressSink(),
            worker_counts=exporter_module.resolve_worker_counts(1),
        )
        assert first_errors == []
        first_hierarchy = first_dimensions["Products"].hierarchies[0]
        assert first_hierarchy.edges.cardinality() == 1

        second_dimensions, second_errors = exporter_module.dimensions_to_model(
            tm1_conn,
            model_id=model_id,
            filter_rules=FilterRules([]),
            progress_sink=exporter_module.NoopProgressSink(),
            worker_counts=exporter_module.resolve_worker_counts(1),
        )

        assert second_errors == []
        assert calls == {"elements_page": 1, "edges_page": 1}
        second_hierarchy = second_dimensions["Products"].hierarchies[0]
        assert len(second_hierarchy.elements) == 2
        assert len(second_hierarchy.edges) == 3
        assert second_hierarchy.edges.cardinality() == 1

    def test_export_no_longer_accepts_serialize(self):
        import inspect

        assert "serialize" not in inspect.signature(exporter_module.export).parameters
        assert "serialize" not in inspect.signature(exporter_module.dimensions_to_model).parameters

    def test_cmd_export_calls_export_without_serialize_then_serializes(self, monkeypatch, tmp_path):
        captured_export_kwargs = {}
        serialized: list[tuple[object, str, dict]] = []
        fake_model = Model(cubes=[], dimensions=[], processes=[], chores=[], model_id="out")

        def fake_export(*args, **kwargs):
            captured_export_kwargs.update(kwargs)
            return fake_model, {}

        monkeypatch.setattr(main_module, "TM1ServersConfig", lambda: mock.Mock(load=lambda: None))
        monkeypatch.setattr(main_module, "_tm1_connection_from_config", lambda *args, **kwargs: mock.Mock())
        monkeypatch.setattr(main_module, "_prepare_model_folder", lambda *args, **kwargs: None)
        monkeypatch.setattr(main_module, "_load_filter_rules", lambda *args, **kwargs: [])
        monkeypatch.setattr(main_module.ModelStore, "for_model_id", classmethod(lambda cls, model_id: mock.Mock()))
        monkeypatch.setattr(main_module, "TqdmProgressSink", mock.Mock())
        monkeypatch.setattr(main_module, "export", fake_export)
        monkeypatch.setattr(
            main_module,
            "serialize_model",
            lambda model, path, **kwargs: serialized.append((model, path, kwargs)),
        )

        output_dir = str(tmp_path / "out")
        args = types.SimpleNamespace(
            server="server",
            model_output_folder=output_dir,
            overwrite=True,
            filter=None,
            max_workers=1,
            log_file=None,
            debug=False,
        )

        main_module._cmd_export(args)

        assert "serialize" not in captured_export_kwargs
        assert captured_export_kwargs["max_workers"] == 1
        assert main_module.TqdmProgressSink.call_args_list == [
            mock.call(worker_count=1, base_position=0, leave=True, thread_tracing_enabled=False),
        ]
        assert serialized == [(fake_model, output_dir, {"progress_sink": mock.ANY, "max_workers": 1})]

    def test_compare_worker_split_helper(self):
        assert main_module._split_compare_workers(8) == (4, 4)
        assert main_module._split_compare_workers(7) == (3, 4)
        assert main_module._split_compare_workers(1) == (1, 1)

    def test_process_service_get_all_names_page_builds_query(self, mocker):
        tm1_conn = mocker.Mock()
        response = mocker.Mock()
        response.json.return_value = {"value": [{"Name": "P1"}], "@odata.count": 10}
        tm1_conn.connection.GET.return_value = response

        result = process_service._get_all_names_page(
            tm1_conn,
            filter="contains(Name,'Load')",
            skip=5,
            top=25,
            count=True,
        )

        tm1_conn.connection.GET.assert_called_once_with(
            "/Processes?$select=Name&$filter=contains(Name,'Load')&$skip=5&$top=25&$count=true"
        )
        assert result.names == ["P1"]
        assert result.count == 10
        assert result.skip == 5
        assert result.top == 25

    def test_process_service_get_all_names_paginates(self, mocker):
        tm1_conn = mocker.Mock()

        mocker.patch.object(
            process_service,
            "_get_all_names_page",
            side_effect=[
                process_service.ProcessNamesResult(names=["P1"], count=2, skip=0, top=1),
                process_service.ProcessNamesResult(names=["P2"], count=None, skip=1, top=1),
            ],
        )

        result = process_service.get_all_names(
            tm1_conn,
            filter="contains(Name,'Load')",
            page_size=1,
        )

        assert result == ["P1", "P2"]

    def test_edge_service_page_result_has_no_page_local_cardinality(self, mocker):
        from tm1_git_py.tm1_api import edge_service

        tm1_conn = mocker.Mock()
        response = mocker.Mock()
        response.json.return_value = {
            "value": [
                {"ParentName": "A", "ComponentName": "Leaf", "Weight": 1},
                {"ParentName": "B", "ComponentName": "Leaf", "Weight": 1},
            ],
            "@odata.count": 2,
        }
        tm1_conn.connection.GET.return_value = response

        result = edge_service._get_edges_page(
            tm1_conn,
            "Products",
            "Products",
            count=True,
        )

        assert result.count == 2
        assert len(result.objects) == 2
        assert result.raw_rows == response.json.return_value["value"]
        assert not hasattr(result, "cardinality")

    def test_procs_to_model_uses_process_service_names(self, mocker):
        from tm1_git_py.services.exporter import procs_to_model

        tm1_conn = mocker.Mock()
        tm1_conn.processes.get_all_names = mocker.Mock()
        mock_get_process_names = mocker.patch(
            "tm1_git_py.services.exporter.get_process_names",
            return_value=["MyProcess"],
        )

        tm1_conn.processes.get.return_value = types.SimpleNamespace(
            name="MyProcess",
            has_security_access=True,
            parameters=[],
            variables=[],
            prolog_procedure="",
            metadata_procedure="",
            data_procedure="",
            epilog_procedure="",
        )

        processes, errors = procs_to_model(
            tm1_conn,
            filter_rules=FilterRules([]),
        )

        mock_get_process_names.assert_called_once_with(
            tm1_conn,
            filter=None,
        )
        tm1_conn.processes.get_all_names.assert_not_called()
        assert "MyProcess" in processes
        assert errors == []

    def test_procs_to_model_reads_datasource_ui_data_and_variables_ui_data_from_process_body(self, mocker):
        from tm1_git_py.services.exporter import procs_to_model

        tm1_conn = mocker.Mock()
        mocker.patch(
            "tm1_git_py.services.exporter.get_process_names",
            return_value=["ProcWithBody"],
        )

        tm1_conn.processes.get.return_value = types.SimpleNamespace(
            name="ProcWithBody",
            has_security_access=False,
            parameters=[],
            variables=[],
            prolog_procedure="",
            metadata_procedure="",
            data_procedure="",
            epilog_procedure="",
            body=json.dumps(
                {
                    "UIData": "CubeAction=1511\fDataAction=1503\fCubeLogChanges=0\f",
                    "DataSource": {
                        "Type": "ASCII",
                        "asciiDelimiterChar": ";",
                        "dataSourceNameForServer": "sample.csv",
                    },
                    "VariablesUIData": ["VarType=32\fColType=827\f"],
                }
            ),
        )

        processes, errors = procs_to_model(
            tm1_conn,
            filter_rules=FilterRules([]),
        )

        assert errors == []
        process_obj = processes["ProcWithBody"]
        assert process_obj.datasource == {
            "Type": "ASCII",
            "asciiDelimiterChar": ";",
            "dataSourceNameForServer": "sample.csv",
        }
        assert process_obj.ui_data == "CubeAction=1511\fDataAction=1503\fCubeLogChanges=0\f"
        assert process_obj.variables_ui_data == ["VarType=32\fColType=827\f"]

    def test_cube_service_get_all_names_page_builds_query(self, mocker):
        tm1_conn = mocker.Mock()
        response = mocker.Mock()
        response.json.return_value = {"value": [{"Name": "Sales"}], "@odata.count": 5}
        tm1_conn.connection.GET.return_value = response

        result = cube_service._get_all_names_page(
            tm1_conn,
            filter="startswith(Name,'Sales')",
            skip=10,
            top=50,
            count=True,
        )

        tm1_conn.connection.GET.assert_called_once_with(
            "/Cubes?$select=Name&$filter=startswith(Name,'Sales')&$skip=10&$top=50&$count=true"
        )
        assert result.names == ["Sales"]
        assert result.count == 5
        assert result.skip == 10
        assert result.top == 50

    def test_subset_service_get_subsets_page_reads_static_element_reference_ids(self, mocker):
        tm1_conn = mocker.Mock()
        response = mocker.Mock()
        first_element_id = "Dimensions('Product')/Hierarchies('Product')/Elements('Bike')"
        second_element_id = "Dimensions('Product')/Hierarchies('Product')/Elements('Helmet')"
        response.json.return_value = {
            "value": [
                {
                    "Name": "Export",
                    "Expression": None,
                    "Elements": [
                        {"@odata.id": first_element_id},
                        {"@odata.id": second_element_id},
                    ],
                }
            ],
            "@odata.count": 1,
        }
        tm1_conn.connection.GET.return_value = response

        result = subset_service._get_subsets_page(
            tm1_conn,
            dimension_name="Product",
            hierarchy_name="Product",
            filter="startswith(Name,'Ex')",
            skip=2,
            top=10,
            count=True,
        )

        tm1_conn.connection.GET.assert_called_once_with(
            "/Dimensions('Product')/Hierarchies('Product')/Subsets"
            "?$select=Name,Expression&$expand=Elements/$ref"
            "&$filter=startswith(Name,'Ex')&$skip=2&$top=10&$count=true",
            async_requests_mode=True,
        )
        assert result.count == 1
        assert result.objects[0].element_ids == [first_element_id, second_element_id]
        assert result.objects[0].is_static is True

    def test_subset_service_get_subsets_page_keeps_dynamic_subset_expression_only(self, mocker):
        tm1_conn = mocker.Mock()
        response = mocker.Mock()
        response.json.return_value = {
            "value": [
                {
                    "Name": "All Products",
                    "Expression": "{[Product].[Product].Members}",
                    "Elements": [
                        {
                            "@odata.id": (
                                "Dimensions('Product')/Hierarchies('Product')/"
                                "Elements('Bike')"
                            )
                        }
                    ],
                }
            ]
        }
        tm1_conn.connection.GET.return_value = response

        result = subset_service._get_subsets_page(
            tm1_conn,
            dimension_name="Product",
            hierarchy_name="Product",
        )

        assert result.objects[0].expression == "{[Product].[Product].Members}"
        assert result.objects[0].element_ids == []
        assert result.objects[0].is_dynamic is True

    def test_cubes_to_model_uses_cube_service_names(self, mocker):
        from tm1_git_py.services.exporter import cubes_to_model

        tm1_conn = mocker.Mock()
        tm1_conn.cubes.get_all_names = mocker.Mock()
        mock_get_cube_names = mocker.patch(
            "tm1_git_py.services.exporter.get_cube_names",
            return_value=[],
        )

        cubes, errors = exporter_module.cubes_to_model(
            tm1_conn,
            _dimensions={},
            filter_rules=FilterRules(["Cubes('Sales*')"]),
        )

        assert cubes == {}
        assert errors == []
        _, kwargs = mock_get_cube_names.call_args
        assert kwargs["filter"] is not None
        tm1_conn.cubes.get_all_names.assert_not_called()

    def test_view_service_get_all_builds_filtered_urls(self, mocker):
        tm1_conn = mocker.Mock()
        mocker.patch.object(
            view_service.MDXView,
            "from_dict",
            return_value=types.SimpleNamespace(name="V1"),
        )
        mocker.patch.object(
            view_service.NativeView,
            "from_dict",
            return_value=types.SimpleNamespace(name="N1"),
        )
        tm1_conn.connection.GET.side_effect = [
            mocker.Mock(
                json=mocker.Mock(
                    return_value={
                        "value": [
                            {"@odata.type": "#ibm.tm1.api.v1.MDXView", "Name": "V1", "MDX": "SELECT 1 ON 0"}
                        ]
                    }
                )
            ),
            mocker.Mock(json=mocker.Mock(return_value={"value": []})),
        ]

        private_views, public_views = view_service.get_all(
            tm1_conn,
            cube_name="Sales",
            filter="startswith(Name,'Main')",
        )

        assert len(private_views) == 1
        assert len(public_views) == 0
        first_url = tm1_conn.connection.GET.call_args_list[0].args[0]
        second_url = tm1_conn.connection.GET.call_args_list[1].args[0]
        assert "/Cubes('Sales')/PrivateViews?" in first_url
        assert "&$filter=startswith(Name,'Main')" in first_url
        assert "/Cubes('Sales')/Views?" in second_url
        assert "&$filter=startswith(Name,'Main')" in second_url

    def test_view_service_normalizes_null_native_view_title_selected(self, mocker):
        tm1_conn = mocker.Mock()
        native_from_dict = mocker.patch.object(
            view_service.NativeView,
            "from_dict",
            return_value=types.SimpleNamespace(name="N1"),
        )
        tm1_conn.connection.GET.side_effect = [
            mocker.Mock(json=mocker.Mock(return_value={"value": []})),
            mocker.Mock(
                json=mocker.Mock(
                    return_value={
                        "value": [
                            {
                                "@odata.type": "#ibm.tm1.api.v1.NativeView",
                                "Name": "N1",
                                "Titles": [
                                    {
                                        "Selected": None,
                                        "Subset": {
                                            "Elements": [
                                                {"Name": "Fallback"}
                                            ]
                                        },
                                    }
                                ],
                                "Columns": [],
                                "Rows": [],
                            }
                        ]
                    }
                )
            ),
        ]

        private_views, public_views = view_service.get_all(tm1_conn, cube_name="Sales")

        assert private_views == []
        assert len(public_views) == 1
        view_payload = native_from_dict.call_args.args[0]
        assert view_payload["Titles"][0]["Selected"] == {"Name": "Fallback"}

    def test_exporter_preserves_raw_native_view_null_title_selected(self):
        tm1py_view = types.SimpleNamespace(
            name="N1",
            columns=[],
            rows=[],
            titles=[],
            suppress_empty_columns=False,
            suppress_empty_rows=False,
            format_string="0.#########",
            _tm1git_raw_view_dict={
                "Name": "N1",
                "Columns": [],
                "Rows": [],
                "Titles": [
                    {
                        "Selected": None,
                        "Subset": {
                            "Hierarchy": {
                                "Name": "H1",
                                "Dimension": {"Name": "D1"},
                            },
                            "Elements": [],
                        },
                    }
                ],
                "SuppressEmptyColumns": False,
                "SuppressEmptyRows": False,
                "FormatString": "0.#########",
            },
        )

        native_view = NativeView.from_tm1py(tm1py_view)

        assert native_view.titles[0]["Selected"] is None
        assert native_view.titles[0]["Subset"]["Hierarchy"] == {
            "@id": "Dimensions('D1')/Hierarchies('H1')"
        }

    def test_cubes_to_model_uses_view_service(self, mocker):
        from tm1_git_py.services.exporter import cubes_to_model

        tm1_conn = mocker.Mock()
        mocker.patch("tm1_git_py.services.exporter.get_cube_names", return_value=["Sales"])
        mock_get_views = mocker.patch("tm1_git_py.services.exporter.get_views", return_value=([], []))
        tm1_conn.cubes.get.return_value = types.SimpleNamespace(
            dimensions=[],
            has_rules=False,
            rules=types.SimpleNamespace(body=""),
        )

        cubes_to_model(
            tm1_conn,
            _dimensions={},
            filter_rules=FilterRules(["!Cubes('Sales')/Views('Main*')"]),
        )

        _, kwargs = mock_get_views.call_args
        assert kwargs["cube_name"] == "Sales"
        assert kwargs["filter"] is not None

    def test_cubes_to_model_keeps_filtered_dimension_references(self, mocker):
        from tm1_git_py.services.exporter import cubes_to_model

        tm1_conn = mocker.Mock()
        mocker.patch("tm1_git_py.services.exporter.get_cube_names", return_value=["Organization Units Settings"])
        mocker.patch("tm1_git_py.services.exporter.get_views", return_value=([], []))
        tm1_conn.cubes.get.return_value = types.SimpleNamespace(
            dimensions=["Versions", "Organization Units"],
            has_rules=False,
            rules=types.SimpleNamespace(body=""),
        )

        cubes, errors = cubes_to_model(
            tm1_conn,
            _dimensions={},
            filter_rules=FilterRules([
                "Dimensions('Organization Units')",
                "Dimensions('Versions')",
            ]),
        )

        assert errors == []
        cube = cubes["Organization Units Settings"]
        assert cube.dimensions == ["Versions", "Organization Units"]

        cube_json = json.loads(cube.as_json())
        assert cube_json["Dimensions"] == [
            {"@id": "Dimensions('Versions')"},
            {"@id": "Dimensions('Organization Units')"},
        ]

    def test_cubes_to_model_exports_drillthrough_rules_from_technical_cube(self, mocker):
        from tm1_git_py.services.exporter import cubes_to_model

        tm1_conn = mocker.Mock()
        mocker.patch("tm1_git_py.services.exporter.get_cube_names", return_value=["Sales"])
        mocker.patch("tm1_git_py.services.exporter.get_views", return_value=([], []))

        source_cube = types.SimpleNamespace(
            dimensions=["Versions"],
            has_rules=True,
            rules=types.SimpleNamespace(body="[] = N: 1;"),
        )
        drill_cube = types.SimpleNamespace(
            dimensions=["Versions", "}CubeDrillString"],
            has_rules=True,
            rules=types.SimpleNamespace(body="[]=s:'simple_drillthrough';"),
        )
        tm1_conn.cubes.exists.return_value = True
        tm1_conn.cubes.get.side_effect = lambda cube_name: {
            "Sales": source_cube,
            "}CubeDrill_Sales": drill_cube,
        }[cube_name]

        cubes, errors = cubes_to_model(
            tm1_conn,
            _dimensions={},
            filter_rules=FilterRules([]),
        )

        assert errors == []
        cube = cubes["Sales"]
        assert cube.get_rule_text() == "[] = N: 1;"
        assert cube.get_drillthrough_rule_text() == "[]=s:'simple_drillthrough';"

        cube_json = json.loads(cube.as_json())
        assert cube_json["Rules@Code.link"] == "Sales.rules"
        assert cube_json["DrillthroughRules@Code.link"] == "Sales.drillthrough.rules"

    def test_cubes_to_model_omits_drillthrough_rules_without_technical_cube(self, mocker):
        from tm1_git_py.services.exporter import cubes_to_model

        tm1_conn = mocker.Mock()
        mocker.patch("tm1_git_py.services.exporter.get_cube_names", return_value=["Sales"])
        mocker.patch("tm1_git_py.services.exporter.get_views", return_value=([], []))
        tm1_conn.cubes.exists.return_value = False
        tm1_conn.cubes.get.return_value = types.SimpleNamespace(
            dimensions=[],
            has_rules=False,
            rules=types.SimpleNamespace(body=""),
        )

        cubes, errors = cubes_to_model(
            tm1_conn,
            _dimensions={},
            filter_rules=FilterRules([]),
        )

        assert errors == []
        cube = cubes["Sales"]
        assert cube.drillthrough_rules == []
        assert "DrillthroughRules@Code.link" not in json.loads(cube.as_json())

    def test_cubes_to_model_filters_drillthrough_rules_by_uri(self, mocker):
        from tm1_git_py.services.exporter import cubes_to_model

        tm1_conn = mocker.Mock()
        mocker.patch("tm1_git_py.services.exporter.get_cube_names", return_value=["Sales"])
        mocker.patch("tm1_git_py.services.exporter.get_views", return_value=([], []))
        tm1_conn.cubes.exists.return_value = True
        tm1_conn.cubes.get.side_effect = lambda cube_name: {
            "Sales": types.SimpleNamespace(
                dimensions=[],
                has_rules=False,
                rules=types.SimpleNamespace(body=""),
            ),
            "}CubeDrill_Sales": types.SimpleNamespace(
                dimensions=[],
                has_rules=True,
                rules=types.SimpleNamespace(body="[]=s:'simple_drillthrough';"),
            ),
        }[cube_name]

        cubes, errors = cubes_to_model(
            tm1_conn,
            _dimensions={},
            filter_rules=FilterRules(["Cubes('Sales')/DrillthroughRules('default')"]),
        )

        assert errors == []
        assert cubes["Sales"].drillthrough_rules == []

    def test_export_no_filter_rules_disables_skip_control_flags(self, mocker):
        tm1_service = mocker.Mock()
        mock_dimensions = mocker.patch("tm1_git_py.services.exporter.dimensions_to_model", return_value=({}, []))
        mock_cubes = mocker.patch("tm1_git_py.services.exporter.cubes_to_model", return_value=({}, []))
        mock_processes = mocker.patch("tm1_git_py.services.exporter.procs_to_model", return_value=({}, []))
        mock_chores = mocker.patch("tm1_git_py.services.exporter.chores_to_model", return_value=({}, []))

        model, errors = export(tm1_service, model_id="unit-export", filter_rules=None)

        assert isinstance(model, Model)
        assert errors == []
        mock_dimensions.assert_called_once()
        args, kwargs = mock_dimensions.call_args
        expected_pf = apply_default_filter_rules(None)
        assert kwargs["filter_rules"]._normalized_rules == expected_pf._normalized_rules

    def test_export_returns_collected_errors_notifies_callback_and_logs_once(self, mocker, caplog):
        error = exporter_module.WorkflowError(
            workflow="export",
            phase="cubes",
            subject="Cubes('Sales')",
            exception_type="RuntimeError",
            message="Cube unavailable",
            severity="recoverable",
        )
        mocker.patch(
            "tm1_git_py.services.exporter.dimensions_to_model",
            return_value=({}, []),
        )
        mocker.patch(
            "tm1_git_py.services.exporter.cubes_to_model",
            return_value=({}, [error]),
        )
        mocker.patch(
            "tm1_git_py.services.exporter.procs_to_model",
            return_value=({}, []),
        )
        mocker.patch(
            "tm1_git_py.services.exporter.chores_to_model",
            return_value=({}, []),
        )
        received = []

        with caplog.at_level(exporter_module.logging.INFO, logger=exporter_module.__name__):
            model, errors = export(
                mocker.Mock(),
                model_id="unit-export",
                error_callback=received.append,
            )

        assert isinstance(model, Model)
        assert errors == [error]
        assert received == [error]
        error_logs = [
            record for record in caplog.records
            if record.name == exporter_module.__name__
            and record.getMessage().startswith("TM1 export error ")
        ]
        assert len(error_logs) == 1

    def test_export_returns_partial_model_after_fatal_phase_failure(self, mocker):
        mocker.patch(
            "tm1_git_py.services.exporter.dimensions_to_model",
            side_effect=RuntimeError("model store unavailable"),
        )
        mocker.patch(
            "tm1_git_py.services.exporter.cubes_to_model",
            return_value=({}, []),
        )
        mocker.patch(
            "tm1_git_py.services.exporter.procs_to_model",
            return_value=({}, []),
        )
        mocker.patch(
            "tm1_git_py.services.exporter.chores_to_model",
            return_value=({}, []),
        )

        model, errors = export(mocker.Mock(), model_id="unit-export")

        assert isinstance(model, Model)
        assert model.dimensions == []
        assert len(errors) == 1
        assert errors[0].phase == "dimensions"
        assert errors[0].severity == "fatal"

    def test_export_logs_info_lifecycle_milestones(self, mocker, caplog):
        mocker.patch(
            "tm1_git_py.services.exporter.dimensions_to_model",
            return_value=({}, []),
        )
        mocker.patch(
            "tm1_git_py.services.exporter.cubes_to_model",
            return_value=({}, []),
        )
        mocker.patch(
            "tm1_git_py.services.exporter.procs_to_model",
            return_value=({}, []),
        )
        mocker.patch(
            "tm1_git_py.services.exporter.chores_to_model",
            return_value=({}, []),
        )

        with caplog.at_level(exporter_module.logging.INFO, logger=exporter_module.__name__):
            export(mocker.Mock(), model_id="unit-export", max_workers=1)

        messages = [
            record.getMessage()
            for record in caplog.records
            if record.name == exporter_module.__name__
        ]
        assert any(message.startswith("Starting TM1 export ") for message in messages)
        assert any(message.startswith("TM1 export model assembled ") for message in messages)
        assert "TM1 export completed without errors" in messages

    def test_export_preserves_interrupt_and_closes_progress_manager(self, mocker):
        managers = []

        class FakeProgressManager:
            def __init__(self, sink):
                self.sink = sink
                self.started = False
                self.closed = False
                managers.append(self)

            def start(self):
                self.started = True

            def get_multi_process_progress_queue_sink(self):
                return mocker.Mock()

            def close(self):
                self.closed = True

        progress_sink = mocker.Mock()
        progress_sink.on_event.side_effect = KeyboardInterrupt
        mocker.patch(
            "tm1_git_py.services.exporter.MultiProcessProgressManager",
            FakeProgressManager,
        )

        with pytest.raises(KeyboardInterrupt):
            export(
                mocker.Mock(),
                model_id="unit-export",
                progress_sink=progress_sink,
                max_workers=8,
            )

        assert len(managers) == 1
        assert managers[0].started is True
        assert managers[0].closed is True

    def test_process_body_parse_failure_preserves_process_and_returns_diagnostic(self, mocker):
        tm1_conn = mocker.Mock()
        mocker.patch(
            "tm1_git_py.services.exporter.get_process_names",
            return_value=["ProcWithInvalidBody"],
        )
        tm1_conn.processes.get.return_value = types.SimpleNamespace(
            name="ProcWithInvalidBody",
            has_security_access=False,
            parameters=[],
            variables=[],
            prolog_procedure="",
            metadata_procedure="",
            data_procedure="",
            epilog_procedure="",
            body="{invalid json",
        )

        processes, errors = exporter_module.procs_to_model(
            tm1_conn,
            filter_rules=FilterRules([]),
        )

        assert "ProcWithInvalidBody" in processes
        assert [(error.phase, error.subject, error.severity) for error in errors] == [
            ("process_body", "Processes('ProcWithInvalidBody')", "recoverable"),
        ]

    def test_cubes_to_model_returns_structured_error_for_failed_cube(self, mocker):
        tm1_conn = mocker.Mock()
        mocker.patch("tm1_git_py.services.exporter.get_cube_names", return_value=["Sales"])
        tm1_conn.cubes.get.side_effect = RuntimeError("Cube unavailable")

        cubes, errors = exporter_module.cubes_to_model(
            tm1_conn,
            _dimensions={},
            filter_rules=FilterRules([]),
        )

        assert cubes == {}
        assert len(errors) == 1
        assert errors[0].workflow == "export"
        assert errors[0].phase == "cubes"
        assert errors[0].subject == "Cubes('Sales')"
        assert errors[0].exception_type == "RuntimeError"
        assert errors[0].severity == "recoverable"

    def test_export_non_technical_filter_rules_keep_skip_control_disabled(self, mocker):
        tm1_service = mocker.Mock()
        filter_rules = ["Processes('MyProcess*')"]
        mock_dimensions = mocker.patch("tm1_git_py.services.exporter.dimensions_to_model", return_value=({}, []))
        mock_cubes = mocker.patch("tm1_git_py.services.exporter.cubes_to_model", return_value=({}, []))
        mock_processes = mocker.patch("tm1_git_py.services.exporter.procs_to_model", return_value=({}, []))
        mocker.patch("tm1_git_py.services.exporter.chores_to_model", return_value=({}, []))

        export(tm1_service, model_id="unit-export", filter_rules=FilterRules(filter_rules))

        expected_pf = apply_default_filter_rules(FilterRules(filter_rules))
        mock_dimensions.assert_called_once()
        args, kwargs = mock_dimensions.call_args
        assert kwargs["filter_rules"]._normalized_rules == expected_pf._normalized_rules
        mock_cubes.assert_called_once()
        _, cube_kw = mock_cubes.call_args
        assert cube_kw["filter_rules"]._normalized_rules == expected_pf._normalized_rules
        mock_processes.assert_called_once()
        _, proc_kw = mock_processes.call_args
        assert proc_kw["filter_rules"]._normalized_rules == expected_pf._normalized_rules

    def test_export_technical_intent_filter_rules_enable_skip_control_flags(self, mocker):
        tm1_service = mocker.Mock()
        filter_rules = ["Dimensions('}*')", "Cubes('}*')", "Processes('}*')"]
        mock_dimensions = mocker.patch("tm1_git_py.services.exporter.dimensions_to_model", return_value=({}, []))
        mock_cubes = mocker.patch("tm1_git_py.services.exporter.cubes_to_model", return_value=({}, []))
        mock_processes = mocker.patch("tm1_git_py.services.exporter.procs_to_model", return_value=({}, []))
        mocker.patch("tm1_git_py.services.exporter.chores_to_model", return_value=({}, []))

        export(tm1_service, model_id="unit-export", filter_rules=FilterRules(filter_rules))

        expected_pf = apply_default_filter_rules(FilterRules(filter_rules))
        mock_dimensions.assert_called_once()
        args, kwargs = mock_dimensions.call_args
        assert kwargs["filter_rules"]._normalized_rules == expected_pf._normalized_rules
        mock_cubes.assert_called_once()
        _, cube_kw = mock_cubes.call_args
        assert cube_kw["filter_rules"]._normalized_rules == expected_pf._normalized_rules
        mock_processes.assert_called_once()
        _, proc_kw = mock_processes.call_args
        assert proc_kw["filter_rules"]._normalized_rules == expected_pf._normalized_rules

    def test_export_custom_filter_rules_are_forwarded_as_is(self, mocker):
        tm1_service = mocker.Mock()
        filter_rules = ["Dimensions('TestDim1*')", "Cubes('TestCube1*')"]
        mock_dimensions = mocker.patch("tm1_git_py.services.exporter.dimensions_to_model", return_value=({}, []))
        mock_cubes = mocker.patch("tm1_git_py.services.exporter.cubes_to_model", return_value=({}, []))
        mock_processes = mocker.patch("tm1_git_py.services.exporter.procs_to_model", return_value=({}, []))
        mocker.patch("tm1_git_py.services.exporter.chores_to_model", return_value=({}, []))

        export(tm1_service, model_id="unit-export", filter_rules=FilterRules(filter_rules))

        expected_pf = apply_default_filter_rules(FilterRules(filter_rules))
        mock_dimensions.assert_called_once()
        args, kwargs = mock_dimensions.call_args
        assert kwargs["filter_rules"]._normalized_rules == expected_pf._normalized_rules
        mock_cubes.assert_called_once()
        _, cube_kw = mock_cubes.call_args
        assert cube_kw["filter_rules"]._normalized_rules == expected_pf._normalized_rules
        _, proc_kw = mock_processes.call_args
        assert proc_kw["filter_rules"]._normalized_rules == expected_pf._normalized_rules
        mock_processes.assert_called_once()
        assert mock_processes.call_args.args == (tm1_service,)

    def test_export_shorthand_filter_rules_are_forwarded_canonically(self, mocker):
        tm1_service = mocker.Mock()
        filter_rules = ["Cubes/Views", "Dimensions/Hierarchies/Subsets('}*')"]
        mock_dimensions = mocker.patch("tm1_git_py.services.exporter.dimensions_to_model", return_value=({}, []))
        mock_cubes = mocker.patch("tm1_git_py.services.exporter.cubes_to_model", return_value=({}, []))
        mock_processes = mocker.patch("tm1_git_py.services.exporter.procs_to_model", return_value=({}, []))
        mocker.patch("tm1_git_py.services.exporter.chores_to_model", return_value=({}, []))

        export(tm1_service, model_id="unit-export", filter_rules=FilterRules(filter_rules))

        expected_pf = apply_default_filter_rules(FilterRules(filter_rules))
        expected_rules = expected_pf._normalized_rules
        _, dim_kw = mock_dimensions.call_args
        _, cube_kw = mock_cubes.call_args
        _, proc_kw = mock_processes.call_args
        assert dim_kw["filter_rules"]._normalized_rules == expected_rules
        assert cube_kw["filter_rules"]._normalized_rules == expected_rules
        assert proc_kw["filter_rules"]._normalized_rules == expected_rules

    def test_export_force_include_leaves_does_not_inject_default_leaves_exclude(self, mocker):
        tm1_service = mocker.Mock()
        filter_rules = ["!Dimensions('*')/Hierarchies('Leaves')"]
        mock_dimensions = mocker.patch(
            "tm1_git_py.services.exporter.dimensions_to_model",
            return_value=({}, []),
        )
        mocker.patch("tm1_git_py.services.exporter.cubes_to_model", return_value=({}, []))
        mocker.patch("tm1_git_py.services.exporter.procs_to_model", return_value=({}, []))
        mocker.patch("tm1_git_py.services.exporter.chores_to_model", return_value=({}, []))

        export(tm1_service, model_id="unit-export", filter_rules=FilterRules(filter_rules))

        _, kwargs = mock_dimensions.call_args
        expected_pf = apply_default_filter_rules(FilterRules(filter_rules))
        assert kwargs["filter_rules"]._normalized_rules == expected_pf._normalized_rules

    def test_should_exclude_path_supports_tm1project_filter_format(self):
        filter_rules = [
            "Cubes('views*')",
            "Dimensions('product*')",
            "Processes('zsys analogic operation version copy*')",
        ]

        assert should_exclude_path("Cubes('viewsSales')", filter_rules)
        assert should_exclude_path("Dimensions('ProductHierarchy')", filter_rules)
        assert should_exclude_path(
            "Processes('zsys analogic operation version copy')",
            filter_rules,
        )
        assert not should_exclude_path("Cubes('SalesCube')", filter_rules)

    def test_should_exclude_path_supports_tm1git_shorthand_child_rules(self):
        assert should_exclude_path(
            "Cubes('Sales')/Views('Default')",
            ["Cubes/Views"],
        )
        assert should_exclude_path(
            "Dimensions('Product')/Hierarchies('Product')/Subsets('}Clients')",
            ["Dimensions/Hierarchies/Subsets('}*')"],
        )
        assert not should_exclude_path(
            "Dimensions('Product')/Hierarchies('Product')/Subsets('Public')",
            ["Dimensions/Hierarchies/Subsets('}*')"],
        )

    def test_filter_rules_canonicalize_tm1git_shorthand_child_rules(self):
        pf = FilterRules([
            "Cubes/Views",
            "Dimensions/Hierarchies/Subsets('}*')",
        ])

        assert pf._normalized_rules == [
            "Cubes('*')/Views('*')",
            "Dimensions('*')/Hierarchies('*')/Subsets('}*')",
        ]
        assert pf.should_exclude("Cubes('Sales')/Views('Default')")
        assert pf.should_exclude(
            "Dimensions('Product')/Hierarchies('Product')/Subsets('}Clients')"
        )
        assert not pf.should_exclude(
            "Dimensions('Product')/Hierarchies('Product')/Subsets('Public')"
        )
        assert pf.get_rules_for_entity("view") == ["Cubes('*')/Views('*')"]
        assert pf.get_rules_for_entity("subset") == [
            "Dimensions('*')/Hierarchies('*')/Subsets('}*')"
        ]

    def test_filter_rules_canonicalize_tm1git_shorthand_root_rules(self):
        pf = FilterRules(["Processes", "Chores/Tasks"])

        assert pf._normalized_rules == [
            "Processes('*')",
            "Chores('*')/Tasks('*')",
        ]
        assert pf.should_exclude("Processes('LoadSales')")
        assert pf.should_exclude("Chores('Daily')/Tasks('LoadSales')")
        assert not pf.should_exclude("Cubes('Sales')")

    def test_filter_rules_canonicalize_shorthand_force_include(self):
        pf = FilterRules([
            "Cubes/Views",
            "!Cubes/Views('Public*')",
        ])

        assert pf._normalized_rules == [
            "Cubes('*')/Views('*')",
            "!Cubes('*')/Views('Public*')",
        ]
        assert not pf.should_exclude("Cubes('Sales')/Views('PublicDefault')")
        assert pf.should_exclude("Cubes('Sales')/Views('PrivateDefault')")

    def test_filter_rules_canonicalize_shorthand_collection_names_case_insensitively(self):
        pf = FilterRules(["cubes/views", "!dimensions/hierarchies('Leaves')"])

        assert pf._normalized_rules == [
            "Cubes('*')/Views('*')",
            "!Dimensions('*')/Hierarchies('Leaves')",
        ]
        assert pf.get_rules_for_entity("hierarchy") == [
            "!Dimensions('*')/Hierarchies('Leaves')"
        ]

    def test_filter_rules_canonicalize_shorthand_equivalent_to_explicit_rules(self):
        shorthand = FilterRules(["Dimensions/Hierarchies/Subsets('}*')"])
        explicit = FilterRules(["Dimensions('*')/Hierarchies('*')/Subsets('}*')"])

        shorthand_result = shorthand.to_tm1_subset_name_filter("Product", "Product")
        explicit_result = explicit.to_tm1_subset_name_filter("Product", "Product")

        assert shorthand_result.filter_expr == explicit_result.filter_expr
        assert shorthand_result.skip_all == explicit_result.skip_all

    def test_filter_rules_shorthand_root_odata_filter_matches_explicit_wildcard(self):
        shorthand = FilterRules(["Processes"])
        explicit = FilterRules(["Processes('*')"])

        shorthand_result = shorthand.to_tm1_name_filter("process")
        explicit_result = explicit.to_tm1_name_filter("process")

        assert shorthand_result.filter_expr == explicit_result.filter_expr
        assert shorthand_result.skip_all == explicit_result.skip_all
        assert shorthand_result.applicable_rules == ["Processes('*')"]

    def test_filter_rules_shorthand_view_odata_filter_matches_explicit_wildcard(self):
        shorthand = FilterRules(["Cubes/Views"])
        explicit = FilterRules(["Cubes('*')/Views('*')"])

        shorthand_result = shorthand.to_tm1_child_name_filter(
            parent_entity_type=EntityType.CUBE,
            parent_name="Sales",
            child_entity_type=EntityType.VIEW,
        )
        explicit_result = explicit.to_tm1_child_name_filter(
            parent_entity_type=EntityType.CUBE,
            parent_name="Sales",
            child_entity_type=EntityType.VIEW,
        )

        assert shorthand_result.filter_expr == explicit_result.filter_expr
        assert shorthand_result.skip_all == explicit_result.skip_all
        assert shorthand_result.applicable_rules == ["Cubes('*')/Views('*')"]

    def test_filter_rules_canonicalize_shorthand_preserves_rule_area_suffix(self):
        pf = FilterRules(["Cubes/Rules('default')|[Sales]=N:1;"])

        assert pf._normalized_rules == ["Cubes('*')/Rules('default')|[Sales]=N:1;"]
        assert pf.should_exclude("Cubes('Sales')/Rules('default')|[Sales]=N:1;")

    def test_filter_rules_canonicalize_shorthand_preserves_edge_identifier_slash(self):
        pf = FilterRules(["Dimensions/Hierarchies/Edges('Total'/'Leaf')"])

        assert pf._normalized_rules == [
            "Dimensions('*')/Hierarchies('*')/Edges('Total'/'Leaf')"
        ]
        result = pf.to_tm1_edge_name_filter("Product", "Product")
        assert result.filter_expr == (
            "not ((ParentName eq 'Total') and (ComponentName eq 'Leaf'))"
        )
        assert result.skip_all is False

    def test_filter_rules_canonicalize_drillthrough_rule_shorthand(self):
        pf = FilterRules(["Cubes/DrillthroughRules"])

        assert pf._normalized_rules == ["Cubes('*')/DrillthroughRules('*')"]
        assert pf.should_exclude("Cubes('Sales')/DrillthroughRules('default')")

    def test_filter_rules_keep_unknown_shorthand_invalid_in_strict_mode(self):
        with pytest.raises(ValueError, match="does not match any entity pattern"):
            FilterRules(["Servers/Cubes"], raise_on_invalid_rule=True)

    def test_import_filter_ignores_hash_comment_lines(self, tmp_path):
        rules_file = tmp_path / "filter.txt"
        rules_file.write_text(
            "# comment line\n"
            "Dimensions('A*')\n"
            "   # spaced comment line\n"
            "\n"
            "Cubes('Sales*')\n",
            encoding="utf-8",
        )

        from tm1_git_py.services.filter import import_filter

        rules = import_filter(str(rules_file))
        assert rules == ["Dimensions('A*')", "Cubes('Sales*')"]

    def test_path_filter_should_exclude(self):
        pf = FilterRules(["Dimensions('product*')", "Cubes('views*')"])
        assert pf.should_exclude("Dimensions('ProductHierarchy')")
        assert pf.should_exclude("Cubes('viewsSales')")
        assert not pf.should_exclude("Cubes('SalesCube')")

    def test_path_filter_force_include_element_keeps_parent_path_only(self):
        pf = FilterRules(
            [
                "Dimensions('Sales')",
                "!Dimensions('Sales')/Hierarchies('Main')/Elements('LeafA')",
            ]
        )
        assert not pf.should_exclude("Dimensions('Sales')")
        assert not pf.should_exclude("Dimensions('Sales')/Hierarchies('Main')")
        assert not pf.should_exclude("Dimensions('Sales')/Hierarchies('Main')/Elements('LeafA')")
        assert pf.should_exclude("Dimensions('Sales')/Hierarchies('Main')/Elements('LeafB')")
        assert pf.should_exclude("Dimensions('Sales')/Hierarchies('Other')")

    def test_path_filter_force_include_hierarchy_keeps_only_related_hierarchy(self):
        pf = FilterRules(
            [
                "Dimensions('Sales')",
                "!Dimensions('Sales')/Hierarchies('Main')",
            ]
        )
        assert not pf.should_exclude("Dimensions('Sales')")
        assert not pf.should_exclude("Dimensions('Sales')/Hierarchies('Main')")
        assert not pf.should_exclude("Dimensions('Sales')/Hierarchies('Main')/Elements('LeafA')")
        assert pf.should_exclude("Dimensions('Sales')/Hierarchies('Other')")

    def test_path_filter_element_validation_accepts_startswith_endswith(self):
        """URL identifier patterns with * at start or end are valid."""
        pf = FilterRules(["Dimensions('prod*')"])
        assert pf.has_rules
        pf2 = FilterRules(["Dimensions('*prod')"])
        assert pf2.has_rules

    def test_path_filter_element_validation_rejects_wildcard_in_middle(self):
        """URL identifier patterns with * in middle are invalid and skipped."""
        pf = FilterRules(["Dimensions('asd*asd')"])
        assert not pf.has_rules

    def test_get_relevant_name_rules_for_dimension(self):
        pf = FilterRules(
            [
                "Dimensions('BW*')",
                "!Dimensions('BW Comp*')",
                "Cubes('Sales*')",
                "Dimensions('Product')/Hierarchies('Main')",
            ]
        )

        assert pf.get_rules_for_entity("dimension") == [
            "Dimensions('BW*')",
            "!Dimensions('BW Comp*')",
        ]

    def test_get_rules_for_entity_uses_entity_regex(self):
        pf = FilterRules(
            [
                "Dimensions('*')/Hierarchies('}*')",
                "!Dimensions('Sales')/Hierarchies('Main*')",
                "Dimensions('Sales')/Hierarchies('Main*')/Elements('X*')",
                "Chores('Daily*')/Tasks('LoadData')",
            ]
        )

        assert pf.get_rules_for_entity("hierarchy") == [
            "Dimensions('*')/Hierarchies('}*')",
            "!Dimensions('Sales')/Hierarchies('Main*')",
        ]
        assert pf.get_rules_for_entity("task") == [
            "Chores('Daily*')/Tasks('LoadData')",
        ]

    def test_to_tm1_name_filter_for_dimension_with_include_and_exclude(self):
        pf = FilterRules(
            [
                "Dimensions('BW Comp*')",
                "!Dimensions('BW*')",
            ]
        )

        result = pf.to_tm1_name_filter("dimension")
        assert result.filter_expr == "(not (startswith(Name, 'BW Comp'))) or (startswith(Name, 'BW'))"
        assert result.skip_all is False

    def test_to_tm1_name_filter_for_dimension_exclude_only(self):
        pf = FilterRules(["Dimensions('BW Comp*')"])
        result = pf.to_tm1_name_filter("dimension")
        assert result.filter_expr == "not (startswith(Name, 'BW Comp'))"
        assert result.skip_all is False

    def test_to_tm1_name_filter_skip_all_when_exclude_all(self):
        """Dimensions('*') as exclude sets skip_all=True."""
        pf = FilterRules(["Dimensions('*')"])
        result = pf.to_tm1_name_filter("dimension")
        assert result.filter_expr is not None
        assert result.skip_all is True

    def test_to_tm1_dimension_filter_inherits_force_include_from_child_rules(self):
        pf = FilterRules(
            [
                "Dimensions('*')",
                "!Dimensions('BW Customers Bill To*')/Hierarchies('*')/Elements('(CH) CH AJACCIO*')",
            ]
        )
        result = pf.to_tm1_name_filter("dimension")
        assert result.filter_expr is not None
        assert "startswith(Name, 'BW Customers Bill To')" in result.filter_expr
        assert result.skip_all is False

    def test_to_tm1_hierarchy_name_filter_scopes_to_current_dimension(self):
        pf = FilterRules(
            [
                "Dimensions('*')/Hierarchies('}*')",
                "!Dimensions('Sales')/Hierarchies('Main*')",
                "Dimensions('Finance')/Hierarchies('Fin*')",
            ]
        )

        sales_result = pf.to_tm1_hierarchy_name_filter("Sales")
        assert sales_result.filter_expr == "(not (startswith(Name, '}'))) or (startswith(Name, 'Main'))"
        assert sales_result.skip_all is False
        marketing_result = pf.to_tm1_hierarchy_name_filter("Marketing")
        assert marketing_result.filter_expr == "not (startswith(Name, '}'))"
        assert marketing_result.skip_all is False

    def test_to_tm1_hierarchy_name_filter_inherits_force_include_from_element_rules(self):
        pf = FilterRules(
            [
                "Dimensions('Sales')/Hierarchies('Main*')",
                "Dimensions('Sales')/Hierarchies('Main*')/Elements('X*')",
                "!Dimensions('Sales')/Hierarchies('LeafOnly')/Elements('LeafA')",
            ]
        )

        result = pf.to_tm1_hierarchy_name_filter("Sales")
        assert result.filter_expr is not None
        assert "not (startswith(Name, 'Main'))" in result.filter_expr
        assert "Name eq 'LeafOnly'" in result.filter_expr

    def test_to_tm1_element_name_filter_uses_3_level_rules(self):
        """3-level rules (dim/hier/elem) apply when building element filter. ! = include."""
        pf = FilterRules(
            [
                "!Dimensions('Sales')/Hierarchies('Main')/Elements('X*')",
                "Dimensions('Sales')/Hierarchies('Main')/Elements('Total*')",
            ]
        )
        result = pf.to_tm1_element_name_filter("Sales", "Main")
        assert result.filter_expr == "(not (startswith(Name, 'Total'))) or (startswith(Name, 'X'))"
        assert result.skip_all is False

    def test_to_tm1_element_name_filter_ignores_2_level_rules(self):
        """2-level rules (dim/hier) do not affect element filter."""
        pf = FilterRules(
            [
                "Dimensions('Sales')/Hierarchies('Main*')",
            ]
        )
        result = pf.to_tm1_element_name_filter("Sales", "Main")
        assert result.filter_expr is None

    def test_to_tm1_subset_name_filter_uses_3_level_rules(self):
        """3-level rules (dim/hier/subset) apply when building subset filter. ! = include."""
        pf = FilterRules(
            [
                "!Dimensions('Sales')/Hierarchies('Main')/Subsets('Default*')",
            ]
        )
        result = pf.to_tm1_subset_name_filter("Sales", "Main")
        assert result.filter_expr == "startswith(Name, 'Default')"
        assert result.skip_all is False

    def test_to_tm1_child_name_filter_with_parent_chain(self):
        """to_tm1_child_name_filter accepts parent_chain for 3-level. ! = include."""
        pf = FilterRules(
            [
                "!Dimensions('Sales')/Hierarchies('Main')/Elements('X*')",
            ]
        )
        result = pf.to_tm1_child_name_filter(
            parent_chain=[
                (EntityType.DIMENSION, "Sales"),
                (EntityType.HIERARCHY, "Main"),
            ],
            child_entity_type=EntityType.ELEMENT,
        )
        assert result.filter_expr == "startswith(Name, 'X')"

    def test_to_tm1_name_filter_multiple_excludes_are_anded(self):
        pf = FilterRules(
            [
                "Dimensions('BW*')",
                "Dimensions('*Comp')",
            ]
        )

        result = pf.to_tm1_name_filter("dimension")
        assert result.filter_expr == "(not (startswith(Name, 'BW'))) and (not (endswith(Name, 'Comp')))"
        assert result.skip_all is False

    def test_to_tm1_edge_name_filter_parent_component_format(self):
        """Edge rules use Edges('parentName'/'componentName') format."""
        pf = FilterRules(
            [
                "Dimensions('Sales')/Hierarchies('Main')/Edges('Total*'/'*')",
                "!Dimensions('Sales')/Hierarchies('Main')/Edges('*'/'Leaf*')",
            ]
        )
        result = pf.to_tm1_edge_name_filter("Sales", "Main")
        assert "ParentName" in result.filter_expr
        assert "ComponentName" in result.filter_expr
        assert "Total" in result.filter_expr
        assert "Leaf" in result.filter_expr
        assert result.skip_all is False

    def test_to_tm1_edge_name_filter_wildcard_all(self):
        """Edges('*') as exclude rule sets skip_all=True (no TM1 call needed)."""
        pf = FilterRules(
            ["Dimensions('*')/Hierarchies('*')/Edges('*')"]  # exclude all edges
        )
        result = pf.to_tm1_edge_name_filter("Dim", "Hier")
        assert result.filter_expr is not None
        assert result.skip_all is True

    def test_to_tm1_filter_skip_all_false_when_include_present(self):
        """skip_all is False when any include rule exists."""
        pf = FilterRules(
            ["!Dimensions('*')/Hierarchies('*')/Edges('*')"]  # include all
        )
        result = pf.to_tm1_edge_name_filter("Dim", "Hier")
        assert result.skip_all is False

    def test_filter_rules_raises_on_invalid_rule_when_strict(self):
        """When raise_on_invalid_rule=True, invalid rules raise ValueError."""
        with pytest.raises(ValueError, match="does not match any entity pattern"):
            FilterRules(
                ["Dimensions('BW*')", "InvalidRule('x')"],
                raise_on_invalid_rule=True,
            )

    def test_filter_rules_skips_invalid_rule_when_not_strict(self):
        """When raise_on_invalid_rule=False, invalid rules are silently skipped."""
        pf = FilterRules(
            ["Dimensions('BW*')", "InvalidRule('x')"],
            raise_on_invalid_rule=False,
        )
        assert pf.get_rules_for_entity("dimension") == ["Dimensions('BW*')"]
