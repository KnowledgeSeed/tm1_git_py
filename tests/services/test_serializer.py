from tests.unit_common import *
import pickle
import threading


class TestSerializer:

    def test_serialize_model_uses_serial_mode_when_resolved_cpu_worker_is_one(self, tmp_path, monkeypatch):
        import tm1_git_py.services.serializer as serializer_module

        seen = {}

        class FakeProcessPoolExecutor:
            def __init__(self, *_args, **_kwargs):
                raise AssertionError("process pool should not be created")

        monkeypatch.setattr(serializer_module, "ProcessPoolExecutor", FakeProcessPoolExecutor)
        monkeypatch.setattr(
            serializer_module,
            "serialize_dimensions",
            lambda *_args, **kwargs: seen.setdefault("dimension_process_pool", kwargs.get("process_pool")),
        )
        monkeypatch.setattr(
            serializer_module,
            "serialize_cubes",
            lambda *_args, **kwargs: seen.setdefault("cube_workers", kwargs.get("max_workers")),
        )
        monkeypatch.setattr(
            serializer_module,
            "serialize_processes",
            lambda *_args, **kwargs: seen.setdefault("process_workers", kwargs.get("max_workers")),
        )
        monkeypatch.setattr(
            serializer_module,
            "serialize_chores",
            lambda *_args, **kwargs: seen.setdefault("chore_workers", kwargs.get("max_workers")),
        )

        model = Model(
            cubes=[],
            dimensions=[Dimension(name="D", hierarchies=[], defaultHierarchy=None)],
            processes=[],
            chores=[],
        )

        serialize_model(model, str(tmp_path), max_workers=1)

        assert seen == {
            "dimension_process_pool": None,
        }

    def test_serializer_round_trip_sanity_check(self, tmp_path):
        model = build_mock_model()
        assert serialize_model(model, str(tmp_path), max_workers=1) == []
        model_deserialized, errors = deserialize_model(str(tmp_path))
        assert model.to_dict() == model_deserialized.to_dict()

    def test_serialize_model_collects_serial_hierarchy_failure_and_keeps_siblings(self, tmp_path, monkeypatch):
        import tm1_git_py.services.serializer as serializer_module

        successful_hierarchy = Hierarchy(
            name="Working",
            dimension_name="D",
            elements=[Element(name="E1", type="Numeric")],
            edges=[],
            subsets=[],
        )
        failed_hierarchy = Hierarchy(
            name="Broken",
            dimension_name="D",
            elements=[Element(name="E2", type="Numeric")],
            edges=[],
            subsets=[],
        )
        model = Model(
            cubes=[],
            dimensions=[
                Dimension(
                    name="D",
                    hierarchies=[successful_hierarchy, failed_hierarchy],
                    defaultHierarchy=successful_hierarchy,
                )
            ],
            processes=[],
            chores=[],
        )
        original_serialize_hierarchy_job = serializer_module._serialize_hierarchy_job

        def fail_one_hierarchy(job, progress_sink):
            if job["name"] == "Broken":
                raise OSError("hierarchy output is unavailable")
            return original_serialize_hierarchy_job(job, progress_sink)

        monkeypatch.setattr(
            serializer_module,
            "_serialize_hierarchy_job",
            fail_one_hierarchy,
        )
        callback_errors = []

        errors = serialize_model(
            model,
            str(tmp_path),
            max_workers=1,
            error_callback=callback_errors.append,
        )

        assert len(errors) == 1
        assert errors == callback_errors
        assert errors[0].workflow == "serialize"
        assert errors[0].phase == "hierarchy"
        assert errors[0].severity == "recoverable"
        assert errors[0].subject == str(tmp_path / "dimensions" / "D.hierarchies" / "Broken.json")
        assert (tmp_path / "dimensions" / "D.json").is_file()
        assert (tmp_path / "dimensions" / "D.hierarchies" / "Working.json").is_file()
        assert not (tmp_path / "dimensions" / "D.hierarchies" / "Broken.json").exists()

    def test_serialize_model_continues_independent_phases_after_failure(self, tmp_path, monkeypatch):
        import tm1_git_py.services.serializer as serializer_module

        model = Model(
            cubes=[Cube(name="C", dimensions=[], rules=[], views=[])],
            dimensions=[Dimension(name="D", hierarchies=[], defaultHierarchy=None)],
            processes=[],
            chores=[],
        )
        phases = []

        def fail_dimensions(*_args, **_kwargs):
            phases.append("dimensions")
            raise RuntimeError("dimension phase failed")

        def serialize_cubes_after_dimension_failure(*_args, **_kwargs):
            phases.append("cubes")

        monkeypatch.setattr(serializer_module, "serialize_dimensions", fail_dimensions)
        monkeypatch.setattr(serializer_module, "serialize_cubes", serialize_cubes_after_dimension_failure)

        errors = serialize_model(model, str(tmp_path), max_workers=1)

        assert phases == ["dimensions", "cubes"]
        assert [(error.phase, error.severity) for error in errors] == [
            ("dimensions", "fatal"),
        ]

    def test_serialize_dimensions_collects_process_pool_failures_in_submission_order(self, tmp_path, monkeypatch):
        from concurrent.futures import Future
        import tm1_git_py.services.serializer as serializer_module

        class EagerProcessPool:
            def submit(self, fn, *args):
                future = Future()
                try:
                    future.set_result(fn(*args))
                except Exception as exc:
                    future.set_exception(exc)
                return future

        first_hierarchy = Hierarchy(
            name="FirstBroken",
            dimension_name="D",
            elements=[],
            edges=[],
            subsets=[],
        )
        working_hierarchy = Hierarchy(
            name="Working",
            dimension_name="D",
            elements=[],
            edges=[],
            subsets=[],
        )
        last_hierarchy = Hierarchy(
            name="LastBroken",
            dimension_name="D",
            elements=[],
            edges=[],
            subsets=[],
        )
        dimension = Dimension(
            name="D",
            hierarchies=[first_hierarchy, working_hierarchy, last_hierarchy],
            defaultHierarchy=working_hierarchy,
        )
        original_serialize_hierarchy_job = serializer_module._serialize_hierarchy_job

        def fail_selected_hierarchies(job, progress_sink):
            if job["name"] in {"FirstBroken", "LastBroken"}:
                raise OSError(f"cannot write {job['name']}")
            return original_serialize_hierarchy_job(job, progress_sink)

        monkeypatch.setattr(
            serializer_module,
            "_serialize_hierarchy_job",
            fail_selected_hierarchies,
        )
        dim_dir = tmp_path / "dimensions"
        dim_dir.mkdir()

        errors = serializer_module.serialize_dimensions(
            [dimension],
            str(dim_dir),
            process_pool=EagerProcessPool(),
            progress_sink=serializer_module.NoopProgressSink(),
        )

        assert [error.subject for error in errors] == [
            str(dim_dir / "D.hierarchies" / "FirstBroken.json"),
            str(dim_dir / "D.hierarchies" / "LastBroken.json"),
        ]
        assert all(error.severity == "recoverable" for error in errors)
        assert (dim_dir / "D.json").is_file()
        assert (dim_dir / "D.hierarchies" / "Working.json").is_file()

    def test_serialize_cubes_collects_io_failures_and_keeps_siblings(self, tmp_path, monkeypatch):
        import tm1_git_py.services.serializer as serializer_module

        cubes_dir = tmp_path / "cubes"
        cubes_dir.mkdir()
        working_cube = Cube(name="Working", dimensions=[], rules=[], views=[])
        broken_cube = Cube(name="Broken", dimensions=[], rules=[], views=[])
        original_serialize_cube = serializer_module._serialize_cube

        def fail_one_cube(cube_job, output_dir, progress_sink):
            if cube_job["name"] == "Broken":
                raise OSError("cube output is unavailable")
            return original_serialize_cube(cube_job, output_dir, progress_sink)

        monkeypatch.setattr(serializer_module, "_serialize_cube", fail_one_cube)

        errors = serializer_module.serialize_cubes(
            [working_cube, broken_cube],
            str(cubes_dir),
            process_pool=None,
            progress_sink=serializer_module.NoopProgressSink(),
        )

        assert [(error.phase, error.subject, error.severity) for error in errors] == [
            ("cube", str(cubes_dir / "Broken.json"), "recoverable"),
        ]
        assert (cubes_dir / "Working.json").is_file()
        assert not (cubes_dir / "Broken.json").exists()

    def test_serialize_cubes_collects_process_pool_io_failures_in_submission_order(self, tmp_path, monkeypatch):
        from concurrent.futures import Future
        import tm1_git_py.services.serializer as serializer_module

        class EagerProcessPool:
            def submit(self, fn, *args):
                future = Future()
                try:
                    future.set_result(fn(*args))
                except Exception as exc:
                    future.set_exception(exc)
                return future

        cubes_dir = tmp_path / "cubes"
        cubes_dir.mkdir()
        first_broken_cube = Cube(name="FirstBroken", dimensions=[], rules=[], views=[])
        working_cube = Cube(name="Working", dimensions=[], rules=[], views=[])
        last_broken_cube = Cube(name="LastBroken", dimensions=[], rules=[], views=[])
        original_serialize_cube = serializer_module._serialize_cube

        def fail_selected_cubes(cube_job, output_dir, progress_sink):
            if cube_job["name"] in {"FirstBroken", "LastBroken"}:
                raise OSError(f"cannot write {cube_job['name']}")
            return original_serialize_cube(cube_job, output_dir, progress_sink)

        monkeypatch.setattr(serializer_module, "_serialize_cube", fail_selected_cubes)

        errors = serializer_module.serialize_cubes(
            [first_broken_cube, working_cube, last_broken_cube],
            str(cubes_dir),
            process_pool=EagerProcessPool(),
            progress_sink=serializer_module.NoopProgressSink(),
        )

        assert [error.subject for error in errors] == [
            str(cubes_dir / "FirstBroken.json"),
            str(cubes_dir / "LastBroken.json"),
        ]
        assert all(error.severity == "recoverable" for error in errors)
        assert (cubes_dir / "Working.json").is_file()

    def test_serialize_model_logs_diagnostics_once_and_at_info_summary(self, tmp_path, monkeypatch, caplog):
        import logging
        import tm1_git_py.services.serializer as serializer_module

        model = Model(
            cubes=[Cube(name="Broken", dimensions=[], rules=[], views=[])],
            dimensions=[],
            processes=[],
            chores=[],
        )

        def fail_cube(*_args, **_kwargs):
            raise OSError("cube output is unavailable")

        monkeypatch.setattr(serializer_module, "_serialize_cube", fail_cube)
        with caplog.at_level(logging.INFO, logger="tm1_git_py.services.serializer"):
            errors = serialize_model(model, str(tmp_path), max_workers=1)

        assert len(errors) == 1
        diagnostic_records = [
            record for record in caplog.records
            if "Serialization diagnostic" in record.getMessage()
        ]
        assert len(diagnostic_records) == 1
        assert diagnostic_records[0].levelno == logging.WARNING
        assert any(
            record.levelno == logging.INFO
            and "Model serialization finished" in record.getMessage()
            for record in caplog.records
        )

    def test_serialize_dimensions_updates_store_backed_source_json_mtime(self, tmp_path):
        import uuid

        model_id = f"serialize_mtime_{uuid.uuid4().hex}"
        hierarchy_obj = Hierarchy(
            name="MyHier",
            dimension_name="MyDim",
            model_id=model_id,
        )
        hierarchy_obj.elements.extend([Element(name="E1", type="Numeric")])
        hierarchy_obj.edges.extend([Edge(parent="E1", component_name="E2", weight=1)])
        hierarchy_obj.subsets.extend([Subset(name="S1", expression="{[MyDim].[MyHier].Members}")])
        dimension_obj = Dimension(
            name="MyDim",
            hierarchies=[hierarchy_obj],
            defaultHierarchy=hierarchy_obj,
        )

        dim_dir = tmp_path / "dimensions"
        dim_dir.mkdir()
        serialize_dimensions(
            [dimension_obj],
            str(dim_dir),
            process_pool=None,
            progress_sink=NoopProgressSink(),
        )

        hierarchy_path = dim_dir / "MyDim.hierarchies" / "MyHier.json"
        expected_mtime_ns = int(hierarchy_path.stat().st_mtime_ns)
        assert hierarchy_obj.elements.source_json_mtime_ns() == expected_mtime_ns
        assert hierarchy_obj.edges.source_json_mtime_ns() == expected_mtime_ns
        assert hierarchy_obj.subsets.source_json_mtime_ns() == expected_mtime_ns

    def test_hierarchy_sort_metadata_explicit_default_types_are_serialized_but_senses_are_omitted(self):
        hierarchy = Hierarchy(
            name="MyHier",
            elements=[Element(name="E1", type="Numeric")],
            edges=[],
            subsets=[],
            elements_sort_type="BYNAME",
            elements_sort_sense="ASCENDING",
            components_sort_type="ByName",
            components_sort_sense="Ascending",
        )

        payload = json.loads(hierarchy.as_json())

        assert hierarchy.effective_elements_sort_type == "ByName"
        assert hierarchy.effective_elements_sort_sense == "Ascending"
        assert hierarchy.effective_components_sort_type == "ByName"
        assert hierarchy.effective_components_sort_sense == "Ascending"
        assert payload["ElementsSortType"] == "ByName"
        assert "ElementsSortSense" not in payload
        assert payload["ComponentsSortType"] == "ByName"
        assert "ComponentsSortSense" not in payload

    def test_hierarchy_sort_metadata_missing_defaults_are_effective_but_omitted(self):
        hierarchy = Hierarchy(
            name="MyHier",
            elements=[Element(name="E1", type="Numeric")],
            edges=[],
            subsets=[],
        )

        payload = json.loads(hierarchy.as_json())

        assert hierarchy.effective_elements_sort_type == "ByName"
        assert hierarchy.effective_elements_sort_sense == "Ascending"
        assert hierarchy.effective_components_sort_type == "ByName"
        assert hierarchy.effective_components_sort_sense == "Ascending"
        assert "ElementsSortType" not in payload
        assert "ElementsSortSense" not in payload
        assert "ComponentsSortType" not in payload
        assert "ComponentsSortSense" not in payload

    def test_hierarchy_sort_metadata_serializes_non_defaults_with_tm1git_casing(self):
        hierarchy = Hierarchy(
            name="MyHier",
            elements=[Element(name="E1", type="Numeric")],
            edges=[],
            subsets=[],
            elements_sort_type="BYINPUT",
            elements_sort_sense="DESCENDING",
            components_sort_type="BYHIERARCHY",
            components_sort_sense="DESCENDING",
        )

        payload = json.loads(hierarchy.as_json())

        assert payload["ElementsSortType"] == "ByInput"
        assert payload["ElementsSortSense"] == "Descending"
        assert payload["ComponentsSortType"] == "ByHierarchy"
        assert payload["ComponentsSortSense"] == "Descending"

    def test_hierarchy_sort_metadata_omits_default_sense_but_keeps_explicit_type(self):
        hierarchy = Hierarchy(
            name="MyHier",
            elements=[Element(name="E1", type="Numeric")],
            edges=[],
            subsets=[],
            elements_sort_type="BYHIERARCHY",
            elements_sort_sense="ASCENDING",
            components_sort_type="BYNAME",
            components_sort_sense="ASCENDING",
        )

        payload = json.loads(hierarchy.as_json())

        assert payload["ElementsSortType"] == "ByHierarchy"
        assert "ElementsSortSense" not in payload
        assert payload["ComponentsSortType"] == "ByName"
        assert "ComponentsSortSense" not in payload

    def test_hierarchy_serialization_uses_internal_indexes_when_sort_metadata_exists(self):
        hierarchy = Hierarchy(
            name="MyHier",
            elements=[
                Element(name="C", type="Numeric", element_index=2),
                Element(name="A", type="Numeric", element_index=0),
                Element(name="B", type="Numeric", element_index=1),
            ],
            edges=[
                Edge(parent="P", component_name="B", weight=1, component_index=1),
                Edge(parent="P", component_name="A", weight=1, component_index=0),
            ],
            subsets=[],
            elements_sort_type="ByInput",
            components_sort_type="ByInput",
        )

        payload = json.loads(hierarchy.as_json())

        assert [element["Name"] for element in payload["Elements"]] == ["A", "B", "C"]
        assert [edge["ComponentName"] for edge in payload["Edges"]] == ["A", "B"]

    def test_hierarchy_serialization_falls_back_to_name_order_without_sort_metadata(self):
        hierarchy = Hierarchy(
            name="MyHier",
            elements=[
                Element(name="C", type="Numeric", element_index=0),
                Element(name="A", type="Numeric", element_index=2),
                Element(name="B", type="Numeric", element_index=1),
            ],
            edges=[
                Edge(parent="P", component_name="B", weight=1, component_index=0),
                Edge(parent="P", component_name="A", weight=1, component_index=1),
            ],
            subsets=[],
        )

        payload = json.loads(hierarchy.as_json())

        assert [element["Name"] for element in payload["Elements"]] == ["A", "B", "C"]
        assert [edge["ComponentName"] for edge in payload["Edges"]] == ["A", "B"]

    def test_hierarchy_sort_metadata_round_trips_from_tm1git_json_keys(self):
        hierarchy = Hierarchy.from_dict(
            {
                "Name": "MyHier",
                "Elements": [{"Name": "E1", "Type": "Numeric"}],
                "Edges": [],
                "ElementsSortType": "BYLEVEL",
                "ElementsSortSense": "DESCENDING",
                "ComponentsSortType": "BYINPUT",
                "ComponentsSortSense": "DESCENDING",
            }
        )

        assert hierarchy.to_dict()["elements_sort_type"] == "ByLevel"
        assert hierarchy.to_dict()["elements_sort_sense"] == "Descending"
        assert hierarchy.to_dict()["components_sort_type"] == "ByInput"
        assert hierarchy.to_dict()["components_sort_sense"] == "Descending"

    def test_serialize_dimensions_preserves_store_backed_hierarchy_sort_metadata(self, tmp_path):
        import uuid

        model_id = f"serialize_sort_metadata_{uuid.uuid4().hex}"
        hierarchy_obj = Hierarchy(
            name="MyHier",
            dimension_name="MyDim",
            model_id=model_id,
            elements_sort_type="BYINPUT",
            elements_sort_sense="DESCENDING",
            components_sort_type="BYHIERARCHY",
            components_sort_sense="DESCENDING",
        )
        hierarchy_obj.elements.extend([Element(name="E1", type="Numeric")])
        dimension_obj = Dimension(
            name="MyDim",
            hierarchies=[hierarchy_obj],
            defaultHierarchy=hierarchy_obj,
        )

        dim_dir = tmp_path / "dimensions"
        dim_dir.mkdir()
        serialize_dimensions(
            [dimension_obj],
            str(dim_dir),
            process_pool=None,
            progress_sink=NoopProgressSink(),
        )

        payload = json.loads(
            (dim_dir / "MyDim.hierarchies" / "MyHier.json").read_text(encoding="utf-8")
        )
        assert payload["ElementsSortType"] == "ByInput"
        assert payload["ElementsSortSense"] == "Descending"
        assert payload["ComponentsSortType"] == "ByHierarchy"
        assert payload["ComponentsSortSense"] == "Descending"

    def test_serialize_dimensions_uses_store_backed_internal_indexes_when_sort_metadata_exists(self, tmp_path):
        import uuid

        model_id = f"serialize_sort_order_{uuid.uuid4().hex}"
        hierarchy_obj = Hierarchy(
            name="MyHier",
            dimension_name="MyDim",
            model_id=model_id,
            elements_sort_type="ByInput",
            components_sort_type="ByInput",
        )
        hierarchy_obj.elements.extend_payloads(
            [
                {"Name": "C", "Type": "Numeric"},
                {"Name": "D", "Type": "Numeric"},
            ],
            start_index=2,
        )
        hierarchy_obj.elements.extend_payloads(
            [
                {"Name": "A", "Type": "Numeric"},
                {"Name": "B", "Type": "Numeric"},
            ],
            start_index=0,
        )
        hierarchy_obj.edges.extend_payloads(
            [
                {"ParentName": "P", "ComponentName": "B", "Weight": 1},
                {"ParentName": "P", "ComponentName": "A", "Weight": 1},
            ],
            start_index=0,
        )
        dimension_obj = Dimension(
            name="MyDim",
            hierarchies=[hierarchy_obj],
            defaultHierarchy=hierarchy_obj,
        )

        dim_dir = tmp_path / "dimensions"
        dim_dir.mkdir()
        serialize_dimensions(
            [dimension_obj],
            str(dim_dir),
            process_pool=None,
            progress_sink=NoopProgressSink(),
        )

        payload = json.loads(
            (dim_dir / "MyDim.hierarchies" / "MyHier.json").read_text(encoding="utf-8")
        )
        assert [element["Name"] for element in payload["Elements"]] == ["A", "B", "C", "D"]
        assert [edge["ComponentName"] for edge in payload["Edges"]] == ["B", "A"]

    def test_store_backed_and_in_memory_hierarchy_serialization_use_same_index_order(self, tmp_path):
        import uuid

        in_memory_hierarchy = Hierarchy(
            name="MyHier",
            elements=[
                Element(name="C", type="Numeric", element_index=2),
                Element(name="A", type="Numeric", element_index=0),
                Element(name="B", type="Numeric", element_index=1),
            ],
            edges=[
                Edge(parent="P", component_name="B", weight=1, component_index=1),
                Edge(parent="P", component_name="A", weight=1, component_index=0),
            ],
            subsets=[],
            elements_sort_type="ByInput",
            components_sort_type="ByInput",
        )
        in_memory_dimension = Dimension(
            name="MyDim",
            hierarchies=[in_memory_hierarchy],
            defaultHierarchy=in_memory_hierarchy,
        )

        model_id = f"serialize_store_memory_equiv_{uuid.uuid4().hex}"
        store_backed_hierarchy = Hierarchy(
            name="MyHier",
            dimension_name="MyDim",
            model_id=model_id,
            elements_sort_type="ByInput",
            components_sort_type="ByInput",
        )
        store_backed_hierarchy.elements.extend_payloads(
            [
                {"Name": "C", "Type": "Numeric"},
            ],
            start_index=2,
        )
        store_backed_hierarchy.elements.extend_payloads(
            [
                {"Name": "A", "Type": "Numeric"},
                {"Name": "B", "Type": "Numeric"},
            ],
            start_index=0,
        )
        store_backed_hierarchy.edges.extend_payloads(
            [
                {"ParentName": "P", "ComponentName": "B", "Weight": 1},
            ],
            start_index=1,
        )
        store_backed_hierarchy.edges.extend_payloads(
            [
                {"ParentName": "P", "ComponentName": "A", "Weight": 1},
            ],
            start_index=0,
        )
        store_backed_dimension = Dimension(
            name="MyDim",
            hierarchies=[store_backed_hierarchy],
            defaultHierarchy=store_backed_hierarchy,
        )

        in_memory_dir = tmp_path / "in_memory" / "dimensions"
        store_backed_dir = tmp_path / "store_backed" / "dimensions"
        in_memory_dir.mkdir(parents=True)
        store_backed_dir.mkdir(parents=True)
        serialize_dimensions(
            [in_memory_dimension],
            str(in_memory_dir),
            process_pool=None,
            progress_sink=NoopProgressSink(),
        )
        serialize_dimensions(
            [store_backed_dimension],
            str(store_backed_dir),
            process_pool=None,
            progress_sink=NoopProgressSink(),
        )

        in_memory_payload = json.loads(
            (in_memory_dir / "MyDim.hierarchies" / "MyHier.json").read_text(encoding="utf-8")
        )
        store_backed_payload = json.loads(
            (store_backed_dir / "MyDim.hierarchies" / "MyHier.json").read_text(encoding="utf-8")
        )
        assert store_backed_payload == in_memory_payload

    def test_serialize_store_backed_subsets_writes_dynamic_or_static_payload_shape(self, tmp_path):
        import uuid

        first_element_id = "Dimensions('Product')/Hierarchies('Product')/Elements('Bike')"
        second_element_id = "Dimensions('Product')/Hierarchies('Product')/Elements('Helmet')"
        model_id = f"serialize_subsets_{uuid.uuid4().hex}"
        hierarchy_obj = Hierarchy(
            name="Product",
            dimension_name="Product",
            model_id=model_id,
        )
        hierarchy_obj.subsets.extend(
            [
                Subset(name="Dynamic", expression="{[Product].[Product].Members}"),
                Subset(name="Static", element_ids=[first_element_id, second_element_id]),
            ]
        )
        dimension_obj = Dimension(
            name="Product",
            hierarchies=[hierarchy_obj],
            defaultHierarchy=hierarchy_obj,
        )

        dim_dir = tmp_path / "dimensions"
        dim_dir.mkdir()
        serialize_dimensions(
            [dimension_obj],
            str(dim_dir),
            process_pool=None,
            progress_sink=NoopProgressSink(),
        )

        subset_dir = dim_dir / "Product.hierarchies" / "Product.subsets"
        dynamic_payload = json.loads((subset_dir / "Dynamic.json").read_text(encoding="utf-8"))
        static_payload = json.loads((subset_dir / "Static.json").read_text(encoding="utf-8"))

        assert dynamic_payload == {
            "@type": "Subset",
            "Name": "Dynamic",
            "Expression": "{[Product].[Product].Members}",
        }
        assert "Elements" not in dynamic_payload
        assert static_payload == {
            "@type": "Subset",
            "Name": "Static",
            "Elements": [{"@id": first_element_id}, {"@id": second_element_id}],
        }
        assert "Expression" not in static_payload


    def test_serialize_dimensions_creates_hierarchy_and_subset_files(self, tmp_path):
        model = build_mock_model()
        serialize_model(model, str(tmp_path), max_workers=1)

        dim_dir = tmp_path / 'dimensions'
        assert dim_dir.exists()

        dimension = model.dimensions[0]
        dim_file = dim_dir / f"{dimension.name}.json"
        hierarchy_dir = dim_dir / f"{dimension.name}.hierarchies"
        hierarchy = dimension.hierarchies[0]
        hierarchy_file = hierarchy_dir / f"{hierarchy.name}.json"
        subset_dir = hierarchy_dir / f"{hierarchy.name}.subsets"

        assert dim_file.exists(), f"Dimension file missing: {dim_file}"
        dim_json = json.loads(dim_file.read_text(encoding='utf-8'))
        assert dim_json["Name"] == dimension.name

        assert hierarchy_file.exists(), f"Hierarchy file missing: {hierarchy_file}"
        hierarchy_json = json.loads(hierarchy_file.read_text(encoding='utf-8'))
        assert hierarchy_json["Name"] == hierarchy.name
        assert hierarchy_json["Elements"], "Hierarchy elements should be serialized"

        if hierarchy.subsets:
            for subset in hierarchy.subsets:
                subset_file = subset_dir / f"{subset.name}.json"
                assert subset_file.exists(), f"Subset file missing: {subset_file}"
                subset_json = json.loads(subset_file.read_text(encoding='utf-8'))
                assert subset_json["Name"] == subset.name


    def test_serialize_processes_creates_ti_and_json(self, tmp_path):
        model = build_mock_model()
        serialize_model(model, str(tmp_path), max_workers=1)

        process_dir = tmp_path / 'processes'
        assert process_dir.exists()

        process = model.processes[0]
        json_file = process_dir / f"{process.name}.json"
        ti_file = process_dir / f"{process.name}.ti"

        assert json_file.exists(), f"Process JSON file missing: {json_file}"
        json_data = json.loads(json_file.read_text(encoding='utf-8'))
        assert json_data["Name"] == process.name
        assert json_data["Code@Code.link"] == process.code_link

        assert ti_file.exists(), f"Process TI file missing: {ti_file}"
        assert ti_file.read_text(encoding='utf-8') == process.ti.ti_as_string()

    def test_serialize_processes_preserves_full_ascii_datasource_ui_data_and_variables_ui_data(self, tmp_path):
        process = Process(
            name="Proc_ASCII",
            hasSecurityAccess=False,
            code_link="Proc_ASCII.ti",
            datasource={
                "Type": "ASCII",
                "asciiDecimalSeparator": ",",
                "asciiDelimiterChar": ";",
                "asciiDelimiterType": "Character",
                "asciiHeaderRecords": 1,
                "asciiQuoteCharacter": "\"",
                "asciiThousandSeparator": ".",
                "dataSourceNameForClient": "file.csv",
                "dataSourceNameForServer": "file.csv",
            },
            parameters=[],
            variables=[],
            ti=TI("", "", "", ""),
            ui_data="CubeAction=1511\fDataAction=1503\fCubeLogChanges=0\f",
            variables_ui_data=["VarType=32\fColType=827\f"],
        )
        model = Model(cubes=[], dimensions=[], processes=[process], chores=[])

        serialize_model(model, str(tmp_path), max_workers=1)

        json_file = tmp_path / "processes" / "Proc_ASCII.json"
        payload = json.loads(json_file.read_text(encoding="utf-8"))
        assert payload["DataSource"] == {
            "Type": "ASCII",
            "asciiDecimalSeparator": ",",
            "asciiDelimiterChar": ";",
            "asciiDelimiterType": "Character",
            "asciiHeaderRecords": 1,
            "asciiQuoteCharacter": "\"",
            "asciiThousandSeparator": ".",
            "dataSourceNameForClient": "file.csv",
            "dataSourceNameForServer": "file.csv",
        }
        assert payload["UIData"] == "CubeAction=1511\fDataAction=1503\fCubeLogChanges=0\f"
        assert payload["VariablesUIData"] == ["VarType=32\fColType=827\f"]
        assert list(payload.keys()) == [
            "@type",
            "Name",
            "HasSecurityAccess",
            "Code@Code.link",
            "UIData",
            "VariablesUIData",
            "DataSource",
            "Parameters",
            "Variables",
        ]

    def test_serialize_processes_omits_ui_data_and_variables_ui_data_when_empty(self, tmp_path):
        process = Process(
            name="Proc_No_UI",
            hasSecurityAccess=False,
            code_link="Proc_No_UI.ti",
            datasource="None",
            parameters=[],
            variables=[],
            ti=TI("", "", "", ""),
            ui_data=" ",
            variables_ui_data=[],
        )
        model = Model(cubes=[], dimensions=[], processes=[process], chores=[])

        serialize_model(model, str(tmp_path), max_workers=1)

        json_file = tmp_path / "processes" / "Proc_No_UI.json"
        payload = json.loads(json_file.read_text(encoding="utf-8"))
        assert "UIData" not in payload
        assert "VariablesUIData" not in payload


    def test_serialize_chores_creates_json(self, tmp_path):
        model = build_mock_model(include_chore=True)
        serialize_model(model, str(tmp_path), max_workers=1)

        chore_dir = tmp_path / 'chores'
        assert chore_dir.exists()

        chore = model.chores[0]
        chore_file = chore_dir / f"{chore.name}.json"

        assert chore_file.exists(), f"Chore JSON file missing: {chore_file}"
        chore_data = json.loads(chore_file.read_text(encoding='utf-8'))
        assert chore_data["Name"] == chore.name
        assert chore_data["Tasks"] == chore.tasks


    def test_serialize_cubes_creates_json_views_and_rules(self, tmp_path):
        model = build_mock_model(include_rules=True, additional_views=True)
        serialize_model(model, str(tmp_path), max_workers=1)

        cube_dir = tmp_path / 'cubes'
        assert cube_dir.exists()

        cube = model.cubes[0]
        cube_json = cube_dir / f"{cube.name}.json"
        rules_file = cube_dir / f"{cube.name}.rules"
        views_dir = cube_dir / f"{cube.name}.views"

        assert cube_json.exists(), f"Cube JSON missing: {cube_json}"
        assert json.loads(cube_json.read_text(encoding='utf-8'))["Name"] == cube.name

        if cube.rules:
            assert rules_file.exists(), "Rules file should exist when cube has rules"

        assert views_dir.exists(), "Views directory missing"
        for view in cube.views:
            view_json = views_dir / f"{view.name}.json"
            view_mdx = views_dir / f"{view.name}.mdx"
            assert view_json.exists() and view_mdx.exists(), (
                f"View files missing for {view.name}: {view_json}, {view_mdx}"
            )
            assert json.loads(view_json.read_text(encoding='utf-8'))["Name"] == view.name
            assert view_mdx.read_text(encoding='utf-8') == view.mdx

    def test_serialize_cubes_creates_drillthrough_rule_link_and_file(self, tmp_path):
        cube = Cube(
            name="Sales",
            dimensions=["Versions"],
            rules=[Rule(area="[default]", full_statement="[] = N: 1;", name="default")],
            views=[],
            drillthrough_rules=[
                Rule(
                    area="[default]",
                    full_statement="[]=s:'simple_drillthrough';",
                    name="default",
                )
            ],
        )
        model = Model(cubes=[cube], dimensions=[], processes=[], chores=[])

        serialize_model(model, str(tmp_path), max_workers=1)

        cube_dir = tmp_path / "cubes"
        cube_json = json.loads((cube_dir / "Sales.json").read_text(encoding="utf-8"))
        assert cube_json["Rules@Code.link"] == "Sales.rules"
        assert cube_json["DrillthroughRules@Code.link"] == "Sales.drillthrough.rules"
        assert (cube_dir / "Sales.rules").read_text(encoding="utf-8") == "[] = N: 1;"
        assert (
            cube_dir / "Sales.drillthrough.rules"
        ).read_text(encoding="utf-8") == "[]=s:'simple_drillthrough';"

    def test_serialize_cubes_process_pool_ignores_unpicklable_cube_state(self, tmp_path):
        import tm1_git_py.services.serializer as serializer_module

        class LockBackedView:
            def __init__(self, name: str, mdx: str):
                self.name = name
                self.type = "MDXView"
                self.mdx = mdx

            def as_json(self) -> str:
                return json.dumps({"Name": self.name})

        class LockBackedCube:
            def __init__(self):
                self.name = "LockCube"
                self.rules = [object()]
                self.views = [LockBackedView(name="Default", mdx="SELECT 1 ON 0 FROM [LockCube]")]
                self._lock = threading.Lock()

            def get_rule_text(self) -> str:
                return "# rule body"

            def as_json(self) -> str:
                return json.dumps({"Name": self.name})

        cubes_dir = tmp_path / "cubes"
        cubes_dir.mkdir()
        cube = LockBackedCube()
        cube_job = serializer_module._build_cube_serialize_job(cube)
        pickle.dumps((serializer_module._serialize_cube, (cube_job, str(cubes_dir), serializer_module.NoopProgressSink())))

        serializer_module.serialize_cubes(
            [cube],
            str(cubes_dir),
            process_pool=None,
            progress_sink=serializer_module.NoopProgressSink(),
        )

        assert (cubes_dir / "LockCube.json").exists()
        assert (cubes_dir / "LockCube.rules").exists()
        assert (cubes_dir / "LockCube.views" / "Default.json").exists()
        assert (cubes_dir / "LockCube.views" / "Default.mdx").exists()


    def test_serialize_handles_special_character_names(self, tmp_path):
        special_dim_name = "}Tech Dimension"
        special_hier_name = "}Tech Hierarchy"
        special_cube_name = "}Tech Cube"
        special_view_name = "View With Space"
        special_process_name = "}Tech Process"

        hierarchy = Hierarchy(
            name=special_hier_name,
            elements=[Element(name="Item 1", type="Numeric")],
            edges=[],
            subsets=[],
        )
        dimension = Dimension(
            name=special_dim_name,
            hierarchies=[hierarchy],
            defaultHierarchy=hierarchy,
        )
        view = MDXView(
            name=special_view_name,
            mdx="SELECT {TM1SUBSETALL([}Tech Dimension].[}Tech Hierarchy])} ON 0 FROM [}Tech Cube]",
        )
        cube = Cube(
            name=special_cube_name,
            dimensions=[dimension.name],
            rules=[],
            views=[view],
        )
        ti_stub = TI("# prolog", "# metadata", "# data", "# epilog")
        process = Process(
            name=special_process_name,
            hasSecurityAccess=False,
            code_link=f"{special_process_name}.ti",
            datasource=None,
            parameters=[],
            variables=[],
            ti=ti_stub,
        )

        special_model = Model(
            cubes=[cube],
            dimensions=[dimension],
            processes=[process],
            chores=[]
        )

        serialize_model(special_model, str(tmp_path), max_workers=1)

        dim_path = tmp_path / "dimensions" / f"{special_dim_name}.json"
        cube_path = tmp_path / "cubes" / f"{special_cube_name}.json"
        view_json_path = tmp_path / "cubes" / f"{special_cube_name}.views" / f"{special_view_name}.json"
        process_json_path = tmp_path / "processes" / f"{special_process_name}.json"

        for path in [dim_path, cube_path, view_json_path, process_json_path]:
            assert path.exists(), f"Serialized file missing: {path}"
