from concurrent.futures import ThreadPoolExecutor

from tests.unit_common import *

from tm1_git_py.internal.content_hash_calculator import (
    ContentHashCalculator,
    calculate_group_content_signature,
    store_group_content_signature,
)
from tm1_git_py.reporting.progress_reporting import NoopProgressSink as _NoopProgressSink


def _call_deserialize_dimensions(dimensions_dir, model_id, **extra):
    """Wrap deserialize_dimensions with the runtime collaborators tests expect.

    Tests originally exercised the simpler positional contract; the production
    code now requires explicit progress sink / IO executor / hash calculator.
    Building them once per call keeps the test bodies focused on assertions.
    """

    store = ModelStore.for_model_id(model_id)
    with ThreadPoolExecutor(max_workers=1) as tpe, \
            ContentHashCalculator(db_path=store.db_path, max_workers=1) as chc:
        return deserialize_dimensions(
            dimensions_dir,
            model_id,
            progress=_NoopProgressSink(),
            thread_pool_executor=tpe,
            content_hash_calculator=chc,
            **extra,
        )


class TestDeserializer:

    def test_deserialize_chores(self, chores_dir=test_model_dir_base / 'chores'):
        chores, errors = deserialize_chores(chore_dir=chores_dir)
        for chore in chores.values():
            assert isinstance(chore, Chore)


    def test_deserialize_dimensions(self, dimensions_dir=test_model_dir_base / 'dimensions'):
        dimensions, errors = _call_deserialize_dimensions(dimensions_dir, test_model_dir_base.name)
        for dimension in dimensions.values():
            assert isinstance(dimension, Dimension)

    def test_deserialize_model_forwards_max_workers(self, monkeypatch, tmp_path):
        monkeypatch.setattr(deserializer_module, "deserialize_processes", lambda *_a, **_k: ({}, []))
        monkeypatch.setattr(deserializer_module, "deserialize_chores", lambda *_a, **_k: ({}, []))
        monkeypatch.setattr(deserializer_module, "deserialize_cubes", lambda *_a, **_k: ({}, []))
        observed: dict[str, object] = {}

        def _fake_dimensions(*_args, **kwargs):
            observed["thread_pool_executor"] = kwargs.get("thread_pool_executor")
            observed["content_hash_calculator"] = kwargs.get("content_hash_calculator")
            return {}, []

        monkeypatch.setattr(deserializer_module, "deserialize_dimensions", _fake_dimensions)

        model, errors = deserializer_module.deserialize_model(str(tmp_path), max_workers=8)
        # max_workers now flows through worker_counts into the IO executor and
        # the content-hash calculator that deserialize_dimensions consumes.
        assert observed["thread_pool_executor"] is not None
        assert observed["content_hash_calculator"] is not None
        from tm1_git_py.internal.worker_config import resolve_worker_counts
        expected_io_workers = resolve_worker_counts(max_workers=8, io_ratio=1).io_workers
        assert observed["thread_pool_executor"]._max_workers == expected_io_workers
        assert model.total_object_count == 0
        assert errors == []

    def test_deserialize_model_merges_helper_errors_and_notifies_callback_in_phase_order(
        self, monkeypatch, tmp_path
    ):
        process_error = deserializer_module.WorkflowError(
            workflow="deserialize",
            phase="process_json",
            subject=str(tmp_path / "processes" / "Broken.json"),
            exception_type="JSONDecodeError",
            message="invalid process",
            severity="recoverable",
        )
        chore_error = deserializer_module.WorkflowError(
            workflow="deserialize",
            phase="chore",
            subject=str(tmp_path / "chores" / "Broken.json"),
            exception_type="KeyError",
            message="missing chore field",
            severity="recoverable",
        )
        dimension_error = deserializer_module.WorkflowError(
            workflow="deserialize",
            phase="dimension_json",
            subject=str(tmp_path / "dimensions" / "Broken.json"),
            exception_type="JSONDecodeError",
            message="invalid dimension",
            severity="recoverable",
        )
        cube_error = deserializer_module.WorkflowError(
            workflow="deserialize",
            phase="cube_view",
            subject=str(tmp_path / "cubes" / "Broken.views" / "View.json"),
            exception_type="JSONDecodeError",
            message="invalid view",
            severity="recoverable",
        )
        monkeypatch.setattr(
            deserializer_module,
            "deserialize_processes",
            lambda *_a, **_k: ({}, [process_error]),
        )
        monkeypatch.setattr(
            deserializer_module,
            "deserialize_chores",
            lambda *_a, **_k: ({}, [chore_error]),
        )
        monkeypatch.setattr(
            deserializer_module,
            "deserialize_dimensions",
            lambda *_a, **_k: ({}, [dimension_error]),
        )
        monkeypatch.setattr(
            deserializer_module,
            "deserialize_cubes",
            lambda *_a, **_k: ({}, [cube_error]),
        )
        received = []

        model, errors = deserializer_module.deserialize_model(
            str(tmp_path),
            max_workers=2,
            error_callback=received.append,
        )

        assert isinstance(model, Model)
        assert errors == [process_error, chore_error, dimension_error, cube_error]
        assert received == errors

    def test_deserialize_model_returns_partial_model_after_fatal_phase_failure(
        self, monkeypatch, tmp_path
    ):
        def fail_processes(*_args, **_kwargs):
            raise RuntimeError("process folder unavailable")

        deserialize_chores = mock.Mock(return_value=({}, []))
        deserialize_dimensions = mock.Mock(return_value=({}, []))
        deserialize_cubes = mock.Mock(return_value=({}, []))
        monkeypatch.setattr(deserializer_module, "deserialize_chores", deserialize_chores)
        monkeypatch.setattr(deserializer_module, "deserialize_dimensions", deserialize_dimensions)
        monkeypatch.setattr(deserializer_module, "deserialize_cubes", deserialize_cubes)
        monkeypatch.setattr(deserializer_module, "deserialize_processes", fail_processes)

        model, errors = deserializer_module.deserialize_model(str(tmp_path), max_workers=2)

        assert isinstance(model, Model)
        assert model.processes == []
        assert [(error.phase, error.severity) for error in errors] == [
            ("processes", "fatal"),
        ]
        deserialize_chores.assert_called_once()
        deserialize_dimensions.assert_called_once()
        deserialize_cubes.assert_called_once()

    def test_deserialize_model_returns_fatal_assembly_diagnostic_and_notifies_callback(
        self, monkeypatch, tmp_path
    ):
        original_model = deserializer_module.Model
        model_construction_calls = 0

        monkeypatch.setattr(deserializer_module, "deserialize_processes", lambda *_a, **_k: ({}, []))
        monkeypatch.setattr(deserializer_module, "deserialize_chores", lambda *_a, **_k: ({}, []))
        monkeypatch.setattr(deserializer_module, "deserialize_dimensions", lambda *_a, **_k: ({}, []))
        monkeypatch.setattr(deserializer_module, "deserialize_cubes", lambda *_a, **_k: ({}, []))

        def fail_first_model_construction(*args, **kwargs):
            nonlocal model_construction_calls
            model_construction_calls += 1
            if model_construction_calls == 1:
                raise RuntimeError("model store unavailable")
            return original_model(*args, **kwargs)

        monkeypatch.setattr(deserializer_module, "Model", fail_first_model_construction)
        reported = []

        model, errors = deserializer_module.deserialize_model(
            str(tmp_path),
            max_workers=2,
            error_callback=reported.append,
        )

        assert isinstance(model, original_model)
        assert model.total_object_count == 0
        assert [(error.phase, error.subject, error.severity) for error in errors] == [
            ("model_assembly", str(tmp_path), "fatal"),
        ]
        assert reported == errors

    def test_deserialize_model_preserves_interrupt_and_closes_progress_manager(
        self, monkeypatch, tmp_path
    ):
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
                return self.sink

            def close(self):
                self.closed = True

        progress_sink = mock.Mock()
        progress_sink.on_event.side_effect = KeyboardInterrupt
        monkeypatch.setattr(
            deserializer_module,
            "MultiProcessProgressManager",
            FakeProgressManager,
        )

        with pytest.raises(KeyboardInterrupt):
            deserializer_module.deserialize_model(
                str(tmp_path),
                progress_sink=progress_sink,
                max_workers=8,
            )

        assert len(managers) == 1
        assert managers[0].started is True
        assert managers[0].closed is True

    def test_deserialize_model_logs_lifecycle_and_each_collected_error_once(
        self, monkeypatch, tmp_path, caplog
    ):
        error = deserializer_module.WorkflowError(
            workflow="deserialize",
            phase="process_json",
            subject=str(tmp_path / "processes" / "Broken.json"),
            exception_type="JSONDecodeError",
            message="invalid process",
            severity="recoverable",
        )
        monkeypatch.setattr(
            deserializer_module,
            "deserialize_processes",
            lambda *_a, **_k: ({}, [error]),
        )
        monkeypatch.setattr(deserializer_module, "deserialize_chores", lambda *_a, **_k: ({}, []))
        monkeypatch.setattr(deserializer_module, "deserialize_dimensions", lambda *_a, **_k: ({}, []))
        monkeypatch.setattr(deserializer_module, "deserialize_cubes", lambda *_a, **_k: ({}, []))

        with caplog.at_level("DEBUG", logger=deserializer_module.__name__):
            _model, errors = deserializer_module.deserialize_model(str(tmp_path), max_workers=2)

        messages = [
            record.getMessage()
            for record in caplog.records
            if record.name == deserializer_module.__name__
        ]
        error_messages = [
            message for message in messages
            if message.startswith("Model deserialization error ")
        ]
        assert errors == [error]
        assert any(message.startswith("Starting model deserialization ") for message in messages)
        assert "Model deserialization completed with errors total=1 recoverable=1 fatal=0" in messages
        assert len(error_messages) == 1
        assert not [
            record for record in caplog.records
            if record.name == deserializer_module.__name__
            and record.levelname == "DEBUG"
        ]

    def test_deserialize_chores_keeps_valid_chore_after_unsupported_artifact(self, tmp_path):
        chores_dir = tmp_path / "chores"
        chores_dir.mkdir()
        (chores_dir / "Good.json").write_text(
            json.dumps(
                {
                    "Name": "Good",
                    "StartTime": "00:00:00",
                    "DSTSensitive": False,
                    "Active": True,
                    "ExecutionMode": "SingleCommit",
                    "Frequency": "P0Y0M1DT0H0M0S",
                    "Tasks": [],
                }
            ),
            encoding="utf-8",
        )
        unsupported_path = chores_dir / "notes.txt"
        unsupported_path.write_text("not a chore", encoding="utf-8")

        chores, errors = deserialize_chores(chores_dir)

        assert list(chores) == ["Good"]
        assert [(error.phase, error.subject, error.severity) for error in errors] == [
            ("chore_artifact", str(unsupported_path), "recoverable"),
        ]

    def test_deserialize_cubes_keeps_valid_cube_after_malformed_cube_artifact(self, tmp_path):
        cubes_dir = tmp_path / "cubes"
        cubes_dir.mkdir()
        broken_cube_path = cubes_dir / "Broken.json"
        broken_cube_path.write_text("{invalid json", encoding="utf-8")
        (cubes_dir / "Good.json").write_text(
            json.dumps({"Name": "Good", "Dimensions": []}),
            encoding="utf-8",
        )

        cubes, errors = deserialize_cubes(cubes_dir, _dimensions={})

        assert list(cubes) == ["Good"]
        assert [(error.phase, error.subject, error.severity) for error in errors] == [
            ("cube_json", str(broken_cube_path), "recoverable"),
        ]

    def test_deserialize_process_reports_missing_ti_pair_with_the_missing_path(self, tmp_path):
        processes_dir = tmp_path / "processes"
        processes_dir.mkdir()
        (processes_dir / "MissingTi.json").write_text(
            json.dumps(
                {
                    "Name": "MissingTi",
                    "HasSecurityAccess": False,
                    "Code@Code.link": "MissingTi.ti",
                    "Parameters": [],
                    "Variables": [],
                }
            ),
            encoding="utf-8",
        )

        processes, errors = deserialize_processes(processes_dir)

        assert processes == {}
        assert [(error.phase, error.subject, error.exception_type) for error in errors] == [
            ("process_ti", str(processes_dir / "MissingTi.ti"), "FileNotFoundError"),
        ]

    def test_deserialize_dimensions_keeps_valid_hierarchy_when_configured_default_fails(
        self, tmp_path, monkeypatch
    ):
        dimensions_dir = tmp_path / "dimensions"
        hierarchy_dir = dimensions_dir / "MyDim.hierarchies"
        hierarchy_dir.mkdir(parents=True)
        dimension_path = dimensions_dir / "MyDim.json"
        dimension_path.write_text(
            json.dumps(
                {
                    "Name": "MyDim",
                    "DefaultHierarchy": {
                        "@id": "Dimensions('MyDim')/Hierarchies('Broken')"
                    },
                }
            ),
            encoding="utf-8",
        )
        (hierarchy_dir / "Broken.json").write_text("{}", encoding="utf-8")
        (hierarchy_dir / "Valid.json").write_text("{}", encoding="utf-8")

        def fake_hierarchy(*, hierarchy_name, **_kwargs):
            if hierarchy_name == "Broken":
                raise RuntimeError("broken hierarchy")
            return Hierarchy(name=hierarchy_name, elements=[], edges=[], subsets=[])

        monkeypatch.setattr(deserializer_module, "_deserialize_single_hierarchy", fake_hierarchy)

        dimensions, errors = _call_deserialize_dimensions(dimensions_dir, tmp_path.name)

        assert list(dimensions) == ["MyDim"]
        assert dimensions["MyDim"].defaultHierarchy.name == "Valid"
        assert [(error.phase, error.subject) for error in errors] == [
            ("hierarchy", "Dimensions('MyDim')/Hierarchies('Broken')"),
            ("default_hierarchy", str(dimension_path)),
        ]

    def test_deserialize_dimensions_orders_hierarchy_future_errors_by_submission(
        self, tmp_path, monkeypatch
    ):
        import time

        dimensions_dir = tmp_path / "dimensions"
        hierarchy_dir = dimensions_dir / "MyDim.hierarchies"
        hierarchy_dir.mkdir(parents=True)
        (dimensions_dir / "MyDim.json").write_text(
            json.dumps(
                {
                    "Name": "MyDim",
                    "DefaultHierarchy": {
                        "@id": "Dimensions('MyDim')/Hierarchies('Valid')"
                    },
                }
            ),
            encoding="utf-8",
        )
        for name in ("AFail", "BFail", "Valid"):
            (hierarchy_dir / f"{name}.json").write_text("{}", encoding="utf-8")

        def fake_hierarchy(*, hierarchy_name, **_kwargs):
            if hierarchy_name == "AFail":
                time.sleep(0.03)
                raise RuntimeError("A failed")
            if hierarchy_name == "BFail":
                raise RuntimeError("B failed")
            return Hierarchy(name=hierarchy_name, elements=[], edges=[], subsets=[])

        monkeypatch.setattr(deserializer_module, "_deserialize_single_hierarchy", fake_hierarchy)
        store = ModelStore.for_model_id(tmp_path.name + "_ordered")
        with ThreadPoolExecutor(max_workers=3) as thread_pool_executor, ContentHashCalculator(
            db_path=store.db_path,
            max_workers=1,
        ) as content_hash_calculator:
            dimensions, errors = deserializer_module.deserialize_dimensions(
                dimensions_dir,
                tmp_path.name + "_ordered",
                progress=_NoopProgressSink(),
                thread_pool_executor=thread_pool_executor,
                content_hash_calculator=content_hash_calculator,
            )

        assert list(dimensions) == ["MyDim"]
        assert [error.subject for error in errors] == [
            "Dimensions('MyDim')/Hierarchies('AFail')",
            "Dimensions('MyDim')/Hierarchies('BFail')",
        ]

    def test_tqdm_deserializer_sink_uses_dynamic_worker_slots(self, tmp_path):
        sink = TqdmProgressSink(worker_count=6, thread_tracing_enabled=True)
        try:
            assert sink.worker_count == 6
            assert len(sink._worker_bars) == 6
        finally:
            sink.close()

    def test_tqdm_deserializer_sink_extends_generic_sink(self, tmp_path):
        sink = TqdmProgressSink(worker_count=3, thread_tracing_enabled=True)
        try:
            assert isinstance(sink, TqdmProgressSink)
            assert sink.worker_count == 3
            assert len(sink._worker_bars) == 3
        finally:
            sink.close()


    def test_deserialize_dimension_with_children(self, dimensions_dir=test_model_dir_base / 'dimensions'):
        dimensions, errors = _call_deserialize_dimensions(dimensions_dir, test_model_dir_base.name)
        dim_version = dimensions.get('testbenchVersion')
        hier_version = dim_version.hierarchies[0]
        assert dim_version.name == 'testbenchVersion'
        assert hier_version.name == 'testbenchVersion'
        assert hier_version.elements[0].to_dict() == {"Name": "Actual", "Type": "Numeric"}

    def test_hierarchy_constructor_with_model_id_creates_store_backed_refs(self, tmp_path):
        model_id = f"{tmp_path.name}_internal"
        hierarchy_obj = Hierarchy(
            name="MyHier",
            dimension_name="MyDim",
            model_id=model_id,
        )
        assert isinstance(hierarchy_obj.elements, StoreBackedSequence)
        assert isinstance(hierarchy_obj.edges, StoreBackedSequence)
        assert isinstance(hierarchy_obj.subsets, StoreBackedSequence)

    def test_hierarchy_constructor_without_model_id_uses_provided_refs(self):
        elements = [Element(name="E1", type="Numeric")]
        edges = [Edge(parent="P", component_name="C", weight=1)]
        subsets = [Subset(name="S1", expression="{E1}")]
        hierarchy_obj = Hierarchy(name="MyHier", elements=elements, edges=edges, subsets=subsets)
        assert hierarchy_obj.elements is elements
        assert hierarchy_obj.edges is edges
        assert hierarchy_obj.subsets is subsets

    def test_hierarchy_as_json_in_memory_collections_remains_parseable(self):
        hierarchy_obj = Hierarchy(
            name="MyHier",
            elements=[Element(name="E1", type="Numeric")],
            edges=[Edge(parent="P", component_name="E1", weight=1)],
            subsets=[Subset(name="S1", expression="{E1}")],
        )

        payload = json.loads(hierarchy_obj.as_json())
        assert payload["@type"] == "Hierarchy"
        assert payload["Name"] == "MyHier"
        assert payload["Elements"] == [{"Name": "E1", "Type": "Numeric"}]
        assert payload["Edges"] == [{"ParentName": "P", "ComponentName": "E1", "Weight": 1}]
        assert payload["Subsets@Code.links"] == ["MyHier.subsets/S1.json"]

    def test_hierarchy_as_json_streams_store_backed_collections(self, tmp_path):
        hierarchy_obj = Hierarchy(
            name="MyHier",
            dimension_name="MyDim",
            model_id=tmp_path.name,
        )
        hierarchy_obj.elements.extend(
            [
                Element(name="E1", type="Numeric"),
                Element(name="E2", type="String"),
            ]
        )
        hierarchy_obj.edges.extend(
            [
                Edge(parent="E1", component_name="E2", weight=1),
            ]
        )
        hierarchy_obj.subsets.extend(
            [
                Subset(name="S1", expression="{E1}"),
            ]
        )

        payload = json.loads(hierarchy_obj.as_json())
        assert payload["Elements"] == [
            {"Name": "E1", "Type": "Numeric"},
            {"Name": "E2", "Type": "String"},
        ]
        assert payload["Edges"] == [
            {"ParentName": "E1", "ComponentName": "E2", "Weight": 1},
        ]
        assert payload["Subsets@Code.links"] == ["MyHier.subsets/S1.json"]

    def test_hierarchy_finalize_sorts_store_backed_groups(self, tmp_path):
        hierarchy_obj = Hierarchy(
            name="MyHier",
            dimension_name="MyDim",
            model_id=tmp_path.name,
        )
        hierarchy_obj.elements.extend(
            [
                Element(name="C", type="Numeric"),
                Element(name="B", type="String"),
                Element(name="A", type="Numeric"),
            ]
        )
        hierarchy_obj.edges.extend(
            [
                Edge(parent="Root", component_name="B", weight=2),
                Edge(parent="Root", component_name="A", weight=1),
            ]
        )
        hierarchy_obj.subsets.extend(
            [
                Subset(name="Subset_B", expression="{B}"),
                Subset(name="Subset_A", expression="{A}"),
            ]
        )

        payload = json.loads(hierarchy_obj.as_json())
        assert [(e["Name"], e["Type"]) for e in payload["Elements"]] == [
            ("A", "Numeric"),
            ("B", "String"),
            ("C", "Numeric"),
        ]
        assert [(e["ParentName"], e["ComponentName"], e["Weight"]) for e in payload["Edges"]] == [
            ("Root", "A", 1),
            ("Root", "B", 2),
        ]
        assert payload["Subsets@Code.links"] == [
            "MyHier.subsets/Subset_A.json",
            "MyHier.subsets/Subset_B.json",
        ]

        assert hierarchy_obj.elements.source_json_mtime_ns() is None
        assert hierarchy_obj.edges.source_json_mtime_ns() is None
        assert hierarchy_obj.subsets.source_json_mtime_ns() is None

    def test_hierarchy_staged_writer_overwrites_partial_content(self, tmp_path):
        model_id = tmp_path.name

        first = Hierarchy(name="MyHier", dimension_name="MyDim", model_id=model_id)
        first.elements.extend([Element(name="Old", type="Numeric")])
        # Simulate interrupted export by not finalizing/writing.

        second = Hierarchy(name="MyHier", dimension_name="MyDim", model_id=model_id)
        second.elements.extend([Element(name="New", type="Numeric")])
        payload = json.loads(second.as_json())

        assert payload["Elements"] == [{"Name": "New", "Type": "Numeric"}]

    def test_serialize_dimensions_uses_hierarchy_write_json(self, tmp_path, monkeypatch):
        hierarchy_obj = Hierarchy(
            name="MyHier",
            elements=[Element(name="E1", type="Numeric")],
            edges=[],
            subsets=[],
        )
        dimension_obj = Dimension(name="MyDim", hierarchies=[hierarchy_obj], defaultHierarchy=hierarchy_obj)
        monkeypatch.setattr(Hierarchy, "as_json", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("as_json should not be used")))

        serialize_dimensions([dimension_obj], str(tmp_path), process_pool=None, progress_sink=NoopProgressSink())

        hierarchy_file = tmp_path / "MyDim.hierarchies" / "MyHier.json"
        assert hierarchy_file.exists()
        payload = json.loads(hierarchy_file.read_text(encoding="utf-8"))
        assert payload["Name"] == "MyHier"

    def test_serialize_dimensions_writes_default_hierarchy_as_id_object(self, tmp_path):
        hierarchy_obj = Hierarchy(
            name="MyHier",
            elements=[Element(name="E1", type="Numeric")],
            edges=[],
            subsets=[],
        )
        dimension_obj = Dimension(
            name="MyDim",
            hierarchies=[hierarchy_obj],
            defaultHierarchy=hierarchy_obj,
        )

        serialize_dimensions([dimension_obj], str(tmp_path), process_pool=None, progress_sink=NoopProgressSink())

        dimension_file = tmp_path / "MyDim.json"
        payload = json.loads(dimension_file.read_text(encoding="utf-8"))
        assert payload["DefaultHierarchy"] == {
            "@id": "Dimensions('MyDim')/Hierarchies('MyHier')"
        }

    def test_serialize_dimensions_writes_model_id_backed_hierarchy(self, tmp_path):
        hierarchy_obj = Hierarchy(
            name="MyHier",
            dimension_name="MyDim",
            model_id=tmp_path.name,
        )
        hierarchy_obj.elements.extend([Element(name="E1", type="Numeric")])
        dimension_obj = Dimension(name="MyDim", hierarchies=[hierarchy_obj], defaultHierarchy=hierarchy_obj)

        dim_dir = tmp_path / "dimensions"
        dim_dir.mkdir(exist_ok=True)
        serialize_dimensions([dimension_obj], str(dim_dir), process_pool=None, progress_sink=NoopProgressSink())

        hierarchy_file = dim_dir / "MyDim.hierarchies" / "MyHier.json"
        assert hierarchy_file.exists()
        payload = json.loads(hierarchy_file.read_text(encoding="utf-8"))
        assert payload["Elements"] == [{"Name": "E1", "Type": "Numeric"}]

    def test_static_subset_elements_round_trip_through_store_backed_deserialize(self, tmp_path):
        first_element_id = "Dimensions('Product')/Hierarchies('Product')/Elements('Bike')"
        second_element_id = "Dimensions('Product')/Hierarchies('Product')/Elements('Helmet')"
        hierarchy_obj = Hierarchy(
            name="Product",
            dimension_name="Product",
            model_id=tmp_path.name + "_source",
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

        source_dimensions_dir = tmp_path / "source" / "dimensions"
        source_dimensions_dir.mkdir(parents=True)
        serialize_dimensions(
            [dimension_obj],
            str(source_dimensions_dir),
            process_pool=None,
            progress_sink=NoopProgressSink(),
        )

        dimensions, errors = _call_deserialize_dimensions(
            source_dimensions_dir,
            tmp_path.name + "_deserialized",
        )
        assert errors == []

        roundtrip_dimensions_dir = tmp_path / "roundtrip" / "dimensions"
        roundtrip_dimensions_dir.mkdir(parents=True)
        serialize_dimensions(
            list(dimensions.values()),
            str(roundtrip_dimensions_dir),
            process_pool=None,
            progress_sink=NoopProgressSink(),
        )

        subsets_dir = roundtrip_dimensions_dir / "Product.hierarchies" / "Product.subsets"
        dynamic_payload = json.loads((subsets_dir / "Dynamic.json").read_text(encoding="utf-8"))
        static_payload = json.loads((subsets_dir / "Static.json").read_text(encoding="utf-8"))
        assert dynamic_payload == {
            "@type": "Subset",
            "Name": "Dynamic",
            "Expression": "{[Product].[Product].Members}",
        }
        assert static_payload == {
            "@type": "Subset",
            "Name": "Static",
            "Elements": [{"@id": first_element_id}, {"@id": second_element_id}],
        }

    def test_subset_shapes_round_trip_from_in_memory_serializer(self, tmp_path):
        first_element_id = "Dimensions('Product')/Hierarchies('Product')/Elements('Bike')"
        second_element_id = "Dimensions('Product')/Hierarchies('Product')/Elements('Helmet')"
        hierarchy_obj = Hierarchy(
            name="Product",
            elements=[],
            edges=[],
            subsets=[
                Subset(name="Dynamic", expression="{[Product].[Product].Members}"),
                Subset(name="Static", element_ids=[first_element_id, second_element_id]),
            ],
        )
        dimension_obj = Dimension(
            name="Product",
            hierarchies=[hierarchy_obj],
            defaultHierarchy=hierarchy_obj,
        )

        source_dimensions_dir = tmp_path / "source_in_memory" / "dimensions"
        source_dimensions_dir.mkdir(parents=True)
        serialize_dimensions(
            [dimension_obj],
            str(source_dimensions_dir),
            process_pool=None,
            progress_sink=NoopProgressSink(),
        )

        dimensions, errors = _call_deserialize_dimensions(
            source_dimensions_dir,
            tmp_path.name + "_in_memory_deserialized",
        )
        assert errors == []

        roundtrip_dimensions_dir = tmp_path / "roundtrip_in_memory" / "dimensions"
        roundtrip_dimensions_dir.mkdir(parents=True)
        serialize_dimensions(
            list(dimensions.values()),
            str(roundtrip_dimensions_dir),
            process_pool=None,
            progress_sink=NoopProgressSink(),
        )

        subsets_dir = roundtrip_dimensions_dir / "Product.hierarchies" / "Product.subsets"
        assert json.loads((subsets_dir / "Dynamic.json").read_text(encoding="utf-8")) == {
            "@type": "Subset",
            "Name": "Dynamic",
            "Expression": "{[Product].[Product].Members}",
        }
        assert json.loads((subsets_dir / "Static.json").read_text(encoding="utf-8")) == {
            "@type": "Subset",
            "Name": "Static",
            "Elements": [{"@id": first_element_id}, {"@id": second_element_id}],
        }

    def test_serialize_dimensions_skips_rewrite_when_hierarchy_staged_writer_enabled(self, tmp_path, monkeypatch):
        hierarchy_obj = Hierarchy(
            name="MyHier",
            dimension_name="MyDim",
            model_id=tmp_path.name,
        )
        hierarchy_obj.elements.extend([Element(name="E1", type="Numeric")])
        dimension_obj = Dimension(name="MyDim", hierarchies=[hierarchy_obj], defaultHierarchy=hierarchy_obj)

        monkeypatch.setattr(
            Hierarchy,
            "write_json",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("write_json should be skipped")
            ),
        )

        dim_dir = tmp_path / "dimensions"
        dim_dir.mkdir(exist_ok=True)
        serialize_dimensions([dimension_obj], str(dim_dir), process_pool=None, progress_sink=NoopProgressSink())

        hierarchy_file = dim_dir / "MyDim.hierarchies" / "MyHier.json"
        assert hierarchy_file.exists()
        payload = json.loads(hierarchy_file.read_text(encoding="utf-8"))
        assert payload["Elements"] == [{"Name": "E1", "Type": "Numeric"}]

    def test_hierarchy_staged_writer_writes_to_dimensions_parent(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        model_id = "fallback_model"
        hierarchy_obj = Hierarchy(
            name="MyHier",
            dimension_name="MyDim",
            model_id=model_id,
        )
        hierarchy_obj.elements.extend([Element(name="E1", type="Numeric")])
        dimension_obj = Dimension(name="MyDim", hierarchies=[hierarchy_obj], defaultHierarchy=hierarchy_obj)

        dim_dir = tmp_path / "dimensions"
        dim_dir.mkdir(exist_ok=True)
        serialize_dimensions([dimension_obj], str(dim_dir), process_pool=None, progress_sink=NoopProgressSink())

        canonical_hierarchy_file = dim_dir / "MyDim.hierarchies" / "MyHier.json"
        assert canonical_hierarchy_file.exists()
        assert json.loads(canonical_hierarchy_file.read_text(encoding="utf-8"))["Elements"] == [
            {"Name": "E1", "Type": "Numeric"}
        ]

    def test_deserialize_dimensions_ignores_inprogress_hierarchy_files(self, tmp_path):
        src_dimensions = test_model_dir_base / "dimensions"
        dimensions_dir = tmp_path / "dimensions"
        shutil.copytree(src_dimensions, dimensions_dir)

        hier_dir = dimensions_dir / "testbenchVersion.hierarchies"
        inprogress = hier_dir / ".testbenchVersion.json.inprogress"
        inprogress.write_text('{"Name":"broken"', encoding="utf-8")

        dimensions, errors = _call_deserialize_dimensions(dimensions_dir, tmp_path.name)
        assert "testbenchVersion" in dimensions
        assert not any("inprogress" in (error.subject or "") for error in errors)

    def test_deserialize_dimensions_accepts_default_hierarchy_id_object(self, tmp_path):
        src_dimensions = test_model_dir_base / "dimensions"
        dimensions_dir = tmp_path / "dimensions"
        shutil.copytree(src_dimensions, dimensions_dir)

        dimension_file = dimensions_dir / "testbenchVersion.json"
        payload = json.loads(dimension_file.read_text(encoding="utf-8"))
        payload["DefaultHierarchy"] = {
            "@id": "Dimensions('testbenchVersion')/Hierarchies('testbenchVersion')"
        }
        dimension_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

        dimensions, errors = _call_deserialize_dimensions(dimensions_dir, tmp_path.name)
        assert "testbenchVersion" in dimensions
        assert dimensions["testbenchVersion"].defaultHierarchy.name == "testbenchVersion"
        assert not errors

    def test_get_subsets_uses_collector(self, monkeypatch):
        tm1_conn = mock.Mock()
        collector: list[Subset] = []

        sequence = iter([
                subset_service.PaginatedSubsetsResult(
                    objects=[Subset(name="S1", expression="{A}")],
                    count=2,
                    skip=0,
                    top=1,
                    raw_rows=[{"Name": "S1", "Expression": "{A}"}],
                ),
                subset_service.PaginatedSubsetsResult(
                    objects=[Subset(name="S2", expression="{B}")],
                    count=None,
                    skip=1,
                    top=1,
                    raw_rows=[{"Name": "S2", "Expression": "{B}"}],
                ),
            ]
        )
        monkeypatch.setattr(subset_service, "_get_subsets_page", lambda *args, **kwargs: next(sequence))

        result = subset_service.get_subsets(
            tm1_conn,
            dimension_name="MyDim",
            hierarchy_name="MyHier",
            page_size=1,
            collector=collector,
        )

        assert result is collector
        assert [s.name for s in collector] == ["S1", "S2"]

    def test_deserialize_dimensions_uses_store_backed_hierarchy_collections(self, tmp_path):
        src_dimensions = test_model_dir_base / "dimensions"
        dimensions_dir = tmp_path / "dimensions"
        shutil.copytree(src_dimensions, dimensions_dir)

        dimensions, _ = _call_deserialize_dimensions(dimensions_dir, tmp_path.name)
        dim_version = dimensions.get("testbenchVersion")
        assert dim_version is not None
        hier_version = dim_version.hierarchies[0]
        assert isinstance(hier_version.elements, StoreBackedSequence)
        assert isinstance(hier_version.edges, StoreBackedSequence)
        assert isinstance(hier_version.subsets, StoreBackedSequence)

    def test_deserialize_dimensions_creates_sqlite_internal_store(self, tmp_path):
        src_dimensions = test_model_dir_base / "dimensions"
        dimensions_dir = tmp_path / "dimensions"
        shutil.copytree(src_dimensions, dimensions_dir)

        _call_deserialize_dimensions(dimensions_dir, tmp_path.name)
        _call_deserialize_dimensions(dimensions_dir, tmp_path.name)
        db_path = Path(ModelStore._db_path_for_model_id(tmp_path.name))
        assert db_path.is_file(), f"expected internal model store sqlite at {db_path}"

    def test_deserialize_dimensions_populates_store_group_metadata(self, tmp_path):
        src_dimensions = test_model_dir_base / "dimensions"
        dimensions_dir = tmp_path / "dimensions"
        shutil.copytree(src_dimensions, dimensions_dir)

        dimensions, _ = _call_deserialize_dimensions(dimensions_dir, tmp_path.name)
        hierarchy_obj = dimensions["testbenchVersion"].hierarchies[0]
        assert hierarchy_obj.elements.content_signature()[0] == len(hierarchy_obj.elements)
        assert hierarchy_obj.edges.content_signature()[0] == len(hierarchy_obj.edges)
        assert hierarchy_obj.subsets.content_signature()[0] == len(hierarchy_obj.subsets)

    def test_deserialize_dimensions_preserves_hierarchy_sort_metadata_and_indexes(self, tmp_path):
        dimensions_dir = tmp_path / "dimensions"
        hierarchy_dir = dimensions_dir / "MyDim.hierarchies"
        hierarchy_dir.mkdir(parents=True)
        (dimensions_dir / "MyDim.json").write_text(
            json.dumps(
                {
                    "@type": "Dimension",
                    "Name": "MyDim",
                    "DefaultHierarchy": {
                        "@id": "Dimensions('MyDim')/Hierarchies('MyHier')"
                    },
                }
            ),
            encoding="utf-8",
        )
        (hierarchy_dir / "MyHier.json").write_text(
            json.dumps(
                {
                    "@type": "Hierarchy",
                    "Name": "MyHier",
                    "ElementsSortType": "BYINPUT",
                    "ElementsSortSense": "DESCENDING",
                    "ComponentsSortType": "BYHIERARCHY",
                    "ComponentsSortSense": "DESCENDING",
                    "Elements": [
                        {"Name": "B", "Type": "Numeric"},
                        {"Name": "A", "Type": "Numeric"},
                    ],
                    "Edges": [
                        {"ParentName": "P", "ComponentName": "B", "Weight": 1},
                        {"ParentName": "P", "ComponentName": "A", "Weight": 1},
                    ],
                    "Subsets@Code.links": [],
                }
            ),
            encoding="utf-8",
        )

        dimensions, errors = _call_deserialize_dimensions(dimensions_dir, tmp_path.name)

        assert errors == []
        hierarchy_obj = dimensions["MyDim"].hierarchies[0]
        assert hierarchy_obj.elements_sort_type == "ByInput"
        assert hierarchy_obj.elements_sort_sense == "Descending"
        assert hierarchy_obj.components_sort_type == "ByHierarchy"
        assert hierarchy_obj.components_sort_sense == "Descending"
        assert hierarchy_obj.elements.sort_metadata() == {
            "ElementsSortType": "ByInput",
            "ElementsSortSense": "Descending",
            "ComponentsSortType": "ByHierarchy",
            "ComponentsSortSense": "Descending",
        }
        assert [
            (payload["Name"], payload["ElementIndex"])
            for payload in hierarchy_obj.elements.iter_payloads(
                order_by_internal_index=True,
                include_internal_indexes=True,
            )
        ] == [("B", 0), ("A", 1)]
        assert [
            (payload["ComponentName"], payload["ComponentIndex"])
            for payload in hierarchy_obj.edges.iter_payloads(
                order_by_internal_index=True,
                include_internal_indexes=True,
            )
        ] == [("B", 0), ("A", 1)]

    def test_deserialize_dimensions_keeps_missing_sort_metadata_missing(self, tmp_path):
        dimensions_dir = tmp_path / "dimensions"
        hierarchy_dir = dimensions_dir / "MyDim.hierarchies"
        hierarchy_dir.mkdir(parents=True)
        (dimensions_dir / "MyDim.json").write_text(
            json.dumps(
                {
                    "@type": "Dimension",
                    "Name": "MyDim",
                    "DefaultHierarchy": {
                        "@id": "Dimensions('MyDim')/Hierarchies('MyHier')"
                    },
                }
            ),
            encoding="utf-8",
        )
        (hierarchy_dir / "MyHier.json").write_text(
            json.dumps(
                {
                    "@type": "Hierarchy",
                    "Name": "MyHier",
                    "Elements": [{"Name": "A", "Type": "Numeric"}],
                    "Subsets@Code.links": [],
                }
            ),
            encoding="utf-8",
        )

        dimensions, errors = _call_deserialize_dimensions(dimensions_dir, tmp_path.name)

        assert errors == []
        hierarchy_obj = dimensions["MyDim"].hierarchies[0]
        assert hierarchy_obj.elements_sort_type is None
        assert hierarchy_obj.elements_sort_sense is None
        assert hierarchy_obj.components_sort_type is None
        assert hierarchy_obj.components_sort_sense is None
        assert hierarchy_obj.effective_elements_sort_type == "ByName"
        assert hierarchy_obj.effective_elements_sort_sense == "Ascending"
        assert hierarchy_obj.elements.sort_metadata() == {}

    def test_deserialize_dimensions_keeps_explicit_default_types_but_omits_default_senses(self, tmp_path):
        dimensions_dir = tmp_path / "dimensions"
        hierarchy_dir = dimensions_dir / "MyDim.hierarchies"
        hierarchy_dir.mkdir(parents=True)
        (dimensions_dir / "MyDim.json").write_text(
            json.dumps(
                {
                    "@type": "Dimension",
                    "Name": "MyDim",
                    "DefaultHierarchy": {
                        "@id": "Dimensions('MyDim')/Hierarchies('MyHier')"
                    },
                }
            ),
            encoding="utf-8",
        )
        (hierarchy_dir / "MyHier.json").write_text(
            json.dumps(
                {
                    "@type": "Hierarchy",
                    "Name": "MyHier",
                    "ElementsSortType": "BYNAME",
                    "ElementsSortSense": "ASCENDING",
                    "ComponentsSortType": "BYNAME",
                    "ComponentsSortSense": "ASCENDING",
                    "Elements": [{"Name": "A", "Type": "Numeric"}],
                    "Subsets@Code.links": [],
                }
            ),
            encoding="utf-8",
        )

        dimensions, errors = _call_deserialize_dimensions(dimensions_dir, tmp_path.name)

        assert errors == []
        hierarchy_obj = dimensions["MyDim"].hierarchies[0]
        assert hierarchy_obj.elements_sort_type == "ByName"
        assert hierarchy_obj.elements_sort_sense == "Ascending"
        assert hierarchy_obj.components_sort_type == "ByName"
        assert hierarchy_obj.components_sort_sense == "Ascending"
        assert hierarchy_obj.elements.sort_metadata() == {
            "ElementsSortType": "ByName",
            "ComponentsSortType": "ByName",
        }

    def test_deserialize_rebuilds_store_groups_when_source_hierarchy_is_newer(self, tmp_path):
        src_dimensions = test_model_dir_base / "dimensions"
        dimensions_dir = tmp_path / "dimensions"
        shutil.copytree(src_dimensions, dimensions_dir)

        _call_deserialize_dimensions(dimensions_dir, tmp_path.name)

        hierarchy_json = dimensions_dir / "testbenchVersion.hierarchies" / "testbenchVersion.json"
        with open(hierarchy_json, "r+", encoding="utf-8") as fh:
            payload = json.load(fh)
            payload["Name"] = payload.get("Name", "testbenchVersion")
            fh.seek(0)
            fh.write(json.dumps(payload, ensure_ascii=False, indent=2))
            fh.truncate()

        dimensions_after, _ = _call_deserialize_dimensions(dimensions_dir, tmp_path.name)
        after_hier = dimensions_after["testbenchVersion"].hierarchies[0]
        assert after_hier.elements.source_json_mtime_ns() == int(hierarchy_json.stat().st_mtime_ns)

    def test_deserialize_uses_store_group_builder(self, tmp_path, monkeypatch):
        src_dimensions = test_model_dir_base / "dimensions"
        dimensions_dir = tmp_path / "dimensions"
        shutil.copytree(src_dimensions, dimensions_dir)

        calls = {"group_builder": 0}

        import tm1_git_py.services.deserializer as deserializer_module

        original_builder = deserializer_module._ensure_hierarchy_store_groups

        def _builder_spy(*args, **kwargs):
            calls["group_builder"] += 1
            return original_builder(*args, **kwargs)

        monkeypatch.setattr(deserializer_module, "_ensure_hierarchy_store_groups", _builder_spy)

        _call_deserialize_dimensions(dimensions_dir, tmp_path.name)
        assert calls["group_builder"] >= 1

    def test_deserialize_dimensions_uses_single_thread_for_hierarchy_builds(self, tmp_path, monkeypatch):
        import threading
        import tm1_git_py.services.deserializer as deserializer_module

        src_dimensions = test_model_dir_base / "dimensions"
        dimensions_dir = tmp_path / "dimensions"
        shutil.copytree(src_dimensions, dimensions_dir)

        thread_ids = set()
        original = deserializer_module._ensure_hierarchy_store_groups

        def _builder_spy(*args, **kwargs):
            thread_ids.add(threading.get_ident())
            return original(*args, **kwargs)

        monkeypatch.setattr(deserializer_module, "_ensure_hierarchy_store_groups", _builder_spy)

        _call_deserialize_dimensions(dimensions_dir, tmp_path.name)
        assert len(thread_ids) == 1

    def test_deserialize_progress_sink_accepts_byte_events(self, tmp_path):
        import tm1_git_py.services.deserializer as deserializer_module

        model_dir = tmp_path / "model"
        dimensions_dir = model_dir / "dimensions"
        cubes_dir = model_dir / "cubes"
        dimensions_dir.mkdir(parents=True)
        cubes_dir.mkdir(parents=True)
        f1 = dimensions_dir / "a.json"
        f2 = cubes_dir / "b.json"
        f1.write_text("{}", encoding="utf-8")
        f2.write_text('{"k":1}', encoding="utf-8")

        progress = TqdmProgressSink(worker_count=1)
        progress.on_event(
            ProgressEvent.make(
                kind=ProgressKind.START,
                scope=ProgressScope.WORKER,
                unit=ProgressUnit.BYTE,
                current=0,
                total=max(1, int(f1.stat().st_size)),
                message="reading file",
                path=str(f1),
            )
        )
        progress.on_event(
            ProgressEvent.make(
                kind=ProgressKind.UPDATE,
                scope=ProgressScope.WORKER,
                unit=ProgressUnit.BYTE,
                current=max(1, int(f1.stat().st_size)),
                total=max(1, int(f1.stat().st_size)),
                message="completed",
                path=str(f1),
            )
        )
        progress.close()

    def test_deserialize_subsets_rebuilds_store_only_when_subset_source_changes(self, tmp_path):
        src_dimensions = test_model_dir_base / "dimensions"
        dimensions_dir = tmp_path / "dimensions"
        shutil.copytree(src_dimensions, dimensions_dir)

        dimensions_before, _ = _call_deserialize_dimensions(dimensions_dir, tmp_path.name)
        first_hier = dimensions_before["testbenchVersion"].hierarchies[0]
        before_subset_mtime = first_hier.subsets.source_json_mtime_ns()

        dimensions_again, _ = _call_deserialize_dimensions(dimensions_dir, tmp_path.name)
        same_hier = dimensions_again["testbenchVersion"].hierarchies[0]
        assert same_hier.subsets.source_json_mtime_ns() == before_subset_mtime

    def test_store_backed_sequence_append_updates_signature(self, tmp_path):
        store = ModelStore.for_model_id(tmp_path.name)
        seq = StoreBackedSequence.for_elements_sink(
            store=store,
            dimension_name="MyDim",
            hierarchy_name="MyHier",
        )
        seq.replace_with_payloads(())
        seq.append(Element(name="E1", type="Numeric"))
        seq.append(Element(name="E2", type="String"))
        signature = seq.recalculate_content_signature_parallel()
        assert signature is not None
        assert signature[0] == 2

    def test_store_backed_sequence_replace_and_filter_refresh_hash(self, tmp_path):
        store = ModelStore.for_model_id(tmp_path.name)
        seq = StoreBackedSequence.for_elements_sink(
            store=store,
            dimension_name="MyDim",
            hierarchy_name="MyHier",
        )
        seq.replace_with_payloads(
            [
                {"Name": "A", "Type": "Numeric"},
                {"Name": "B", "Type": "Numeric"},
                {"Name": "C", "Type": "String"},
            ]
        )
        seq.replace_with_payloads(
            [
                {"Name": "A", "Type": "Numeric"},
                {"Name": "B", "Type": "String"},
            ]
        )
        seq.filter_in_place(lambda item: item.name != "A")
        signature = seq.content_signature()
        assert signature is not None
        assert signature[0] == 1

    def test_store_backed_sequence_ordered_identity_iteration(self, tmp_path):
        store = ModelStore.for_model_id(tmp_path.name)
        seq = StoreBackedSequence.for_elements_sink(
            store=store,
            dimension_name="MyDim",
            hierarchy_name="MyHier",
        )
        seq.replace_with_payloads(
            [
                {"Name": "C", "Type": "Numeric"},
                {"Name": "A", "Type": "Numeric"},
                {"Name": "B", "Type": "String"},
            ]
        )
        sorted_names = [
            Element.from_dict(payload).name
            for payload in seq.iter_payloads(ordered_by_identity=True)
        ]
        assert sorted_names == ["A", "B", "C"]

    def test_parallel_signature_elements_order_invariant(self, tmp_path):
        store_a = ModelStore.for_model_id("a")
        seq_a = StoreBackedSequence.for_elements_sink(
            store=store_a,
            dimension_name="MyDim",
            hierarchy_name="MyHier",
        )
        seq_a.replace_with_payloads(
            [
                {"Name": "C", "Type": "Numeric"},
                {"Name": "A", "Type": "Numeric"},
                {"Name": "B", "Type": "String"},
            ]
        )
        sig_a = store_group_content_signature(
            store=store_a,
            group_id=seq_a.group_id,
            chunk_size=1,
            count=len(seq_a),
        )

        store_b = ModelStore.for_model_id("b")
        seq_b = StoreBackedSequence.for_elements_sink(
            store=store_b,
            dimension_name="MyDim",
            hierarchy_name="MyHier",
        )
        seq_b.replace_with_payloads(
            [
                {"Name": "A", "Type": "Numeric"},
                {"Name": "B", "Type": "String"},
                {"Name": "C", "Type": "Numeric"},
            ]
        )
        sig_b = store_group_content_signature(
            store=store_b,
            group_id=seq_b.group_id,
            chunk_size=2,
            count=len(seq_b),
        )
        assert sig_a == sig_b

    def test_parallel_signature_edges_order_invariant(self, tmp_path):
        store_a = ModelStore.for_model_id("a")
        seq_a = StoreBackedSequence.for_edges_sink(
            store=store_a,
            dimension_name="MyDim",
            hierarchy_name="MyHier",
        )
        seq_a.replace_with_payloads(
            [
                {"ParentName": "P2", "ComponentName": "C2", "Weight": 1},
                {"ParentName": "P1", "ComponentName": "C1", "Weight": 2},
                {"ParentName": "P1", "ComponentName": "C3", "Weight": 3},
            ]
        )
        sig_a = store_group_content_signature(
            store=store_a,
            group_id=seq_a.group_id,
            chunk_size=1,
            count=len(seq_a),
        )

        store_b = ModelStore.for_model_id("b")
        seq_b = StoreBackedSequence.for_edges_sink(
            store=store_b,
            dimension_name="MyDim",
            hierarchy_name="MyHier",
        )
        seq_b.replace_with_payloads(
            [
                {"ParentName": "P1", "ComponentName": "C1", "Weight": 2},
                {"ParentName": "P1", "ComponentName": "C3", "Weight": 3},
                {"ParentName": "P2", "ComponentName": "C2", "Weight": 1},
            ]
        )
        sig_b = store_group_content_signature(
            store=store_b,
            group_id=seq_b.group_id,
            chunk_size=2,
            count=len(seq_b),
        )
        assert sig_a == sig_b

    @pytest.mark.parametrize(
        ("object_type", "payloads"),
        [
            (
                "elements",
                [
                    {"Name": "C", "Type": "Numeric"},
                    {"Name": "A", "Type": "String"},
                    {"Name": "B", "Type": "Numeric"},
                ],
            ),
            (
                "edges",
                [
                    {"ParentName": "P2", "ComponentName": "C2", "Weight": 1},
                    {"ParentName": "P1", "ComponentName": "C1", "Weight": 2},
                    {"ParentName": "P1", "ComponentName": "C3", "Weight": 3},
                ],
            ),
            (
                "subsets",
                [
                    {"Name": "Subset_B", "Expression": "{B}"},
                    {"Name": "Subset_A", "Expression": "{A}"},
                ],
            ),
        ],
    )
    def test_plain_sqlite_hash_calculator_matches_existing_calculator(self, object_type, payloads):
        import uuid

        model_id = f"hash_calc_{object_type}_{uuid.uuid4().hex}"
        store = ModelStore.for_model_id(model_id)
        if object_type == "elements":
            seq = StoreBackedSequence.for_elements_sink(
                store=store,
                model_id=model_id,
                dimension_name="MyDim",
                hierarchy_name="MyHier",
            )
        elif object_type == "edges":
            seq = StoreBackedSequence.for_edges_sink(
                store=store,
                model_id=model_id,
                dimension_name="MyDim",
                hierarchy_name="MyHier",
            )
        else:
            seq = StoreBackedSequence.for_subsets_sink(
                store=store,
                model_id=model_id,
                dimension_name="MyDim",
                hierarchy_name="MyHier",
            )

        seq.replace_with_payloads(payloads)
        existing_signature = store_group_content_signature(
            store=store,
            group_id=seq.group_id,
            chunk_size=1,
            count=len(seq),
        )
        plain_sqlite_signature = calculate_group_content_signature(
            db_path=store.db_path,
            group_id=seq.group_id,
            object_type=object_type,
            chunk_size=1,
        )

        assert plain_sqlite_signature == existing_signature

    def test_plain_sqlite_hash_calculator_parallel_matches_existing_parallel_calculator(self):
        import uuid

        model_id = f"hash_calc_parallel_{uuid.uuid4().hex}"
        store = ModelStore.for_model_id(model_id)
        seq = StoreBackedSequence.for_edges_sink(
            store=store,
            model_id=model_id,
            dimension_name="MyDim",
            hierarchy_name="MyHier",
        )
        seq.replace_with_payloads(
            [
                {"ParentName": f"P{i % 5}", "ComponentName": f"C{i}", "Weight": i}
                for i in range(12)
            ]
        )

        try:
            existing_signature = store_group_content_signature(
                store=store,
                group_id=seq.group_id,
                chunk_size=3,
                max_workers=2,
                count=len(seq),
            )
        except (OSError, NotImplementedError) as exc:
            pytest.skip(f"ProcessPoolExecutor unavailable in this environment: {exc}")
        plain_sqlite_signature = calculate_group_content_signature(
            db_path=store.db_path,
            group_id=seq.group_id,
            object_type="edges",
            chunk_size=3,
            max_workers=2,
        )

        assert plain_sqlite_signature == existing_signature

    def test_plain_sqlite_hash_calculator_max_workers_one_runs_chunks_inline(self, monkeypatch):
        import uuid
        import tm1_git_py.internal.content_hash_calculator as hash_module

        model_id = f"hash_calc_inline_{uuid.uuid4().hex}"
        store = ModelStore.for_model_id(model_id)
        seq = StoreBackedSequence.for_edges_sink(
            store=store,
            model_id=model_id,
            dimension_name="MyDim",
            hierarchy_name="MyHier",
        )
        seq.replace_with_payloads(
            [
                {"ParentName": f"P{i % 2}", "ComponentName": f"C{i}", "Weight": i}
                for i in range(5)
            ]
        )

        original_worker = hash_module._hash_chunk_worker
        chunk_indices = []

        def spy_hash_chunk_worker(job, progress_sink):
            chunk_indices.append(int(job["chunk_idx"]))
            return original_worker(job, progress_sink)

        existing_signature = hash_module.calculate_group_content_signature(
            db_path=store.db_path,
            group_id=seq.group_id,
            object_type="edges",
            chunk_size=2,
            max_workers=1,
        )

        process_pool_ctor = mock.Mock(side_effect=AssertionError("process pool should not be created"))
        monkeypatch.setattr(hash_module, "ProcessPoolExecutor", process_pool_ctor)
        monkeypatch.setattr(hash_module, "_hash_chunk_worker", spy_hash_chunk_worker)

        plain_sqlite_signature = hash_module.calculate_group_content_signature(
            db_path=store.db_path,
            group_id=seq.group_id,
            object_type="edges",
            chunk_size=2,
            max_workers=1,
        )

        assert plain_sqlite_signature == existing_signature
        assert chunk_indices == [0, 1, 2]
        process_pool_ctor.assert_not_called()

    def test_db_session_reuses_model_store_without_closing_it(self):
        import uuid

        model_id = f"db_session_{uuid.uuid4().hex}"
        store = ModelStore.for_model_id(model_id)
        assert ModelStore.for_model_id(model_id) is store
        seq = StoreBackedSequence.for_elements_sink(
            store=store,
            model_id=model_id,
            dimension_name="MyDim",
            hierarchy_name="MyHier",
        )
        seq.extend_payloads([{"Name": "A", "Type": "Numeric"}])
        assert len(seq) == 1

        assert not store._closed
        replacement = ModelStore.for_model_id(model_id)
        assert replacement is store
        assert not replacement._closed

    def test_store_backed_sequence_extend_payloads_round_trip_element(self, tmp_path):
        store = ModelStore.for_model_id(tmp_path.name)
        seq = StoreBackedSequence.for_elements_sink(
            store=store,
            dimension_name="MyDim",
            hierarchy_name="MyHier",
        )
        seq.replace_with_payloads(())
        seq.extend_payloads([{"Name": "A", "Type": "Numeric"}])
        assert len(seq) == 1
        assert seq[0] == Element(name="A", type="Numeric")

    def test_element_and_edge_internal_indexes_do_not_affect_json_or_equality(self):
        indexed_element = Element(name="A", type="Numeric", element_index=7)
        plain_element = Element(name="A", type="Numeric")
        indexed_edge = Edge(parent="P", component_name="C", weight=1, component_index=9)
        plain_edge = Edge(parent="P", component_name="C", weight=1)

        assert indexed_element == plain_element
        assert indexed_edge == plain_edge
        assert "ElementIndex" not in indexed_element.to_dict()
        assert "ElementIndex" not in json.loads(indexed_element.as_json())
        assert "ComponentIndex" not in indexed_edge.to_dict()
        assert "ComponentIndex" not in json.loads(indexed_edge.as_json())

    def test_store_backed_sequence_retains_internal_indexes_without_exposing_by_default(self, tmp_path):
        store = ModelStore.for_model_id(tmp_path.name + "_indexes")
        elements = StoreBackedSequence.for_elements_sink(
            store=store,
            dimension_name="D",
            hierarchy_name="H",
        )
        edges = StoreBackedSequence.for_edges_sink(
            store=store,
            dimension_name="D",
            hierarchy_name="H",
        )
        elements.replace_with_payloads(())
        edges.replace_with_payloads(())

        elements.extend_payloads(
            [
                {"Name": "B", "Type": "Numeric", "ElementIndex": 10},
                {"Name": "A", "Type": "String", "ElementIndex": 11},
            ]
        )
        edges.extend(
            [
                Edge(parent="P2", component_name="C2", weight=1, component_index=20),
                Edge(parent="P1", component_name="C1", weight=2, component_index=21),
            ]
        )

        default_element_payload = next(elements.iter_payloads())
        default_edge_payload = next(edges.iter_payloads())
        assert "ElementIndex" not in default_element_payload
        assert "ComponentIndex" not in default_edge_payload

        indexed_elements = list(elements.iter_payloads(include_internal_indexes=True))
        indexed_edges = list(edges.iter_payloads(include_internal_indexes=True))
        assert [payload["ElementIndex"] for payload in indexed_elements] == [11, 10]
        assert [payload["ComponentIndex"] for payload in indexed_edges] == [21, 20]

    def test_store_backed_sequence_assigns_internal_indexes_when_missing(self, tmp_path):
        store = ModelStore.for_model_id(tmp_path.name + "_auto_indexes")
        seq = StoreBackedSequence.for_elements_sink(
            store=store,
            dimension_name="D",
            hierarchy_name="H",
        )
        seq.replace_with_payloads(())
        seq.extend_payloads(
            [
                {"Name": "B", "Type": "Numeric"},
                {"Name": "A", "Type": "String"},
            ]
        )

        payloads = list(seq.iter_payloads(include_internal_indexes=True))
        assert [payload["ElementIndex"] for payload in payloads] == [1, 0]

    def test_store_backed_sequence_uses_explicit_start_index_for_out_of_order_pages(self, tmp_path):
        store = ModelStore.for_model_id(tmp_path.name + "_page_indexes")
        seq = StoreBackedSequence.for_elements_sink(
            store=store,
            dimension_name="D",
            hierarchy_name="H",
        )
        seq.replace_with_payloads(())

        seq.extend_payloads(
            [
                {"Name": "C", "Type": "Numeric"},
                {"Name": "D", "Type": "String"},
            ],
            start_index=2,
        )
        seq.extend_payloads(
            [
                {"Name": "A", "Type": "Numeric"},
                {"Name": "B", "Type": "String"},
            ],
            start_index=0,
        )

        by_identity = list(seq.iter_payloads(include_internal_indexes=True))
        by_index = list(
            seq.iter_payloads(
                order_by_internal_index=True,
                include_internal_indexes=True,
            )
        )
        assert [(payload["Name"], payload["ElementIndex"]) for payload in by_identity] == [
            ("A", 0),
            ("B", 1),
            ("C", 2),
            ("D", 3),
        ]
        assert [payload["Name"] for payload in by_index] == ["A", "B", "C", "D"]

    def test_deserializer_batch_insert_assigns_indexes_from_json_order(self, tmp_path):
        store = ModelStore.for_model_id(tmp_path.name + "_deserialize_indexes")
        seq = StoreBackedSequence.for_edges_sink(
            store=store,
            dimension_name="D",
            hierarchy_name="H",
        )
        seq.replace_with_payloads(())

        deserializer_module._append_payloads_in_batches(
            store=store,
            group_id=seq.group_id,
            payloads=[
                {"ParentName": "P", "ComponentName": "B", "Weight": 1},
                {"ParentName": "P", "ComponentName": "A", "Weight": 1},
            ],
            start_index=0,
        )

        payloads = list(
            seq.iter_payloads(
                order_by_internal_index=True,
                include_internal_indexes=True,
            )
        )
        assert [(payload["ComponentName"], payload["ComponentIndex"]) for payload in payloads] == [
            ("B", 0),
            ("A", 1),
        ]

    def test_store_backed_sequence_persists_empty_string_for_null_identity_fields(self, tmp_path):
        store = ModelStore.for_model_id(tmp_path.name + "_empty_identity")
        elements = StoreBackedSequence.for_elements_sink(
            store=store,
            dimension_name="D",
            hierarchy_name="H",
        )
        edges = StoreBackedSequence.for_edges_sink(
            store=store,
            dimension_name="D",
            hierarchy_name="H",
        )
        subsets = StoreBackedSequence.for_subsets_sink(
            store=store,
            dimension_name="D",
            hierarchy_name="H",
        )
        elements.replace_with_payloads(())
        edges.replace_with_payloads(())
        subsets.replace_with_payloads(())

        elements.extend_payloads([{"Name": None, "Type": "Numeric"}])
        edges.extend_payloads([{"ParentName": None, "ComponentName": None, "Weight": 1}])
        subsets.extend_payloads([{"Name": None, "Expression": "{A}"}])

        assert list(elements.iter_payloads(ordered_by_identity=True))[0]["Name"] == ""
        edge_payload = list(edges.iter_payloads(ordered_by_identity=True))[0]
        assert edge_payload["ParentName"] == ""
        assert edge_payload["ComponentName"] == ""
        assert list(subsets.iter_payloads(ordered_by_identity=True))[0]["name"] == ""

    def test_store_backed_sequence_extend_payloads_matches_extend_elements(self, tmp_path):
        store_e = ModelStore.for_model_id(tmp_path.name + "_e")
        store_p = ModelStore.for_model_id(tmp_path.name + "_p")
        seq_e = StoreBackedSequence.for_elements_sink(
            store=store_e,
            dimension_name="D",
            hierarchy_name="H",
        )
        seq_p = StoreBackedSequence.for_elements_sink(
            store=store_p,
            dimension_name="D",
            hierarchy_name="H",
        )
        seq_e.replace_with_payloads(())
        seq_p.replace_with_payloads(())
        seq_e.extend([Element(name="A", type="Numeric"), Element(name="B", type="String")])
        seq_p.extend_payloads(
            [
                {"Name": "A", "Type": "Numeric"},
                {"Name": "B", "Type": "String"},
            ]
        )
        assert list(seq_e.iter_payloads()) == list(seq_p.iter_payloads())

    def test_store_backed_sequence_extend_payloads_matches_extend_edges(self, tmp_path):
        store_e = ModelStore.for_model_id(tmp_path.name + "_e2")
        store_p = ModelStore.for_model_id(tmp_path.name + "_p2")
        seq_e = StoreBackedSequence.for_edges_sink(
            store=store_e,
            dimension_name="D",
            hierarchy_name="H",
        )
        seq_p = StoreBackedSequence.for_edges_sink(
            store=store_p,
            dimension_name="D",
            hierarchy_name="H",
        )
        seq_e.replace_with_payloads(())
        seq_p.replace_with_payloads(())
        seq_e.extend(
            [
                Edge(parent="P", component_name="C1", weight=1.0),
                Edge(parent="P", component_name="C2", weight=2.0),
            ]
        )
        seq_p.extend_payloads(
            [
                {"ParentName": "P", "ComponentName": "C1", "Weight": 1.0},
                {"ParentName": "P", "ComponentName": "C2", "Weight": 2.0},
            ]
        )
        assert list(seq_e.iter_payloads()) == list(seq_p.iter_payloads())

    def test_store_backed_sequence_keeps_dynamic_and_static_subset_payload_shapes(self, tmp_path):
        first_element_id = "Dimensions('Product')/Hierarchies('Product')/Elements('Bike')"
        second_element_id = "Dimensions('Product')/Hierarchies('Product')/Elements('Helmet')"
        store = ModelStore.for_model_id(tmp_path.name + "_subset_payloads")
        subsets = StoreBackedSequence.for_subsets_sink(
            store=store,
            dimension_name="Product",
            hierarchy_name="Product",
        )
        subsets.replace_with_payloads(())

        subsets.extend_payloads(
            [
                {"Name": "Dynamic", "Expression": "{[Product].[Product].Members}"},
                {
                    "Name": "Static",
                    "Expression": None,
                    "Elements": [
                        {"@odata.id": first_element_id},
                        {"@odata.id": second_element_id},
                    ],
                },
            ]
        )

        payloads = list(subsets.iter_payloads(ordered_by_identity=True))
        assert payloads == [
            {"name": "Dynamic", "expression": "{[Product].[Product].Members}"},
            {
                "name": "Static",
                "Elements": [{"@id": first_element_id}, {"@id": second_element_id}],
            },
        ]
        assert subsets[0] == Subset(name="Dynamic", expression="{[Product].[Product].Members}")
        assert subsets[1] == Subset(name="Static", element_ids=[first_element_id, second_element_id])

    def test_store_backed_sequence_subset_extend_keeps_static_element_ids(self, tmp_path):
        element_id = "Dimensions('Product')/Hierarchies('Product')/Elements('Bike')"
        store = ModelStore.for_model_id(tmp_path.name + "_subset_items")
        subsets = StoreBackedSequence.for_subsets_sink(
            store=store,
            dimension_name="Product",
            hierarchy_name="Product",
        )
        subsets.replace_with_payloads(())

        subsets.extend([Subset(name="Static", element_ids=[element_id])])

        assert list(subsets.iter_payloads()) == [
            {"name": "Static", "Elements": [{"@id": element_id}]},
        ]
        assert subsets[0].element_ids == [element_id]


    def test_deserialize_cubes(self, cubes_dir=test_model_dir_base / 'cubes'):
        expected_cube_names = ['testbenchSales']
        dimensions, errors = _call_deserialize_dimensions(test_model_dir_base / "dimensions", test_model_dir_base.name)
        cubes, errors = deserialize_cubes(cubes_dir=cubes_dir, _dimensions=dimensions)
        diff_cube_names = set(expected_cube_names) - set(cubes.keys())
        assert len(diff_cube_names) == 0

    def test_deserialize_cubes_keeps_dimension_references_without_dimension_objects(self, tmp_path):
        cubes_dir = tmp_path / "cubes"
        cubes_dir.mkdir()
        (cubes_dir / "Organization Units Settings.json").write_text(
            json.dumps({
                "@type": "Cube",
                "Name": "Organization Units Settings",
                "Dimensions": [
                    {"@id": "Dimensions('Versions')"},
                    {"@id": "Dimensions('Organization Units')"},
                ],
                "Views@Code.links": [],
            }),
            encoding="utf-8",
        )

        cubes, errors = deserialize_cubes(cubes_dir=cubes_dir, _dimensions={})

        assert errors == []
        assert cubes["Organization Units Settings"].dimensions == ["Versions", "Organization Units"]

    def test_deserialize_cubes_loads_drillthrough_rules_from_technical_rule_file(self, tmp_path):
        cubes_dir = tmp_path / "cubes"
        cubes_dir.mkdir()
        (cubes_dir / "Sales.json").write_text(
            json.dumps({
                "@type": "Cube",
                "Name": "Sales",
                "Dimensions": [
                    {"@id": "Dimensions('Versions')"},
                ],
                "Rules@Code.link": "Sales.rules",
                "DrillthroughRules@Code.link": "Sales.drillthrough.rules",
                "Views@Code.links": [],
            }),
            encoding="utf-8",
        )
        (cubes_dir / "Sales.rules").write_text("[] = N: 1;", encoding="utf-8")
        (cubes_dir / "}CubeDrill_Sales.rules").write_text(
            "[]=s:'simple_drillthrough';",
            encoding="utf-8",
        )

        cubes, errors = deserialize_cubes(cubes_dir=cubes_dir, _dimensions={})

        assert errors == []
        cube = cubes["Sales"]
        assert cube.get_rule_text() == "[] = N: 1;"
        assert cube.get_drillthrough_rule_text() == "[]=s:'simple_drillthrough';"

    def test_deserialize_cubes_without_technical_drillthrough_rule_file_is_backward_compatible(self, tmp_path):
        cubes_dir = tmp_path / "cubes"
        cubes_dir.mkdir()
        (cubes_dir / "Sales.json").write_text(
            json.dumps({
                "@type": "Cube",
                "Name": "Sales",
                "Dimensions": [],
                "DrillthroughRules@Code.link": "Sales.drillthrough.rules",
                "Views@Code.links": [],
            }),
            encoding="utf-8",
        )

        cubes, errors = deserialize_cubes(cubes_dir=cubes_dir, _dimensions={})

        assert errors == []
        assert cubes["Sales"].drillthrough_rules == []


    def test_deserialize_process(self, processes_dir=test_model_dir_base / 'processes'):
        processes, errors = deserialize_processes(process_dir=processes_dir)
        for process in processes.values():
            assert isinstance(process, Process)

    def test_deserialize_process_preserves_datasource_ui_data_and_variables_ui_data(self, tmp_path):
        processes_dir = tmp_path / "processes"
        processes_dir.mkdir()

        process_name = "Proc_With_Metadata"
        (processes_dir / f"{process_name}.json").write_text(
            json.dumps(
                {
                    "@type": "Process",
                    "Name": process_name,
                    "HasSecurityAccess": False,
                    "Code@Code.link": f"{process_name}.ti",
                    "UIData": "CubeAction=1511\fDataAction=1503\fCubeLogChanges=0\f",
                    "DataSource": {
                        "Type": "ASCII",
                        "asciiDelimiterChar": ";",
                        "dataSourceNameForServer": "sample.csv",
                    },
                    "VariablesUIData": [
                        "VarType=32\fColType=827\f",
                    ],
                    "Parameters": [],
                    "Variables": [],
                }
            ),
            encoding="utf-8",
        )
        (processes_dir / f"{process_name}.ti").write_text(
            "#region Prolog\n#endregion\n#region Metadata\n#endregion\n#region Data\n#endregion\n#region Epilog\n#endregion\n",
            encoding="utf-8",
        )

        processes, errors = deserialize_processes(process_dir=processes_dir)

        assert errors == []
        proc = processes[process_name]
        assert proc.datasource == {
            "Type": "ASCII",
            "asciiDelimiterChar": ";",
            "dataSourceNameForServer": "sample.csv",
        }
        assert proc.ui_data == "CubeAction=1511\fDataAction=1503\fCubeLogChanges=0\f"
        assert proc.variables_ui_data == ["VarType=32\fColType=827\f"]


    @pytest.mark.parametrize("data", dim_data)
    def test_deserialize_dimensions_error_propagation(self, tmp_path, data):
        dimensions_dir = tmp_path / "dimensions"
        dimensions_dir.mkdir()
        broken_dims = dimensions_dir / f"BrokenDimension.json"

        broken_dims.write_text(data, encoding="utf-8")

        dimensions, errors = _call_deserialize_dimensions(dimensions_dir, tmp_path.name)

        assert not dimensions, f"Broken {type(dimensions.values())} file should not deserialize successfully"
        assert len(errors) == 1
        assert errors[0].workflow == "deserialize"
        assert Path(errors[0].subject).parent == broken_dims.parent


    @pytest.mark.parametrize("data", chore_data)
    def test_deserialize_chore_error_propagation(self, tmp_path, data):
        chores_dir = tmp_path / "chores"
        chores_dir.mkdir()
        broken_chore = chores_dir / f"BrokenChores.json"

        broken_chore.write_text(data, encoding="utf-8")

        chores, errors = deserialize_chores(chores_dir)

        assert not chores, f"Broken {type(chores.values())} file should not deserialize successfully"
        assert len(errors) == 1
        assert errors[0].workflow == "deserialize"
        assert errors[0].subject == str(broken_chore)


    @pytest.mark.parametrize("data", process_data)
    def test_deserialize_process_error_propagation(self, tmp_path, data):
        processes_dir = tmp_path / "processes"
        processes_dir.mkdir()
        broken_process = processes_dir / f"BrokenProcess.json"

        broken_process.write_text(data, encoding="utf-8")

        processes, errors = deserialize_processes(processes_dir)

        assert not processes, f"Broken {type(processes.values())} file should not deserialize successfully"
        assert len(errors) == 1
        assert errors[0].workflow == "deserialize"
        assert Path(errors[0].subject).parent == broken_process.parent
