import filecmp
import json
import tempfile
from pathlib import Path

import pytest
from TM1py import TM1Service

from test_integration.test_base import (
    DEFAULT_MAX_WORKERS,
    load_fixture_model_tm1gitpy,
    export_check_no_errors,
    tm1_service,
)
from tm1_git_py.model.hierarchy import hierarchy_sort_metadata_json
from tm1_git_py.services.exporter import export
from tm1_git_py.services.filter import FilterRules
from tm1_git_py.services.sort_metadata import get_hierarchy_sort_metadata
from tm1_git_py.services.serializer import serialize_model


@pytest.mark.usefixtures("tm1_service")
class TestExport:

    @pytest.fixture(autouse=True)
    def _tm1_service(self, tm1_service):
        self.tm1_service : TM1Service = tm1_service

    def test_export_no_error_matching_folder(self):
        
        # given
        fixture_tm1gitpy_dir, _ = load_fixture_model_tm1gitpy(self)

        with tempfile.TemporaryDirectory() as temp_dir:
        
            # when
            test_tm1gitpy_dir = str(Path(temp_dir) / "test_tm1gitpy_dir")
            test_tm1gitpy_model = export_check_no_errors(self, model_id=Path(test_tm1gitpy_dir).name)
            serialize_model(
                test_tm1gitpy_model,
                test_tm1gitpy_dir,
                max_workers=DEFAULT_MAX_WORKERS,
            )
            
            # then 
            cmp = filecmp.dircmp(test_tm1gitpy_dir, fixture_tm1gitpy_dir)
            assert not cmp.left_only, f"Files only in left directory: {cmp.left_only}"
            assert not cmp.right_only, f"Files only in right directory: {cmp.right_only}"
            assert not cmp.diff_files, f"Files that differ: {cmp.diff_files}"

    def test_export_preserves_default_hierarchy_sort_metadata_and_order(self):
        dimension_name = "}Views_TestCube3WithView"
        hierarchy_name = "}Views_testcube3withview"
        expected_elements = ["testcube3withview_view1", "testcube3withview_view2"]

        raw_metadata = get_hierarchy_sort_metadata(
            self.tm1_service,
            dimension_name,
            [hierarchy_name],
        )
        assert raw_metadata.get((dimension_name, hierarchy_name)), (
            f"Expected }}DimensionProperties sort metadata for {dimension_name}/{hierarchy_name}"
        )

        model = export_check_no_errors(
            self,
            filter_rules=[f"!Dimensions('{dimension_name}')"],
            model_id="integration-export-sort-default",
        )
        dimension = next(dim for dim in model.dimensions if dim.name == dimension_name)
        hierarchy = next(hier for hier in dimension.hierarchies if hier.name == hierarchy_name)

        assert hierarchy.effective_elements_sort_type == raw_metadata[(dimension_name, hierarchy_name)]["ElementsSortType"]
        assert hierarchy.effective_elements_sort_sense == raw_metadata[(dimension_name, hierarchy_name)]["ElementsSortSense"]
        assert [payload["Name"] for payload in hierarchy.elements.iter_payloads(order_by_internal_index=True)] == expected_elements

        with tempfile.TemporaryDirectory() as temp_dir:
            serialize_model(model, temp_dir, max_workers=DEFAULT_MAX_WORKERS)
            hierarchy_json_path = (
                Path(temp_dir)
                / "dimensions"
                / f"{dimension_name}.hierarchies"
                / f"{hierarchy_name}.json"
            )
            payload = json.loads(hierarchy_json_path.read_text(encoding="utf-8"))

        assert [element["Name"] for element in payload["Elements"]] == expected_elements
        serialized_metadata = hierarchy_sort_metadata_json(hierarchy)
        for key, value in serialized_metadata.items():
            assert payload[key] == value

    def test_export_preserves_test_dim_sorting_hierarchy_metadata_and_order(self):
        """
        TestDimSorting hierarchy sort scenarios:

        | Hierarchy | Elements type | Elements sense | Components type | Components sense |
        | --- | --- | --- | --- | --- |
        | TestDimSorting | default | default | default | default |
        | TestDimSorting_elements_byhierdesc_components_byinputasc | ByHierarchy | Descending | ByInput | Ascending |
        | TestDimSorting_elements_byinput_components_byinput | ByInput | Ascending | ByInput | Ascending |
        | TestDimSorting_elements_byinput_components_bynameasc | ByInput | Ascending | ByName | Ascending |
        | TestDimSorting_elements_byinput_components_bynamedesc | ByInput | Ascending | ByName | Descending |
        | TestDimSorting_elements_bylevelasc_components_bynameasc | ByLevel | Ascending | ByName | Ascending |
        | TestDimSorting_elements_bynameasc_components_bynameasc | ByName | Ascending | ByName | Ascending |
        | TestDimSorting_elements_bynamedesc_components_bynamedesc | ByName | Descending | ByName | Descending |
        """
        dimension_name = "TestDimSorting"
        expected_hierarchy_dir = (
            Path(__file__).resolve().parent
            / "fixture_model_tm1gitpy"
            / "dimensions"
            / f"{dimension_name}.hierarchies"
        )

        model = export_check_no_errors(
            self,
            filter_rules=[f"!Dimensions('{dimension_name}')"],
            model_id="integration-export-test-dim-sorting",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            serialize_model(model, temp_dir, max_workers=DEFAULT_MAX_WORKERS)
            actual_hierarchy_dir = (
                Path(temp_dir)
                / "dimensions"
                / f"{dimension_name}.hierarchies"
            )

            expected_paths = [
                path
                for path in sorted(expected_hierarchy_dir.glob("*.json"))
            ]
            assert expected_paths

            for expected_path in expected_paths:
                actual_path = actual_hierarchy_dir / expected_path.name
                assert actual_path.exists(), f"Missing exported hierarchy: {actual_path}"

                expected_payload = json.loads(expected_path.read_text(encoding="utf-8"))
                actual_payload = json.loads(actual_path.read_text(encoding="utf-8"))
                assert actual_payload == expected_payload

    def test_export_filters_control_objects_with_skip_flags(self):
        model, errors = export(
            self.tm1_service,
            model_id="integration-export",
            filter_rules=FilterRules(["Cubes('}*')", "Dimensions('}*')", "Processes('}*')"]),
            max_workers=DEFAULT_MAX_WORKERS,
        )
        assert not errors, f"Found errors: {errors}"

        assert all(not d.name.startswith("}") for d in model.dimensions)
        assert all(not c.name.startswith("}") for c in model.cubes)
        assert all(not p.name.startswith("}") for p in model.processes)

    def test_export_applies_custom_filter_rules_during_assembly(self):
        model, errors = export(
            self.tm1_service,
            model_id="integration-export",
            filter_rules=FilterRules([
                "Dimensions('TestDim1*')",
                "Cubes('TestCube1*')",
                "Cubes('TestCube2*')",
                "Cubes('TestCube3*')",
            ]),
            max_workers=DEFAULT_MAX_WORKERS,
        )
        assert not errors, f"Found errors: {errors}"

        dimension_names = [d.name for d in model.dimensions]
        cube_names = [c.name for c in model.cubes]
        assert not any(name.startswith("TestDim1") for name in dimension_names)
        assert not any(name.startswith("TestCube1") for name in cube_names)
        assert not any(name.startswith("TestCube2") for name in cube_names)
        assert not any(name.startswith("TestCube3") for name in cube_names)

    def test_export_applies_tm1project_filter_style_rules(self):
        example_filter_path = Path(__file__).resolve().parents[1] / "examples" / "tm1project_filter.txt"
        filter_rules = [
            line.strip()
            for line in example_filter_path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        filter_rules.append("Cubes/Views")

        model, errors = export(
            self.tm1_service,
            model_id="integration-export",
            filter_rules=FilterRules(filter_rules),
            max_workers=DEFAULT_MAX_WORKERS,
        )
        assert not errors, f"Found errors: {errors}"

        dimension_names = [d.name for d in model.dimensions]
        cube_names = [c.name for c in model.cubes]
        assert not any(name.startswith("Channel") for name in dimension_names)
        assert not any(name.startswith("zSYS Maintenance Parameter") for name in cube_names)
        assert all(not cube.views for cube in model.cubes)

    def test_export_force_includes_technical_cube_while_other_technical_objects_stay_ignored(self):
        forced_cube_name = "}StatsByCube"

        model, errors = export(
            self.tm1_service,
            model_id="integration-export-force-technical-cube",
            filter_rules=FilterRules([f"!Cubes('{forced_cube_name}')"]),
            max_workers=DEFAULT_MAX_WORKERS,
        )
        assert not errors, f"Found errors: {errors}"

        cube_names = [c.name for c in model.cubes]
        assert forced_cube_name in cube_names
        assert "}StatsByRule" not in cube_names
        assert all(not d.name.startswith("}") for d in model.dimensions)
        assert all(not p.name.startswith("}") for p in model.processes)
