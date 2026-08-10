from tests.unit_common import *


class TestComparator:

    mock_changeset_data = _build_mock_changeset_data()

    @staticmethod
    def _changes_by_type(changeset: Changeset, change_type: ChangeType) -> list[Change]:
        return [c for c in changeset.changes if c.change_type == change_type]

    @staticmethod
    def _bodies_by(change_set: list[Change], body_type: type) -> list:
        return [c.body for c in change_set if isinstance(c.body, body_type)]

    def test_compare_returns_diagnostics_and_keeps_independent_changes(self):
        class ExplodingProcess(Process):
            def __eq__(self, other):
                raise RuntimeError("broken process comparison")

        bad_old = ExplodingProcess("Broken", True, "Broken.ti", "None", [], [], None)
        bad_new = ExplodingProcess("Broken", True, "Broken.ti", "None", [], [], None)
        good_old = make_process(name="Good", parameters=[])
        good_new = make_process(name="Good", parameters=[{"name": "p", "value": "new"}])
        reported = []

        result = Comparator().compare(
            Model(cubes=[], dimensions=[], processes=[bad_old, good_old], chores=[]),
            Model(cubes=[], dimensions=[], processes=[bad_new, good_new], chores=[]),
            error_callback=reported.append,
        )
        changeset, errors = result

        assert isinstance(result, tuple)
        assert any(
            change.object_type == ObjectType.PROCESS
            and change.change_type == ChangeType.MODIFY
            and change.body.name == "Good"
            for change in changeset.changes
        )
        assert [(error.workflow, error.phase, error.subject, error.exception_type, error.severity) for error in errors] == [
            ("compare", "object_comparison", "Processes('Broken')", "RuntimeError", "recoverable"),
        ]
        assert reported == errors

    def test_compare_reports_diagnostics_in_stable_callback_order(self):
        class ExplodingProcess(Process):
            def __eq__(self, other):
                raise RuntimeError("broken process comparison")

        old_processes = [
            ExplodingProcess("Second", True, "Second.ti", "None", [], [], None),
            ExplodingProcess("First", True, "First.ti", "None", [], [], None),
        ]
        new_processes = [
            ExplodingProcess("Second", True, "Second.ti", "None", [], [], None),
            ExplodingProcess("First", True, "First.ti", "None", [], [], None),
        ]
        reported = []

        _, errors = Comparator().compare(
            Model(cubes=[], dimensions=[], processes=old_processes, chores=[]),
            Model(cubes=[], dimensions=[], processes=new_processes, chores=[]),
            error_callback=reported.append,
        )

        assert [error.subject for error in errors] == ["Processes('First')", "Processes('Second')"]
        assert reported == errors

    def test_compare_keeps_diagnostic_when_error_callback_fails(self, caplog):
        class ExplodingProcess(Process):
            def __eq__(self, other):
                raise RuntimeError("broken process comparison")

        old_process = ExplodingProcess("Broken", True, "Broken.ti", "None", [], [], None)
        new_process = ExplodingProcess("Broken", True, "Broken.ti", "None", [], [], None)

        def failing_callback(_error):
            raise RuntimeError("callback failure")

        caplog.set_level("DEBUG")
        _, errors = Comparator().compare(
            Model(cubes=[], dimensions=[], processes=[old_process], chores=[]),
            Model(cubes=[], dimensions=[], processes=[new_process], chores=[]),
            error_callback=failing_callback,
        )

        assert len(errors) == 1
        assert "Error callback failed while reporting compare/object_comparison" in caplog.text

    def test_compare_continues_after_a_top_level_phase_failure(self, monkeypatch):
        comparator = Comparator()
        original_compare = comparator._compare_with_children

        def fail_cube_phase(old_rows, new_rows, parent_cls, *args, **kwargs):
            if parent_cls is Cube:
                raise RuntimeError("cube collection unavailable")
            return original_compare(old_rows, new_rows, parent_cls, *args, **kwargs)

        monkeypatch.setattr(comparator, "_compare_with_children", fail_cube_phase)
        old_process = make_process(name="Good", parameters=[])
        new_process = make_process(name="Good", parameters=[{"name": "p", "value": "new"}])

        changeset, errors = comparator.compare(
            Model(cubes=[], dimensions=[], processes=[old_process], chores=[]),
            Model(cubes=[], dimensions=[], processes=[new_process], chores=[]),
        )

        assert any(change.object_type == ObjectType.PROCESS for change in changeset.changes)
        assert [(error.phase, error.subject, error.severity) for error in errors] == [
            ("object_phase", "Cube", "fatal"),
        ]

    def test_compare_records_child_relation_error_and_keeps_other_relations(self):
        class ExplodingIterable:
            def __iter__(self):
                raise RuntimeError("views cannot be read")

        broken_old = make_cube(name="Broken")
        broken_new = make_cube(name="Broken")
        broken_new.views = ExplodingIterable()
        good_old = make_cube(name="Good")
        good_new = make_cube(name="Good")
        good_new.views = [make_mdx_view(name="AddedView")]

        changeset, errors = Comparator().compare(
            Model(cubes=[broken_old, good_old], dimensions=[], processes=[], chores=[], total_object_count=2),
            Model(cubes=[broken_new, good_new], dimensions=[], processes=[], chores=[], total_object_count=2),
        )

        assert any(
            change.object_type == ObjectType.MDX_VIEW and change.body.name == "AddedView"
            for change in changeset.changes
        )
        assert [(error.phase, error.subject, error.severity) for error in errors] == [
            ("child_relation", "Cubes('Broken')", "recoverable"),
        ]


    def test_objects_equal(self, objects_equal_data):
        obj1, obj2, shallow_fn, expect_strict_equal = objects_equal_data

        if expect_strict_equal:
            assert obj1 == obj2
        else:
            assert obj1 != obj2

        if shallow_fn:
            assert shallow_fn(obj1, obj2)

    def test_process_equality_uses_ti_section_equality(self):
        exported_ti = TI(
            prolog_procedure="AsciiOutput('file.txt', 'prolog');\n",
            metadata_procedure="\n# Metadata",
            data_procedure="# Data\n\n",
            epilog_procedure="\n# Epilog\n",
        )
        deserialized_ti = TI.from_string(exported_ti.ti_as_string())
        exported_process = Process(
            name="P",
            hasSecurityAccess=True,
            code_link="P.ti",
            datasource=None,
            parameters=[],
            variables=[],
            ti=exported_ti,
        )
        deserialized_process = Process(
            name="P",
            hasSecurityAccess=True,
            code_link="P.ti",
            datasource=None,
            parameters=[],
            variables=[],
            ti=deserialized_ti,
        )

        assert exported_ti == deserialized_ti
        assert exported_process == deserialized_process

    def test_comparator_detects_edge_weight_only_change_in_memory(self):
        """Edge-only weight delta must appear as an Edge MODIFY (list-backed hierarchies)."""
        hier_old = Hierarchy(
            name="H",
            elements=[
                Element(name="A", type="Numeric"),
                Element(name="B", type="Numeric"),
            ],
            edges=[Edge("A", "B", 1)],
            subsets=[],
        )
        hier_new = Hierarchy(
            name="H",
            elements=[
                Element(name="A", type="Numeric"),
                Element(name="B", type="Numeric"),
            ],
            edges=[Edge("A", "B", 2)],
            subsets=[],
        )
        dim_old = Dimension(name="D", hierarchies=[hier_old], defaultHierarchy=hier_old)
        dim_new = Dimension(name="D", hierarchies=[hier_new], defaultHierarchy=hier_new)
        model_old = Model(dimensions=[dim_old], cubes=[], processes=[], chores=[])
        model_new = Model(dimensions=[dim_new], cubes=[], processes=[], chores=[])
        changeset = Comparator().compare(model_old, model_new, mode="full")
        modified_edges = self._bodies_by(
            self._changes_by_type(changeset, ChangeType.MODIFY),
            Edge,
        )
        assert len(modified_edges) == 1 and modified_edges[0].weight == 2

    def test_edge_store_content_signature_includes_weight(self, tmp_path):
        """Regression: parallel edge hash must depend on Weight or comparator skips streaming diff."""
        store = ModelStore.for_model_id(f"edge_sig_{tmp_path.name}")
        seq = StoreBackedSequence.for_edges_sink(
            store=store,
            model_id=f"edge_sig_{tmp_path.name}",
            dimension_name="D",
            hierarchy_name="H",
        )
        seq.replace_with_payloads(())
        seq.append(Edge("P", "C", 1))
        sig_w1 = seq.recalculate_content_signature_parallel()
        seq.replace_with_payloads(())
        seq.append(Edge("P", "C", 2))
        sig_w2 = seq.recalculate_content_signature_parallel()
        assert sig_w1 != sig_w2

    def test_subset_store_content_signature_includes_static_element_ids(self, tmp_path):
        """Static subset member changes must not be hidden behind a matching store hash."""
        model_id = f"subset_sig_{tmp_path.name}"
        first_element_id = "Dimensions('Product')/Hierarchies('Product')/Elements('Bike')"
        second_element_id = "Dimensions('Product')/Hierarchies('Product')/Elements('Helmet')"
        old_hierarchy = Hierarchy(
            name="Product",
            dimension_name="Product",
            model_id=model_id + "_old",
        )
        new_hierarchy = Hierarchy(
            name="Product",
            dimension_name="Product",
            model_id=model_id + "_new",
        )
        matching_hierarchy = Hierarchy(
            name="Product",
            dimension_name="Product",
            model_id=model_id + "_matching",
        )
        old_hierarchy.subsets.append(Subset(name="Static", element_ids=[first_element_id]))
        new_hierarchy.subsets.append(Subset(name="Static", element_ids=[second_element_id]))
        matching_hierarchy.subsets.append(Subset(name="Static", element_ids=[first_element_id]))

        old_signature = old_hierarchy.subsets.recalculate_content_signature_parallel()
        new_signature = new_hierarchy.subsets.recalculate_content_signature_parallel()
        matching_signature = matching_hierarchy.subsets.recalculate_content_signature_parallel()
        assert old_signature != new_signature
        assert old_signature == matching_signature

        old_dimension = Dimension(
            name="Product",
            hierarchies=[old_hierarchy],
            defaultHierarchy=old_hierarchy,
        )
        new_dimension = Dimension(
            name="Product",
            hierarchies=[new_hierarchy],
            defaultHierarchy=new_hierarchy,
        )
        changeset = Comparator().compare(
            Model(dimensions=[old_dimension], cubes=[], processes=[], chores=[]),
            Model(dimensions=[new_dimension], cubes=[], processes=[], chores=[]),
            mode="full",
        )
        modified_subsets = self._bodies_by(
            self._changes_by_type(changeset, ChangeType.MODIFY),
            Subset,
        )
        assert modified_subsets == [Subset(name="Static", element_ids=[second_element_id])]


    def test_comparator_no_changes_round_trip(self, tmp_path):
        model1, error1 = deserialize_model(str(test_model_dir_base))
        serialize_model(model=model1, dir=str(tmp_path), max_workers=1)
        model2, error2 = deserialize_model(str(tmp_path))
        
        comparator = Comparator()
        changeset = comparator.compare(model1, model2, mode='full')
        assert len(changeset.changes) == 0


    def test_comparator_has_changes_add_only(self):
        model1, error1 = deserialize_model(str(test_model_dir_base))
        model2, error2 = deserialize_model(str(test_model_dir_diff))

        comparator = Comparator()
        changeset = comparator.compare(model1, model2, mode='add_only')
        added = self._changes_by_type(changeset, ChangeType.ADD)
        modified = self._changes_by_type(changeset, ChangeType.MODIFY)
        removed = self._changes_by_type(changeset, ChangeType.REMOVE)

        assert len(added) == 6
        assert len(modified) == 5
        assert len(removed) == 0


    def test_comparator_has_changes_full(self):
        model1, error1 = deserialize_model(str(test_model_dir_base))
        model2, error2 = deserialize_model(str(test_model_dir_diff))

        comparator = Comparator()
        changeset = comparator.compare(model1, model2, mode='full')
        added = self._changes_by_type(changeset, ChangeType.ADD)
        modified = self._changes_by_type(changeset, ChangeType.MODIFY)
        removed = self._changes_by_type(changeset, ChangeType.REMOVE)

        assert len(added) == 6
        assert len(modified) == 5
        assert len(removed) == 5


    def test_comparator_dimensions_change_propagation(self):
        """Test if adding a new Subset does not propagate as a change to the Dimension object"""
        model1, error1 = deserialize_model(str(test_model_dir_base))
        model2, error2 = deserialize_model(str(test_model_dir_diff))

        comparator = Comparator()
        changeset = comparator.compare(model1, model2, mode='full')
        added = self._bodies_by(self._changes_by_type(changeset, ChangeType.ADD), Subset)
        modified = self._bodies_by(self._changes_by_type(changeset, ChangeType.MODIFY), Hierarchy)

        assert (isinstance(added[0], Subset) and added[0].name == "}Temp_Subset_Discount")
        assert not modified

    def test_comparator_adds_dimension_children_when_dimension_missing(self):
        hierarchy_obj = Hierarchy(
            name="DimA",
            elements=[Element(name="Elem1", type="Numeric")],
            edges=[],
            subsets=[],
        )
        dimension_obj = Dimension(
            name="DimA",
            hierarchies=[hierarchy_obj],
            defaultHierarchy=hierarchy_obj,
        )
        model_old = Model(cubes=[], dimensions=[], processes=[], chores=[])
        model_new = Model(cubes=[], dimensions=[dimension_obj], processes=[], chores=[])

        changeset = Comparator().compare(model_old, model_new, mode="full")
        added = self._changes_by_type(changeset, ChangeType.ADD)

        assert any(isinstance(change.body, Dimension) and change.body.name == "DimA" for change in added)
        assert any(
            isinstance(change.body, Hierarchy) and change.uri == "Dimensions('DimA')/Hierarchies('DimA')"
            for change in added
        )
        assert any(
            isinstance(change.body, Element)
            and change.uri == "Dimensions('DimA')/Hierarchies('DimA')/Elements('Elem1')"
            for change in added
        )


    def test_comparator_cubes_change_propagation(self):
        model1, error1 = deserialize_model(str(test_model_dir_base))
        model2, error2 = deserialize_model(str(test_model_dir_diff))

        comparator = Comparator()
        changeset = comparator.compare(model1, model2, mode='full')
        added = self._bodies_by(self._changes_by_type(changeset, ChangeType.ADD), MDXView)
        removed = self._bodies_by(self._changes_by_type(changeset, ChangeType.REMOVE), MDXView)
        modified = self._bodies_by(self._changes_by_type(changeset, ChangeType.MODIFY), Cube)
        modified_rule_changes = [
            c for c in self._changes_by_type(changeset, ChangeType.MODIFY)
            if c.object_type == ObjectType.RULE
        ]
        modified_rules = [c.body for c in modified_rule_changes]

        old_cube = next(c for c in model1.cubes if c.name == "testbenchSales")
        new_cube = next(c for c in model2.cubes if c.name == "testbenchSales")

        assert (isinstance(added[0], MDXView) and added[0].name == "tm1_bedrock_py_gp0vkg064lilmmga")
        assert not modified
        assert (old_cube.rules != new_cube.rules)
        assert len(modified_rules) == 1
        unified_rule = modified_rules[0]
        assert isinstance(unified_rule, Rule)
        assert unified_rule.name == "default"
        assert modified_rule_changes[0].uri == "Cubes('testbenchSales')/Rules('default')"
        assert unified_rule.full_statement == new_cube.get_rule_text()
        assert (isinstance(removed[0], MDXView) and removed[0].name == "tm1_bedrock_py_fp0vkg064lilmmga")

    def test_comparator_detects_drillthrough_rule_modify(self):
        old_cube = Cube(
            name="Sales",
            dimensions=["Versions"],
            rules=[],
            views=[],
            drillthrough_rules=[
                Rule(area="[default]", full_statement="[]=s:'old_drill';", name="default")
            ],
        )
        new_cube = Cube(
            name="Sales",
            dimensions=["Versions"],
            rules=[],
            views=[],
            drillthrough_rules=[
                Rule(area="[default]", full_statement="[]=s:'new_drill';", name="default")
            ],
        )
        model_old = Model(cubes=[old_cube], dimensions=[], processes=[], chores=[])
        model_new = Model(cubes=[new_cube], dimensions=[], processes=[], chores=[])

        changeset = Comparator().compare(model_old, model_new, mode="full")
        modified_rule_changes = [
            c for c in self._changes_by_type(changeset, ChangeType.MODIFY)
            if c.object_type == ObjectType.RULE
        ]

        assert len(modified_rule_changes) == 1
        assert modified_rule_changes[0].uri == "Cubes('Sales')/DrillthroughRules('default')"
        assert modified_rule_changes[0].body.full_statement == "[]=s:'new_drill';"

    def test_comparator_unchanged_drillthrough_rule_creates_no_change(self):
        drillthrough_rule = Rule(
            area="[default]",
            full_statement="[]=s:'simple_drillthrough';",
            name="default",
        )
        old_cube = Cube(
            name="Sales",
            dimensions=["Versions"],
            rules=[],
            views=[],
            drillthrough_rules=[drillthrough_rule],
        )
        new_cube = Cube(
            name="Sales",
            dimensions=["Versions"],
            rules=[],
            views=[],
            drillthrough_rules=[drillthrough_rule],
        )
        model_old = Model(cubes=[old_cube], dimensions=[], processes=[], chores=[])
        model_new = Model(cubes=[new_cube], dimensions=[], processes=[], chores=[])

        changeset = Comparator().compare(model_old, model_new, mode="full")

        assert not changeset.has_changes()

    def test_comparator_skips_store_backed_compare_when_hash_matches(self, tmp_path, caplog):
        store = ModelStore.for_model_id(tmp_path.name)

        def disk_elements(group_suffix: str, *elems: Element) -> StoreBackedSequence:
            db = StoreBackedSequence.for_elements_sink(
                store=store,
                dimension_name="DimA",
                hierarchy_name=group_suffix,
            )
            db.replace_with_payloads([e.to_dict() for e in sorted(elems, key=lambda x: (x.name or ""))])
            return db

        h_old = Hierarchy(
            name="H1",
            elements=disk_elements(
                "H1_old_hash",
                Element(name="A", type="Numeric"),
                Element(name="B", type="Numeric"),
            ),
            edges=[],
            subsets=[],
        )
        h_new = Hierarchy(
            name="H1",
            elements=disk_elements(
                "H1_new_hash",
                Element(name="A", type="Numeric"),
                Element(name="B", type="Numeric"),
            ),
            edges=[],
            subsets=[],
        )
        d_old = Dimension(name="DimA", hierarchies=[h_old], defaultHierarchy=h_old)
        d_new = Dimension(name="DimA", hierarchies=[h_new], defaultHierarchy=h_new)
        model_old = Model(dimensions=[d_old], cubes=[], processes=[], chores=[])
        model_new = Model(dimensions=[d_new], cubes=[], processes=[], chores=[])

        caplog.set_level("INFO")
        changeset = Comparator().compare(model_old, model_new, mode="full")

        assert len(changeset.changes) == 0
        assert "Skipping Element streaming compare: count+hash match" in caplog.text

    def test_comparator_ignores_leaf_hierarchy_elements_by_default(self):
        model1 = build_mock_model()
        model2 = build_mock_model()

        leaf_hierarchy_old = Hierarchy(
            name="Leaves",
            elements=[Element(name="LeafA", type="Numeric")],
            edges=[],
            subsets=[],
        )
        leaf_hierarchy_new = Hierarchy(
            name="Leaves",
            elements=[
                Element(name="LeafA", type="Numeric"),
                Element(name="LeafB", type="Numeric"),
            ],
            edges=[],
            subsets=[],
        )
        model1.dimensions[0].hierarchies.append(leaf_hierarchy_old)
        model2.dimensions[0].hierarchies.append(leaf_hierarchy_new)

        changeset = Comparator().compare(model1, model2, mode='full')
        leaf_element_changes = [
            change for change in changeset.changes
            if change.object_type == ObjectType.ELEMENT and "Hierarchies('Leaves')" in change.uri
        ]
        assert not leaf_element_changes
        assert any(h.name == "Leaves" for h in model1.dimensions[0].hierarchies)
        assert any(h.name == "Leaves" for h in model2.dimensions[0].hierarchies)

    def test_comparator_can_force_include_leaves_hierarchy_via_filter_rules(self):
        model1 = build_mock_model()
        model2 = build_mock_model()

        leaf_hierarchy_old = Hierarchy(
            name="Leaves",
            elements=[Element(name="LeafA", type="Numeric")],
            edges=[],
            subsets=[],
        )
        leaf_hierarchy_new = Hierarchy(
            name="Leaves",
            elements=[
                Element(name="LeafA", type="Numeric"),
                Element(name="LeafB", type="Numeric"),
            ],
            edges=[],
            subsets=[],
        )
        model1.dimensions[0].hierarchies.append(leaf_hierarchy_old)
        model2.dimensions[0].hierarchies.append(leaf_hierarchy_new)

        changeset = Comparator().compare(
            model1,
            model2,
            mode="full",
            filter_rules=FilterRules(["!Dimensions('*')/Hierarchies('Leaves')"]),
        )
        leaf_element_changes = [
            change
            for change in changeset.changes
            if change.object_type == ObjectType.ELEMENT
            and "Hierarchies('Leaves')" in change.uri
        ]
        assert len(leaf_element_changes) == 1
        assert leaf_element_changes[0].change_type == ChangeType.ADD
        assert leaf_element_changes[0].body.name == "LeafB"
        assert any(h.name == "Leaves" for h in model1.dimensions[0].hierarchies)
        assert any(h.name == "Leaves" for h in model2.dimensions[0].hierarchies)

    def test_comparator_uses_canonical_tm1git_shorthand_filter_rules(self):
        model1 = build_mock_model()
        model2 = build_mock_model()
        model2.cubes[0].views[0].mdx = (
            "SELECT {TM1FILTERBYLEVEL({TM1SUBSETALL([MockDim].[MockHier])}, 0)} "
            "ON 0 FROM [MockCube]"
        )

        unfiltered = Comparator().compare(model1, model2, mode="full")
        assert any(change.object_type == ObjectType.MDX_VIEW for change in unfiltered.changes)

        filtered = Comparator().compare(
            model1,
            model2,
            mode="full",
            filter_rules=FilterRules(["Cubes/Views"]),
        )
        assert not any(change.object_type == ObjectType.MDX_VIEW for change in filtered.changes)

    def test_comparator_streaming_store_backed_elements_and_edges(self, tmp_path):
        """StoreBackedSequence merge compare should match in-memory list compare."""

        store = ModelStore.for_model_id(tmp_path.name)

        def disk_elements(group_suffix: str, *elems: Element) -> StoreBackedSequence:
            db = StoreBackedSequence.for_elements_sink(
                store=store,
                dimension_name="DimA",
                hierarchy_name=group_suffix,
            )
            db.replace_with_payloads(())
            for e in sorted(elems, key=lambda x: (x.name or "")):
                db.append(e)
            return db

        def disk_edges(group_suffix: str, *edges: Edge) -> StoreBackedSequence:
            db = StoreBackedSequence.for_edges_sink(
                store=store,
                dimension_name="DimA",
                hierarchy_name=group_suffix,
            )
            db.replace_with_payloads(())
            for ed in sorted(edges, key=lambda x: (x.parent or "", x.component_name or "")):
                db.append(ed)
            return db

        h_old = Hierarchy(
            name="H1",
            elements=disk_elements(
                "H1_old",
                Element(name="A", type="Numeric"),
                Element(name="C", type="Numeric"),
            ),
            edges=disk_edges(
                "H1_old",
                Edge(parent="R", component_name="A", weight=1.0),
            ),
            subsets=[],
        )
        h_new = Hierarchy(
            name="H1",
            elements=disk_elements(
                "H1_new",
                Element(name="A", type="String"),
                Element(name="B", type="Numeric"),
                Element(name="C", type="Numeric"),
            ),
            edges=disk_edges(
                "H1_new",
                Edge(parent="R", component_name="A", weight=2.0),
                Edge(parent="R", component_name="B", weight=1.0),
            ),
            subsets=[],
        )
        d_disk_old = Dimension(name="DimA", hierarchies=[h_old], defaultHierarchy=h_old)
        d_disk_new = Dimension(name="DimA", hierarchies=[h_new], defaultHierarchy=h_new)

        mh_old = Hierarchy(
            name="H1",
            elements=[
                Element(name="A", type="Numeric"),
                Element(name="C", type="Numeric"),
            ],
            edges=[Edge(parent="R", component_name="A", weight=1.0)],
            subsets=[],
        )
        mh_new = Hierarchy(
            name="H1",
            elements=[
                Element(name="A", type="String"),
                Element(name="B", type="Numeric"),
                Element(name="C", type="Numeric"),
            ],
            edges=[
                Edge(parent="R", component_name="A", weight=2.0),
                Edge(parent="R", component_name="B", weight=1.0),
            ],
            subsets=[],
        )
        d_mem_old = Dimension(name="DimA", hierarchies=[mh_old], defaultHierarchy=mh_old)
        d_mem_new = Dimension(name="DimA", hierarchies=[mh_new], defaultHierarchy=mh_new)

        model_disk = Model(dimensions=[d_disk_old], cubes=[], processes=[], chores=[])
        model_disk_b = Model(dimensions=[d_disk_new], cubes=[], processes=[], chores=[])
        model_mem = Model(dimensions=[d_mem_old], cubes=[], processes=[], chores=[])
        model_mem_b = Model(dimensions=[d_mem_new], cubes=[], processes=[], chores=[])

        comp = Comparator()
        cs_disk = comp.compare(model_disk, model_disk_b, mode="full")
        cs_mem = comp.compare(model_mem, model_mem_b, mode="full")

        def change_key(c: Change) -> tuple:
            return (
                c.change_type,
                c.object_type,
                c.uri,
                type(c.body).__name__,
                getattr(c.body, "name", None),
                getattr(c.body, "parent", None),
                getattr(c.body, "component_name", None),
                getattr(c.body, "type", None),
                getattr(c.body, "weight", None),
            )

        assert sorted(change_key(c) for c in cs_disk.changes) == sorted(
            change_key(c) for c in cs_mem.changes
        )

    def test_comparator_sorts_unsorted_store_backed_lists_before_merge(self, tmp_path):
        store = ModelStore.for_model_id(tmp_path.name)

        def disk_elements_unsorted(group_suffix: str, *elems: Element) -> StoreBackedSequence:
            db = StoreBackedSequence.for_elements_sink(
                store=store,
                dimension_name="DimA",
                hierarchy_name=group_suffix,
            )
            db.replace_with_payloads(())
            for e in elems:
                db.append(e)
            return db

        old_elements = disk_elements_unsorted(
            "H1_old_unsorted",
            Element(name="C", type="Numeric"),
            Element(name="A", type="Numeric"),
        )
        new_elements = disk_elements_unsorted(
            "H1_new_unsorted",
            Element(name="A", type="Numeric"),
            Element(name="B", type="Numeric"),
            Element(name="C", type="Numeric"),
        )

        h_old = Hierarchy(name="H1", elements=old_elements, edges=[], subsets=[])
        h_new = Hierarchy(name="H1", elements=new_elements, edges=[], subsets=[])
        d_old = Dimension(name="DimA", hierarchies=[h_old], defaultHierarchy=h_old)
        d_new = Dimension(name="DimA", hierarchies=[h_new], defaultHierarchy=h_new)

        changeset = Comparator().compare(
            Model(dimensions=[d_old], cubes=[], processes=[], chores=[]),
            Model(dimensions=[d_new], cubes=[], processes=[], chores=[]),
            mode="full",
        )

        add_names = sorted(
            c.body.name
            for c in changeset.changes
            if c.change_type == ChangeType.ADD and c.object_type == ObjectType.ELEMENT
        )
        remove_names = sorted(
            c.body.name
            for c in changeset.changes
            if c.change_type == ChangeType.REMOVE and c.object_type == ObjectType.ELEMENT
        )
        assert add_names == ["B"]
        assert remove_names == []

    def test_comparator_uses_identity_ordered_iteration_for_store_backed_lists(self, tmp_path):
        store = ModelStore.for_model_id(tmp_path.name)

        def disk_elements_sorted(group_suffix: str, *elems: Element) -> StoreBackedSequence:
            db = StoreBackedSequence.for_elements_sink(
                store=store,
                dimension_name="DimA",
                hierarchy_name=group_suffix,
            )
            db.replace_with_payloads(())
            for e in sorted(elems, key=lambda x: (x.name or "", x.type or "")):
                db.append(e)
            return db

        old_elements = disk_elements_sorted(
            "H1_old_sorted",
            Element(name="A", type="Numeric"),
            Element(name="C", type="Numeric"),
        )
        new_elements = disk_elements_sorted(
            "H1_new_sorted",
            Element(name="A", type="Numeric"),
            Element(name="B", type="Numeric"),
            Element(name="C", type="Numeric"),
        )

        h_old = Hierarchy(name="H1", elements=old_elements, edges=[], subsets=[])
        h_new = Hierarchy(name="H1", elements=new_elements, edges=[], subsets=[])
        d_old = Dimension(name="DimA", hierarchies=[h_old], defaultHierarchy=h_old)
        d_new = Dimension(name="DimA", hierarchies=[h_new], defaultHierarchy=h_new)
        changeset = Comparator().compare(
            Model(dimensions=[d_old], cubes=[], processes=[], chores=[]),
            Model(dimensions=[d_new], cubes=[], processes=[], chores=[]),
            mode="full",
        )
        add_names = sorted(
            c.body.name
            for c in changeset.changes
            if c.change_type == ChangeType.ADD and c.object_type == ObjectType.ELEMENT
        )
        assert add_names == ["B"]

    def test_comparator_streaming_filter_rules_keep_inputs_unchanged(self, tmp_path):
        store = ModelStore.for_model_id(f"stream_filter_{tmp_path.name}")

        def disk_elements(group_suffix: str, *elems: Element) -> StoreBackedSequence:
            db = StoreBackedSequence.for_elements_sink(
                store=store,
                dimension_name="DimA",
                hierarchy_name=group_suffix,
            )
            db.replace_with_payloads(())
            for e in sorted(elems, key=lambda x: (x.name or "")):
                db.append(e)
            return db

        old_elements = disk_elements(
            "H1_old_filter",
            Element(name="A", type="Numeric"),
            Element(name="B", type="Numeric"),
        )
        new_elements = disk_elements(
            "H1_new_filter",
            Element(name="A", type="Numeric"),
            Element(name="B", type="String"),
        )

        h_old = Hierarchy(name="H1", elements=old_elements, edges=[], subsets=[])
        h_new = Hierarchy(name="H1", elements=new_elements, edges=[], subsets=[])
        d_old = Dimension(name="DimA", hierarchies=[h_old], defaultHierarchy=h_old)
        d_new = Dimension(name="DimA", hierarchies=[h_new], defaultHierarchy=h_new)
        model_old = Model(dimensions=[d_old], cubes=[], processes=[], chores=[])
        model_new = Model(dimensions=[d_new], cubes=[], processes=[], chores=[])

        before_old_payloads = list(old_elements.iter_payloads())
        before_new_payloads = list(new_elements.iter_payloads())

        changeset = Comparator().compare(
            model_old,
            model_new,
            mode="full",
            filter_rules=FilterRules(["Dimensions('DimA')/Hierarchies('H1')/Elements('B')"]),
        )

        element_changes = [c for c in changeset.changes if c.object_type == ObjectType.ELEMENT]
        assert element_changes == []
        assert before_old_payloads == list(old_elements.iter_payloads())
        assert before_new_payloads == list(new_elements.iter_payloads())

    def test_comparator_coerces_empty_list_to_store_backed_from_peer(self, tmp_path):
        store = ModelStore.for_model_id(f"coerce_{tmp_path.name}")
        peer = StoreBackedSequence.for_subsets_sink(
            store=store,
            model_id=f"coerce_{tmp_path.name}",
            dimension_name="DimA",
            hierarchy_name="H1",
        )
        coerced = Comparator._coerce_empty_collection_to_store_backed([], peer, child_cls=Subset)
        assert isinstance(coerced, StoreBackedSequence)
        assert len(coerced) == 0

    def test_comparator_uses_streaming_path_when_old_side_is_empty_list(self, tmp_path, caplog):
        model_id = f"coerce_stream_{tmp_path.name}"
        h_new = Hierarchy(
            name="H1",
            dimension_name="DimA",
            model_id=model_id,
            elements_filter_rules=[],
            edges_filter_rules=[],
            subsets_filter_rules=[],
        )
        h_new.elements.replace_with_payloads([Element(name="A", type="Numeric").to_dict()])
        h_new.edges.replace_with_payloads([Edge(parent="A", component_name="A", weight=1).to_dict()])
        d_new = Dimension(name="DimA", hierarchies=[h_new], defaultHierarchy=h_new)
        model_old = Model(dimensions=[], cubes=[], processes=[], chores=[])
        model_new = Model(dimensions=[d_new], cubes=[], processes=[], chores=[])

        caplog.set_level("INFO")
        changeset = Comparator().compare(model_old, model_new, mode="full")
        assert any(c.object_type == ObjectType.ELEMENT and c.change_type == ChangeType.ADD for c in changeset.changes)
        assert "Starting compare phase=Element streaming old_size=0" in caplog.text
        assert "Starting compare phase=Edge streaming old_size=0" in caplog.text

    def test_comparator_detects_hierarchy_sort_metadata_change(self):
        h_old = Hierarchy(name="H1", elements=[], edges=[], subsets=[])
        h_new = Hierarchy(
            name="H1",
            elements=[],
            edges=[],
            subsets=[],
            elements_sort_type="ByInput",
        )
        d_old = Dimension(name="DimA", hierarchies=[h_old], defaultHierarchy=h_old)
        d_new = Dimension(name="DimA", hierarchies=[h_new], defaultHierarchy=h_new)

        changeset = Comparator().compare(
            Model(dimensions=[d_old], cubes=[], processes=[], chores=[]),
            Model(dimensions=[d_new], cubes=[], processes=[], chores=[]),
            mode="full",
        )

        hierarchy_changes = [c for c in changeset.changes if c.object_type == ObjectType.HIERARCHY]
        assert len(hierarchy_changes) == 1
        assert hierarchy_changes[0].change_type == ChangeType.MODIFY
        assert hierarchy_changes[0].uri == "Dimensions('DimA')/Hierarchies('H1')"

    def test_comparator_detects_element_move_as_hierarchy_change_without_element_noise(self):
        h_old = Hierarchy(
            name="H1",
            elements=[
                Element(name="A", type="Numeric", element_index=0),
                Element(name="B", type="Numeric", element_index=1),
            ],
            edges=[],
            subsets=[],
            elements_sort_type="ByInput",
        )
        h_new = Hierarchy(
            name="H1",
            elements=[
                Element(name="A", type="Numeric", element_index=1),
                Element(name="B", type="Numeric", element_index=0),
            ],
            edges=[],
            subsets=[],
            elements_sort_type="ByInput",
        )
        d_old = Dimension(name="DimA", hierarchies=[h_old], defaultHierarchy=h_old)
        d_new = Dimension(name="DimA", hierarchies=[h_new], defaultHierarchy=h_new)

        changeset = Comparator().compare(
            Model(dimensions=[d_old], cubes=[], processes=[], chores=[]),
            Model(dimensions=[d_new], cubes=[], processes=[], chores=[]),
            mode="full",
        )

        assert [c.object_type for c in changeset.changes].count(ObjectType.HIERARCHY) == 1
        assert not [c for c in changeset.changes if c.object_type == ObjectType.ELEMENT]

    def test_comparator_detects_store_backed_edge_move_without_edge_noise(self, tmp_path):
        store = ModelStore.for_model_id(f"cmp_order_{tmp_path.name}")

        old_edges = StoreBackedSequence.for_edges_sink(
            store=store,
            dimension_name="DimA",
            hierarchy_name="H1_old_order",
        )
        new_edges = StoreBackedSequence.for_edges_sink(
            store=store,
            dimension_name="DimA",
            hierarchy_name="H1_new_order",
        )
        old_edges.replace_with_payloads(())
        new_edges.replace_with_payloads(())
        old_edges.extend_payloads(
            [
                {"ParentName": "P", "ComponentName": "A", "Weight": 1},
                {"ParentName": "P", "ComponentName": "B", "Weight": 1},
            ],
            start_index=0,
        )
        new_edges.extend_payloads(
            [
                {"ParentName": "P", "ComponentName": "B", "Weight": 1},
                {"ParentName": "P", "ComponentName": "A", "Weight": 1},
            ],
            start_index=0,
        )
        h_old = Hierarchy(name="H1", elements=[], edges=old_edges, subsets=[], components_sort_type="ByInput")
        h_new = Hierarchy(name="H1", elements=[], edges=new_edges, subsets=[], components_sort_type="ByInput")
        d_old = Dimension(name="DimA", hierarchies=[h_old], defaultHierarchy=h_old)
        d_new = Dimension(name="DimA", hierarchies=[h_new], defaultHierarchy=h_new)

        changeset = Comparator().compare(
            Model(dimensions=[d_old], cubes=[], processes=[], chores=[]),
            Model(dimensions=[d_new], cubes=[], processes=[], chores=[]),
            mode="full",
        )

        assert [c.object_type for c in changeset.changes].count(ObjectType.HIERARCHY) == 1
        assert not [c for c in changeset.changes if c.object_type == ObjectType.EDGE]

    def test_comparator_tracks_native_view_changes(self):
        model1 = build_mock_model()
        model2 = build_mock_model()

        model2.cubes[0].views.append(
            NativeView(
                name="DefaultNative",
                columns=[],
                rows=[],
                titles=[],
                suppress_empty_columns=True,
                suppress_empty_rows=True,
                format_string="0.#########",
            )
        )

        changeset = Comparator().compare(model1, model2, mode='full')
        native_adds = [
            change for change in changeset.changes
            if change.change_type == ChangeType.ADD and change.object_type == ObjectType.NATIVE_VIEW
        ]
        assert len(native_adds) == 1
        assert native_adds[0].body.name == "DefaultNative"


    def test_comparator_process_change_propagation(self):
        model1, error1 = deserialize_model(str(test_model_dir_base))
        model2, error2 = deserialize_model(str(test_model_dir_diff))

        comparator = Comparator()
        changeset = comparator.compare(model1, model2, mode='full')
        removed = self._bodies_by(self._changes_by_type(changeset, ChangeType.REMOVE), Process)
        modified = self._bodies_by(self._changes_by_type(changeset, ChangeType.MODIFY), Process)

        assert (isinstance(removed[0], Process) and removed[0].name == "Mock Process Load Product Data")
        assert (isinstance(modified[0], Process) and modified[0].name == "Mock Process Export Dimension")


    def test_comparator_chores_change_propagation(self):
        model1, error1 = deserialize_model(str(test_model_dir_base))
        model2, error2 = deserialize_model(str(test_model_dir_diff))

        expected_chores = ["Mock Nightly Maintenance", "Mock Weekly Export"]

        comparator = Comparator()
        changeset = comparator.compare(model1, model2, mode='full')
        removed = self._bodies_by(self._changes_by_type(changeset, ChangeType.REMOVE), Chore)
        modified = self._bodies_by(self._changes_by_type(changeset, ChangeType.MODIFY), Chore)

        for chore_new in modified:
            assert (isinstance(chore_new, Chore) and chore_new.name in expected_chores )
        assert any(isinstance(chore_old, Chore) and chore_old.name == "Mock Weekly Export" for chore_old in removed)

    def test_comparator_emits_progress_events_with_sink(self):
        class _CaptureSink:
            def __init__(self):
                self.events = []

            def on_event(self, event):
                self.events.append(event)

            def close(self):
                return

        model1 = build_mock_model()
        model2 = build_mock_model()
        sink = _CaptureSink()

        changeset, errors = Comparator().compare(
            model1,
            model2,
            mode="full",
            progress_sink=sink,
        )

        assert isinstance(changeset, Changeset)
        assert errors == []
        assert any(event.scope.value == "TOTAL" and event.kind.value == "start" for event in sink.events)
        assert any(event.scope.value == "TOTAL" and event.kind.value == "update" for event in sink.events)

    def test_comparator_result_unchanged_with_progress_sink(self):
        class _NoopSink:
            def on_event(self, event):
                _ = event

            def close(self):
                return

        model_old = build_mock_model()
        model_new = build_mock_model()
        model_new.cubes[0].views.append(
            NativeView(
                name="ExtraNative",
                columns=[],
                rows=[],
                titles=[],
                suppress_empty_columns=True,
                suppress_empty_rows=True,
                format_string="0.#########",
            )
        )

        cs_plain = Comparator().compare(model_old, model_new, mode="full")
        cs_with_progress = Comparator().compare(
            model_old,
            model_new,
            mode="full",
            progress_sink=_NoopSink(),
        )

        def _change_key(change: Change) -> tuple:
            return (
                change.change_type,
                change.object_type,
                change.uri,
                type(change.body).__name__,
                getattr(change.body, "name", None),
                getattr(change.body, "parent", None),
                getattr(change.body, "component_name", None),
                getattr(change.body, "type", None),
                getattr(change.body, "weight", None),
            )

        assert sorted(_change_key(item) for item in cs_plain.changes) == sorted(
            _change_key(item) for item in cs_with_progress.changes
        )
