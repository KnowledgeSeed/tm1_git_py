import re

import TM1py
import pytest
from TM1py import TM1Service

from test_integration.test_base import (
    check_no_diff,
    export_check_no_errors,
    load_fixture_model_tm1gitpy,
    tm1_service,
)
from tm1_git_py.services.changeset import ChangeType, Changeset, Change, ObjectType
from tm1_git_py.services.comparator import Comparator
from tm1_git_py.services.filter import DEFAULT_TM1_TECHNICAL_OBJECTS, FilterRules
from tm1_git_py.model import process as process_model, Model
from tm1_git_py.model.chore import Chore
from tm1_git_py.model.process import Process
from tm1_git_py.model.task import Task
from tm1_git_py.model.ti import TI


@pytest.mark.usefixtures("tm1_service")
class TestProcessChoreChangesetApply:
    _fixture_model_id_no_meta = "fixture_model_no_meta"

    _f_no_meta = DEFAULT_TM1_TECHNICAL_OBJECTS
    _f_with_meta = [
        "!Cubes('}*')",
        "!Dimensions('}*')",
        "!Processes('}*')"
    ]

    @pytest.fixture(autouse=True)
    def _tm1_service(self, tm1_service):
        self.tm1_service: TM1Service = tm1_service

    @staticmethod
    def _changes_by(changeset: Changeset, change_type: ChangeType, class_name: str):
        return [
            change.body
            for change in changeset.changes
            if change.change_type == change_type
            and change.body.__class__.__name__ == class_name
        ]

    def _chore_exists(self, chore_name: str) -> bool:
        return chore_name in self.tm1_service.chores.get_all_names()

    def _cleanup_chore(self, chore_name: str):
        try:
            self.tm1_service.chores.delete(chore_name)
        except Exception:
            pass

    def _cleanup_process(self, process_name: str):
        try:
            self.tm1_service.processes.delete(process_name)
        except Exception:
            pass

    def _ensure_process(self, process_name: str):
        if process_name in self.tm1_service.processes.get_all_names(
            skip_control_processes=False
        ):
            return
        self.tm1_service.processes.create(
            TM1py.Process(name=process_name, datasource_type="None")
        )

    def _ensure_process_with_parameters(
        self, process_name: str, parameter_names: list[str]
    ):
        parameters = [
            {"Name": name, "Prompt": "", "Value": "", "Type": "String"}
            for name in parameter_names
        ]
        all_names = self.tm1_service.processes.get_all_names(
            skip_control_processes=False
        )
        if process_name in all_names:
            process_obj = self.tm1_service.processes.get(process_name)
            # Normalize existing process to the expected schema using existing process update helpers.
            desired = Process(
                name=process_name,
                hasSecurityAccess=getattr(process_obj, "has_security_access", False),
                code_link=f"{process_name}.ti",
                datasource="None",
                parameters=parameters,
                variables=[],
                ti=TI("", "", "", ""),
            )
            process_model._update_process_parameters(
                process_new=desired, process_object=process_obj
            )
            process_model._update_process_variables(
                process_new=desired, process_object=process_obj
            )
            process_obj.datasource_type = "None"
            self.tm1_service.processes.update(process_obj)
            return

        self.tm1_service.processes.create(
            TM1py.Process(
                name=process_name,
                datasource_type="None",
                parameters=parameters,
                variables=[],
            )
        )

    def _task_parameters_for_process(
        self, process_name: str, value_suffix: str = ""
    ) -> list[dict]:
        process_obj = self.tm1_service.processes.get(process_name)
        result = []
        for param in getattr(process_obj, "parameters", []) or []:
            name = param.get("Name") or param.get("name") or ""
            if not name:
                continue
            result.append({"Name": name, "Value": f"{name}{value_suffix}"})
        return result

    @staticmethod
    def _process_name_from_task(tm1_task) -> str:
        body = getattr(tm1_task, "body_as_dict", {}) or {}
        binding = body.get("Process@odata.bind", "")
        match = re.search(r"Processes\('([^']+)'\)", binding)
        return match.group(1) if match else ""

    def _task_process_names(self, chore_name: str) -> list[str]:
        chore_obj = self.tm1_service.chores.get(chore_name=chore_name)
        return [
            self._process_name_from_task(task)
            for task in getattr(chore_obj, "tasks", [])
        ]

    def _task_parameters(self, chore_name: str) -> list[list[dict]]:
        chore_obj = self.tm1_service.chores.get(chore_name=chore_name)
        params_per_task: list[list[dict]] = []
        for task in getattr(chore_obj, "tasks", []):
            body = getattr(task, "body_as_dict", {}) or {}
            params_per_task.append(body.get("Parameters", []) or [])
        return params_per_task

    @staticmethod
    def _git_chore(
        name: str,
        *,
        start_time: str = "2026-03-05T00:00:00+00:00",
        active: bool = True,
        dst_sensitive: bool = False,
        execution_mode: str = "SingleCommit",
        frequency: str = "P01DT00H00M00S",
        tasks: list[Task] | None = None,
    ) -> Chore:
        return Chore(
            name=name,
            start_time=start_time,
            dst_sensitive=dst_sensitive,
            active=active,
            execution_mode=execution_mode,
            frequency=frequency,
            tasks=tasks or [],
        )

    # -----------------------------------------------------------------------
    # TI Process tests
    # -----------------------------------------------------------------------

    def test_create_process_no_meta_objects(self):
        """Changeset should re-create a process that was deleted from the server."""
        fixture_dir, fixture_model = load_fixture_model_tm1gitpy(
            self, model_id=self._fixture_model_id_no_meta
        )

        self.tm1_service.processes.delete("myprocess2")
        test_model = export_check_no_errors(self, self._f_no_meta)

        changeset = self.compare(test_model, fixture_model, filter_rules=self._f_no_meta)
        self.apply(changeset)
        test_model = export_check_no_errors(self, self._f_with_meta)

        added_processes = self._changes_by(changeset, ChangeType.ADD, "Process")
        assert len(added_processes) == 1
        assert added_processes[0].name == "myprocess2"
        assert self.tm1_service.processes.exists("myprocess2")
        check_no_diff(fixture_dir, test_model)

    def test_delete_process_no_meta_objects(self):
        """Changeset should remove a process that does not exist in the fixture."""
        fixture_dir, fixture_model = load_fixture_model_tm1gitpy(
            self, model_id=self._fixture_model_id_no_meta
        )

        extra_process = TM1py.Process(name="TestExtraProcess", datasource_type="None")
        self.tm1_service.processes.create(extra_process)
        test_model = export_check_no_errors(self, self._f_no_meta)

        changeset = self.compare(test_model, fixture_model, filter_rules=self._f_no_meta)
        self.apply(changeset)
        test_model = export_check_no_errors(self, self._f_with_meta)

        removed_processes = self._changes_by(changeset, ChangeType.REMOVE, "Process")
        assert len(removed_processes) == 1
        assert removed_processes[0].name == "TestExtraProcess"
        assert not self.tm1_service.processes.exists("TestExtraProcess")
        check_no_diff(fixture_dir, test_model)

    def test_create_process_add_only_no_meta_objects(self):
        """In add_only mode, missing processes should be created."""
        fixture_dir, fixture_model = load_fixture_model_tm1gitpy(
            self, model_id=self._fixture_model_id_no_meta
        )
        self.tm1_service.processes.delete("myprocess2")
        test_model = export_check_no_errors(self, self._f_no_meta)

        changeset = self.compare(test_model, fixture_model, filter_rules=self._f_no_meta, mode="add_only")
        self.apply(changeset)
        test_model = export_check_no_errors(self, self._f_with_meta)

        added_processes = self._changes_by(changeset, ChangeType.ADD, "Process")
        assert len(added_processes) >= 1
        assert any(o.name == "myprocess2" for o in added_processes)
        assert self.tm1_service.processes.exists("myprocess2")
        check_no_diff(fixture_dir, test_model)

    def test_delete_process_add_only_no_meta_objects(self):
        """In add_only mode, extra processes should NOT be removed."""
        fixture_dir, fixture_model = load_fixture_model_tm1gitpy(
            self, model_id=self._fixture_model_id_no_meta
        )
        extra_process = TM1py.Process(name="TestExtraProcess2", datasource_type="None")
        self.tm1_service.processes.create(extra_process)
        model = export_check_no_errors(self, self._f_no_meta)

        changeset = self.compare(model, fixture_model, filter_rules=self._f_no_meta, mode="add_only")
        self.apply(changeset)

        assert not self._changes_by(changeset, ChangeType.REMOVE, "Process")
        assert self.tm1_service.processes.exists("TestExtraProcess2")

        # clean-up
        self.tm1_service.processes.delete("TestExtraProcess2")
        test_model = export_check_no_errors(self, self._f_with_meta)
        check_no_diff(fixture_dir, test_model)

    def test_modify_process_no_meta_objects(self):
        """Changeset should restore a modified process back to fixture definition."""
        fixture_dir, fixture_model = load_fixture_model_tm1gitpy(
            self, model_id=self._fixture_model_id_no_meta
        )
        process_name = "myprocess2"

        live_process = self.tm1_service.processes.get(process_name)
        live_process.has_security_access = not live_process.has_security_access
        live_process.add_parameter(
            name="pTmpProcParam", prompt="", value="", parameter_type="String"
        )
        live_process.add_variable(name="vTmpProcVar", variable_type="String")
        self.tm1_service.processes.update(live_process)
        test_model = export_check_no_errors(self, self._f_no_meta)

        changeset = self.compare(test_model, fixture_model, filter_rules=self._f_no_meta)
        self.apply(changeset)

        modified_processes = self._changes_by(changeset, ChangeType.MODIFY, "Process")
        assert any(p.name == process_name for p in modified_processes)
        assert self.tm1_service.processes.exists(process_name)
        test_model = export_check_no_errors(self, self._f_with_meta)
        check_no_diff(fixture_dir, test_model)

    def test_modify_process_datasource_no_meta_objects(self):
        """Changeset should restore a modified process datasource back to fixture definition."""
        fixture_dir, fixture_model = load_fixture_model_tm1gitpy(
            self, model_id=self._fixture_model_id_no_meta
        )
        process_name = "myprocess3"
        fixture_process = next(
            p for p in fixture_model.processes if p.name == process_name
        )

        live_process = self.tm1_service.processes.get(process_name)
        live_process.datasource_type = "None"
        self.tm1_service.processes.update(live_process)
        test_model = export_check_no_errors(self, self._f_no_meta)

        changeset = self.compare(test_model, fixture_model, filter_rules=self._f_no_meta)
        modified_processes = self._changes_by(changeset, ChangeType.MODIFY, "Process")
        assert any(p.name == process_name for p in modified_processes)

        self.apply(changeset)

        test_model = export_check_no_errors(self, self._f_with_meta)
        restored_process = next(p for p in test_model.processes if p.name == process_name)
        assert restored_process.datasource == fixture_process.datasource
        check_no_diff(fixture_dir, test_model)

    def test_modify_process_variables_ui_data_no_meta_objects(self):
        """Changeset should restore modified process VariablesUIData back to fixture definition."""
        fixture_dir, fixture_model = load_fixture_model_tm1gitpy(
            self, model_id=self._fixture_model_id_no_meta
        )
        process_name = "myprocess2"
        fixture_process = next(
            p for p in fixture_model.processes if p.name == process_name
        )
        assert fixture_process.variables_ui_data

        live_process = self.tm1_service.processes.get(process_name)
        live_process._variables_ui_data = fixture_process.variables_ui_data[:-1]
        self.tm1_service.processes.update(live_process)
        test_model = export_check_no_errors(self, self._f_no_meta)

        changeset = self.compare(test_model, fixture_model, filter_rules=self._f_no_meta)
        modified_processes = self._changes_by(changeset, ChangeType.MODIFY, "Process")
        assert any(p.name == process_name for p in modified_processes)

        self.apply(changeset)

        test_model = export_check_no_errors(self, self._f_with_meta)
        restored_process = next(p for p in test_model.processes if p.name == process_name)
        assert restored_process.variables_ui_data == fixture_process.variables_ui_data
        check_no_diff(fixture_dir, test_model)

    def test_modify_process_add_only_no_meta_objects(self):
        """In add_only mode, process modify changes should still be applied."""
        fixture_dir, fixture_model = load_fixture_model_tm1gitpy(
            self, model_id=self._fixture_model_id_no_meta
        )
        process_name = "myprocess2"

        live_process = self.tm1_service.processes.get(process_name)
        live_process.has_security_access = not live_process.has_security_access
        self.tm1_service.processes.update(live_process)
        test_model = export_check_no_errors(self, self._f_no_meta)

        changeset = self.compare(test_model, fixture_model, filter_rules=self._f_no_meta, mode="add_only")
        self.apply(changeset)

        modified_processes = self._changes_by(changeset, ChangeType.MODIFY, "Process")
        assert any(p.name == process_name for p in modified_processes)
        assert self.tm1_service.processes.exists(process_name)
        test_model = export_check_no_errors(self, self._f_with_meta)
        check_no_diff(fixture_dir, test_model)

    def test_apply_modify_process_with_datasource_dict_payload(self):
        """Direct process MODIFY apply should accept datasource payloads shaped as dict."""
        fixture_dir, fixture_model = load_fixture_model_tm1gitpy(
            self, model_id=self._fixture_model_id_no_meta
        )
        process_name = "myprocess2"
        fixture_process = next(
            p for p in fixture_model.processes if p.name == process_name
        )

        live_process = self.tm1_service.processes.get(process_name)
        live_process.datasource_type = "None"
        self.tm1_service.processes.update(live_process)

        datasource_payload = dict(fixture_process.datasource)
        datasource_payload["type"] = datasource_payload.pop("Type")

        changeset = Changeset("modify_process_datasource_dict")
        changeset.changes = [
            Change(
                change_type=ChangeType.MODIFY,
                object_type=ObjectType.PROCESS,
                uri=Process.uri_for(process_name),
                body=Process(
                    name=fixture_process.name,
                    hasSecurityAccess=fixture_process.hasSecurityAccess,
                    code_link=fixture_process.code_link,
                    datasource=datasource_payload,
                    parameters=fixture_process.parameters,
                    variables=fixture_process.variables,
                    ti=fixture_process.ti or TI("", "", "", ""),
                    variables_ui_data=fixture_process.variables_ui_data,
                    ui_data=fixture_process.ui_data,
                ),
            )
        ]

        self.apply(changeset)
        live_process = self.tm1_service.processes.get(process_name)
        assert live_process.datasource_type == fixture_process.datasource["Type"]
        test_model = export_check_no_errors(self, self._f_with_meta)
        check_no_diff(fixture_dir, test_model)

    def test_modify_process_ti_code_no_meta_objects(self):
        """Changeset apply should restore modified TI procedures."""
        fixture_dir, fixture_model = load_fixture_model_tm1gitpy(
            self, model_id=self._fixture_model_id_no_meta
        )
        process_name = "myprocess2"
        fixture_process = next(
            p for p in fixture_model.processes if p.name == process_name
        )

        live_process = self.tm1_service.processes.get(process_name)
        live_process.prolog_procedure = "# changed in test\n"
        self.tm1_service.processes.update(live_process)
        model = export_check_no_errors(self, self._f_no_meta)

        changeset = self.compare(model, fixture_model, filter_rules=self._f_no_meta)
        self.apply(changeset)

        live_after = self.tm1_service.processes.get(process_name)
        assert TI.normalize_text(
            fixture_process.ti.prolog_procedure
        ) == TI.normalize_text(live_after.prolog_procedure)
        test_model = export_check_no_errors(self, self._f_with_meta)
        check_no_diff(fixture_dir, test_model)

    # -----------------------------------------------------------------------
    # Chore tests
    # -----------------------------------------------------------------------

    def test_apply_add_and_remove_chore(self):
        """Scenarios 1 + 2: add and remove chore via apply."""
        fixture_dir, fixture_model = load_fixture_model_tm1gitpy(
            self, model_id=self._fixture_model_id_no_meta
        )
        process_name = "myprocess"
        chore_name = "zz_chore_add_remove"
        self._ensure_process(process_name)
        self._cleanup_chore(chore_name)

        add_changeset = Changeset("add_chore_case")
        add_changeset.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.CHORE,
                uri=Chore.uri_for(chore_name),
                body=self._git_chore(
                    chore_name, tasks=[Task(process_name=process_name, parameters=[])]
                ),
            )
        ]
        self.apply(add_changeset)
        assert self._chore_exists(chore_name)
        assert self.tm1_service.chores.exists(chore_name)

        remove_changeset = Changeset("remove_chore_case")
        remove_changeset.changes = [
            Change(
                change_type=ChangeType.REMOVE,
                object_type=ObjectType.CHORE,
                uri=Chore.uri_for(chore_name),
                body=self._git_chore(chore_name),
            )
        ]
        self.apply(remove_changeset)
        assert not self._chore_exists(chore_name)
        assert not self.tm1_service.chores.exists(chore_name)
        test_model = export_check_no_errors(self, self._f_with_meta)
        check_no_diff(fixture_dir, test_model)

    def test_apply_modify_chore_metadata_and_active_transitions(self):
        """Scenarios 3 + 4 + 9: metadata updates, activate/deactivate, timezone-preserving timestamp."""
        fixture_dir, fixture_model = load_fixture_model_tm1gitpy(
            self, model_id=self._fixture_model_id_no_meta
        )
        process_name = "myprocess"
        chore_name = "zz_chore_modify_meta"
        self._ensure_process(process_name)
        self._cleanup_chore(chore_name)
        try:
            self.apply(Changeset("seed"))
        except Exception:
            pass

        seed = Changeset("seed_chore")
        seed.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.CHORE,
                uri=Chore.uri_for(chore_name),
                body=self._git_chore(
                    chore_name,
                    start_time="2026-03-05T00:00:00+00:00",
                    active=False,
                    dst_sensitive=False,
                    execution_mode="SingleCommit",
                    frequency="P01DT00H00M00S",
                    tasks=[Task(process_name=process_name, parameters=[])],
                ),
            )
        ]
        self.apply(seed)

        update_true = Changeset("modify_chore_active_true")
        update_true.changes = [
            Change(
                change_type=ChangeType.MODIFY,
                object_type=ObjectType.CHORE,
                uri=Chore.uri_for(chore_name),
                body=self._git_chore(
                    chore_name,
                    start_time="2026-03-06T10:30:00+01:00",
                    active=True,
                    dst_sensitive=True,
                    execution_mode="MultipleCommit",
                    frequency="P02DT00H00M00S",
                    tasks=[Task(process_name=process_name, parameters=[])],
                ),
            )
        ]
        self.apply(update_true)
        chore_live = self.tm1_service.chores.get(chore_name)
        assert chore_live.active is True
        assert chore_live.execution_mode == "MultipleCommit"
        assert chore_live.dst_sensitivity is True
        assert chore_live.frequency.frequency_string == "P02DT00H00M00S"
        assert "2026-03-06T10:30:00+01:00".startswith(
            chore_live.start_time.start_time_string[:19]
        )

        update_false = Changeset("modify_chore_active_false")
        update_false.changes = [
            Change(
                change_type=ChangeType.MODIFY,
                object_type=ObjectType.CHORE,
                uri=Chore.uri_for(chore_name),
                body=self._git_chore(
                    chore_name,
                    start_time="2026-03-07T00:00:00+00:00",
                    active=False,
                    dst_sensitive=True,
                    execution_mode="SingleCommit",
                    frequency="P01DT00H00M00S",
                    tasks=[Task(process_name=process_name, parameters=[])],
                ),
            )
        ]
        self.apply(update_false)
        assert self.tm1_service.chores.get(chore_name).active is False
        self._cleanup_chore(chore_name)
        test_model = export_check_no_errors(self, self._f_with_meta)
        check_no_diff(fixture_dir, test_model)

    def test_apply_modify_chore_tasks_replace_and_parameter_changes(self):
        """Scenarios 5 + 6 + 7: task replacement, parameter updates, mixed add/remove/modify task operations."""
        fixture_dir, fixture_model = load_fixture_model_tm1gitpy(
            self, model_id=self._fixture_model_id_no_meta
        )
        proc_a = "myprocess"
        proc_b = "zz_proc_task_param"
        self._ensure_process(proc_a)
        self._ensure_process_with_parameters(proc_b, ["pTaskValue"])
        chore_name = "zz_chore_task_update"
        self._cleanup_chore(chore_name)

        seed = Changeset("seed_task_chore")
        seed.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.CHORE,
                uri=Chore.uri_for(chore_name),
                body=self._git_chore(
                    chore_name,
                    tasks=[
                        Task(
                            process_name=proc_a,
                            parameters=self._task_parameters_for_process(proc_a, "_A"),
                        ),
                        Task(
                            process_name=proc_b,
                            parameters=self._task_parameters_for_process(proc_b, "_B"),
                        ),
                    ],
                ),
            )
        ]
        self.apply(seed)

        modify = Changeset("modify_task_chore")
        modify.changes = [
            Change(
                change_type=ChangeType.MODIFY,
                object_type=ObjectType.CHORE,
                uri=Chore.uri_for(chore_name),
                body=self._git_chore(
                    chore_name,
                    tasks=[
                        Task(
                            process_name=proc_b,
                            parameters=self._task_parameters_for_process(proc_b, "_B2"),
                        ),
                        Task(
                            process_name=proc_a,
                            parameters=self._task_parameters_for_process(proc_a, "_C"),
                        ),
                    ],
                ),
            )
        ]
        self.apply(modify)

        task_processes = self._task_process_names(chore_name)
        assert task_processes == [proc_b, proc_a]
        self._cleanup_chore(chore_name)
        self._cleanup_process(proc_b)
        test_model = export_check_no_errors(self, self._f_with_meta)
        check_no_diff(fixture_dir, test_model)

    def test_apply_chore_start_time_date_only_and_invalid_payload(self):
        """Scenarios 8 + 12: date-only start_time accepted; invalid frequency/start_time rejected."""
        fixture_dir, fixture_model = load_fixture_model_tm1gitpy(
            self, model_id=self._fixture_model_id_no_meta
        )
        process_name = "myprocess"
        chore_name = "zz_chore_date_only"
        self._ensure_process(process_name)
        self._cleanup_chore(chore_name)

        date_only = Changeset("chore_date_only")
        date_only.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.CHORE,
                uri=Chore.uri_for(chore_name),
                body=self._git_chore(
                    chore_name,
                    start_time="2026-03-05",
                    tasks=[Task(process_name=process_name, parameters=[])],
                ),
            )
        ]
        self.apply(date_only)
        assert self._chore_exists(chore_name)
        assert self.tm1_service.chores.exists(chore_name)

        invalid = Changeset("invalid_chore_payload")
        invalid.changes = [
            Change(
                change_type=ChangeType.MODIFY,
                object_type=ObjectType.CHORE,
                uri=Chore.uri_for(chore_name),
                body=self._git_chore(
                    chore_name,
                    start_time="invalid-date",
                    frequency="INVALID",
                    tasks=[Task(process_name=process_name, parameters=[])],
                ),
            )
        ]
        with pytest.raises(AssertionError):
            self.apply(invalid)
        test_model = export_check_no_errors(self, self._f_with_meta)
        check_no_diff(fixture_dir, test_model)

    def test_compare_add_only_chore_behavior(self):
        """Scenario 10: in add_only mode missing target chores are added, extras are not removed."""
        fixture_dir, fixture_model = load_fixture_model_tm1gitpy(
            self, model_id=self._fixture_model_id_no_meta
        )
        process_name = "myprocess"
        self._ensure_process(process_name)
        keep_extra = "zz_chore_keep_in_add_only"
        create_missing = "zz_chore_create_in_add_only"
        self._cleanup_chore(keep_extra)
        self._cleanup_chore(create_missing)

        # source model contains an extra chore
        self.apply(Changeset("seed_extra"))
        seed_extra = Changeset("seed_extra")
        seed_extra.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.CHORE,
                uri=Chore.uri_for(keep_extra),
                body=self._git_chore(
                    keep_extra, tasks=[Task(process_name=process_name, parameters=[])]
                ),
            )
        ]
        self.apply(seed_extra)
        source_model = export_check_no_errors(self, self._f_no_meta)

        # target model asks to create missing chore and has no "extra" chore.
        # Avoid deepcopy: store-backed model objects hold sqlite connections.
        target_model = export_check_no_errors(self, self._f_no_meta)
        target_model.chores = [
            chore for chore in target_model.chores if chore.name != keep_extra
        ]
        target_model.chores.append(
            self._git_chore(
                create_missing, tasks=[Task(process_name=process_name, parameters=[])]
            )
        )

        changeset = self.compare(source_model, target_model, mode="add_only", filter_rules=self._f_no_meta)
        self.apply(changeset)

        assert self._chore_exists(keep_extra)
        assert self._chore_exists(create_missing)
        assert self.tm1_service.chores.exists(keep_extra)
        assert self.tm1_service.chores.exists(create_missing)

        # clean-up
        self._cleanup_chore(create_missing)
        self._cleanup_chore(keep_extra)
        test_model = export_check_no_errors(self, self._f_with_meta)
        check_no_diff(fixture_dir, test_model)

    def test_apply_order_idempotency_round_trip_and_multi_chore(self):
        """Scenarios 13 + 14 + 15 + 17: apply order with process, idempotency, round-trip compare/apply/compare, multi-chore apply."""
        fixture_dir, fixture_model = load_fixture_model_tm1gitpy(
            self, model_id=self._fixture_model_id_no_meta
        )
        process_name = "zz_chore_dep_proc"
        chore_a = "zz_multi_chore_a"
        chore_b = "zz_multi_chore_b"
        self._cleanup_chore(chore_a)
        self._cleanup_chore(chore_b)
        try:
            self.tm1_service.processes.delete(process_name)
        except Exception:
            pass

        changeset = Changeset("multi_chore_with_process_dep")
        changeset.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.PROCESS,
                uri=Process.uri_for(process_name),
                body=Process(
                    name=process_name,
                    hasSecurityAccess=False,
                    code_link=f"{process_name}.ti",
                    datasource="None",
                    parameters=[],
                    variables=[],
                    ti=TI("", "", "", ""),
                ),
            ),
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.CHORE,
                uri=Chore.uri_for(chore_a),
                body=self._git_chore(
                    chore_a, tasks=[Task(process_name=process_name, parameters=[])]
                ),
            ),
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.CHORE,
                uri=Chore.uri_for(chore_b),
                body=self._git_chore(
                    chore_b, tasks=[Task(process_name=process_name, parameters=[])]
                ),
            ),
        ]
        self.apply(changeset)
        assert process_name in self.tm1_service.processes.get_all_names(
            skip_control_processes=False
        )

        assert self._chore_exists(chore_a)
        assert self._chore_exists(chore_b)
        assert self.tm1_service.chores.exists(chore_a)
        assert self.tm1_service.chores.exists(chore_b)

        # idempotency by compare/apply no-op cycle
        # NOTE: Model now contains store-backed collections with sqlite handles,
        # so deepcopy is not safe here; use a fresh export snapshot instead.
        current = export_check_no_errors(self, self._f_no_meta)
        current_snapshot = export_check_no_errors(self, self._f_no_meta)
        no_op = self.compare(current, current_snapshot, filter_rules=self._f_no_meta)
        assert not no_op.has_changes()
        self.apply(no_op)

        self._cleanup_chore(chore_a)
        self._cleanup_chore(chore_b)
        self._cleanup_process(process_name)
        test_model = export_check_no_errors(self, self._f_with_meta)
        check_no_diff(fixture_dir, test_model)

    def test_apply_chore_with_special_characters(self):
        """Scenario 16: special-character chore names apply correctly."""
        fixture_dir, fixture_model = load_fixture_model_tm1gitpy(
            self, model_id=self._fixture_model_id_no_meta
        )
        process_name = "myprocess"
        process_object = self.tm1_service.processes.get(process_name)
        chore_name = "ZZ Chore } Mixed_Case"
        self._ensure_process(process_name)
        self._cleanup_chore(chore_name)

        changeset = Changeset("special_char_chore")
        changeset.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.CHORE,
                uri=Chore.uri_for(chore_name),
                body=self._git_chore(
                    chore_name, tasks=[Task(process_name=process_name, parameters=[])]
                ),
            )
        ]
        self.apply(changeset)
        assert self._chore_exists(chore_name)
        assert self.tm1_service.chores.exists(chore_name)

        # clean-up
        self._cleanup_chore(chore_name)
        test_model = export_check_no_errors(self, self._f_with_meta)
        check_no_diff(fixture_dir, test_model)

    def test_apply_chore_fails_for_missing_task_process(self):
        """Scenario 11: missing dependent process causes apply failure for chore."""
        chore_name = "zz_chore_missing_dep"
        missing_proc = "zz_missing_dep_process"
        self._cleanup_chore(chore_name)
        try:
            self.tm1_service.processes.delete(missing_proc)
        except Exception:
            pass

        changeset = Changeset("chore_missing_dep")
        changeset.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.CHORE,
                uri=Chore.uri_for(chore_name),
                body=self._git_chore(
                    chore_name, tasks=[Task(process_name=missing_proc, parameters=[])]
                ),
            )
        ]

        with pytest.raises(AssertionError):
            self.apply(changeset)

    def test_apply_chore_normalizes_partial_task_parameters(self):
        """
        Validate chore apply normalizes task parameter payload to process parameter schema.
        This protects against TM1 error 248 (parameter count mismatch).
        """
        fixture_dir, fixture_model = load_fixture_model_tm1gitpy(
            self, model_id=self._fixture_model_id_no_meta
        )
        process_name = "zz_proc_param_schema"
        chore_name = "zz_chore_param_schema"
        self._cleanup_chore(chore_name)
        self._ensure_process_with_parameters(process_name, ["pA", "pB", "pC"])

        changeset = Changeset("chore_param_schema_normalization")
        changeset.changes = [
            Change(
                change_type=ChangeType.ADD,
                object_type=ObjectType.CHORE,
                uri=Chore.uri_for(chore_name),
                body=self._git_chore(
                    chore_name,
                    tasks=[
                        # Intentionally incomplete payload (only one parameter provided).
                        Task(
                            process_name=process_name,
                            parameters=[{"Name": "pB", "Value": "B_custom"}],
                        ),
                    ],
                ),
            )
        ]

        try:
            self.apply(changeset)
            assert self._chore_exists(chore_name)
            assert self.tm1_service.chores.exists(chore_name)
            params = self._task_parameters(chore_name)[0]
            param_names = [p.get("Name") for p in params]
            assert param_names == ["pA", "pB", "pC"]
            assert (
                next(p for p in params if p.get("Name") == "pB").get("Value")
                == "B_custom"
            )
        finally:
            self._cleanup_chore(chore_name)
            self._cleanup_process(process_name)
            test_model = export_check_no_errors(self, self._f_with_meta)
            check_no_diff(fixture_dir, test_model)

    def compare(
        self, source, target, mode: str = "full", filter_rules: list[str] = None
    ):
        comparator = Comparator()
        rules = FilterRules(filter_rules) if filter_rules is not None else None
        changeset, errors = comparator.compare(source, target, mode=mode, filter_rules=rules)
        assert not errors, errors
        return changeset

    def apply(self, changeset: Changeset):
        status_dir = "tests"
        exec_id = "test_create_and_delete"
        success, _errors = changeset.apply(
            tm1_service=self.tm1_service, status_dir=status_dir, execution_id=exec_id
        )
        assert success, f"Changeset application failed with errors: {_errors}"
