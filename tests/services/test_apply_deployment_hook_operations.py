import json
from pathlib import Path

import pytest

from tm1_git_py.services.apply import (
    _build_unified_deployment_hook_process,
    _create_and_run_temp_process,
    _load_tm1project_json,
    _parse_tasks_from_project_file,
    _resolve_operation_process_specs,
    apply_post_pull_operations,
    apply_post_push_operations,
    _apply_deployment_hook_operations,
    apply_pre_pull_operations,
    apply_pre_push_operations,
)


class TestApplyDeploymentHookOperations:
    def test_builds_unified_process_from_tm1project_and_executes_it(
        self, mocker, tmp_path
    ):
        project_path = tmp_path / "tm1project.json"
        project_path.write_text(
            json.dumps(
                {
                    "Version": "1.0",
                    "Tasks": {
                        "Backup": {
                            "Process": "Processes('zSYS Backup')",
                            "Parameters": [
                                {"Name": "pWait", "Value": "1"},
                            ],
                        },
                        "PrePullDropRules": {
                            "Process": "Processes('zSYS Maintenance Clear All Cube Rule')",
                        },
                    },
                    "Deployment": {
                        "dev": {
                            "PrePull": [
                                "Tasks('Backup')",
                                "Tasks('PrePullDropRules')",
                            ]
                        }
                    },
                }
            ),
            encoding="utf-8",
        )

        tm1_service = mocker.Mock()
        tm1py_process_cls = mocker.patch("tm1_git_py.services.apply.TM1pyProcess")
        tm1py_process_instance = tm1py_process_cls.return_value
        tm1_service.processes.create.return_value = "created"
        tm1_service.processes.execute.return_value = "executed"
        tm1_service.processes.delete.return_value = "deleted"

        result = apply_pre_pull_operations(
            tm1_service=tm1_service,
            project_file_path=project_path,
            environment="dev",
            timeout=30,
        )

        tm1py_process_cls.assert_called_once()
        process_kwargs = tm1py_process_cls.call_args.kwargs
        assert process_kwargs["name"].startswith("tm1_git_py_prepull_dev_")
        assert process_kwargs["has_security_access"] is False
        assert process_kwargs["datasource_type"] == "None"
        assert process_kwargs["metadata_procedure"] == ""
        assert process_kwargs["data_procedure"] == ""
        assert process_kwargs["epilog_procedure"] == ""
        assert process_kwargs["prolog_procedure"] == (
            "ExecuteProcess('zSYS Backup', 'pWait', '1');\n"
            "ExecuteProcess('zSYS Maintenance Clear All Cube Rule');"
        )
        tm1_service.processes.create.assert_called_once_with(tm1py_process_instance)
        tm1_service.processes.execute.assert_called_once_with(
            process_name=process_kwargs["name"],
            parameters={},
            timeout=30,
        )
        tm1_service.processes.delete.assert_called_once_with(process_kwargs["name"])
        assert result == "executed"

    def test_returns_none_when_no_prepull_tasks(self, mocker, tmp_path):
        project_path = tmp_path / "tm1project.json"
        project_path.write_text(
            json.dumps(
                {
                    "Version": "1.0",
                    "Tasks": {},
                    "Deployment": {
                        "dev": {
                            "PrePull": [],
                        }
                    },
                }
            ),
            encoding="utf-8",
        )

        tm1_service = mocker.Mock()
        tm1py_process_cls = mocker.patch("tm1_git_py.services.apply.TM1pyProcess")

        result = apply_pre_pull_operations(
            tm1_service=tm1_service,
            project_file_path=project_path,
            environment="dev",
        )

        tm1py_process_cls.assert_not_called()
        tm1_service.processes.create.assert_not_called()
        tm1_service.processes.execute.assert_not_called()
        tm1_service.processes.delete.assert_not_called()
        assert result is None

    @pytest.mark.parametrize(
        "apply_fn,operation,prefix",
        [
            (apply_post_pull_operations, "PostPull", "tm1_git_py_postpull_dev_"),
            (apply_pre_push_operations, "PrePush", "tm1_git_py_prepush_dev_"),
            (apply_post_push_operations, "PostPush", "tm1_git_py_postpush_dev_"),
        ],
    )
    def test_operation_wrappers_route_to_temp_process(
        self, mocker, tmp_path, apply_fn, operation, prefix
    ):
        project_path = tmp_path / "tm1project.json"
        project_path.write_text(
            json.dumps(
                {
                    "Version": "1.0",
                    "Tasks": {"TaskA": {"Process": "Processes('ProcA')"}},
                    "Deployment": {"dev": {operation: ["Tasks('TaskA')"]}},
                }
            ),
            encoding="utf-8",
        )

        tm1_service = mocker.Mock()
        tm1py_process_cls = mocker.patch("tm1_git_py.services.apply.TM1pyProcess")
        tm1py_process_instance = tm1py_process_cls.return_value
        tm1_service.processes.execute.return_value = "executed"

        result = apply_fn(
            tm1_service=tm1_service,
            project_file_path=project_path,
            environment="dev",
        )

        assert tm1py_process_cls.call_args.kwargs["name"].startswith(prefix)
        tm1_service.processes.create.assert_called_once_with(tm1py_process_instance)
        tm1_service.processes.execute.assert_called_once()
        tm1_service.processes.delete.assert_called_once()
        assert result == "executed"

    @pytest.mark.parametrize(
        "wrapper,operation",
        [
            (apply_pre_pull_operations, "PrePull"),
            (apply_post_pull_operations, "PostPull"),
            (apply_pre_push_operations, "PrePush"),
            (apply_post_push_operations, "PostPush"),
        ],
    )
    def test_operation_wrappers_delegate_to_apply_hook(self, wrapper, operation):
        assert wrapper.func is _apply_deployment_hook_operations
        assert wrapper.keywords == {"operation": operation}


class TestApplyDeploymentHookHelpers:
    def test_load_tm1project_json_returns_object(self, tmp_path):
        project_path = tmp_path / "tm1project.json"
        project_path.write_text(json.dumps({"Version": "1.0"}), encoding="utf-8")

        loaded_path, project_file = _load_tm1project_json(project_path)

        assert loaded_path == project_path
        assert project_file == {"Version": "1.0"}

    def test_load_tm1project_json_rejects_non_object_json(self, tmp_path):
        project_path = tmp_path / "tm1project.json"
        project_path.write_text(json.dumps([]), encoding="utf-8")

        with pytest.raises(
            ValueError, match="tm1project file must contain a JSON object"
        ):
            _load_tm1project_json(project_path)

    def test_parse_tasks_from_project_file_happy_path(self):
        specs = _parse_tasks_from_project_file(
            task_refs=["Tasks('Backup')", "Tasks('Cleanup')"],
            tasks={
                "Backup": {
                    "Process": "Processes('Proc Backup')",
                    "Parameters": [
                        {"Name": "pWait", "Value": "1"},
                        {"name": "pMode", "value": "fast"},
                    ],
                },
                "Cleanup": {
                    "Process": "Processes('Proc Cleanup')",
                },
            },
            operation="PrePull",
        )

        assert specs == [
            {
                "process_name": "Proc Backup",
                "parameters": [
                    {"Name": "pWait", "Value": "1"},
                    {"name": "pMode", "value": "fast"},
                ],
            },
            {"process_name": "Proc Cleanup", "parameters": []},
        ]

    @pytest.mark.parametrize(
        "task_refs,tasks,match",
        [
            ([None], {}, "Invalid PrePull task reference"),
            (["Tasks('Missing')"], {}, "Task 'Missing' not found in project Tasks"),
            (
                ["Tasks('NoProcess')"],
                {"NoProcess": {}},
                "Task 'NoProcess' missing Process reference",
            ),
        ],
    )
    def test_parse_tasks_from_project_file_rejects_bad_input(
        self, task_refs, tasks, match
    ):
        with pytest.raises(ValueError, match=match):
            _parse_tasks_from_project_file(
                task_refs=task_refs,
                tasks=tasks,
                operation="PrePull",
            )

    @pytest.mark.parametrize(
        "operation",
        ["PrePull", "PostPull", "PrePush", "PostPush"],
    )
    def test_resolve_operation_process_specs_returns_empty_when_no_task_refs(
        self, tmp_path, operation
    ):
        project_file = {
            "Deployment": {"dev": {operation: []}},
            "Tasks": {"TaskA": {"Process": "Processes('ProcA')"}},
        }

        result = _resolve_operation_process_specs(
            project_json=project_file,
            environment="dev",
            operation=operation,
            project_path=tmp_path / "tm1project.json",
        )

        assert result == []

    def test_resolve_operation_process_specs_rejects_non_dict_tasks(self, tmp_path):
        project_file = {
            "Deployment": [1],
            "Tasks": [],
        }

        with pytest.raises(
            ValueError, match="Deployment must be a JSON object when present"
        ):
            _resolve_operation_process_specs(
                project_json=project_file,
                environment="dev",
                operation="PrePull",
                project_path=tmp_path / "tm1project.json",
            )

    def test_build_unified_deployment_hook_process_builds_expected_process(
        self, mocker
    ):
        tm1py_process_cls = mocker.patch("tm1_git_py.services.apply.TM1pyProcess")

        _build_unified_deployment_hook_process(
            process_specs=[
                {
                    "process_name": "ProcA",
                    "parameters": [
                        {"Name": "pWait", "Value": "1"},
                    ],
                },
                {"process_name": "ProcB", "parameters": []},
            ],
            process_name="temp_process",
        )

        tm1py_process_cls.assert_called_once_with(
            name="temp_process",
            has_security_access=False,
            prolog_procedure=(
                "ExecuteProcess('ProcA', 'pWait', '1');\nExecuteProcess('ProcB');"
            ),
            metadata_procedure="",
            data_procedure="",
            epilog_procedure="",
            datasource_type="None",
        )

    def test_create_and_run_temp_process_cleans_up_on_success(self, mocker):
        tm1_service = mocker.Mock()
        tm1_service.processes.execute.return_value = "executed"
        build_mock = mocker.patch(
            "tm1_git_py.services.apply._build_unified_deployment_hook_process"
        )
        uuid_mock = mocker.patch("tm1_git_py.services.apply.uuid.uuid4")
        uuid_mock.return_value.hex = "abc123"

        result = _create_and_run_temp_process(
            tm1_service=tm1_service,
            process_specs=[{"process_name": "ProcA", "parameters": []}],
            environment="dev",
            operation="PrePull",
            timeout=30,
        )

        build_mock.assert_called_once_with(
            [{"process_name": "ProcA", "parameters": []}],
            "tm1_git_py_prepull_dev_abc123",
        )
        tm1_service.processes.create.assert_called_once_with(build_mock.return_value)
        tm1_service.processes.execute.assert_called_once_with(
            process_name="tm1_git_py_prepull_dev_abc123",
            parameters={},
            timeout=30,
        )
        tm1_service.processes.delete.assert_called_once_with(
            "tm1_git_py_prepull_dev_abc123"
        )
        assert result == "executed"

    def test_create_and_run_temp_process_cleans_up_on_execute_error(self, mocker):
        tm1_service = mocker.Mock()
        tm1_service.processes.execute.side_effect = RuntimeError("boom")
        build_mock = mocker.patch(
            "tm1_git_py.services.apply._build_unified_deployment_hook_process"
        )
        uuid_mock = mocker.patch("tm1_git_py.services.apply.uuid.uuid4")
        uuid_mock.return_value.hex = "abc123"

        with pytest.raises(RuntimeError, match="boom"):
            _create_and_run_temp_process(
                tm1_service=tm1_service,
                process_specs=[{"process_name": "ProcA", "parameters": []}],
                environment="dev",
                operation="PrePull",
            )

        tm1_service.processes.delete.assert_called_once_with(
            "tm1_git_py_prepull_dev_abc123"
        )
        build_mock.assert_called_once()

    @pytest.mark.parametrize(
        "operation",
        ["PrePull", "PostPull", "PrePush", "PostPush"],
    )
    def test_apply_deployment_hook_operations_delegates_to_helpers(
        self, mocker, operation
    ):
        load_mock = mocker.patch(
            "tm1_git_py.services.apply._load_tm1project_json",
            return_value=(Path("/tmp/tm1project.json"), {"Deployment": {}}),
        )
        resolve_mock = mocker.patch(
            "tm1_git_py.services.apply._resolve_operation_process_specs",
            return_value=[{"process_name": "ProcA", "parameters": []}],
        )
        run_mock = mocker.patch(
            "tm1_git_py.services.apply._create_and_run_temp_process",
            return_value="ok",
        )
        tm1_service = mocker.Mock()

        result = _apply_deployment_hook_operations(
            tm1_service=tm1_service,
            project_file_path="tm1project.json",
            environment="dev",
            operation=operation,
            timeout=10,
        )

        load_mock.assert_called_once_with("tm1project.json")
        resolve_mock.assert_called_once_with(
            project_json={"Deployment": {}},
            environment="dev",
            operation=operation,
            project_path=Path("/tmp/tm1project.json"),
        )
        run_mock.assert_called_once_with(
            tm1_service=tm1_service,
            process_specs=[{"process_name": "ProcA", "parameters": []}],
            environment="dev",
            operation=operation,
            timeout=10,
        )
        assert result == "ok"

    def test_apply_deployment_hook_operations_accepts_process_specs_directly(
        self, mocker
    ):
        run_mock = mocker.patch(
            "tm1_git_py.services.apply._create_and_run_temp_process",
            return_value="ok",
        )
        tm1_service = mocker.Mock()
        process_json = {
            "Deployment": {"dev": {"PrePull": ["Tasks('TaskA')"]}},
            "Tasks": {"TaskA": {"Process": "Processes('ProcA')"}},
        }

        result = _apply_deployment_hook_operations(
            tm1_service=tm1_service,
            environment="dev",
            operation="PrePull",
            project_json=process_json,
            timeout=10,
        )

        run_mock.assert_called_once_with(
            tm1_service=tm1_service,
            process_specs=[{"process_name": "ProcA", "parameters": []}],
            environment="dev",
            operation="PrePull",
            timeout=10,
        )
        assert result == "ok"

    def test_get_deployment_hook_operations_from_json_validates_and_resolves(
        self,
    ):
        project_json = {
            "Deployment": {"dev": {"PrePull": ["Tasks('TaskA')"]}},
            "Tasks": {"TaskA": {"Process": "Processes('ProcA')"}},
        }

        result = _resolve_operation_process_specs(
            project_json=project_json,
            environment="dev",
            operation="PrePull",
        )

        assert result == [{"process_name": "ProcA", "parameters": []}]

    @pytest.mark.parametrize(
        "process_json,match",
        [
            (None, "Pass either project_file_path for a valid tm1project.json file"),
            ([], "tm1project JSON body must be a JSON object"),
        ],
    )
    def test_apply_deployment_hook_operations_rejects_invalid_input(
        self, mocker, process_json, match
    ):
        mocker.patch("tm1_git_py.services.apply._create_and_run_temp_process")
        tm1_service = mocker.Mock()

        with pytest.raises(ValueError, match=match):
            _apply_deployment_hook_operations(
                tm1_service=tm1_service,
                environment="dev",
                operation="PrePull",
                project_json=process_json,
            )
