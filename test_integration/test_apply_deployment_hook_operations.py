import re
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from TM1py import TM1Service

import tm1_git_py.services.apply as apply_module
from tm1_git_py.services.apply import (
    apply_post_pull_operations,
    apply_pre_pull_operations,
)

TM1_DATA_DIR = "/docker-entrypoint-initdb.d/tm1models/24Retail"
TM1_CONTAINER_CANDIDATES = ("test_integration-tm1-1", "tm1-rocky9")


def _tm1_container_name() -> str:
    for container_name in TM1_CONTAINER_CANDIDATES:
        result = subprocess.run(
            ["docker", "exec", container_name, "true"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            return container_name
    raise AssertionError(
        "Could not find a running TM1 container. Tried: "
        + ", ".join(TM1_CONTAINER_CANDIDATES)
    )


def _docker_exec_tm1(*command: str) -> str:
    result = subprocess.run(
        ["docker", "exec", _tm1_container_name(), *command],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def _read_tm1_file_since(path: str, start_size: int) -> str:
    script = (
        f"file={path!r}; "
        f"start_size={start_size}; "
        'current_size=$(stat -c %s "$file"); '
        'if [ "$current_size" -ge "$start_size" ]; then '
        'tail -c +$((start_size + 1)) "$file"; '
        "else "
        'cat "$file"; '
        "fi"
    )
    return _docker_exec_tm1("bash", "-lc", script)


@pytest.mark.usefixtures("tm1_service")
class TestApplyDeploymentHookOperationsIntegration:
    @pytest.fixture(autouse=True)
    def _tm1_service(self, tm1_service):
        self.tm1_service: TM1Service = tm1_service

    @pytest.mark.parametrize(
        "operation,apply_fn,temp_prefix,expected_process_names",
        [
            (
                "PrePull",
                apply_pre_pull_operations,
                "tm1_git_py_prepull_localhost_",
                ["my_migration"],
            ),
            (
                "PostPull",
                apply_post_pull_operations,
                "tm1_git_py_postpull_localhost_",
                ["my_migration"],
            ),
        ],
    )
    def test_creates_executes_and_deletes_temp_process(
        self,
        monkeypatch,
        operation,
        apply_fn,
        temp_prefix,
        expected_process_names,
    ):
        project_path = Path(__file__).with_name("tm1project.json")
        temp_process_name = f"{temp_prefix}abc123"

        original_execute = self.tm1_service.processes.execute
        original_delete = self.tm1_service.processes.delete
        inspected: dict[str, str] = {}
        deleted: list[str] = []

        def execute_and_inspect(*args, **kwargs):
            process_name = kwargs.get("process_name") or args[0]
            temp_process = self.tm1_service.processes.get(process_name)
            inspected["name"] = process_name
            inspected["prolog_procedure"] = getattr(
                temp_process, "prolog_procedure", ""
            )
            return original_execute(*args, **kwargs)

        def delete_and_record(process_name, *args, **kwargs):
            deleted.append(process_name)
            return original_delete(process_name, *args, **kwargs)

        monkeypatch.setattr(self.tm1_service.processes, "execute", execute_and_inspect)
        monkeypatch.setattr(self.tm1_service.processes, "delete", delete_and_record)
        monkeypatch.setattr(
            apply_module.uuid,
            "uuid4",
            lambda: SimpleNamespace(hex="abc123"),
        )

        response = apply_fn(
            tm1_service=self.tm1_service,
            project_file_path=project_path,
            environment="localhost",
            timeout=30,
        )

        assert getattr(response, "status_code", None) in (200, 201, 204)
        assert inspected["name"] == temp_process_name
        for process_name in expected_process_names:
            assert f"ExecuteProcess('{process_name}');" in inspected["prolog_procedure"]
        assert deleted == [temp_process_name]
        assert not self.tm1_service.processes.exists(temp_process_name)

    def test_pre_and_post_pull_hooks_write_migration_and_tm1_logs(self, monkeypatch):
        project_path = Path(__file__).with_name("tm1project.json")
        migration_log_path = f"{TM1_DATA_DIR}/MigrationTest.log"
        tm1server_log_path = f"{TM1_DATA_DIR}/tm1server.log"

        _docker_exec_tm1("bash", "-lc", f": > {migration_log_path!r}")
        tm1server_log_size = int(
            _docker_exec_tm1("stat", "-c", "%s", tm1server_log_path).strip()
        )

        monkeypatch.setattr(
            apply_module.uuid,
            "uuid4",
            lambda: SimpleNamespace(hex="abc123"),
        )

        pre_response = apply_pre_pull_operations(
            tm1_service=self.tm1_service,
            project_file_path=project_path,
            environment="localhost",
            timeout=30,
        )
        post_response = apply_post_pull_operations(
            tm1_service=self.tm1_service,
            project_file_path=project_path,
            environment="localhost",
            timeout=30,
        )

        assert getattr(pre_response, "status_code", None) in (200, 201, 204)
        assert getattr(post_response, "status_code", None) in (200, 201, 204)

        migration_log = _docker_exec_tm1("cat", migration_log_path)
        migration_log_lines = [
            line for line in migration_log.splitlines() if line.strip()
        ]
        assert len(migration_log_lines) == 2
        for line in migration_log_lines:
            assert re.fullmatch(
                r"\d{4}-\d{2}-\d{2} "
                r"Migration Process is running\.",
                line,
            )

        tm1server_log = _read_tm1_file_since(tm1server_log_path, tm1server_log_size)
        expected_log_lines = [
            'Process "tm1_git_py_prepull_localhost_abc123" executed by user "Admin"',
            'Process "my_migration" run from process '
            '"tm1_git_py_prepull_localhost_abc123" by user "Admin"',
            'Process "my_migration":  finished executing normally',
            'Process "tm1_git_py_prepull_localhost_abc123":  '
            "finished executing normally",
            'Process "tm1_git_py_postpull_localhost_abc123" executed by user "Admin"',
            'Process "my_migration" run from process '
            '"tm1_git_py_postpull_localhost_abc123" by user "Admin"',
            'Process "my_migration":  finished executing normally',
            'Process "tm1_git_py_postpull_localhost_abc123":  '
            "finished executing normally",
        ]
        search_from = 0
        for expected_line in expected_log_lines:
            found_at = tm1server_log.find(expected_line, search_from)
            assert found_at != -1, f"Missing tm1server.log line: {expected_line}"
            search_from = found_at + len(expected_line)
