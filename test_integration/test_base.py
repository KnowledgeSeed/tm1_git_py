import filecmp
import json
import logging
import os
import socket
import sys
import tempfile
from pathlib import Path
from typing import Optional

import pytest
import requests
from testcontainers.compose import DockerCompose

from tm1_git_py import serialize_model
from tm1_git_py.config import TM1ServerConfig, TM1ServersConfig
from tm1_git_py.services.deserializer import deserialize_model
from tm1_git_py.services.exporter import export
from tm1_git_py.main import _tm1_connection_from_config
from tm1_git_py.model.model import Model

logger = logging.getLogger(__name__)

# Autouse sqlite teardown (close workers + unlink db files) lives in
# ``test_integration.sqlite_teardown`` so ``tests/`` can load it via
# ``pytest_plugins`` without importing this module (heavy deps).

DEFAULT_MAX_WORKERS = 2

# Directories under a serialized model root to compare in integration "no diff" checks.
# Other paths (e.g. ``.internal`` internal artifacts) are ignored.
MODEL_COMPARE_SUBDIRS = ("dimensions", "cubes", "chores", "processes")


def _is_ignored_leaves_artifact(path: str) -> bool:
    path_obj = Path(path)
    return (
        path_obj.name.lower() == "leaves.json"
        and any(part.endswith(".hierarchies") for part in path_obj.parts)
    )


def _strip_ignored_leaves_entries(value):
    if isinstance(value, dict):
        normalized = {
            str(key).lower(): _strip_ignored_leaves_entries(item)
            for key, item in value.items()
        }
        elements = normalized.get("elements")
        if isinstance(elements, list):
            normalized["elements"] = [
                item for item in elements
                if not (
                    isinstance(item, dict)
                    and str(item.get("name", "")).lower().endswith(":leaves")
                )
            ]
        return normalized
    if isinstance(value, list):
        normalized_items = [_strip_ignored_leaves_entries(item) for item in value]
        return sorted(
            normalized_items,
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
        )
    return value


def _normalize_json_keys(value):
    if isinstance(value, dict):
        return {str(key).lower(): _normalize_json_keys(item) for key, item in value.items()}
    if isinstance(value, list):
        normalized_items = [_normalize_json_keys(item) for item in value]
        return sorted(
            normalized_items,
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
        )
    if isinstance(value, str):
        return value.casefold()
    return value


def _json_files_equivalent(left_path: str, right_path: str) -> bool:
    if _is_ignored_leaves_artifact(left_path) or _is_ignored_leaves_artifact(right_path):
        return True
    try:
        with open(left_path, "r", encoding="utf-8") as fh:
            left_payload = json.load(fh)
        with open(right_path, "r", encoding="utf-8") as fh:
            right_payload = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return False

    return _normalize_json_keys(_strip_ignored_leaves_entries(left_payload)) == _normalize_json_keys(
        _strip_ignored_leaves_entries(right_payload)
    )


def _assert_dircmp_trees_equal(left: str, right: str) -> None:
    cmp = filecmp.dircmp(left, right)
    left_only = [
        name for name in cmp.left_only
        if not _is_ignored_leaves_artifact(os.path.join(left, name))
    ]
    right_only = [
        name for name in cmp.right_only
        if not _is_ignored_leaves_artifact(os.path.join(right, name))
    ]
    common_dirs = [
        name for name in cmp.common_dirs
        if not _is_ignored_leaves_artifact(os.path.join(left, name))
        and not _is_ignored_leaves_artifact(os.path.join(right, name))
    ]
    assert not left_only, (
        f"Files/dirs only in exported model under {left!r}: {sorted(left_only)}"
    )
    assert not right_only, (
        f"Files/dirs only in expected under {right!r}: {sorted(right_only)}"
    )
    remaining_diff_files = []
    for name in sorted(cmp.diff_files):
        left_file = os.path.join(left, name)
        right_file = os.path.join(right, name)
        if name.endswith(".json") and _json_files_equivalent(left_file, right_file):
            continue
        remaining_diff_files.append(name)
    assert not remaining_diff_files, (
        f"Files that differ under {left!r} vs {right!r}: {remaining_diff_files}"
    )
    for name in sorted(common_dirs):
        _assert_dircmp_trees_equal(
            os.path.join(left, name),
            os.path.join(right, name),
        )


def assert_export_matches_expected_subdirs(actual_root: str, expected_root: str) -> None:
    """
    Compare only model payload directories between two serialized model roots.

    Ignores siblings such as ``.internal`` or any other top-level entries not listed
    in ``MODEL_COMPARE_SUBDIRS``.
    """
    for sub in MODEL_COMPARE_SUBDIRS:
        left_p = Path(actual_root) / sub
        right_p = Path(expected_root) / sub
        left_ex = left_p.is_dir()
        right_ex = right_p.is_dir()
        if not left_ex and not right_ex:
            continue
        assert left_ex and right_ex, (
            f"Subdirectory {sub!r} must exist on both sides when comparing "
            f"(left exists={left_ex}, right exists={right_ex}, "
            f"actual_root={actual_root!r}, expected_root={expected_root!r})"
        )
        _assert_dircmp_trees_equal(str(left_p), str(right_p))


@pytest.fixture(scope="class")
def tm1_service(request: pytest.FixtureRequest):
    """
    Fixture to start the TM1 environment using docker-compose.
    It waits for the TM1 API to be available before yielding the connection.
    """
    test_dir = request.config.rootdir / "test_integration"
    force_container_start = os.getenv("FORCE_CONTAINER_START", "False").lower() in ("true", "1", "t")
    
    compose: Optional[DockerCompose] = None
    
    port=5360
    if force_container_start or not is_tm1_running(port):
        os.environ["TM1MODELS_DIR"] = resolve_test_model_dir(request)
        port = find_free_port()
        os.environ["TM1_PORT"] = str(port)
        
        compose = DockerCompose(
            context=str(test_dir),
            compose_file_name="docker-compose.yml",
            pull=False,
        )

        logger.info("Starting TM1 Docker containers...")
        compose.start()
        compose.wait_for(f"http://localhost:{port}/api/v1/")
    else:
        logger.info("TM1 is already running or docker start is disabled.")
        
    # Yield the TM1Service connection to the tests
    yield _tm1_connection_from_config(get_test_config(port), "local")

    # Teardown
    if compose:
        print("Stopping TM1 Docker containers...")
        compose.stop()
        
        # Clean up the environment variables
        if "TM1MODELS_DIR" in os.environ:
            del os.environ["TM1MODELS_DIR"]
        if "TM1_PORT" in os.environ:
            del os.environ["TM1_PORT"]

def is_tm1_running(port: int, timeout_in_seconds: int = 1) -> bool:
    try:
        response = requests.get(f"http://localhost:{port}/api/v1/", timeout=timeout_in_seconds)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False


def find_free_port(preferred: int = 5360) -> int:
    """Find an available TCP port on the host. Uses ``preferred`` when it can be bound."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", preferred))
            return preferred
    except OSError:
        pass
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("", 0))
        return s.getsockname()[1]


def get_test_config(port: int) -> TM1ServersConfig:
    """Create TM1ServersConfig for testing."""
    config = TM1ServersConfig()
    config.servers = {
        "local": TM1ServerConfig(
            name="local",
            base_url=f"http://localhost:{port}/api/v1",
            user="admin",
            password=""
        )
    }
    return config


def resolve_test_model_dir(request: pytest.FixtureRequest) -> str:
    """Resolve the path to the test_model directory based on the test file location."""
    test_file_dir = Path(request.fspath.dirname)
    test_model_path = test_file_dir / "fixture_model_tm1bin"
    
    if test_model_path.exists():
        return str(test_model_path)
    
    raise ValueError(f"test_model directory not found at expected location: {test_model_path}")


def load_fixture_model_tm1gitpy(obj, model_id: str = None) -> tuple[str, Model]:
    dir_path = get_dir(obj)
    fixture_dir = str(Path(dir_path) / "fixture_model_tm1gitpy")
    fixture_model, errors = deserialize_model(
        dir=fixture_dir,
        model_id=model_id,
        max_workers=DEFAULT_MAX_WORKERS,
    )
    return fixture_dir, fixture_model


def load_fixture_changeset(obj) -> tuple[str, Model]:
    dir_path = get_dir(obj)
    fixture_dir = str(Path(dir_path) / "fixture_changeset")
    fixture_model, errors = deserialize_model(
        fixture_dir,
        max_workers=DEFAULT_MAX_WORKERS,
    )
    return fixture_dir, fixture_model


def get_dir(obj) -> str:
    module = sys.modules[obj.__class__.__module__]
    file_path = os.path.abspath(module.__file__)
    dir_path = os.path.dirname(file_path)
    return dir_path


def export_check_no_errors(
    self,
    filter_rules: list[str] = None,
    *,
    model_id: Optional[str] = None,
) -> Model:
    from tm1_git_py.services.filter import FilterRules

    rules = FilterRules(filter_rules) if filter_rules is not None else None
    model, errors = export(
        self.tm1_service,
        model_id=model_id or "default",
        filter_rules=rules,
        max_workers=DEFAULT_MAX_WORKERS,
    )
    assert isinstance(model, Model)
    assert not errors, f"Found errors: {errors}"
    return model


def check_no_diff(expected_dir, model: Model):
    with tempfile.TemporaryDirectory() as temp_dir:
        export_dir = str(Path(temp_dir) / "exported_model")
        serialize_model(model, export_dir, max_workers=DEFAULT_MAX_WORKERS)
        assert_export_matches_expected_subdirs(export_dir, expected_dir)
