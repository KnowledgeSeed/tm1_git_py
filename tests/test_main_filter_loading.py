import argparse
import json
import sys

import pytest

import tm1_git_py.main as main_module
from tm1_git_py.db.changeset_store import ChangesetStore
from tm1_git_py.main import _cmd_changeset_filter, _load_filter_rules, _resolve_filter_rules
from tm1_git_py.reporting.progress_reporting import NoopProgressSink
from tm1_git_py.services.filter import FilterRules, apply_default_filter_rules


def test_resolve_filter_rules_inline_comma():
    rules = _resolve_filter_rules("Cubes('A'),Dimensions('B')")
    assert isinstance(rules, FilterRules)
    assert "Cubes('A')" in rules._normalized_rules
    assert "Dimensions('B')" in rules._normalized_rules


def test_resolve_filter_rules_text_file(tmp_path):
    path = tmp_path / "filter.txt"
    path.write_text("Cubes('Sales*')\n# comment\n", encoding="utf-8")
    rules = _resolve_filter_rules(f"file://{path}")
    assert rules is not None
    assert "Cubes('Sales*')" in rules._normalized_rules


def test_resolve_filter_rules_tm1project_file(tmp_path):
    path = tmp_path / "tm1project.json"
    path.write_text(
        json.dumps({"Version": "1.0", "Ignore": ["Cubes/Views"]}),
        encoding="utf-8",
    )
    rules = _resolve_filter_rules(f"file://{path}")
    expected = apply_default_filter_rules(FilterRules(["Cubes/Views"]))
    assert rules._normalized_rules == expected._normalized_rules


def test_load_filter_rules_tm1project_returns_ignore_only(tmp_path):
    path = tmp_path / "tm1project.json"
    path.write_text(
        json.dumps({"Version": "1.0", "Ignore": ["Cubes/Views", "!Cubes('A')"]}),
        encoding="utf-8",
    )
    lines = _load_filter_rules(f"file://{path}")
    assert lines == ["Cubes/Views", "!Cubes('A')"]


def test_resolve_filter_rules_missing_file_exits(tmp_path, monkeypatch):
    path = tmp_path / "missing.txt"
    monkeypatch.setattr(sys, "exit", lambda code=1: (_ for _ in ()).throw(SystemExit(code)))
    with pytest.raises(SystemExit):
        _resolve_filter_rules(f"file://{path}")


def test_changeset_filter_cli_uses_home_cache_by_default(tmp_path, monkeypatch):
    working_dir = tmp_path / "working-dir"
    working_dir.mkdir()
    monkeypatch.chdir(working_dir)
    monkeypatch.setattr(main_module, "TqdmProgressSink", lambda **_kwargs: NoopProgressSink())
    changeset_path = tmp_path / "changeset.yaml"
    changeset_path.write_text("changeset_id: cli-home-cache\nchanges:\n", encoding="utf-8")

    _cmd_changeset_filter(
        argparse.Namespace(
            changeset_path=str(changeset_path),
            filter_rules=None,
            debug=False,
        )
    )

    cache_path = ChangesetStore.path_for(changeset_id="cli-home-cache")
    assert cache_path.exists()
    assert cache_path.parent == (tmp_path / "home" / ".tm1gitpy" / ".cache").resolve()
    assert not (working_dir / ".tm1gitpy").exists()
