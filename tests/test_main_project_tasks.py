import argparse

import pytest

import tm1_git_py.main as main_module


@pytest.mark.parametrize(
    ("deployment", "source_control", "runner_name"),
    [
        ("pre", None, "apply_pre_pull_operations"),
        ("post", None, "apply_post_pull_operations"),
        (None, "pre", "apply_pre_push_operations"),
        (None, "post", "apply_post_push_operations"),
    ],
)
def test_run_hook_selects_expected_runner(
    tmp_path,
    mocker,
    deployment,
    source_control,
    runner_name,
):
    project_path = tmp_path / "tm1project.json"
    project_path.write_text("{}", encoding="utf-8")
    tm1_service = mocker.sentinel.tm1_service
    mocker.patch.object(main_module, "_tm1_connection", return_value=tm1_service)
    runners = {
        name: mocker.patch.object(main_module, name)
        for name in (
            "apply_pre_pull_operations",
            "apply_post_pull_operations",
            "apply_pre_push_operations",
            "apply_post_push_operations",
        )
    }

    main_module._cmd_run_hook(
        argparse.Namespace(
            server="local",
            tm1project=f"file://{project_path}",
            environment="test",
            deployment=deployment,
            source_control=source_control,
        )
    )

    runners[runner_name].assert_called_once_with(
        tm1_service=tm1_service,
        environment="test",
        project_file_path=project_path.resolve(),
    )
    for name, runner in runners.items():
        if name != runner_name:
            runner.assert_not_called()


def test_run_hook_requires_exactly_one_phase(monkeypatch, tmp_path):
    project_path = tmp_path / "tm1project.json"
    project_path.write_text("{}", encoding="utf-8")
    base_args = [
        "tm1gitpy",
        "run-hook",
        "--server",
        "local",
        "--tm1project",
        str(project_path),
        "--environment",
        "test",
    ]

    for phase_args in ([], ["--deployment", "pre", "--source-control", "post"]):
        monkeypatch.setattr(main_module.sys, "argv", base_args + phase_args)
        with pytest.raises(SystemExit, match="2"):
            main_module.main()
