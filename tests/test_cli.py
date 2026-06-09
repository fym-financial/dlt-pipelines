import sys

from dlt_pipelines import cli


def test_refresh_typed_command_prints_result(monkeypatch, capsys) -> None:
    class Result:
        provider = "unl"
        schema_name = "typed"
        table_name = "unl_fym_policy"
        row_count = 99

    monkeypatch.setattr(cli, "refresh_typed_dataset", lambda provider: Result())
    monkeypatch.setattr(sys, "argv", ["dlt-pipeline", "refresh-typed", "unl"])

    cli.main()

    captured = capsys.readouterr()
    assert "Refreshed typed.unl_fym_policy for provider unl with 99 row(s)." in captured.out


def test_load_s3_refreshes_typed_by_default(monkeypatch, capsys) -> None:
    calls = {"refreshed": False}

    class Result:
        provider = "unl"
        schema_name = "typed"
        table_name = "unl_fym_policy"
        row_count = 10

    monkeypatch.setattr(cli, "run_pipeline", lambda **kwargs: "loaded")
    monkeypatch.setattr(cli, "plan_landed_files_archive", lambda provider: [])
    monkeypatch.setattr(cli, "_record_loaded_files", lambda provider, archive_plan: None)
    monkeypatch.setattr(cli, "archive_landed_files", lambda provider, progress=None: [])
    monkeypatch.setattr(cli, "_record_archived_files", lambda provider, archived: None)

    def fake_refresh(provider: str) -> Result:
        calls["refreshed"] = True
        return Result()

    monkeypatch.setattr(cli, "refresh_typed_dataset", fake_refresh)
    monkeypatch.setattr(
        sys,
        "argv",
        ["dlt-pipeline", "load-s3", "unl", "--no-archive"],
    )

    cli.main()

    captured = capsys.readouterr()
    assert calls["refreshed"] is True
    assert "loaded" in captured.out
    assert "Refreshed typed.unl_fym_policy for provider unl with 10 row(s)." in captured.out


def test_load_s3_no_refresh_typed_flag_skips_refresh(monkeypatch, capsys) -> None:
    calls = {"refreshed": False}

    monkeypatch.setattr(cli, "run_pipeline", lambda **kwargs: "loaded")
    monkeypatch.setattr(cli, "plan_landed_files_archive", lambda provider: [])
    monkeypatch.setattr(cli, "_record_loaded_files", lambda provider, archive_plan: None)
    monkeypatch.setattr(cli, "refresh_typed_dataset", lambda provider: None)

    def fail_refresh(provider: str):
        calls["refreshed"] = True
        raise AssertionError("refresh should not have been called")

    monkeypatch.setattr(cli, "_refresh_typed_and_print", fail_refresh)
    monkeypatch.setattr(
        sys,
        "argv",
        ["dlt-pipeline", "load-s3", "unl", "--no-refresh-typed", "--no-archive"],
    )

    cli.main()

    captured = capsys.readouterr()
    assert calls["refreshed"] is False
    assert "loaded" in captured.out
