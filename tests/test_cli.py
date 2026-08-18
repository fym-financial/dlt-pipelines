import sys
from datetime import datetime, timedelta, timezone

from dlt_pipelines import cli
from dlt_pipelines.db import ExpectedFileCheckResult
from dlt_pipelines.transfers import ArchivePlanItem


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


def test_refresh_typed_command_supports_ahl(monkeypatch, capsys) -> None:
    class Result:
        provider = "ahl"
        schema_name = "typed"
        table_name = "ahl_fym_policy"
        row_count = 1921

    monkeypatch.setattr(cli, "refresh_typed_dataset", lambda provider: Result())
    monkeypatch.setattr(sys, "argv", ["dlt-pipeline", "refresh-typed", "ahl"])

    cli.main()

    captured = capsys.readouterr()
    assert "Refreshed typed.ahl_fym_policy for provider ahl with 1921 row(s)." in captured.out


def test_refresh_typed_command_supports_manhattan(monkeypatch, capsys) -> None:
    class Result:
        provider = "manhattan"
        schema_name = "typed"
        table_name = "manhattan_policy"
        row_count = 318

    monkeypatch.setattr(cli, "refresh_typed_dataset", lambda provider: Result())
    monkeypatch.setattr(
        sys,
        "argv",
        ["dlt-pipeline", "refresh-typed", "manhattan"],
    )

    cli.main()

    captured = capsys.readouterr()
    assert (
        "Refreshed typed.manhattan_policy for provider manhattan with 318 row(s)."
        in captured.out
    )


def test_load_heartland_loads_raw_and_refreshes_typed(monkeypatch, capsys) -> None:
    calls = {}

    class Result:
        provider = "heartland"
        schema_name = "typed"
        table_name = "heartland_inforced_policy"
        row_count = 222

    def fake_load(**kwargs):
        calls["load"] = kwargs
        return "raw loaded"

    monkeypatch.setattr(cli, "run_pipeline", fake_load)
    monkeypatch.setattr(cli, "refresh_typed_dataset", lambda provider: Result())
    monkeypatch.setattr(sys, "argv", ["dlt-pipeline", "load-heartland"])

    cli.main()

    assert calls["load"] == {
        "source_name": "heartland",
        "dataset_name": "raw",
        "pipeline_name": "heartland_api",
    }
    captured = capsys.readouterr()
    assert "raw loaded" in captured.out
    assert (
        "Refreshed typed.heartland_inforced_policy for provider heartland "
        "with 222 row(s)."
    ) in captured.out


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
    monkeypatch.setattr(
        cli,
        "archive_landed_files",
        lambda provider, progress=None, plan=None: [],
    )
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


def test_load_s3_does_not_audit_refresh_or_archive_unverified_file(
    monkeypatch,
) -> None:
    archive_plan = [
        ArchivePlanItem(
            source_path="landing-bucket/unl/inbound/GTLFYM_Policy_missing.csv",
            archive_path=(
                "landing-bucket/unl/inbound/Archive/GTLFYM_Policy_missing.csv"
            ),
            table_name="gtl_fym_policy",
        )
    ]
    monkeypatch.setattr(cli, "run_pipeline", lambda **kwargs: "loaded")
    monkeypatch.setattr(cli, "plan_landed_files_archive", lambda provider: archive_plan)

    def fail_verification(expected_files, *, schema_name):
        assert expected_files == [("gtl_fym_policy", "GTLFYM_Policy_missing.csv")]
        assert schema_name == "raw"
        raise RuntimeError("raw load verification failed")

    monkeypatch.setattr(cli, "verify_raw_file_loads", fail_verification)
    monkeypatch.setattr(
        cli,
        "_record_loaded_files",
        lambda *args: (_ for _ in ()).throw(AssertionError("must not record loaded")),
    )
    monkeypatch.setattr(
        cli,
        "_refresh_typed_and_print",
        lambda *args: (_ for _ in ()).throw(AssertionError("must not refresh typed")),
    )
    monkeypatch.setattr(
        cli,
        "archive_landed_files",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not archive")),
    )
    monkeypatch.setattr(sys, "argv", ["dlt-pipeline", "load-s3", "gtl"])

    try:
        cli.main()
    except RuntimeError as exc:
        assert str(exc) == "raw load verification failed"
    else:
        raise AssertionError("Expected raw verification failure")


def test_run_sftp_flow_exits_cleanly_when_no_files_moved(monkeypatch, capsys) -> None:
    calls = {"loaded": False}

    monkeypatch.setattr(cli, "move_sftp_files_to_s3", lambda provider, progress=None: [])
    monkeypatch.setattr(cli, "_record_moved_files", lambda provider, moved: None)

    def fail_load(**kwargs):
        calls["loaded"] = True
        raise AssertionError("run_pipeline should not have been called")

    monkeypatch.setattr(cli, "run_pipeline", fail_load)
    monkeypatch.setattr(
        sys,
        "argv",
        ["dlt-pipeline", "run-sftp-flow", "unl"],
    )

    cli.main()

    captured = capsys.readouterr()
    assert calls["loaded"] is False
    assert "Moved 0 file(s) to S3." in captured.out
    assert "No files moved for provider unl; skipping S3 load." in captured.out


def test_check_fym_policy_load_prints_found_result(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "check_unl_fym_policy_loaded",
        lambda max_age: ExpectedFileCheckResult(
            provider="unl",
            file_type="fym_policy",
            file_pattern=r"^(UNL)?FYM_Policy_[0-9]{14}\.csv$",
            found=True,
            current_time=datetime(2026, 7, 6, 16, tzinfo=timezone.utc),
            max_age=max_age,
            file_name="FYM_Policy_20260706100022.csv",
            landed_at=datetime(2026, 7, 6, 11, tzinfo=timezone.utc),
            age=timedelta(hours=5),
            status="loaded_to_postgres",
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["dlt-pipeline", "check-fym-policy-load", "--max-age-hours", "30"],
    )

    cli.main()

    captured = capsys.readouterr()
    assert "Latest loaded UNL FYM policy file is recent" in captured.out
    assert "FYM_Policy_20260706100022.csv" in captured.out
    assert "age 5h 0m" in captured.out


def test_check_fym_policy_load_exits_nonzero_when_missing(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "check_unl_fym_policy_loaded",
        lambda max_age: ExpectedFileCheckResult(
            provider="unl",
            file_type="fym_policy",
            file_pattern=r"^(UNL)?FYM_Policy_[0-9]{14}\.csv$",
            found=False,
            current_time=datetime(2026, 7, 6, 16, tzinfo=timezone.utc),
            max_age=max_age,
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["dlt-pipeline", "check-fym-policy-load"],
    )

    try:
        cli.main()
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("Expected missing file check to exit nonzero")

    captured = capsys.readouterr()
    assert "Missing loaded UNL FYM policy file" in captured.out
    assert r"^(UNL)?FYM_Policy_[0-9]{14}\.csv$" in captured.out


def test_check_fym_policy_load_exits_nonzero_when_stale(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "check_unl_fym_policy_loaded",
        lambda max_age: ExpectedFileCheckResult(
            provider="unl",
            file_type="fym_policy",
            file_pattern=r"^(UNL)?FYM_Policy_[0-9]{14}\.csv$",
            found=True,
            current_time=datetime(2026, 7, 6, 18, tzinfo=timezone.utc),
            max_age=max_age,
            file_name="FYM_Policy_20260705100022.csv",
            landed_at=datetime(2026, 7, 5, 11, tzinfo=timezone.utc),
            age=timedelta(hours=31),
            status="loaded_to_postgres",
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["dlt-pipeline", "check-fym-policy-load", "--max-age-hours", "30"],
    )

    try:
        cli.main()
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("Expected stale file check to exit nonzero")

    captured = capsys.readouterr()
    assert "Latest loaded UNL FYM policy file is stale" in captured.out
    assert "age 31h 0m" in captured.out
    assert "max age 30h 0m" in captured.out
