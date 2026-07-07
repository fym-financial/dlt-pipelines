from datetime import datetime, timedelta, timezone

from dlt_pipelines.db import (
    FileAuditEvent,
    check_unl_fym_policy_loaded,
    check_postgres_connection,
    record_file_events,
    refresh_typed_dataset,
)
from dlt_pipelines.sources.files import csv_customers
from dlt_pipelines.sources import s3
from dlt_pipelines.sources.s3 import FileRoute, _routes_for_provider, _routes_with_matches
from dlt_pipelines.transfers import (
    ArchivePlanItem,
    SftpToS3Config,
    TransferPlanItem,
    archive_landed_files,
    move_sftp_files_to_s3,
    plan_landed_files_archive,
    plan_sftp_files_to_s3,
)


def test_csv_customers_reads_sample_data() -> None:
    rows = list(csv_customers())

    assert rows
    assert rows[0]["customer_id"] == "1"


def test_unl_routes_are_configured() -> None:
    routes = _routes_for_provider("unl")

    assert routes == [
        FileRoute(
            name="fym_policy",
            file_glob="unl/inbound/FYM_Policy_*.csv",
            parser="csv",
            table_name="unl_fym_policy",
            parser_options={
                "dtype": {
                    "zip": "string",
                    "issue_date": "string",
                    "app_recvd_date": "string",
                    "paid_to_date": "string",
                    "billing_mode": "string",
                }
            },
        ),
        FileRoute(
            name="life_professionals_policy",
            file_glob="unl/inbound/LifeProfessionals_Policy_*.csv",
            parser="csv",
            table_name="unl_life_professionals_policy",
            parser_options={},
        ),
        FileRoute(
            name="weekly_commissions",
            file_glob="unl/inbound/CommissionStatements/WC_*.csv",
            parser="csv",
            table_name="unl_weekly_commissions",
            parser_options={},
        ),
        FileRoute(
            name="monthly_commissions",
            file_glob="unl/inbound/CommissionStatements/MC_*.csv",
            parser="csv",
            table_name="unl_monthly_commissions",
            parser_options={},
        ),
        FileRoute(
            name="weekly_advance_statements",
            file_glob="unl/inbound/CommissionStatements/WA_*.csv",
            parser="csv",
            table_name="unl_weekly_advance_statements",
            parser_options={},
        ),
        FileRoute(
            name="monthly_advance_statements",
            file_glob="unl/inbound/CommissionStatements/MA_*.csv",
            parser="csv",
            table_name="unl_monthly_advance_statements",
            parser_options={},
        ),
    ]


def test_routes_with_matches_skips_empty_patterns(monkeypatch) -> None:
    class FakeS3Fs:
        def glob(self, pattern: str) -> list[str]:
            if pattern == "landing-bucket/unl/inbound/FYM_Policy_*.csv":
                return ["landing-bucket/unl/inbound/FYM_Policy_20260528100022.csv"]
            return []

        def isdir(self, path: str) -> bool:
            return False

    def fake_url_to_fs(url: str, **kwargs: object):
        assert url == "s3://landing-bucket"
        return FakeS3Fs(), "landing-bucket"

    monkeypatch.setattr("dlt_pipelines.sources.s3.fsspec.core.url_to_fs", fake_url_to_fs)

    routes = _routes_with_matches("s3://landing-bucket", _routes_for_provider("unl"))

    assert routes == [
        FileRoute(
            name="fym_policy",
            file_glob="unl/inbound/FYM_Policy_*.csv",
            parser="csv",
            table_name="unl_fym_policy",
            parser_options={
                "dtype": {
                    "zip": "string",
                    "issue_date": "string",
                    "app_recvd_date": "string",
                    "paid_to_date": "string",
                    "billing_mode": "string",
                }
            },
        )
    ]


def test_advance_statement_routes_match_updated_filenames(monkeypatch) -> None:
    class FakeS3Fs:
        def glob(self, pattern: str) -> list[str]:
            if pattern == "landing-bucket/unl/inbound/CommissionStatements/WA_*.csv":
                return [
                    "landing-bucket/unl/inbound/CommissionStatements/"
                    "WA_202JVV00_2026_06_10.csv"
                ]
            if pattern == "landing-bucket/unl/inbound/CommissionStatements/MA_*.csv":
                return [
                    "landing-bucket/unl/inbound/CommissionStatements/"
                    "MA_202JVV00_2026_06_10.csv"
                ]
            return []

        def isdir(self, path: str) -> bool:
            return False

    def fake_url_to_fs(url: str, **kwargs: object):
        assert url == "s3://landing-bucket"
        return FakeS3Fs(), "landing-bucket"

    monkeypatch.setattr("dlt_pipelines.sources.s3.fsspec.core.url_to_fs", fake_url_to_fs)

    routes = _routes_with_matches("s3://landing-bucket", _routes_for_provider("unl"))

    assert routes == [
        FileRoute(
            name="weekly_advance_statements",
            file_glob="unl/inbound/CommissionStatements/WA_*.csv",
            parser="csv",
            table_name="unl_weekly_advance_statements",
            parser_options={},
        ),
        FileRoute(
            name="monthly_advance_statements",
            file_glob="unl/inbound/CommissionStatements/MA_*.csv",
            parser="csv",
            table_name="unl_monthly_advance_statements",
            parser_options={},
        ),
    ]


def test_landed_csv_files_binds_filesystem_to_reader(monkeypatch) -> None:
    class FakeFiles:
        def __or__(self, reader: object) -> "FakePipe":
            return FakePipe(reader)

    class FakePipe:
        def __init__(self, reader: object) -> None:
            self.reader = reader

        def with_name(self, name: str) -> tuple[str, object]:
            return (name, self.reader)

    class FakeReader:
        def __init__(self, options: dict[str, object]) -> None:
            self.options = options

    monkeypatch.setenv(
        "SFTP_PROVIDERS__UNL__S3__LANDING__BUCKET_URL",
        "s3://landing-bucket",
    )
    monkeypatch.setattr(
        s3,
        "_routes_with_matches",
        lambda bucket_url, routes: [
            FileRoute(
                name="fym_policy",
                file_glob="unl/inbound/FYM_Policy_*.csv",
                parser="csv",
                table_name="unl_fym_policy",
                parser_options={
                    "dtype": {
                        "zip": "string",
                        "issue_date": "string",
                        "app_recvd_date": "string",
                        "paid_to_date": "string",
                        "billing_mode": "string",
                    }
                },
            )
        ],
    )
    monkeypatch.setattr(s3, "filesystem", lambda **kwargs: FakeFiles())
    monkeypatch.setattr(s3, "read_csv_with_file_errors", lambda **kwargs: FakeReader(kwargs))

    resources = s3.landed_csv_files("unl")

    assert len(resources) == 1
    assert resources[0][0] == "unl_fym_policy"
    assert isinstance(resources[0][1], FakeReader)
    assert resources[0][1].options == {
        "dtype": {
            "zip": "string",
            "issue_date": "string",
            "app_recvd_date": "string",
            "paid_to_date": "string",
            "billing_mode": "string",
        }
    }


def test_sftp_to_s3_plan_preserves_relative_paths(monkeypatch) -> None:
    class FakeSourceFs:
        def glob(self, pattern: str) -> list[str]:
            if pattern == "./*.csv":
                return ["./FYM_Policy_20260528100022.csv"]
            if pattern == "/*.csv":
                return ["/FYM_Policy_20260528100022.csv"]
            if pattern == "./CommissionStatements/*.csv":
                return []
            if pattern == "/CommissionStatements/*.csv":
                return ["/CommissionStatements/WC_202JVV00_2026_05_27.csv"]
            raise AssertionError(f"Unexpected pattern: {pattern}")

        def isdir(self, path: str) -> bool:
            return path == "CommissionStatements"

    class FakeTargetFs:
        pass

    def fake_url_to_fs(url: str, **kwargs: object):
        if url == "sftp://example.com":
            return FakeSourceFs(), ""
        if url == "s3://landing-bucket":
            return FakeTargetFs(), "landing-bucket"
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr("dlt_pipelines.transfers.fsspec.core.url_to_fs", fake_url_to_fs)

    plan = plan_sftp_files_to_s3(
        "unl",
        SftpToS3Config(
            provider="unl",
            sftp_bucket_url="sftp://example.com",
            sftp_file_globs=("*.csv", "CommissionStatements/*.csv"),
            s3_landing_bucket_url="s3://landing-bucket",
            s3_landing_prefix="unl/inbound",
        ),
    )

    assert plan == [
        TransferPlanItem(
            source_path="./FYM_Policy_20260528100022.csv",
            target_path="landing-bucket/unl/inbound/FYM_Policy_20260528100022.csv",
        ),
        TransferPlanItem(
            source_path="/CommissionStatements/WC_202JVV00_2026_05_27.csv",
            target_path=(
                "landing-bucket/unl/inbound/CommissionStatements/"
                "WC_202JVV00_2026_05_27.csv"
            ),
        ),
    ]


def test_check_postgres_connection_uses_destination_credentials(monkeypatch) -> None:
    calls = {}

    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: str) -> None:
            calls["last_query"] = query

        def fetchone(self) -> tuple[str, str, str]:
            return ("analytics", "analytics_loader", "PostgreSQL 16")

        def fetchall(self) -> list[tuple[str]]:
            return [
                ("audit", True, True, True),
                ("raw", True, True, True),
            ]

    class FakeConnection:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def close(self) -> None:
            calls["closed"] = True

    def fake_connect(**kwargs: object) -> FakeConnection:
        calls["connect_kwargs"] = kwargs
        return FakeConnection()

    monkeypatch.setattr("dlt_pipelines.db.psycopg2.connect", fake_connect)
    monkeypatch.setenv("DESTINATION__POSTGRES__CREDENTIALS__DATABASE", "analytics")
    monkeypatch.setenv("DESTINATION__POSTGRES__CREDENTIALS__USERNAME", "analytics_loader")
    monkeypatch.setenv("DESTINATION__POSTGRES__CREDENTIALS__PASSWORD", "secret")
    monkeypatch.setenv("DESTINATION__POSTGRES__CREDENTIALS__HOST", "db.example.com")
    monkeypatch.setenv("DESTINATION__POSTGRES__CREDENTIALS__PORT", "5432")

    result = check_postgres_connection()

    assert calls["connect_kwargs"] == {
        "dbname": "analytics",
        "user": "analytics_loader",
        "password": "secret",
        "host": "db.example.com",
        "port": "5432",
        "connect_timeout": 15,
    }
    assert calls["closed"] is True
    assert result.database == "analytics"
    assert result.user == "analytics_loader"
    assert [schema.name for schema in result.schemas] == ["audit", "raw"]
    assert all(schema.exists for schema in result.schemas)
    assert all(schema.has_usage for schema in result.schemas)
    assert all(schema.has_create for schema in result.schemas)


def test_record_file_events_upserts_audit_rows(monkeypatch) -> None:
    calls = {"execute": []}

    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: str, params: tuple[object, ...]) -> None:
            calls["execute"].append((query, params))

    class FakeConnection:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            calls["commit"] = True

        def rollback(self) -> None:
            calls["rollback"] = True

        def close(self) -> None:
            calls["closed"] = True

    monkeypatch.setattr("dlt_pipelines.db._connect", lambda: FakeConnection())

    count = record_file_events(
        [
            FileAuditEvent(
                provider="unl",
                source_path="./FYM_Policy_20260528100022.csv",
                target_path="fym-inbound-files/unl/inbound/FYM_Policy_20260528100022.csv",
                file_size_bytes=123,
                status="moved_to_landing",
            )
        ]
    )

    assert count == 1
    assert calls["commit"] is True
    assert calls["closed"] is True
    assert calls["execute"][0][1] == (
        "unl",
        "./FYM_Policy_20260528100022.csv",
        "fym-inbound-files/unl/inbound/FYM_Policy_20260528100022.csv",
        "FYM_Policy_20260528100022.csv",
        123,
        "moved_to_landing",
        None,
    )


def test_check_unl_fym_policy_loaded_uses_latest_loaded_fym_policy(monkeypatch) -> None:
    calls = {}

    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: str, params: tuple[object, ...]) -> None:
            calls["query"] = query
            calls["params"] = params

        def fetchone(self) -> tuple[str, datetime, str]:
            return (
                "FYM_Policy_20260706100022.csv",
                datetime(2026, 7, 6, 11, tzinfo=timezone.utc),
                "loaded_to_postgres",
            )

    class FakeConnection:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def close(self) -> None:
            calls["closed"] = True

    monkeypatch.setattr("dlt_pipelines.db._connect", lambda: FakeConnection())

    result = check_unl_fym_policy_loaded(
        max_age=timedelta(hours=30),
        current_time=datetime(2026, 7, 6, 16, tzinfo=timezone.utc),
    )

    assert "status = 'loaded_to_postgres'" in calls["query"]
    assert "ESCAPE" in calls["query"]
    assert "ORDER BY landed_at DESC, file_name DESC" in calls["query"]
    assert calls["params"] == (r"FYM\_Policy\_%.csv",)
    assert calls["closed"] is True
    assert result.found is True
    assert result.is_recent is True
    assert result.file_name == "FYM_Policy_20260706100022.csv"
    assert result.age == timedelta(hours=5)


def test_check_unl_fym_policy_loaded_reports_missing(monkeypatch) -> None:
    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: str, params: tuple[object, ...]) -> None:
            return None

        def fetchone(self) -> None:
            return None

    class FakeConnection:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def close(self) -> None:
            return None

    monkeypatch.setattr("dlt_pipelines.db._connect", lambda: FakeConnection())

    result = check_unl_fym_policy_loaded(
        max_age=timedelta(hours=30),
        current_time=datetime(2026, 7, 6, 16, tzinfo=timezone.utc),
    )

    assert result.found is False
    assert result.is_recent is False
    assert result.file_pattern == r"FYM\_Policy\_%.csv"


def test_check_unl_fym_policy_loaded_reports_stale(monkeypatch) -> None:
    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: str, params: tuple[object, ...]) -> None:
            return None

        def fetchone(self) -> tuple[str, datetime, str]:
            return (
                "FYM_Policy_20260705100022.csv",
                datetime(2026, 7, 5, 11, tzinfo=timezone.utc),
                "loaded_to_postgres",
            )

    class FakeConnection:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def close(self) -> None:
            return None

    monkeypatch.setattr("dlt_pipelines.db._connect", lambda: FakeConnection())

    result = check_unl_fym_policy_loaded(
        max_age=timedelta(hours=30),
        current_time=datetime(2026, 7, 6, 18, tzinfo=timezone.utc),
    )

    assert result.found is True
    assert result.is_recent is False
    assert result.age == timedelta(hours=31)


def test_refresh_typed_dataset_executes_refresh_sql(monkeypatch) -> None:
    calls = {"execute": [], "copy": []}

    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: str) -> None:
            calls["execute"].append(query)

        def copy_expert(self, query: str, file) -> None:
            calls["copy"].append(query)

        def fetchone(self) -> tuple[int]:
            return (42,)

    class FakeConnection:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            calls["commit"] = True

        def rollback(self) -> None:
            calls["rollback"] = True

        def close(self) -> None:
            calls["closed"] = True

    monkeypatch.setattr("dlt_pipelines.db._connect", lambda: FakeConnection())
    monkeypatch.setattr("dlt_pipelines.db._connect_roster_source", lambda: FakeConnection())

    result = refresh_typed_dataset("unl")

    executed_sql = [query for query in calls["execute"] if isinstance(query, str)]
    assert result.provider == "unl"
    assert result.schema_name == "typed"
    assert result.table_name == "unl_fym_policy"
    assert result.row_count == 42
    assert calls["commit"] is True
    assert calls["closed"] is True
    assert len(calls["copy"]) == 12
    assert any(query.startswith("CREATE SCHEMA IF NOT EXISTS typed") for query in executed_sql)
    assert any(query.startswith("TRUNCATE TABLE typed.unl_fym_policy") for query in executed_sql)
    assert any(query.startswith("INSERT INTO typed.unl_fym_policy") for query in executed_sql)
    assert any(
        query.startswith("CREATE TABLE IF NOT EXISTS typed.unl_weekly_advance_statements")
        for query in executed_sql
    )
    assert any(
        query.startswith("TRUNCATE TABLE typed.unl_weekly_advance_statements")
        for query in executed_sql
    )
    assert any(
        query.startswith("INSERT INTO typed.unl_weekly_advance_statements")
        for query in executed_sql
    )
    latest_load_sql_by_schema = {
        query.split(".")[0].removeprefix("CREATE OR REPLACE VIEW "): query
        for query in executed_sql
        if query.startswith("CREATE OR REPLACE VIEW ")
        and query.endswith("ON policy_roster_hierarchy._dlt_id = p._dlt_id\n")
    }
    assert sorted(latest_load_sql_by_schema) == ["raw", "typed"]
    for schema_name, latest_load_sql in latest_load_sql_by_schema.items():
        assert f"CREATE OR REPLACE VIEW {schema_name}.unl_fym_policy_latest_load" in latest_load_sql
        assert f"FROM {schema_name}.unl_fym_policy AS p" in latest_load_sql
        assert "roster_hierarchy_json" in latest_load_sql
        assert "nullif(trim(levels.writing_number::text), '') AS writing_number" in latest_load_sql
        assert "trim(agency_carrier.writing_number) = levels.writing_number" in latest_load_sql
        assert "trim(agent_carrier.writing_number) = levels.writing_number" in latest_load_sql
        assert "lpad(hierarchy_level::text, 2, '0')" in latest_load_sql
        assert "ORDER BY traversal_depth DESC" in latest_load_sql
    weekly_advance_latest_load_sql_by_schema = {
        query.split(".")[0].removeprefix("CREATE OR REPLACE VIEW "): query
        for query in executed_sql
        if query.startswith("CREATE OR REPLACE VIEW ")
        and "unl_weekly_advance_statements_latest_load" in query
    }
    assert sorted(weekly_advance_latest_load_sql_by_schema) == ["raw", "typed"]
    for schema_name, latest_load_sql in weekly_advance_latest_load_sql_by_schema.items():
        assert (
            f"CREATE OR REPLACE VIEW {schema_name}.unl_weekly_advance_statements_latest_load"
            in latest_load_sql
        )
        assert f"FROM {schema_name}.unl_weekly_advance_statements AS p" in latest_load_sql
        assert "fl.file_name LIKE 'WA_%.csv'" in latest_load_sql
    assert any(
        "ON typed.unl_weekly_advance_statements (_dlt_id)" in query for query in executed_sql
    )
    assert any(
        "ON typed.unl_weekly_advance_statements (file_date)" in query for query in executed_sql
    )
    assert "GRANT SELECT ON raw.unl_fym_policy_latest_load TO unl_fym_policy_reader" in executed_sql
    assert (
        "GRANT SELECT ON raw.unl_weekly_advance_statements_latest_load TO unl_fym_policy_reader"
        in executed_sql
    )
    assert calls["execute"][-1] == "SELECT count(*) FROM typed.unl_fym_policy"


def test_refresh_typed_dataset_rejects_unsupported_provider() -> None:
    try:
        refresh_typed_dataset("acme")
    except RuntimeError as exc:
        assert "not configured" in str(exc)
    else:
        raise AssertionError("Expected refresh_typed_dataset to reject unsupported provider")


def test_move_sftp_files_deletes_source_after_verified_copy(monkeypatch) -> None:
    deleted_paths = []
    written = {}

    class FakeReadable:
        def __init__(self) -> None:
            self._read = False

        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self, size: int = -1) -> bytes:
            if self._read:
                return b""
            self._read = True
            return b"abc"

    class FakeWritable:
        def __init__(self, path: str) -> None:
            self.path = path

        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def write(self, data: bytes) -> int:
            written[self.path] = written.get(self.path, b"") + data
            return len(data)

    class FakeSourceFs:
        def glob(self, pattern: str) -> list[str]:
            return ["./FYM_Policy_20260528100022.csv"] if pattern == "./*.csv" else []

        def isdir(self, path: str) -> bool:
            return False

        def open(self, path: str, mode: str):
            assert mode == "rb"
            return FakeReadable()

        def size(self, path: str) -> int:
            return 3

        def rm(self, path: str) -> None:
            deleted_paths.append(path)

    class FakeTargetFs:
        def makedirs(self, path: str, exist_ok: bool = False) -> None:
            return None

        def open(self, path: str, mode: str):
            assert mode == "wb"
            return FakeWritable(path)

        def size(self, path: str) -> int:
            return len(written[path])

    def fake_url_to_fs(url: str, **kwargs: object):
        if url == "sftp://example.com":
            return FakeSourceFs(), ""
        if url == "s3://landing-bucket":
            return FakeTargetFs(), "landing-bucket"
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr("dlt_pipelines.transfers.fsspec.core.url_to_fs", fake_url_to_fs)

    results = move_sftp_files_to_s3(
        "unl",
        SftpToS3Config(
            provider="unl",
            sftp_bucket_url="sftp://example.com",
            sftp_file_globs=("*.csv",),
            s3_landing_bucket_url="s3://landing-bucket",
            s3_landing_prefix="unl/inbound",
        ),
    )

    assert deleted_paths == ["./FYM_Policy_20260528100022.csv"]
    assert results[0].deleted_source is True
    assert results[0].source_size == 3
    assert results[0].target_size == 3


def test_archive_landed_files_moves_to_archive_subdirectory(monkeypatch) -> None:
    moved_paths = []

    class FakeS3Fs:
        def glob(self, pattern: str) -> list[str]:
            if pattern == "landing-bucket/unl/inbound/FYM_Policy_*.csv":
                return ["landing-bucket/unl/inbound/FYM_Policy_20260528100022.csv"]
            if pattern == "landing-bucket/unl/inbound/CommissionStatements/WC_*.csv":
                return [
                    "landing-bucket/unl/inbound/CommissionStatements/"
                    "WC_202JVV00_2026_05_27.csv"
                ]
            if pattern == "landing-bucket/unl/inbound/CommissionStatements/WA_*.csv":
                return [
                    "landing-bucket/unl/inbound/CommissionStatements/"
                    "WA_202JVV00_2026_05_27.csv"
                ]
            return []

        def isdir(self, path: str) -> bool:
            return False

        def makedirs(self, path: str, exist_ok: bool = False) -> None:
            return None

        def mv(self, source_path: str, archive_path: str) -> None:
            moved_paths.append((source_path, archive_path))

    def fake_url_to_fs(url: str, **kwargs: object):
        if url == "s3://landing-bucket":
            return FakeS3Fs(), "landing-bucket"
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr("dlt_pipelines.transfers.fsspec.core.url_to_fs", fake_url_to_fs)
    monkeypatch.setenv(
        "SFTP_PROVIDERS__UNL__S3__LANDING__BUCKET_URL",
        "s3://landing-bucket",
    )

    plan = plan_landed_files_archive("unl")

    assert plan == [
        ArchivePlanItem(
            source_path="landing-bucket/unl/inbound/FYM_Policy_20260528100022.csv",
            archive_path="landing-bucket/unl/inbound/Archive/FYM_Policy_20260528100022.csv",
        ),
        ArchivePlanItem(
            source_path=(
                "landing-bucket/unl/inbound/CommissionStatements/"
                "WC_202JVV00_2026_05_27.csv"
            ),
            archive_path=(
                "landing-bucket/unl/inbound/CommissionStatements/Archive/"
                "WC_202JVV00_2026_05_27.csv"
            ),
        ),
        ArchivePlanItem(
            source_path=(
                "landing-bucket/unl/inbound/CommissionStatements/"
                "WA_202JVV00_2026_05_27.csv"
            ),
            archive_path=(
                "landing-bucket/unl/inbound/CommissionStatements/Archive/"
                "WA_202JVV00_2026_05_27.csv"
            ),
        ),
    ]

    archived = archive_landed_files("unl")

    assert archived == plan
    assert moved_paths == [
        (item.source_path, item.archive_path)
        for item in plan
    ]
