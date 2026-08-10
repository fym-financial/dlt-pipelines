from datetime import datetime, timedelta, timezone
from io import BytesIO
import json
from urllib.error import HTTPError

from dlt_pipelines.db import (
    FileAuditEvent,
    _heartland_hnl_status_history_select,
    _heartland_inforced_policy_hierarchy_select,
    _heartland_inforced_policy_typed_select,
    _heartland_typed_refresh_statements,
    _unl_fym_policy_typed_select,
    check_unl_fym_policy_loaded,
    check_postgres_connection,
    record_file_events,
    refresh_typed_dataset,
)
from dlt_pipelines.sources import api
from dlt_pipelines.sources.api import HEARTLAND_POLICY_FIELDS, heartland_inforced_policies
from dlt_pipelines.sources.files import csv_customers
from dlt_pipelines.sources import s3
from dlt_pipelines.sources.s3 import FileRoute, _routes_for_provider, _routes_with_matches
from dlt_pipelines.pipelines.typed import run_unl_fym_policy_typed_pipeline
from dlt_pipelines.pipelines.ahl import (
    AHL_TYPED_POLICY_COLUMNS,
    ahl_fym_policy_typed_select,
    ahl_typed_refresh_statements,
)
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


def test_heartland_source_logs_in_and_fetches_policy_snapshot(monkeypatch) -> None:
    calls = []
    source_row = {field: "" for field in HEARTLAND_POLICY_FIELDS}
    source_row.update({"polNo": "HN1", "premium": "123.45", "issAge": "67"})
    responses = [
        BytesIO(b"test-token"),
        BytesIO(json.dumps([source_row]).encode()),
    ]

    def fake_urlopen(request, timeout):
        calls.append((request, timeout))
        return responses.pop(0)

    monkeypatch.setattr(api, "urlopen", fake_urlopen)

    rows = list(
        heartland_inforced_policies(
            username="FYMUser",
            password="secret",
            base_url="https://heartland.test/",
        )
    )

    assert len(rows) == 1
    assert rows[0]["polNo"] == "HN1"
    assert rows[0]["premium"] == "123.45"
    assert set(rows[0]) == set(HEARTLAND_POLICY_FIELDS)

    login_request, login_timeout = calls[0]
    assert login_request.full_url == "https://heartland.test/api/auth/login"
    assert login_request.method == "POST"
    assert login_timeout == 30
    assert login_request.get_header("User-agent") == "curl/8.7.1"
    assert login_request.get_header("Accept") == "*/*"
    assert json.loads(login_request.data) == {
        "Username": "FYMUser",
        "Password": "secret",
    }

    policy_request, policy_timeout = calls[1]
    assert policy_request.full_url == "https://heartland.test/api/FYM/GetPolicies"
    assert policy_request.method == "GET"
    assert policy_request.get_header("Authorization") == "Bearer test-token"
    assert policy_request.get_header("User-agent") == "curl/8.7.1"
    assert policy_request.get_header("Accept") == "*/*"
    assert policy_timeout == 60


def test_heartland_source_identifies_login_http_errors(monkeypatch) -> None:
    def forbidden(request, timeout):
        raise HTTPError(
            request.full_url,
            403,
            "Forbidden",
            hdrs=None,
            fp=BytesIO(b"request blocked"),
        )

    monkeypatch.setattr(api, "urlopen", forbidden)

    try:
        list(
            heartland_inforced_policies(
                username="FYMUser",
                password="secret",
                base_url="https://heartland.test",
            )
        )
    except RuntimeError as exc:
        assert str(exc) == "Heartland login failed with HTTP 403: request blocked"
    else:
        raise AssertionError("Expected the Heartland login request to fail")


def test_heartland_typed_select_applies_conservative_types() -> None:
    select_sql = _heartland_inforced_policy_typed_select()

    assert "premium_text::numeric" in select_sql
    assert "share_text::numeric" in select_sql
    assert "iss_age_text::smallint" in select_sql
    assert "AS app_date" in select_sql
    assert "AS eff_date" in select_sql
    assert "AS birth_date" in select_sql
    assert "AS entry_date" in select_sql
    assert "AS initial_paid_date" in select_sql
    assert "AS paid_to_date" in select_sql
    assert "AS charge_back_dt" in select_sql
    assert "AS end_date" in select_sql
    assert "p.client_zip::text), '') AS client_state" in select_sql
    assert "p.client_state::text), '') AS client_zip" in select_sql
    assert "p.plan::text" in select_sql
    assert "p.draft_day::text" in select_sql

    refresh_statements = _heartland_typed_refresh_statements()
    refresh_sql = "\n".join(refresh_statements)
    assert "raw.heartland_inforced_policy_snapshot" in refresh_sql
    assert "CREATE TABLE IF NOT EXISTS raw.heartland_inforced_policy" in refresh_sql
    assert "CREATE TABLE IF NOT EXISTS typed.heartland_inforced_policy" in refresh_sql
    assert (
        "CREATE TABLE IF NOT EXISTS "
        "typed.heartland_inforced_policy_status_history"
    ) in refresh_sql
    assert (
        "CREATE TABLE IF NOT EXISTS typed.heartland_inforced_policy_hierarchy"
        in refresh_sql
    )
    assert "ON CONFLICT (_row_hash) DO NOTHING" in refresh_sql
    assert "ON CONFLICT (raw_row_hash) DO NOTHING" in refresh_sql
    assert "CREATE OR REPLACE VIEW typed.heartland_inforced_policy_latest" in refresh_sql
    assert "status_history.previous_hnl_status" in refresh_sql
    assert "status_history.previous_hnl_status_date" in refresh_sql
    assert "hierarchy.roster_hierarchy_json" in refresh_sql
    assert (
        "LEFT JOIN typed.heartland_inforced_policy_hierarchy AS hierarchy"
        in refresh_sql
    )
    assert (
        "UPDATE typed.heartland_inforced_policy_status_history AS existing"
        in refresh_sql
    )
    assert (
        "existing.previous_hnl_status_date "
        "IS DISTINCT FROM corrected.previous_hnl_status_date"
        in refresh_sql
    )
    assert "TRUNCATE TABLE raw.heartland_inforced_policy" not in refresh_sql
    assert "TRUNCATE TABLE typed.heartland_inforced_policy" not in refresh_sql
    assert "TRUNCATE TABLE typed.heartland_inforced_policy_status_history" not in refresh_sql
    typed_history_insert = next(
        statement
        for statement in refresh_statements
        if statement.startswith("INSERT INTO typed.heartland_inforced_policy ")
    )
    assert "ON CONFLICT (raw_row_hash) DO NOTHING" in typed_history_insert
    assert "DO UPDATE" not in typed_history_insert
    assert "DELETE FROM raw.heartland_inforced_policy AS" not in refresh_sql
    assert "DELETE FROM typed.heartland_inforced_policy AS" not in refresh_sql


def test_heartland_status_history_tracks_distinct_status_changes() -> None:
    history_sql = _heartland_hnl_status_history_select()

    assert "PARTITION BY p.pol_no, p.agt_code, p.writing_split" in history_sql
    assert "ORDER BY p.first_seen_at, p.raw_row_hash" in history_sql
    assert "lag(p.hnl_status) OVER history_window" in history_sql
    assert (
        "lag((p.first_seen_at AT TIME ZONE 'UTC')::date) OVER history_window"
        in history_sql
    )
    assert "hnl_status IS DISTINCT FROM previous_status_observation" in history_sql
    assert "array_agg(previous_status_observation) FILTER" in history_sql
    assert "array_agg(previous_status_observation_date) FILTER" in history_sql
    assert "AS previous_hnl_status" in history_sql
    assert "AS previous_hnl_status_date" in history_sql


def test_heartland_hierarchy_matches_unl_json_shape_and_is_precomputed() -> None:
    hierarchy_sql = _heartland_inforced_policy_hierarchy_select()
    refresh_sql = "\n".join(_heartland_typed_refresh_statements())

    assert "WITH RECURSIVE latest_policy AS" in hierarchy_sql
    assert "SELECT DISTINCT ON (p.agt_code)" in hierarchy_sql
    assert "substring(p.upline from '-\\s*([0-9]+)\\s*$')" in hierarchy_sql
    assert (
        "regexp_replace(p.upline, '\\s*-\\s*[0-9]+\\s*$', '')"
        in hierarchy_sql
    )
    assert "seed.upline_code IS DISTINCT FROM seed.policy_agt_code" in hierarchy_sql
    assert "ARRAY[seed.policy_agt_code, seed.upline_code]::text[]" in hierarchy_sql
    assert "NOT hierarchy_walk.next_upline_code = ANY(" in hierarchy_sql
    assert "hierarchy_walk.traversal_depth < 99" in hierarchy_sql
    assert "ORDER BY traversal_depth DESC" in hierarchy_sql
    assert "lpad(hierarchy_level::text, 2, '0')" in hierarchy_sql
    assert "'depth'," in hierarchy_sql
    assert "'name'," in hierarchy_sql
    assert "'writing_number'," in hierarchy_sql
    assert "'is_person'," in hierarchy_sql
    assert "true" in hierarchy_sql

    assert (
        "CREATE INDEX IF NOT EXISTS heartland_inforced_policy_latest_idx"
        in refresh_sql
    )
    assert (
        "CREATE INDEX IF NOT EXISTS heartland_inforced_policy_agent_latest_idx"
        in refresh_sql
    )
    assert (
        "CREATE TEMP TABLE heartland_inforced_policy_hierarchy_refresh"
        in refresh_sql
    )
    assert (
        "INSERT INTO typed.heartland_inforced_policy_hierarchy"
        in refresh_sql
    )
    assert "ON CONFLICT (raw_row_hash) DO UPDATE" in refresh_sql
    assert (
        "DELETE FROM typed.heartland_inforced_policy_hierarchy AS existing"
        in refresh_sql
    )


def test_unl_typed_pipeline_uses_sql_cursor_and_insert_only_merge(monkeypatch) -> None:
    calls = {}

    class FakeResource:
        def with_name(self, name: str):
            calls["resource_name"] = name
            return self

    class FakePipeline:
        def run(self, resource):
            calls["run_resource"] = resource
            return "loaded"

    def fake_sql_table(**kwargs):
        calls["sql_table"] = kwargs
        return FakeResource()

    def fake_pipeline(**kwargs):
        calls["pipeline"] = kwargs
        return FakePipeline()

    monkeypatch.setattr("dlt.sources.sql_database.sql_table", fake_sql_table)
    monkeypatch.setattr("dlt.pipeline", fake_pipeline)

    result = run_unl_fym_policy_typed_pipeline(
        source_credentials="postgresql+psycopg2://source",
        initial_load_id="1783955745.5023258",
    )

    assert result == "loaded"
    assert calls["resource_name"] == "unl_fym_policy"
    assert calls["pipeline"] == {
        "pipeline_name": "unl_typed",
        "destination": "postgres",
        "dataset_name": "typed",
    }
    sql_table_kwargs = calls["sql_table"]
    assert sql_table_kwargs["schema"] == "raw"
    assert sql_table_kwargs["table"] == "unl_fym_policy_typed_source"
    assert sql_table_kwargs["backend"] == "pyarrow"
    assert sql_table_kwargs["chunk_size"] == 50_000
    assert sql_table_kwargs["primary_key"] == "raw_dlt_id"
    assert sql_table_kwargs["write_disposition"] == {
        "disposition": "merge",
        "strategy": "insert-only",
    }
    incremental = sql_table_kwargs["incremental"]
    assert incremental.cursor_path == "raw_dlt_load_id"
    assert incremental.initial_value == "1783955745.5023258"
    assert incremental.range_start == "open"


def test_unl_typed_select_preserves_required_dlt_columns() -> None:
    select_sql = _unl_fym_policy_typed_select()

    assert "p._dlt_load_id AS _dlt_load_id" in select_sql
    assert "p._dlt_id AS _dlt_id" in select_sql
    assert "p._dlt_load_id AS raw_dlt_load_id" in select_sql
    assert "p._dlt_id AS raw_dlt_id" in select_sql
    assert "at_risk_policy,\n    _dlt_load_id,\n    _dlt_id\nFROM enriched_rows" in select_sql


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


def test_ahl_route_accepts_csv_files_directly_under_inbound() -> None:
    assert _routes_for_provider("ahl") == [
        FileRoute(
            name="fym_policy",
            file_glob="ahl/inbound/*.csv",
            parser="csv",
            table_name="ahl_fym_policy",
            parser_options={"dtype": "string"},
        )
    ]


def test_ahl_typed_select_retains_source_fields_and_defers_tbd_rules() -> None:
    select_sql = ahl_fym_policy_typed_select()
    refresh_sql = "\n".join(ahl_typed_refresh_statements())

    assert len(AHL_TYPED_POLICY_COLUMNS) == 43
    for column in (
        "date_of_birth",
        "email",
        "resident_state",
        "rider_1_plan_code",
        "rider_5_plan_code",
        "writing_agent_number",
        "ga_number",
        "mga_1_number",
        "mga_2_number",
    ):
        assert column in AHL_TYPED_POLICY_COLUMNS
    assert "FROM raw.ahl_fym_policy AS p" in select_sql
    assert "_source_modified_at::date" in select_sql
    assert "false AS at_risk_policy" in select_sql
    assert "CREATE OR REPLACE VIEW raw.ahl_fym_policy_latest_load" in refresh_sql
    assert "CREATE OR REPLACE VIEW typed.ahl_fym_policy_latest_load" in refresh_sql
    assert "NULL::jsonb AS roster_hierarchy_json" in refresh_sql
    assert "TODO: add AHL roster hierarchy enrichment" in refresh_sql
    assert "TRUNCATE TABLE typed.ahl_fym_policy_at_risk_episodes" in refresh_sql
    assert "ON CONFLICT (_dlt_id) DO NOTHING" in refresh_sql


def test_csv_reader_attaches_source_modification_timestamp() -> None:
    class FakeFile(dict):
        def open(self) -> BytesIO:
            return BytesIO(b"Policy Number,At Risk\n8002016,\n")

    file_item = FakeFile(
        file_name="FYM Policy Data 8_6_26.csv",
        modification_date="2026-08-06T14:30:00+00:00",
    )

    chunks = list(s3._read_csv_with_file_errors(iter([file_item]), dtype="string"))

    assert chunks == [
        [
            {
                "Policy Number": "8002016",
                "At Risk": None,
                "_source_file": "FYM Policy Data 8_6_26.csv",
                "_source_modified_at": "2026-08-06T14:30:00+00:00",
            }
        ]
    ]


def test_refresh_typed_dataset_supports_ahl_without_roster_refresh(monkeypatch) -> None:
    calls: dict[str, object] = {"statements": []}

    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, statement: str) -> None:
            calls["statements"].append(statement)

        def fetchone(self) -> tuple[int]:
            return (1921,)

    class FakeConnection:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            calls["committed"] = True

        def rollback(self) -> None:
            calls["rolled_back"] = True

        def close(self) -> None:
            calls["closed"] = True

    monkeypatch.setattr("dlt_pipelines.db._connect", lambda: FakeConnection())

    result = refresh_typed_dataset("ahl")

    executed_sql = "\n".join(calls["statements"])
    assert result.provider == "ahl"
    assert result.table_name == "ahl_fym_policy"
    assert result.row_count == 1921
    assert "raw_roster" not in executed_sql
    assert calls["committed"] is True
    assert calls["closed"] is True


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
    calls = {"execute": [], "copy": [], "commit_at": []}

    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: str) -> None:
            calls["execute"].append(query)
            self.query = query

        def copy_expert(self, query: str, file) -> None:
            calls["copy"].append(query)

        def fetchone(self):
            if "to_regclass('typed.unl_fym_policy')" in self.query:
                return (True, True)
            if "max(coalesce(raw_dlt_load_id" in self.query:
                return ("1783955745.5023258",)
            return (42,)

    class FakeConnection:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            calls["commit_at"].append(len(calls["execute"]))

        def rollback(self) -> None:
            calls["rollback"] = True

        def close(self) -> None:
            calls["closed"] = True

    monkeypatch.setattr("dlt_pipelines.db._connect", lambda: FakeConnection())
    monkeypatch.setattr("dlt_pipelines.db._connect_roster_source", lambda: FakeConnection())
    monkeypatch.setattr(
        "dlt_pipelines.db._postgres_sqlalchemy_credentials",
        lambda: "postgresql+psycopg2://source",
    )
    typed_pipeline_calls = []
    monkeypatch.setattr(
        "dlt_pipelines.pipelines.typed.run_unl_fym_policy_typed_pipeline",
        lambda **kwargs: typed_pipeline_calls.append(kwargs),
    )

    result = refresh_typed_dataset("unl")

    executed_sql = [query for query in calls["execute"] if isinstance(query, str)]
    assert result.provider == "unl"
    assert result.schema_name == "typed"
    assert result.table_name == "unl_fym_policy"
    assert result.row_count == 42
    assert len(calls["commit_at"]) == 3
    assert typed_pipeline_calls == [
        {
            "source_credentials": "postgresql+psycopg2://source",
            "initial_load_id": "1783955745.5023258",
        }
    ]
    assert calls["closed"] is True
    assert len(calls["copy"]) == 12
    assert any(query.startswith("CREATE SCHEMA IF NOT EXISTS typed") for query in executed_sql)
    assert "TRUNCATE TABLE typed.unl_fym_policy" not in executed_sql
    assert not any(query.startswith("INSERT INTO typed.unl_fym_policy ") for query in executed_sql)
    assert any(
        query.startswith("CREATE OR REPLACE VIEW raw.unl_fym_policy_typed_source")
        for query in executed_sql
    )
    schema_commit_position = calls["commit_at"][1]
    first_truncate_position = executed_sql.index(
        "TRUNCATE TABLE typed.unl_weekly_advance_statements"
    )
    latest_view_positions = [
        index
        for index, query in enumerate(executed_sql)
        if query.startswith("CREATE OR REPLACE VIEW ")
    ]
    assert latest_view_positions
    assert max(latest_view_positions) < schema_commit_position <= first_truncate_position
    assert any(
        query.startswith("CREATE TABLE IF NOT EXISTS typed.unl_fym_policy_change_history")
        for query in executed_sql
    )
    history_column_migration_sql = next(
        query
        for query in executed_sql
        if "ALTER TABLE typed.unl_fym_policy_change_history" in query
        and "RENAME COLUMN at_risk_policy_last_change_date" in query
    )
    assert "TO at_risk_status_last_change_date" in history_column_migration_sql
    assert "ALTER VIEW raw.unl_fym_policy_latest_load" in history_column_migration_sql
    assert "ALTER VIEW typed.unl_fym_policy_latest_load" in history_column_migration_sql
    assert "TRUNCATE TABLE typed.unl_fym_policy_change_history" not in executed_sql
    history_reset_sql = next(
        query
        for query in executed_sql
        if query.startswith("WITH pending_rows AS")
        and "DELETE FROM typed.unl_fym_policy_change_history" in query
    )
    assert "out_of_order_policies AS" in history_reset_sql
    assert "first_pending_file_date <= processed.last_processed_file_date" in history_reset_sql
    history_insert_sql = next(
        query
        for query in executed_sql
        if query.startswith("INSERT INTO typed.unl_fym_policy_change_history")
    )
    assert "FROM typed.unl_fym_policy" in history_insert_sql
    assert "WITH pending_rows AS" in history_insert_sql
    assert "affected_policies AS" in history_insert_sql
    assert "prior_rows AS" in history_insert_sql
    assert "WHERE h._dlt_id IS NULL" in history_insert_sql
    assert "WHERE is_new" in history_insert_sql
    assert "ON CONFLICT (_dlt_id) DO NOTHING" in history_insert_sql
    assert "lag(cntrct_code) OVER history_window" in history_insert_sql
    assert "lag(at_risk_policy) OVER history_window" in history_insert_sql
    assert "array_agg(contract_event_previous_value) FILTER" in history_insert_sql
    assert "cntrct_code IS DISTINCT FROM previous_contract_observation" in history_insert_sql
    assert "array_agg(at_risk_event_previous_value) FILTER" in history_insert_sql
    assert "at_risk_policy IS DISTINCT FROM previous_at_risk_observation" in history_insert_sql
    assert "at_risk_status_last_change_date" in history_insert_sql
    assert any(
        query.startswith(
            "CREATE TABLE IF NOT EXISTS typed.unl_fym_policy_roster_hierarchy"
        )
        for query in executed_sql
    )
    roster_hierarchy_refresh_sql = next(
        query
        for query in executed_sql
        if query.startswith("CREATE TEMP TABLE unl_fym_policy_roster_hierarchy_refresh")
    )
    assert "WITH RECURSIVE latest_file AS" in roster_hierarchy_refresh_sql
    assert "coalesce(p.raw_dlt_id, p._dlt_id) AS source_dlt_id" in (
        roster_hierarchy_refresh_sql
    )
    assert "nullif(trim(levels.writing_number::text), '') AS writing_number" in (
        roster_hierarchy_refresh_sql
    )
    assert "trim(agency_carrier.writing_number) = levels.writing_number" in (
        roster_hierarchy_refresh_sql
    )
    assert "trim(agent_carrier.writing_number) = levels.writing_number" in (
        roster_hierarchy_refresh_sql
    )
    assert "ORDER BY traversal_depth DESC" in roster_hierarchy_refresh_sql
    assert "lpad(hierarchy_level::text, 2, '0')" in roster_hierarchy_refresh_sql
    roster_hierarchy_upsert_sql = next(
        query
        for query in executed_sql
        if query.startswith("INSERT INTO typed.unl_fym_policy_roster_hierarchy")
    )
    assert "ON CONFLICT (_dlt_id) DO UPDATE" in roster_hierarchy_upsert_sql
    assert "roster_hierarchy_json = EXCLUDED.roster_hierarchy_json" in (
        roster_hierarchy_upsert_sql
    )
    assert any(
        query.startswith("DELETE FROM typed.unl_fym_policy_roster_hierarchy")
        and "pg_temp.unl_fym_policy_roster_hierarchy_refresh" in query
        for query in executed_sql
    )
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
    assert any(
        query.startswith("CREATE TABLE IF NOT EXISTS typed.unl_fym_policy_at_risk_episodes")
        for query in executed_sql
    )
    assert "TRUNCATE TABLE typed.unl_fym_policy_at_risk_episodes" in executed_sql
    episode_insert_sql = next(
        query
        for query in executed_sql
        if query.startswith("INSERT INTO typed.unl_fym_policy_at_risk_episodes")
    )
    assert "FROM typed.unl_fym_policy" in episode_insert_sql
    assert "WHERE file_date >= DATE '2026-05-14'" in episode_insert_sql
    assert "AND at_risk_policy = true" in episode_insert_sql
    assert "d.file_date > e.ep_end" in episode_insert_sql
    assert "d.file_date <= e.ep_end + 7" in episode_insert_sql
    assert "left_censored" in episode_insert_sql
    assert "ended_no_resolution" in episode_insert_sql
    weekly_advance_insert_sql = next(
        query
        for query in executed_sql
        if query.startswith("INSERT INTO typed.unl_weekly_advance_statements")
    )
    assert "to_date(effective_date_text, 'YYYY-MM-DD')" in weekly_advance_insert_sql
    assert "to_date(effective_date_text, 'YYYYMMDD')" in weekly_advance_insert_sql
    assert "to_date(effective_date_text, 'MM/DD/YYYY')" in weekly_advance_insert_sql
    assert "to_date(effective_date_text, 'MM-DD-YYYY')" in weekly_advance_insert_sql
    assert "to_date(paid_to_date_text, 'MM/DD/YYYY')" in weekly_advance_insert_sql
    assert "to_date(last_activity_date_text, 'MM-DD-YYYY')" in weekly_advance_insert_sql
    latest_load_sql_by_schema = {
        query.split(".")[0].removeprefix("CREATE OR REPLACE VIEW "): query
        for query in executed_sql
        if query.startswith("CREATE OR REPLACE VIEW ")
        and "unl_fym_policy_latest_load" in query
    }
    assert sorted(latest_load_sql_by_schema) == ["raw", "typed"]
    for schema_name, latest_load_sql in latest_load_sql_by_schema.items():
        assert f"CREATE OR REPLACE VIEW {schema_name}.unl_fym_policy_latest_load" in latest_load_sql
        assert f"FROM {schema_name}.unl_fym_policy AS p" in latest_load_sql
        assert "roster_hierarchy_json" in latest_load_sql
        assert "WITH RECURSIVE" not in latest_load_sql
        assert "LEFT JOIN typed.unl_fym_policy_roster_hierarchy" in latest_load_sql
        assert "LEFT JOIN typed.unl_fym_policy_change_history AS history" in latest_load_sql
        assert "history.previous_contract_code" in latest_load_sql
        assert "history.contract_code_last_change_date" in latest_load_sql
        assert "history.previous_at_risk_status" in latest_load_sql
        assert "history.at_risk_status_last_change_date" in latest_load_sql
        assert latest_load_sql.rindex("policy_roster_hierarchy.roster_hierarchy_json") < (
            latest_load_sql.rindex("history.previous_contract_code")
        )
        assert "'unl'::text AS carrier" in latest_load_sql
        assert latest_load_sql.rindex("'unl'::text AS carrier") < (
            latest_load_sql.rindex("history.previous_contract_code")
        )
        expected_join_id = "p._dlt_id"
        if schema_name == "typed":
            expected_join_id = "coalesce(p.raw_dlt_id, p._dlt_id)"
        assert f"policy_roster_hierarchy._dlt_id = {expected_join_id}" in latest_load_sql
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
        "ON typed.unl_fym_policy_change_history (_dlt_id)" in query for query in executed_sql
    )
    assert any(
        "ON typed.unl_weekly_advance_statements (file_date)" in query for query in executed_sql
    )
    assert any(
        "ON typed.unl_fym_policy (policy_nbr, file_date) INCLUDE (paid_to_date)"
        in query
        for query in executed_sql
    )
    assert any(
        "ON typed.unl_fym_policy (policy_nbr, file_date) "
        "INCLUDE (paid_to_date, cntrct_code)" in query
        for query in executed_sql
    )
    assert any(
        "ON typed.unl_fym_policy_at_risk_episodes (policy_nbr, episode_id)" in query
        for query in executed_sql
    )
    assert any(
        "ON typed.unl_fym_policy_at_risk_episodes (outcome)" in query
        for query in executed_sql
    )
    assert "GRANT SELECT ON raw.unl_fym_policy_latest_load TO unl_fym_policy_reader" in executed_sql
    assert (
        "GRANT SELECT ON raw.unl_weekly_advance_statements_latest_load TO unl_fym_policy_reader"
        in executed_sql
    )
    assert "GRANT CONNECT ON DATABASE fym_prod TO unl_fym_policy_reader" in executed_sql
    assert "GRANT USAGE ON SCHEMA public TO unl_fym_policy_reader" in executed_sql
    assert "GRANT SELECT ON ALL TABLES IN SCHEMA public TO unl_fym_policy_reader" in executed_sql
    assert (
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        "GRANT SELECT ON TABLES TO unl_fym_policy_reader"
    ) in executed_sql
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


def test_ahl_archive_plan_only_matches_direct_inbound_csv_files(monkeypatch) -> None:
    class FakeS3Fs:
        def glob(self, pattern: str) -> list[str]:
            assert pattern == "landing-bucket/ahl/inbound/*.csv"
            return ["landing-bucket/ahl/inbound/FYM Policy Data 8_6_26.csv"]

        def isdir(self, path: str) -> bool:
            return False

    monkeypatch.setattr(
        "dlt_pipelines.transfers.fsspec.core.url_to_fs",
        lambda url, **kwargs: (FakeS3Fs(), "landing-bucket"),
    )
    monkeypatch.setenv(
        "SFTP_PROVIDERS__AHL__S3__LANDING__BUCKET_URL",
        "s3://landing-bucket",
    )

    assert plan_landed_files_archive("ahl") == [
        ArchivePlanItem(
            source_path="landing-bucket/ahl/inbound/FYM Policy Data 8_6_26.csv",
            archive_path=(
                "landing-bucket/ahl/inbound/Archive/FYM Policy Data 8_6_26.csv"
            ),
        )
    ]
