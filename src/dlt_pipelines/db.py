"""Database connectivity checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import PurePosixPath
from urllib.parse import quote_plus

import psycopg2
from psycopg2 import sql

from dlt_pipelines.config import get_setting


@dataclass(frozen=True)
class DatabaseCheckResult:
    database: str
    user: str
    server_version: str
    schemas: tuple["SchemaCheckResult", ...]


@dataclass(frozen=True)
class SchemaCheckResult:
    name: str
    exists: bool
    has_usage: bool
    has_create: bool


@dataclass(frozen=True)
class FileAuditEvent:
    provider: str
    source_path: str
    target_path: str
    status: str
    file_size_bytes: int | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class ExpectedFileCheckResult:
    provider: str
    file_type: str
    file_pattern: str
    found: bool
    current_time: datetime
    max_age: timedelta
    file_name: str | None = None
    landed_at: datetime | None = None
    age: timedelta | None = None
    status: str | None = None

    @property
    def is_recent(self) -> bool:
        return self.found and self.age is not None and self.age <= self.max_age


@dataclass(frozen=True)
class TypedRefreshResult:
    provider: str
    schema_name: str
    table_name: str
    row_count: int


@dataclass(frozen=True)
class RosterTableSnapshot:
    table_name: str
    columns: tuple[tuple[str, str], ...]


ROSTER_SNAPSHOT_SCHEMA = "raw_roster"
ROSTER_TABLE_SNAPSHOTS = (
    RosterTableSnapshot(
        table_name="roster_agent",
        columns=(
            ("id", "text"),
            ("first_name", "text"),
            ("last_name", "text"),
            ("national_producer_number", "text"),
        ),
    ),
    RosterTableSnapshot(
        table_name="roster_agency",
        columns=(
            ("id", "text"),
            ("name", "text"),
            ("is_person", "boolean"),
        ),
    ),
    RosterTableSnapshot(
        table_name="roster_carrier",
        columns=(
            ("id", "text"),
            ("name", "text"),
        ),
    ),
    RosterTableSnapshot(
        table_name="roster_agentcarrier",
        columns=(
            ("id", "text"),
            ("agent_id", "text"),
            ("carrier_id", "text"),
            ("writing_number", "text"),
            ("status", "text"),
            ("start", "date"),
            ("end", "date"),
        ),
    ),
    RosterTableSnapshot(
        table_name="roster_agencycarrier",
        columns=(
            ("id", "text"),
            ("agency_id", "text"),
            ("carrier_id", "text"),
            ("writing_number", "text"),
            ("parent_id", "text"),
            ("status", "text"),
            ("start", "date"),
            ("end", "date"),
        ),
    ),
    RosterTableSnapshot(
        table_name="roster_agentagency",
        columns=(
            ("id", "text"),
            ("agent_id", "text"),
            ("agency_carrier_id", "text"),
            ("status", "text"),
            ("start", "date"),
            ("end", "date"),
            ("created", "timestamp"),
        ),
    ),
)

UNL_TYPED_POLICY_VIEW_COLUMNS = (
    "mga",
    "mga_name",
    "ga",
    "ga_name",
    "wa",
    "wa_name",
    "agent_ga_level_01",
    "agent_level_02",
    "agent_level_03",
    "agent_level_04",
    "agent_level_05",
    "agent_level_06",
    "agent_level_07",
    "agent_level_08",
    "agent_level_09",
    "agent_level_10",
    "plan_code",
    "issue_date",
    "cntrct_code",
    "app_recvd_date",
    "annual_premium",
    "issue_state",
    "policy_nbr",
    "paid_to_date",
    "billing_mode",
    "first_name",
    "last_name",
    "zip",
    "phone_nbr",
    "_source_file",
    "_dlt_load_id",
    "_dlt_id",
    "cntrct_reason",
    "cntrct_date",
    "billing_form",
    "term_date",
    "file_date",
    "at_risk_policy",
)


def check_postgres_connection() -> DatabaseCheckResult:
    """Connect to PostgreSQL with DLT destination credentials and run checks."""
    connection = _connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT current_database(), current_user, version()
                """
            )
            database, user, server_version = cursor.fetchone()
            cursor.execute(
                """
                SELECT
                    schema_name,
                    EXISTS (
                        SELECT 1
                        FROM pg_namespace
                        WHERE nspname = schema_name
                    ) AS schema_exists,
                    has_schema_privilege(current_user, schema_name, 'USAGE') AS has_usage,
                    has_schema_privilege(current_user, schema_name, 'CREATE') AS has_create
                FROM (VALUES ('audit'), ('raw')) AS expected_schemas(schema_name)
                ORDER BY schema_name
                """
            )
            schemas = tuple(
                SchemaCheckResult(
                    name=row[0],
                    exists=row[1],
                    has_usage=row[2],
                    has_create=row[3],
                )
                for row in cursor.fetchall()
            )
    finally:
        connection.close()

    return DatabaseCheckResult(
        database=database,
        user=user,
        server_version=server_version,
        schemas=schemas,
    )


def record_file_events(events: list[FileAuditEvent]) -> int:
    if not events:
        return 0

    connection = _connect()
    try:
        with connection.cursor() as cursor:
            for event in events:
                cursor.execute(
                    """
                    INSERT INTO audit.file_landings (
                        provider,
                        source_path,
                        target_path,
                        file_name,
                        file_size_bytes,
                        status,
                        error_message
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (provider, source_path, target_path)
                    DO UPDATE SET
                        file_size_bytes = COALESCE(EXCLUDED.file_size_bytes, audit.file_landings.file_size_bytes),
                        status = EXCLUDED.status,
                        error_message = EXCLUDED.error_message,
                        landed_at = now()
                    """,
                    (
                        event.provider,
                        event.source_path,
                        event.target_path,
                        PurePosixPath(event.target_path).name,
                        event.file_size_bytes,
                        event.status,
                        event.error_message,
                    ),
                )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    return len(events)


def check_unl_fym_policy_loaded(
    *,
    max_age: timedelta,
    current_time: datetime | None = None,
) -> ExpectedFileCheckResult:
    file_pattern = r"FYM\_Policy\_%.csv"
    checked_at = current_time or datetime.now(timezone.utc)
    if checked_at.tzinfo is None:
        checked_at = checked_at.replace(tzinfo=timezone.utc)

    connection = _connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT file_name, landed_at, status
                FROM audit.file_landings
                WHERE provider = 'unl'
                  AND status = 'loaded_to_postgres'
                  AND file_name LIKE %s
                  ESCAPE '\\'
                ORDER BY landed_at DESC, file_name DESC
                LIMIT 1
                """,
                (file_pattern,),
            )
            row = cursor.fetchone()
    finally:
        connection.close()

    if row is None:
        return ExpectedFileCheckResult(
            provider="unl",
            file_type="fym_policy",
            file_pattern=file_pattern,
            found=False,
            current_time=checked_at,
            max_age=max_age,
        )

    landed_at = row[1]
    if landed_at.tzinfo is None:
        landed_at = landed_at.replace(tzinfo=timezone.utc)

    return ExpectedFileCheckResult(
        provider="unl",
        file_type="fym_policy",
        file_pattern=file_pattern,
        found=True,
        current_time=checked_at,
        max_age=max_age,
        file_name=row[0],
        landed_at=landed_at,
        age=checked_at - landed_at,
        status=row[2],
    )


def refresh_typed_dataset(provider: str) -> TypedRefreshResult:
    if provider == "heartland":
        return _refresh_heartland_typed_dataset()
    if provider != "unl":
        raise RuntimeError(f"Typed refresh is not configured for provider '{provider}'.")

    connection = _connect()
    try:
        with connection.cursor() as cursor:
            for statement in _unl_typed_preparation_statements():
                cursor.execute(statement)
            initial_load_id = _typed_policy_initial_load_id(cursor)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    from dlt_pipelines.pipelines.typed import run_unl_fym_policy_typed_pipeline

    run_unl_fym_policy_typed_pipeline(
        source_credentials=_postgres_sqlalchemy_credentials(),
        initial_load_id=initial_load_id,
    )

    connection = _connect()
    try:
        with connection.cursor() as cursor:
            for statement in _typed_schema_and_view_statements():
                cursor.execute(statement)
        connection.commit()

        with connection.cursor() as cursor:
            _refresh_roster_snapshots(connection)
            for statement in _typed_refresh_statements():
                cursor.execute(statement)
            cursor.execute("SELECT count(*) FROM typed.unl_fym_policy")
            row_count = int(cursor.fetchone()[0])
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    return TypedRefreshResult(
        provider=provider,
        schema_name="typed",
        table_name="unl_fym_policy",
        row_count=row_count,
    )


def _refresh_heartland_typed_dataset() -> TypedRefreshResult:
    """Append unseen Heartland row versions to canonical raw and typed history."""
    connection = _connect()
    try:
        with connection.cursor() as cursor:
            for statement in _heartland_typed_refresh_statements():
                cursor.execute(statement)
            cursor.execute("SELECT count(*) FROM typed.heartland_inforced_policy")
            row_count = int(cursor.fetchone()[0])
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    return TypedRefreshResult(
        provider="heartland",
        schema_name="typed",
        table_name="heartland_inforced_policy",
        row_count=row_count,
    )


def _connect():
    return psycopg2.connect(
        dbname=_postgres_setting("database"),
        user=_postgres_setting("username"),
        password=_postgres_setting("password"),
        host=_postgres_setting("host"),
        port=_postgres_setting("port"),
        connect_timeout=int(_postgres_setting("connect_timeout", default="15")),
    )


def _postgres_sqlalchemy_credentials() -> str:
    username = quote_plus(_postgres_setting("username"))
    password = quote_plus(_postgres_setting("password"))
    host = _postgres_setting("host")
    port = _postgres_setting("port")
    database = _postgres_setting("database")
    return f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"


def _connect_roster_source():
    return psycopg2.connect(
        dbname=_roster_postgres_setting("database", default="fym_prod"),
        user=_roster_postgres_setting("username", default=_postgres_setting("username")),
        password=_roster_postgres_setting("password", default=_postgres_setting("password")),
        host=_roster_postgres_setting("host", default=_postgres_setting("host")),
        port=_roster_postgres_setting("port", default=_postgres_setting("port")),
        connect_timeout=int(
            _roster_postgres_setting(
                "connect_timeout",
                default=_postgres_setting("connect_timeout", default="15"),
            )
        ),
    )


def _postgres_setting(name: str, *, default: str | None = None) -> str:
    value = get_setting(
        f"DESTINATION__POSTGRES__CREDENTIALS__{name.upper()}",
        ("destination", "postgres", "credentials", name),
        default=default,
        required=default is None,
    )
    if value is None:
        raise RuntimeError(f"Missing PostgreSQL setting: {name}")
    return value


def _roster_postgres_setting(name: str, *, default: str | None = None) -> str:
    value = get_setting(
        f"ROSTER__POSTGRES__CREDENTIALS__{name.upper()}",
        ("roster", "postgres", "credentials", name),
        default=default,
        required=default is None,
    )
    if value is None:
        raise RuntimeError(f"Missing roster PostgreSQL setting: {name}")
    return value


def _roster_source_schema() -> str:
    return get_setting(
        "ROSTER__POSTGRES__SCHEMA",
        ("roster", "postgres", "schema"),
        default="public",
    ) or "public"


def _refresh_roster_snapshots(destination_connection) -> None:
    source_connection = _connect_roster_source()
    try:
        with destination_connection.cursor() as destination_cursor:
            destination_cursor.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(ROSTER_SNAPSHOT_SCHEMA)
                )
            )
            for table in ROSTER_TABLE_SNAPSHOTS:
                destination_cursor.execute(_create_roster_snapshot_table_sql(table))
                destination_cursor.execute(
                    sql.SQL("TRUNCATE TABLE {}.{}").format(
                        sql.Identifier(ROSTER_SNAPSHOT_SCHEMA),
                        sql.Identifier(table.table_name),
                    )
                )

        source_schema = _roster_source_schema()
        with source_connection.cursor() as source_cursor, destination_connection.cursor() as destination_cursor:
            for table in ROSTER_TABLE_SNAPSHOTS:
                buffer = StringIO()
                source_cursor.copy_expert(
                    _copy_roster_source_sql(source_schema, table),
                    buffer,
                )
                buffer.seek(0)
                destination_cursor.copy_expert(
                    _copy_roster_destination_sql(table),
                    buffer,
                )
            for statement in _roster_snapshot_index_statements():
                destination_cursor.execute(statement)
    finally:
        source_connection.close()


def _create_roster_snapshot_table_sql(table: RosterTableSnapshot):
    column_definitions = sql.SQL(", ").join(
        sql.SQL("{} {}").format(sql.Identifier(column_name), sql.SQL(column_type))
        for column_name, column_type in table.columns
    )
    return sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
        sql.Identifier(ROSTER_SNAPSHOT_SCHEMA),
        sql.Identifier(table.table_name),
        column_definitions,
    )


def _copy_roster_source_sql(source_schema: str, table: RosterTableSnapshot):
    selected_columns = sql.SQL(", ").join(
        sql.SQL("{}::text").format(sql.Identifier(column_name))
        if column_type != "boolean"
        else sql.Identifier(column_name)
        for column_name, column_type in table.columns
    )
    return sql.SQL("COPY (SELECT {} FROM {}.{}) TO STDOUT WITH CSV").format(
        selected_columns,
        sql.Identifier(source_schema),
        sql.Identifier(table.table_name),
    )


def _copy_roster_destination_sql(table: RosterTableSnapshot):
    columns = sql.SQL(", ").join(sql.Identifier(column_name) for column_name, _ in table.columns)
    return sql.SQL("COPY {}.{} ({}) FROM STDIN WITH CSV").format(
        sql.Identifier(ROSTER_SNAPSHOT_SCHEMA),
        sql.Identifier(table.table_name),
        columns,
    )


def _roster_snapshot_index_statements() -> tuple[str, ...]:
    return (
        (
            "CREATE INDEX IF NOT EXISTS raw_roster_agentcarrier_carrier_writing_number_idx "
            "ON raw_roster.roster_agentcarrier (carrier_id, writing_number)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS raw_roster_agentcarrier_agent_idx "
            "ON raw_roster.roster_agentcarrier (agent_id)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS raw_roster_agencycarrier_carrier_writing_number_idx "
            "ON raw_roster.roster_agencycarrier (carrier_id, writing_number)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS raw_roster_agencycarrier_id_idx "
            "ON raw_roster.roster_agencycarrier (id)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS raw_roster_agencycarrier_parent_idx "
            "ON raw_roster.roster_agencycarrier (parent_id)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS raw_roster_agentagency_agent_idx "
            "ON raw_roster.roster_agentagency (agent_id)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS raw_roster_agentagency_agency_carrier_idx "
            "ON raw_roster.roster_agentagency (agency_carrier_id)"
        ),
        "ANALYZE raw_roster.roster_agent",
        "ANALYZE raw_roster.roster_agency",
        "ANALYZE raw_roster.roster_carrier",
        "ANALYZE raw_roster.roster_agentcarrier",
        "ANALYZE raw_roster.roster_agencycarrier",
        "ANALYZE raw_roster.roster_agentagency",
    )


def _typed_refresh_statements() -> tuple[str, ...]:
    weekly_advance_typed_select = _unl_weekly_advance_statements_typed_select()
    at_risk_episode_select = _unl_fym_policy_at_risk_episode_select()
    roster_hierarchy_select = _unl_fym_policy_roster_hierarchy_select()
    return (
        _unl_fym_policy_out_of_order_history_reset_statement(),
        _unl_fym_policy_incremental_history_insert_statement(),
        (
            "CREATE TEMP TABLE unl_fym_policy_roster_hierarchy_refresh "
            f"ON COMMIT DROP AS {roster_hierarchy_select}"
        ),
        """INSERT INTO typed.unl_fym_policy_roster_hierarchy (
            _dlt_id,
            roster_hierarchy_json
        )
        SELECT
            _dlt_id,
            roster_hierarchy_json
        FROM pg_temp.unl_fym_policy_roster_hierarchy_refresh
        WHERE true
        ON CONFLICT (_dlt_id) DO UPDATE
        SET roster_hierarchy_json = EXCLUDED.roster_hierarchy_json
        """,
        """DELETE FROM typed.unl_fym_policy_roster_hierarchy AS existing
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_temp.unl_fym_policy_roster_hierarchy_refresh AS refreshed
            WHERE refreshed._dlt_id = existing._dlt_id
        )
        """,
        "TRUNCATE TABLE typed.unl_weekly_advance_statements",
        f"INSERT INTO typed.unl_weekly_advance_statements {weekly_advance_typed_select}",
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS unl_fym_policy_typed_dlt_id_idx "
            "ON typed.unl_fym_policy (_dlt_id)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS unl_fym_policy_typed_source_file_idx "
            "ON typed.unl_fym_policy (_source_file)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS unl_fym_policy_typed_file_date_idx "
            "ON typed.unl_fym_policy (file_date)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS unl_fym_policy_typed_policy_nbr_idx "
            "ON typed.unl_fym_policy (policy_nbr)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS unl_fym_policy_typed_paid_to_date_idx "
            "ON typed.unl_fym_policy (paid_to_date)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS unl_fym_policy_typed_issue_date_idx "
            "ON typed.unl_fym_policy (issue_date)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS unl_fym_policy_typed_app_recvd_date_idx "
            "ON typed.unl_fym_policy (app_recvd_date)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS unl_fym_policy_typed_at_risk_idx "
            "ON typed.unl_fym_policy (at_risk_policy) "
            "WHERE at_risk_policy = true"
        ),
        (
            "CREATE INDEX IF NOT EXISTS idx_unl_fym_policy_at_risk_episode "
            "ON typed.unl_fym_policy (policy_nbr, file_date) "
            "INCLUDE (paid_to_date) "
            "WHERE billing_mode = 3 "
            "AND billing_form = 'DIR' "
            "AND cntrct_code = 'A' "
            "AND paid_to_date IS NOT NULL"
        ),
        (
            "CREATE INDEX IF NOT EXISTS idx_unl_fym_policy_policy_file_outcome "
            "ON typed.unl_fym_policy (policy_nbr, file_date) "
            "INCLUDE (paid_to_date, cntrct_code)"
        ),
        (
            "CREATE TABLE IF NOT EXISTS typed.unl_fym_policy_at_risk_episodes AS "
            f"{at_risk_episode_select} WITH NO DATA"
        ),
        "TRUNCATE TABLE typed.unl_fym_policy_at_risk_episodes",
        f"INSERT INTO typed.unl_fym_policy_at_risk_episodes {at_risk_episode_select}",
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS unl_fym_policy_at_risk_episodes_pk "
            "ON typed.unl_fym_policy_at_risk_episodes (policy_nbr, episode_id)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS unl_fym_policy_at_risk_episodes_policy_idx "
            "ON typed.unl_fym_policy_at_risk_episodes (policy_nbr)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS unl_fym_policy_at_risk_episodes_outcome_idx "
            "ON typed.unl_fym_policy_at_risk_episodes (outcome)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS unl_fym_policy_at_risk_episodes_start_idx "
            "ON typed.unl_fym_policy_at_risk_episodes (ep_start)"
        ),
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS unl_weekly_advance_statements_typed_dlt_id_idx "
            "ON typed.unl_weekly_advance_statements (_dlt_id)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS unl_weekly_advance_statements_typed_source_file_idx "
            "ON typed.unl_weekly_advance_statements (_source_file)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS unl_weekly_advance_statements_typed_file_date_idx "
            "ON typed.unl_weekly_advance_statements (file_date)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS unl_weekly_advance_statements_typed_policy_number_idx "
            "ON typed.unl_weekly_advance_statements (policy_number)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS unl_weekly_advance_statements_typed_agent_number_idx "
            "ON typed.unl_weekly_advance_statements (agent_number)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS unl_weekly_advance_statements_typed_effective_date_idx "
            "ON typed.unl_weekly_advance_statements (effective_date)"
        ),
        "GRANT CONNECT ON DATABASE fym_prod TO unl_fym_policy_reader",
        "GRANT USAGE ON SCHEMA public TO unl_fym_policy_reader",
        "GRANT SELECT ON ALL TABLES IN SCHEMA public TO unl_fym_policy_reader",
        (
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
            "GRANT SELECT ON TABLES TO unl_fym_policy_reader"
        ),
        "GRANT USAGE ON SCHEMA raw TO unl_fym_policy_reader",
        "GRANT SELECT ON raw.unl_fym_policy_latest_load TO unl_fym_policy_reader",
        "GRANT SELECT ON raw.unl_weekly_advance_statements_latest_load TO unl_fym_policy_reader",
        "GRANT USAGE ON SCHEMA typed TO unl_fym_policy_reader",
        "GRANT SELECT ON ALL TABLES IN SCHEMA typed TO unl_fym_policy_reader",
        (
            "ALTER DEFAULT PRIVILEGES IN SCHEMA typed "
            "GRANT SELECT ON TABLES TO unl_fym_policy_reader"
        ),
        "ANALYZE typed.unl_fym_policy",
        "ANALYZE typed.unl_fym_policy_change_history",
        "ANALYZE typed.unl_fym_policy_roster_hierarchy",
        "ANALYZE typed.unl_fym_policy_at_risk_episodes",
        "ANALYZE typed.unl_weekly_advance_statements",
    )


def _typed_schema_and_view_statements() -> tuple[str, ...]:
    fym_policy_typed_select = _unl_fym_policy_typed_select()
    fym_policy_change_history_select = _unl_fym_policy_change_history_select()
    weekly_advance_typed_select = _unl_weekly_advance_statements_typed_select()
    return (
        "CREATE SCHEMA IF NOT EXISTS typed",
        f"CREATE TABLE IF NOT EXISTS typed.unl_fym_policy AS {fym_policy_typed_select} WITH NO DATA",
        "ALTER TABLE typed.unl_fym_policy ADD COLUMN IF NOT EXISTS _dlt_load_id text",
        "ALTER TABLE typed.unl_fym_policy ADD COLUMN IF NOT EXISTS _dlt_id text",
        "ALTER TABLE typed.unl_fym_policy ADD COLUMN IF NOT EXISTS raw_dlt_id text",
        (
            "ALTER TABLE typed.unl_fym_policy "
            "ADD COLUMN IF NOT EXISTS raw_dlt_load_id text"
        ),
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS unl_fym_policy_typed_raw_dlt_id_idx "
            "ON typed.unl_fym_policy (raw_dlt_id) WHERE raw_dlt_id IS NOT NULL"
        ),
        (
            "CREATE TABLE IF NOT EXISTS typed.unl_fym_policy_change_history AS "
            f"{fym_policy_change_history_select} WITH NO DATA"
        ),
        """CREATE TABLE IF NOT EXISTS typed.unl_fym_policy_roster_hierarchy (
            _dlt_id text PRIMARY KEY,
            roster_hierarchy_json jsonb
        )""",
        _at_risk_history_column_migration_statement(),
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS unl_fym_policy_change_history_dlt_id_idx "
            "ON typed.unl_fym_policy_change_history (_dlt_id)"
        ),
        (
            "CREATE TABLE IF NOT EXISTS typed.unl_weekly_advance_statements AS "
            f"{weekly_advance_typed_select} WITH NO DATA"
        ),
        _unl_fym_policy_latest_load_view_statement("raw"),
        _unl_fym_policy_latest_load_view_statement("typed"),
        _unl_weekly_advance_latest_load_view_statement("raw"),
        _unl_weekly_advance_latest_load_view_statement("typed"),
    )


def _at_risk_history_column_migration_statement() -> str:
    return """
    DO $migration$
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'typed'
              AND table_name = 'unl_fym_policy_change_history'
              AND column_name = 'at_risk_policy_last_change_date'
        ) AND NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'typed'
              AND table_name = 'unl_fym_policy_change_history'
              AND column_name = 'at_risk_status_last_change_date'
        ) THEN
            ALTER TABLE typed.unl_fym_policy_change_history
                RENAME COLUMN at_risk_policy_last_change_date
                TO at_risk_status_last_change_date;
        END IF;

        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'raw'
              AND table_name = 'unl_fym_policy_latest_load'
              AND column_name = 'at_risk_policy_last_change_date'
        ) THEN
            ALTER VIEW raw.unl_fym_policy_latest_load
                RENAME COLUMN at_risk_policy_last_change_date
                TO at_risk_status_last_change_date;
        END IF;

        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'typed'
              AND table_name = 'unl_fym_policy_latest_load'
              AND column_name = 'at_risk_policy_last_change_date'
        ) THEN
            ALTER VIEW typed.unl_fym_policy_latest_load
                RENAME COLUMN at_risk_policy_last_change_date
                TO at_risk_status_last_change_date;
        END IF;
    END
    $migration$
    """


def _typed_policy_initial_load_id(cursor) -> str | None:
    cursor.execute(
        """
        SELECT
            to_regclass('typed.unl_fym_policy') IS NOT NULL,
            EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'typed'
                  AND table_name = 'unl_fym_policy'
                  AND column_name = 'raw_dlt_load_id'
            )
        """
    )
    table_exists, has_raw_load_id = cursor.fetchone()
    if not table_exists:
        return None
    if has_raw_load_id:
        cursor.execute(
            "SELECT max(coalesce(raw_dlt_load_id, _dlt_load_id)) "
            "FROM typed.unl_fym_policy"
        )
    else:
        cursor.execute("SELECT max(_dlt_load_id) FROM typed.unl_fym_policy")
    value = cursor.fetchone()[0]
    return None if value is None else str(value)


def _unl_fym_policy_typed_source_view_statement() -> str:
    return (
        "CREATE OR REPLACE VIEW raw.unl_fym_policy_typed_source AS "
        f"{_unl_fym_policy_typed_select()}"
    )


def _unl_typed_preparation_statements() -> tuple[str, ...]:
    return (
        (
            "CREATE INDEX IF NOT EXISTS unl_fym_policy_raw_dlt_load_id_idx "
            "ON raw.unl_fym_policy (_dlt_load_id)"
        ),
        _unl_fym_policy_typed_source_view_statement(),
        """
        DO $migration$
        BEGIN
            IF to_regclass('typed.unl_fym_policy') IS NOT NULL THEN
                ALTER TABLE typed.unl_fym_policy
                    ADD COLUMN IF NOT EXISTS raw_dlt_id text;
                ALTER TABLE typed.unl_fym_policy
                    ADD COLUMN IF NOT EXISTS raw_dlt_load_id text;
                CREATE UNIQUE INDEX IF NOT EXISTS unl_fym_policy_typed_raw_dlt_id_idx
                    ON typed.unl_fym_policy (raw_dlt_id)
                    WHERE raw_dlt_id IS NOT NULL;
            END IF;
        END
        $migration$
        """,
    )


def _unl_fym_policy_roster_hierarchy_select() -> str:
    return """WITH RECURSIVE latest_file AS (
    SELECT fl.file_name
    FROM audit.file_landings AS fl
    WHERE fl.provider = 'unl'
      AND fl.status = 'loaded_to_postgres'
      AND fl.file_name LIKE 'FYM_Policy_%.csv'
    ORDER BY fl.landed_at DESC, fl.file_name DESC
    LIMIT 1
),
latest_policy AS (
    SELECT
        coalesce(p.raw_dlt_id, p._dlt_id) AS source_dlt_id,
        p.agent_ga_level_01,
        p.agent_level_02,
        p.agent_level_03,
        p.agent_level_04,
        p.agent_level_05,
        p.agent_level_06,
        p.agent_level_07,
        p.agent_level_08,
        p.agent_level_09,
        p.agent_level_10
    FROM typed.unl_fym_policy AS p
    JOIN latest_file AS lf
      ON lf.file_name = p._source_file
),
policy_levels AS (
    SELECT
        p.source_dlt_id AS _dlt_id,
        levels.level_number,
        nullif(trim(levels.writing_number::text), '') AS writing_number
    FROM latest_policy AS p
    CROSS JOIN LATERAL (
        VALUES
            (1, p.agent_ga_level_01),
            (2, p.agent_level_02),
            (3, p.agent_level_03),
            (4, p.agent_level_04),
            (5, p.agent_level_05),
            (6, p.agent_level_06),
            (7, p.agent_level_07),
            (8, p.agent_level_08),
            (9, p.agent_level_09),
            (10, p.agent_level_10)
    ) AS levels(level_number, writing_number)
    WHERE nullif(trim(levels.writing_number::text), '') IS NOT NULL
),
policy_agency_matches AS (
    SELECT
        levels._dlt_id,
        levels.level_number,
        agency_carrier.id AS agency_carrier_id
    FROM policy_levels AS levels
    JOIN raw_roster.roster_carrier AS carrier
      ON carrier.name ILIKE '%unl%'
    JOIN raw_roster.roster_agencycarrier AS agency_carrier
      ON agency_carrier.carrier_id = carrier.id
     AND trim(agency_carrier.writing_number) = levels.writing_number
),
policy_agent_matches AS (
    SELECT DISTINCT ON (levels._dlt_id, levels.level_number)
        levels._dlt_id,
        levels.level_number,
        agency_carrier.id AS agency_carrier_id
    FROM policy_levels AS levels
    JOIN raw_roster.roster_carrier AS carrier
      ON carrier.name ILIKE '%unl%'
    JOIN raw_roster.roster_agentcarrier AS agent_carrier
      ON agent_carrier.carrier_id = carrier.id
     AND trim(agent_carrier.writing_number) = levels.writing_number
    JOIN raw_roster.roster_agentagency AS agent_agency
      ON agent_agency.agent_id = agent_carrier.agent_id
    JOIN raw_roster.roster_agencycarrier AS agency_carrier
      ON agency_carrier.id = agent_agency.agency_carrier_id
     AND agency_carrier.carrier_id = agent_carrier.carrier_id
    ORDER BY
        levels._dlt_id,
        levels.level_number,
        agent_agency.start DESC NULLS LAST,
        agent_agency.created DESC NULLS LAST
),
seed_matches AS (
    SELECT DISTINCT ON (_dlt_id)
        _dlt_id,
        agency_carrier_id
    FROM (
        SELECT _dlt_id, level_number, agency_carrier_id, 1 AS match_priority
        FROM policy_agency_matches
        UNION ALL
        SELECT _dlt_id, level_number, agency_carrier_id, 2 AS match_priority
        FROM policy_agent_matches
    ) AS matches
    ORDER BY _dlt_id, level_number DESC, match_priority
),
recursive_upline AS (
    SELECT
        seed_matches._dlt_id,
        0 AS traversal_depth,
        agency_carrier.id,
        agency_carrier.parent_id,
        agency_carrier.writing_number,
        agency.name,
        agency.is_person
    FROM seed_matches
    JOIN raw_roster.roster_agencycarrier AS agency_carrier
      ON agency_carrier.id = seed_matches.agency_carrier_id
    JOIN raw_roster.roster_agency AS agency
      ON agency.id = agency_carrier.agency_id

    UNION ALL

    SELECT
        recursive_upline._dlt_id,
        recursive_upline.traversal_depth + 1,
        parent.id,
        parent.parent_id,
        parent.writing_number,
        agency.name,
        agency.is_person
    FROM recursive_upline
    JOIN raw_roster.roster_agencycarrier AS parent
      ON parent.id = recursive_upline.parent_id
    JOIN raw_roster.roster_agency AS agency
      ON agency.id = parent.agency_id
),
renumbered_upline AS (
    SELECT
        _dlt_id,
        row_number() OVER (
            PARTITION BY _dlt_id
            ORDER BY traversal_depth DESC
        ) AS hierarchy_level,
        name,
        writing_number,
        is_person
    FROM recursive_upline
)
SELECT
    _dlt_id,
    jsonb_agg(
        jsonb_build_object(
            'depth',
            lpad(hierarchy_level::text, 2, '0'),
            'name',
            name,
            'writing_number',
            writing_number,
            'is_person',
            is_person
        )
        ORDER BY hierarchy_level
    ) AS roster_hierarchy_json
FROM renumbered_upline
GROUP BY _dlt_id
"""


def _unl_fym_policy_latest_load_view_statement(schema_name: str) -> str:
    canonical_join_id = "p._dlt_id"
    if schema_name == "typed":
        canonical_join_id = "coalesce(p.raw_dlt_id, p._dlt_id)"
    policy_projection = "p.*"
    if schema_name == "typed":
        policy_projection = ",\n    ".join(
            f"p.{column_name}" for column_name in UNL_TYPED_POLICY_VIEW_COLUMNS
        )

    return f"""CREATE OR REPLACE VIEW {schema_name}.unl_fym_policy_latest_load AS
WITH latest_file AS (
    SELECT fl.file_name
    FROM audit.file_landings AS fl
    WHERE fl.provider = 'unl'
      AND fl.status = 'loaded_to_postgres'
      AND fl.file_name LIKE 'FYM_Policy_%.csv'
    ORDER BY fl.landed_at DESC, fl.file_name DESC
    LIMIT 1
),
latest_policy AS (
    SELECT p.*
    FROM {schema_name}.unl_fym_policy AS p
    JOIN latest_file AS lf
      ON lf.file_name = p._source_file
)
SELECT
    {policy_projection},
    policy_roster_hierarchy.roster_hierarchy_json,
    'unl'::text AS carrier,
    history.previous_contract_code,
    history.contract_code_last_change_date,
    history.previous_at_risk_status,
    history.at_risk_status_last_change_date
FROM latest_policy AS p
LEFT JOIN typed.unl_fym_policy_roster_hierarchy AS policy_roster_hierarchy
  ON policy_roster_hierarchy._dlt_id = {canonical_join_id}
LEFT JOIN typed.unl_fym_policy_change_history AS history
  ON history._dlt_id = {canonical_join_id}
"""


def _unl_weekly_advance_latest_load_view_statement(schema_name: str) -> str:
    return f"""CREATE OR REPLACE VIEW {schema_name}.unl_weekly_advance_statements_latest_load AS
WITH latest_file AS (
    SELECT fl.file_name
    FROM audit.file_landings AS fl
    WHERE fl.provider = 'unl'
      AND fl.status = 'loaded_to_postgres'
      AND fl.file_name LIKE 'WA_%.csv'
    ORDER BY fl.landed_at DESC, fl.file_name DESC
    LIMIT 1
)
SELECT p.*
FROM {schema_name}.unl_weekly_advance_statements AS p
JOIN latest_file AS lf
  ON lf.file_name = p._source_file
"""


def _unl_fym_policy_typed_select() -> str:
    return """
WITH base AS (
    SELECT
        p.mga,
        p.mga_name,
        p.ga,
        p.ga_name,
        p.wa,
        p.wa_name,
        p.agent_ga_level_01,
        p.agent_level_02,
        p.agent_level_03,
        p.agent_level_04,
        p.agent_level_05,
        p.agent_level_06,
        p.agent_level_07,
        p.agent_level_08,
        p.agent_level_09,
        p.agent_level_10,
        p.plan_code,
        p.cntrct_code,
        p.annual_premium,
        p.issue_state,
        p.policy_nbr,
        p.first_name,
        p.last_name,
        p.phone_nbr,
        p._source_file,
        p._dlt_load_id AS _dlt_load_id,
        p._dlt_id AS _dlt_id,
        p._dlt_load_id AS raw_dlt_load_id,
        p._dlt_id AS raw_dlt_id,
        p.cntrct_reason,
        p.billing_form,
        coalesce(nullif(p.zip__v_text, ''), lpad(p.zip::text, 5, '0')) AS zip,
        nullif(p.issue_date::text, '') AS issue_date_text,
        nullif(p.app_recvd_date::text, '') AS app_recvd_date_text,
        nullif(p.paid_to_date::text, '') AS paid_to_date_text,
        nullif(split_part(p.cntrct_date::text, '.', 1), '') AS cntrct_date_text,
        nullif(split_part(p.term_date::text, '.', 1), '') AS term_date_text,
        nullif(p.billing_mode::text, '') AS billing_mode_text,
        substring(p._source_file from 'FYM_Policy_(\\d{8})') AS file_date_text
    FROM raw.unl_fym_policy AS p
),
typed_rows AS (
    SELECT
        mga,
        mga_name,
        ga,
        ga_name,
        wa,
        wa_name,
        agent_ga_level_01,
        agent_level_02,
        agent_level_03,
        agent_level_04,
        agent_level_05,
        agent_level_06,
        agent_level_07,
        agent_level_08,
        agent_level_09,
        agent_level_10,
        plan_code,
        CASE
            WHEN issue_date_text ~ '^\\d{8}$'
             AND to_char(to_date(issue_date_text, 'YYYYMMDD'), 'YYYYMMDD') = issue_date_text
            THEN to_date(issue_date_text, 'YYYYMMDD')
        END AS issue_date,
        cntrct_code,
        CASE
            WHEN app_recvd_date_text ~ '^\\d{8}$'
             AND to_char(to_date(app_recvd_date_text, 'YYYYMMDD'), 'YYYYMMDD') = app_recvd_date_text
            THEN to_date(app_recvd_date_text, 'YYYYMMDD')
        END AS app_recvd_date,
        annual_premium,
        issue_state,
        policy_nbr,
        CASE
            WHEN paid_to_date_text ~ '^\\d{8}$'
             AND to_char(to_date(paid_to_date_text, 'YYYYMMDD'), 'YYYYMMDD') = paid_to_date_text
            THEN to_date(paid_to_date_text, 'YYYYMMDD')
        END AS paid_to_date,
        CASE
            WHEN billing_mode_text ~ '^\\d+$'
            THEN billing_mode_text::integer
        END AS billing_mode,
        first_name,
        last_name,
        zip,
        phone_nbr,
        _source_file,
        raw_dlt_load_id,
        raw_dlt_id,
        cntrct_reason,
        CASE
            WHEN cntrct_date_text ~ '^\\d{8}$'
             AND to_char(to_date(cntrct_date_text, 'YYYYMMDD'), 'YYYYMMDD') = cntrct_date_text
            THEN to_date(cntrct_date_text, 'YYYYMMDD')
        END AS cntrct_date,
        billing_form,
        CASE
            WHEN term_date_text ~ '^\\d{8}$'
             AND to_char(to_date(term_date_text, 'YYYYMMDD'), 'YYYYMMDD') = term_date_text
            THEN to_date(term_date_text, 'YYYYMMDD')
        END AS term_date,
        CASE
            WHEN file_date_text ~ '^\\d{8}$'
            THEN to_date(file_date_text, 'YYYYMMDD')
        END AS file_date,
        _dlt_load_id,
        _dlt_id
    FROM base
),
enriched_rows AS (
    SELECT
        typed_rows.*,
        (
            cntrct_code = 'A'
            AND billing_form = 'DIR'
            AND billing_mode = 3
            AND paid_to_date IS NOT NULL
            AND file_date IS NOT NULL
            AND paid_to_date < file_date
        ) AS at_risk_policy
    FROM typed_rows
)
SELECT
    mga,
    mga_name,
    ga,
    ga_name,
    wa,
    wa_name,
    agent_ga_level_01,
    agent_level_02,
    agent_level_03,
    agent_level_04,
    agent_level_05,
    agent_level_06,
    agent_level_07,
    agent_level_08,
    agent_level_09,
    agent_level_10,
    plan_code,
    issue_date,
    cntrct_code,
    app_recvd_date,
    annual_premium,
    issue_state,
    policy_nbr,
    paid_to_date,
    billing_mode,
    first_name,
    last_name,
    zip,
    phone_nbr,
    _source_file,
    raw_dlt_load_id,
    raw_dlt_id,
    cntrct_reason,
    cntrct_date,
    billing_form,
    term_date,
    file_date,
    at_risk_policy,
    _dlt_load_id,
    _dlt_id
FROM enriched_rows
"""


def _unl_fym_policy_change_history_select() -> str:
    return """
WITH sequenced_rows AS (
    SELECT
        policy_nbr,
        file_date,
        _source_file,
        coalesce(raw_dlt_id, _dlt_id) AS _dlt_id,
        cntrct_code,
        at_risk_policy,
        row_number() OVER history_window AS observation_number,
        lag(cntrct_code) OVER history_window AS previous_contract_observation,
        lag(at_risk_policy) OVER history_window AS previous_at_risk_observation
    FROM typed.unl_fym_policy
    WINDOW history_window AS (
        PARTITION BY policy_nbr
        ORDER BY file_date, _source_file, _dlt_id
    )
),
history_rows AS (
    SELECT
        sequenced_rows.*,
        array_agg(previous_contract_observation) FILTER (
            WHERE observation_number > 1
              AND cntrct_code IS DISTINCT FROM previous_contract_observation
        ) OVER history_window AS previous_contract_codes,
        max(file_date) FILTER (
            WHERE observation_number > 1
              AND cntrct_code IS DISTINCT FROM previous_contract_observation
        ) OVER history_window AS contract_code_last_change_date,
        array_agg(previous_at_risk_observation) FILTER (
            WHERE observation_number > 1
              AND at_risk_policy IS DISTINCT FROM previous_at_risk_observation
        ) OVER history_window AS previous_at_risk_statuses,
        max(file_date) FILTER (
            WHERE observation_number > 1
              AND at_risk_policy IS DISTINCT FROM previous_at_risk_observation
        ) OVER history_window AS at_risk_status_last_change_date
    FROM sequenced_rows
    WINDOW history_window AS (
        PARTITION BY policy_nbr
        ORDER BY file_date, _source_file, _dlt_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
)
SELECT
    _dlt_id,
    previous_contract_codes[cardinality(previous_contract_codes)] AS previous_contract_code,
    contract_code_last_change_date,
    previous_at_risk_statuses[cardinality(previous_at_risk_statuses)]
        AS previous_at_risk_status,
    at_risk_status_last_change_date
FROM history_rows
"""


def _unl_fym_policy_incremental_history_insert_statement() -> str:
    return """INSERT INTO typed.unl_fym_policy_change_history (
    _dlt_id,
    previous_contract_code,
    contract_code_last_change_date,
    previous_at_risk_status,
    at_risk_status_last_change_date
)
WITH pending_rows AS (
    SELECT
        p.policy_nbr,
        p.file_date,
        p._source_file,
        coalesce(p.raw_dlt_id, p._dlt_id) AS source_dlt_id,
        p.cntrct_code,
        p.at_risk_policy
    FROM typed.unl_fym_policy AS p
    LEFT JOIN typed.unl_fym_policy_change_history AS h
      ON h._dlt_id = coalesce(p.raw_dlt_id, p._dlt_id)
    WHERE h._dlt_id IS NULL
),
affected_policies AS (
    SELECT DISTINCT policy_nbr
    FROM pending_rows
),
prior_rows AS (
    SELECT DISTINCT ON (p.policy_nbr)
        p.policy_nbr,
        p.file_date,
        p._source_file,
        coalesce(p.raw_dlt_id, p._dlt_id) AS source_dlt_id,
        p.cntrct_code,
        p.at_risk_policy,
        h.previous_contract_code,
        h.contract_code_last_change_date,
        h.previous_at_risk_status,
        h.at_risk_status_last_change_date
    FROM typed.unl_fym_policy AS p
    JOIN affected_policies AS affected
      ON affected.policy_nbr = p.policy_nbr
    JOIN typed.unl_fym_policy_change_history AS h
      ON h._dlt_id = coalesce(p.raw_dlt_id, p._dlt_id)
    ORDER BY
        p.policy_nbr,
        p.file_date DESC,
        p._source_file DESC,
        coalesce(p.raw_dlt_id, p._dlt_id) DESC
),
observations AS (
    SELECT
        policy_nbr,
        file_date,
        _source_file,
        source_dlt_id,
        cntrct_code,
        at_risk_policy,
        false AS is_new,
        previous_contract_code AS seed_previous_contract_code,
        contract_code_last_change_date AS seed_contract_change_date,
        previous_at_risk_status AS seed_previous_at_risk_status,
        at_risk_status_last_change_date AS seed_at_risk_change_date
    FROM prior_rows

    UNION ALL

    SELECT
        policy_nbr,
        file_date,
        _source_file,
        source_dlt_id,
        cntrct_code,
        at_risk_policy,
        true AS is_new,
        NULL::text,
        NULL::date,
        NULL::boolean,
        NULL::date
    FROM pending_rows
),
sequenced_rows AS (
    SELECT
        observations.*,
        lag(source_dlt_id) OVER history_window AS previous_observation_id,
        lag(cntrct_code) OVER history_window AS previous_contract_observation,
        lag(at_risk_policy) OVER history_window AS previous_at_risk_observation
    FROM observations
    WINDOW history_window AS (
        PARTITION BY policy_nbr
        ORDER BY file_date, _source_file, source_dlt_id
    )
),
marked_rows AS (
    SELECT
        sequenced_rows.*,
        (
            NOT is_new
            OR (
                previous_observation_id IS NOT NULL
                AND cntrct_code IS DISTINCT FROM previous_contract_observation
            )
        ) AS contract_event,
        CASE
            WHEN NOT is_new THEN seed_previous_contract_code
            ELSE previous_contract_observation
        END AS contract_event_previous_value,
        CASE
            WHEN NOT is_new THEN seed_contract_change_date
            ELSE file_date
        END AS contract_event_date,
        (
            NOT is_new
            OR (
                previous_observation_id IS NOT NULL
                AND at_risk_policy IS DISTINCT FROM previous_at_risk_observation
            )
        ) AS at_risk_event,
        CASE
            WHEN NOT is_new THEN seed_previous_at_risk_status
            ELSE previous_at_risk_observation
        END AS at_risk_event_previous_value,
        CASE
            WHEN NOT is_new THEN seed_at_risk_change_date
            ELSE file_date
        END AS at_risk_event_date
    FROM sequenced_rows
),
history_rows AS (
    SELECT
        marked_rows.*,
        array_agg(contract_event_previous_value) FILTER (
            WHERE contract_event
        ) OVER history_window AS previous_contract_codes,
        max(contract_event_date) FILTER (
            WHERE contract_event
        ) OVER history_window AS contract_code_last_change_date,
        array_agg(at_risk_event_previous_value) FILTER (
            WHERE at_risk_event
        ) OVER history_window AS previous_at_risk_statuses,
        max(at_risk_event_date) FILTER (
            WHERE at_risk_event
        ) OVER history_window AS at_risk_status_last_change_date
    FROM marked_rows
    WINDOW history_window AS (
        PARTITION BY policy_nbr
        ORDER BY file_date, _source_file, source_dlt_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
)
SELECT
    source_dlt_id,
    previous_contract_codes[cardinality(previous_contract_codes)],
    contract_code_last_change_date,
    previous_at_risk_statuses[cardinality(previous_at_risk_statuses)],
    at_risk_status_last_change_date
FROM history_rows
WHERE is_new
ON CONFLICT (_dlt_id) DO NOTHING
"""


def _unl_fym_policy_out_of_order_history_reset_statement() -> str:
    return """WITH pending_rows AS (
    SELECT
        p.policy_nbr,
        min(p.file_date) AS first_pending_file_date
    FROM typed.unl_fym_policy AS p
    LEFT JOIN typed.unl_fym_policy_change_history AS h
      ON h._dlt_id = coalesce(p.raw_dlt_id, p._dlt_id)
    WHERE h._dlt_id IS NULL
    GROUP BY p.policy_nbr
),
processed_bounds AS (
    SELECT
        p.policy_nbr,
        max(p.file_date) AS last_processed_file_date
    FROM typed.unl_fym_policy AS p
    JOIN typed.unl_fym_policy_change_history AS h
      ON h._dlt_id = coalesce(p.raw_dlt_id, p._dlt_id)
    JOIN pending_rows AS pending
      ON pending.policy_nbr = p.policy_nbr
    GROUP BY p.policy_nbr
),
out_of_order_policies AS (
    SELECT pending.policy_nbr
    FROM pending_rows AS pending
    JOIN processed_bounds AS processed
      ON processed.policy_nbr = pending.policy_nbr
    WHERE pending.first_pending_file_date <= processed.last_processed_file_date
)
DELETE FROM typed.unl_fym_policy_change_history AS history
USING typed.unl_fym_policy AS policy, out_of_order_policies AS out_of_order
WHERE history._dlt_id = coalesce(policy.raw_dlt_id, policy._dlt_id)
  AND policy.policy_nbr = out_of_order.policy_nbr
"""


def _unl_fym_policy_at_risk_episode_select() -> str:
    return """
WITH file_bounds AS (
    SELECT
        MIN(file_date) AS first_file_date,
        MAX(file_date) AS last_file_date
    FROM typed.unl_fym_policy
    WHERE file_date >= DATE '2026-05-14'
),
atrisk AS (
    SELECT
        policy_nbr,
        file_date
    FROM typed.unl_fym_policy
    WHERE file_date >= DATE '2026-05-14'
      AND at_risk_policy = true
      AND policy_nbr IS NOT NULL
),
seq AS (
    SELECT
        policy_nbr,
        file_date,
        file_date - LAG(file_date) OVER (
            PARTITION BY policy_nbr
            ORDER BY file_date
        ) AS gap
    FROM atrisk
),
marked AS (
    SELECT
        policy_nbr,
        file_date,
        SUM(CASE WHEN gap IS NULL OR gap > 7 THEN 1 ELSE 0 END) OVER (
            PARTITION BY policy_nbr
            ORDER BY file_date
        ) AS episode_id
    FROM seq
),
episodes AS (
    SELECT
        policy_nbr,
        episode_id,
        MIN(file_date) AS ep_start,
        MAX(file_date) AS ep_end
    FROM marked
    GROUP BY
        policy_nbr,
        episode_id
),
ep_outcomes AS (
    SELECT
        e.policy_nbr,
        e.episode_id,
        e.ep_start,
        e.ep_end,
        MIN(d.file_date) FILTER (
            WHERE d.paid_to_date IS NOT NULL
              AND d.paid_to_date >= d.file_date
        ) AS cure_date,
        MIN(d.file_date) FILTER (
            WHERE d.cntrct_code = 'T'
        ) AS term_date
    FROM episodes e
    LEFT JOIN typed.unl_fym_policy d
      ON d.policy_nbr = e.policy_nbr
     AND d.file_date > e.ep_end
     AND d.file_date <= e.ep_end + 7
    GROUP BY
        e.policy_nbr,
        e.episode_id,
        e.ep_start,
        e.ep_end
)
SELECT
    ep_outcomes.policy_nbr,
    ep_outcomes.episode_id,
    ep_outcomes.ep_start,
    ep_outcomes.ep_end,
    ep_outcomes.cure_date,
    ep_outcomes.term_date,
    CASE
        WHEN ep_outcomes.ep_start = file_bounds.first_file_date THEN 'left_censored'
        WHEN ep_outcomes.cure_date IS NOT NULL
         AND (ep_outcomes.term_date IS NULL OR ep_outcomes.cure_date <= ep_outcomes.term_date)
            THEN 'saved'
        WHEN ep_outcomes.term_date IS NOT NULL THEN 'lost'
        WHEN ep_outcomes.ep_end >= file_bounds.last_file_date - 7 THEN 'still_open'
        ELSE 'ended_no_resolution'
    END AS outcome
FROM ep_outcomes
CROSS JOIN file_bounds
"""


def _heartland_typed_refresh_statements() -> tuple[str, ...]:
    typed_select = _heartland_inforced_policy_typed_select()
    return (
        "CREATE SCHEMA IF NOT EXISTS raw",
        "CREATE SCHEMA IF NOT EXISTS typed",
        (
            "CREATE TABLE IF NOT EXISTS raw.heartland_inforced_policy AS "
            f"{_heartland_raw_history_select()} WITH NO DATA"
        ),
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS heartland_inforced_policy_row_hash_idx "
            "ON raw.heartland_inforced_policy (_row_hash)"
        ),
        (
            "INSERT INTO raw.heartland_inforced_policy "
            f"{_heartland_raw_history_select(only_unseen=True)} "
            "ON CONFLICT (_row_hash) DO NOTHING"
        ),
        (
            "CREATE TABLE IF NOT EXISTS typed.heartland_inforced_policy AS "
            f"{typed_select} WITH NO DATA"
        ),
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS heartland_inforced_policy_typed_row_hash_idx "
            "ON typed.heartland_inforced_policy (raw_row_hash)"
        ),
        (
            "INSERT INTO typed.heartland_inforced_policy "
            f"SELECT new_rows.* FROM ({typed_select}) AS new_rows "
            "WHERE NOT EXISTS ("
            "SELECT 1 FROM typed.heartland_inforced_policy AS existing "
            "WHERE existing.raw_row_hash = new_rows.raw_row_hash"
            ") ON CONFLICT (raw_row_hash) DO NOTHING"
        ),
        (
            "CREATE INDEX IF NOT EXISTS heartland_inforced_policy_pol_no_idx "
            "ON typed.heartland_inforced_policy (pol_no)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS heartland_inforced_policy_app_guid_idx "
            "ON typed.heartland_inforced_policy (app_guid)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS heartland_inforced_policy_agt_code_idx "
            "ON typed.heartland_inforced_policy (agt_code)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS heartland_inforced_policy_status_idx "
            "ON typed.heartland_inforced_policy (hnl_status)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS heartland_inforced_policy_eff_date_idx "
            "ON typed.heartland_inforced_policy (eff_date)"
        ),
        "CREATE OR REPLACE VIEW typed.heartland_inforced_policy_latest AS "
        "SELECT DISTINCT ON (pol_no, agt_code, writing_split) * "
        "FROM typed.heartland_inforced_policy "
        "ORDER BY pol_no, agt_code, writing_split, first_seen_at DESC, raw_row_hash DESC",
        "ANALYZE raw.heartland_inforced_policy",
        "ANALYZE typed.heartland_inforced_policy",
    )


def _heartland_raw_history_select(*, only_unseen: bool = False) -> str:
    row_hash = _heartland_row_hash_sql("p")
    unseen_filter = ""
    if only_unseen:
        unseen_filter = """
WHERE NOT EXISTS (
    SELECT 1
    FROM raw.heartland_inforced_policy AS existing
    WHERE existing._row_hash = snapshot_rows._row_hash
)
"""
    return f"""
SELECT DISTINCT ON (snapshot_rows._row_hash)
    snapshot_rows.*
FROM (
    SELECT
        p.pol_no,
        p.agt_code,
        p.agt_first_name,
        p.agt_last_name,
        p.amr_status,
        p.app_date,
        p.eff_date,
        p.birth_date,
        p.premium,
        p.plan,
        p.product_desc,
        p.type,
        p.issue_state,
        p.entry_date,
        p.first_name,
        p.last_name,
        p.share,
        p.initial_paid_date,
        p.client_address,
        p.client_address2,
        p.client_city,
        p.client_state,
        p.client_zip,
        p.client_email,
        p.client_phone,
        p.paid_to_date,
        p.draft_day,
        p.hnl_status,
        p.return_descripton,
        p.charge_back_dt,
        p.upline,
        p.iss_age,
        p.end_date,
        p.app_guid,
        p.app_type,
        p.writing_split,
        p._dlt_load_id AS source_dlt_load_id,
        p._dlt_id AS source_dlt_id,
        {row_hash} AS _row_hash,
        now() AS _first_seen_at
    FROM raw.heartland_inforced_policy_snapshot AS p
) AS snapshot_rows
{unseen_filter}
ORDER BY snapshot_rows._row_hash, snapshot_rows.source_dlt_id
"""


def _heartland_row_hash_sql(table_alias: str) -> str:
    fields = (
        "pol_no",
        "agt_code",
        "agt_first_name",
        "agt_last_name",
        "amr_status",
        "app_date",
        "eff_date",
        "birth_date",
        "premium",
        "plan",
        "product_desc",
        "type",
        "issue_state",
        "entry_date",
        "first_name",
        "last_name",
        "share",
        "initial_paid_date",
        "client_address",
        "client_address2",
        "client_city",
        "client_state",
        "client_zip",
        "client_email",
        "client_phone",
        "paid_to_date",
        "draft_day",
        "hnl_status",
        "return_descripton",
        "charge_back_dt",
        "upline",
        "iss_age",
        "end_date",
        "app_guid",
        "app_type",
        "writing_split",
    )
    values = ", ".join(f"{table_alias}.{field}" for field in fields)
    return f"md5(jsonb_build_array({values})::text)"


def _heartland_inforced_policy_typed_select() -> str:
    return f"""
WITH base AS (
    SELECT
        nullif(trim(p.pol_no::text), '') AS pol_no,
        nullif(trim(p.agt_code::text), '') AS agt_code,
        nullif(trim(p.agt_first_name::text), '') AS agt_first_name,
        nullif(trim(p.agt_last_name::text), '') AS agt_last_name,
        nullif(trim(p.amr_status::text), '') AS amr_status,
        nullif(trim(p.app_date::text), '') AS app_date_text,
        nullif(trim(p.eff_date::text), '') AS eff_date_text,
        nullif(trim(p.birth_date::text), '') AS birth_date_text,
        nullif(trim(p.premium::text), '') AS premium_text,
        nullif(trim(p.plan::text), '') AS plan,
        nullif(trim(p.product_desc::text), '') AS product_desc,
        nullif(trim(p.type::text), '') AS type,
        nullif(trim(p.issue_state::text), '') AS issue_state,
        nullif(trim(p.entry_date::text), '') AS entry_date_text,
        nullif(trim(p.first_name::text), '') AS first_name,
        nullif(trim(p.last_name::text), '') AS last_name,
        nullif(trim(p.share::text), '') AS share_text,
        nullif(trim(p.initial_paid_date::text), '') AS initial_paid_date_text,
        nullif(trim(p.client_address::text), '') AS client_address,
        nullif(trim(p.client_address2::text), '') AS client_address2,
        nullif(trim(p.client_city::text), '') AS client_city,
        nullif(trim(p.client_zip::text), '') AS client_state,
        nullif(trim(p.client_state::text), '') AS client_zip,
        nullif(trim(p.client_email::text), '') AS client_email,
        nullif(trim(p.client_phone::text), '') AS client_phone,
        nullif(trim(p.paid_to_date::text), '') AS paid_to_date_text,
        nullif(trim(p.draft_day::text), '') AS draft_day,
        nullif(trim(p.hnl_status::text), '') AS hnl_status,
        nullif(trim(p.return_descripton::text), '') AS return_descripton,
        nullif(trim(p.charge_back_dt::text), '') AS charge_back_dt_text,
        nullif(trim(p.upline::text), '') AS upline,
        nullif(trim(p.iss_age::text), '') AS iss_age_text,
        nullif(trim(p.end_date::text), '') AS end_date_text,
        nullif(trim(p.app_guid::text), '') AS app_guid,
        nullif(trim(p.app_type::text), '') AS app_type,
        nullif(trim(p.writing_split::text), '') AS writing_split,
        p.source_dlt_load_id,
        p.source_dlt_id,
        p._row_hash AS raw_row_hash,
        p._first_seen_at AS first_seen_at
    FROM raw.heartland_inforced_policy AS p
)
SELECT
    pol_no,
    agt_code,
    agt_first_name,
    agt_last_name,
    amr_status,
    CASE {_date_parse_sql("app_date_text")} END AS app_date,
    CASE {_date_parse_sql("eff_date_text")} END AS eff_date,
    CASE {_date_parse_sql("birth_date_text")} END AS birth_date,
    CASE
        WHEN premium_text ~ '^-?\\d+(\\.\\d+)?$'
        THEN premium_text::numeric
    END AS premium,
    plan,
    product_desc,
    type,
    issue_state,
    CASE {_date_parse_sql("entry_date_text")} END AS entry_date,
    first_name,
    last_name,
    CASE
        WHEN share_text ~ '^-?\\d+(\\.\\d+)?$'
        THEN share_text::numeric
    END AS share,
    CASE {_date_parse_sql("initial_paid_date_text")} END AS initial_paid_date,
    client_address,
    client_address2,
    client_city,
    client_state,
    client_zip,
    client_email,
    client_phone,
    CASE {_date_parse_sql("paid_to_date_text")} END AS paid_to_date,
    draft_day,
    hnl_status,
    return_descripton,
    CASE {_date_parse_sql("charge_back_dt_text")} END AS charge_back_dt,
    upline,
    CASE
        WHEN iss_age_text ~ '^\\d+$'
        THEN iss_age_text::smallint
    END AS iss_age,
    CASE {_date_parse_sql("end_date_text")} END AS end_date,
    app_guid,
    app_type,
    writing_split,
    source_dlt_load_id,
    source_dlt_id,
    raw_row_hash,
    first_seen_at
FROM base
"""


def _unl_weekly_advance_statements_typed_select() -> str:
    return f"""
WITH base AS (
    SELECT
        nullif(trim(p.trans_type::text), '') AS transaction_type,
        row_number() OVER (PARTITION BY p._source_file ORDER BY p._dlt_id) AS source_row_number,
        nullif(trim(p.policy_nbr::text), '') AS policy_number,
        nullif(trim(p."desc"::text), '') AS insured_name,
        nullif(trim(p.agent_nbr::text), '') AS agent_number,
        nullif(trim(p.first_name::text), '') AS first_name,
        nullif(trim(p.last_name::text), '') AS last_name,
        nullif(trim(p.agency::text), '') AS agency,
        nullif(trim(p.plan::text), '') AS plan,
        nullif(trim(p.prem_paid_amt::text), '') AS premium_paid_amount_text,
        nullif(trim(p.comm_rate::text), '') AS commission_rate_text,
        nullif(trim(p.comm_prem_amt::text), '') AS commission_premium_amount_text,
        nullif(trim(p.adv_per::text), '') AS advance_percent_text,
        nullif(trim(p.amount::text), '') AS amount_text,
        nullif(trim(p.effective_date::text), '') AS effective_date_text,
        nullif(trim(p.paid_to_date::text), '') AS paid_to_date_text,
        nullif(trim(p.last_activity_date::text), '') AS last_activity_date_text,
        p._source_file,
        p._dlt_load_id,
        p._dlt_id,
        substring(p._source_file from 'WA_202JVV00_(\\d{4}_\\d{2}_\\d{2})') AS file_date_text
    FROM raw.unl_weekly_advance_statements AS p
),
typed_rows AS (
    SELECT
        transaction_type,
        source_row_number AS row_number,
        policy_number,
        insured_name,
        agent_number,
        first_name,
        last_name,
        agency,
        plan,
        CASE
            WHEN premium_paid_amount_text ~ '^-?\\d+(\\.\\d+)?$'
            THEN premium_paid_amount_text::numeric
        END AS premium_paid_amount,
        CASE
            WHEN commission_rate_text ~ '^-?\\d+(\\.\\d+)?$'
            THEN commission_rate_text::numeric
        END AS commission_rate,
        CASE
            WHEN commission_premium_amount_text ~ '^-?\\d+(\\.\\d+)?$'
            THEN commission_premium_amount_text::numeric
        END AS commission_premium_amount,
        CASE
            WHEN advance_percent_text ~ '^-?\\d+(\\.\\d+)?$'
            THEN advance_percent_text::numeric
        END AS advance_percent,
        CASE
            WHEN amount_text ~ '^-?\\d+(\\.\\d+)?$'
            THEN amount_text::numeric
        END AS amount,
        CASE
            {_date_parse_sql("effective_date_text")}
        END AS effective_date,
        CASE
            {_date_parse_sql("paid_to_date_text")}
        END AS paid_to_date,
        CASE
            {_date_parse_sql("last_activity_date_text")}
        END AS last_activity_date,
        _source_file,
        _dlt_load_id,
        _dlt_id,
        CASE
            WHEN file_date_text IS NOT NULL
            THEN to_date(replace(file_date_text, '_', '-'), 'YYYY-MM-DD')
        END AS file_date
    FROM base
)
SELECT *
FROM typed_rows
"""


def _date_parse_sql(column_name: str) -> str:
    return f"""
            WHEN {column_name} ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$'
             AND to_char(to_date({column_name}, 'YYYY-MM-DD'), 'YYYY-MM-DD') = {column_name}
            THEN to_date({column_name}, 'YYYY-MM-DD')
            WHEN {column_name} ~ '^\\d{{8}}$'
             AND to_char(to_date({column_name}, 'YYYYMMDD'), 'YYYYMMDD') = {column_name}
            THEN to_date({column_name}, 'YYYYMMDD')
            WHEN {column_name} ~ '^\\d{{1,2}}/\\d{{1,2}}/\\d{{4}}$'
             AND to_char(to_date({column_name}, 'MM/DD/YYYY'), 'FMMM/FMDD/YYYY') =
                 regexp_replace({column_name}, '^0?(\\d{{1,2}})/0?(\\d{{1,2}})/(\\d{{4}})$', '\\1/\\2/\\3')
            THEN to_date({column_name}, 'MM/DD/YYYY')
            WHEN {column_name} ~ '^\\d{{1,2}}-\\d{{1,2}}-\\d{{4}}$'
             AND to_char(to_date({column_name}, 'MM-DD-YYYY'), 'FMMM-FMDD-YYYY') =
                 regexp_replace({column_name}, '^0?(\\d{{1,2}})-0?(\\d{{1,2}})-(\\d{{4}})$', '\\1-\\2-\\3')
            THEN to_date({column_name}, 'MM-DD-YYYY')"""
