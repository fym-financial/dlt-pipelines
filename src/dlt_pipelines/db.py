"""Database connectivity checks."""

from __future__ import annotations

from dataclasses import dataclass
from io import StringIO
from pathlib import PurePosixPath

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


def refresh_typed_dataset(provider: str) -> TypedRefreshResult:
    if provider != "unl":
        raise RuntimeError(f"Typed refresh is not configured for provider '{provider}'.")

    connection = _connect()
    try:
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


def _connect():
    return psycopg2.connect(
        dbname=_postgres_setting("database"),
        user=_postgres_setting("username"),
        password=_postgres_setting("password"),
        host=_postgres_setting("host"),
        port=_postgres_setting("port"),
        connect_timeout=int(_postgres_setting("connect_timeout", default="15")),
    )


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
    typed_select = _unl_fym_policy_typed_select()
    return (
        "CREATE SCHEMA IF NOT EXISTS typed",
        f"CREATE TABLE IF NOT EXISTS typed.unl_fym_policy AS {typed_select} WITH NO DATA",
        "TRUNCATE TABLE typed.unl_fym_policy",
        f"INSERT INTO typed.unl_fym_policy {typed_select}",
        _unl_fym_policy_latest_load_view_statement(),
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
        "GRANT USAGE ON SCHEMA typed TO unl_fym_policy_reader",
        "GRANT SELECT ON ALL TABLES IN SCHEMA typed TO unl_fym_policy_reader",
        (
            "ALTER DEFAULT PRIVILEGES IN SCHEMA typed "
            "GRANT SELECT ON TABLES TO unl_fym_policy_reader"
        ),
        "ANALYZE typed.unl_fym_policy",
    )


def _unl_fym_policy_latest_load_view_statement() -> str:
    return """CREATE OR REPLACE VIEW typed.unl_fym_policy_latest_load AS
WITH RECURSIVE latest_file AS (
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
    FROM typed.unl_fym_policy AS p
    JOIN latest_file AS lf
      ON lf.file_name = p._source_file
),
policy_levels AS (
    SELECT
        p._dlt_id,
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
),
policy_roster_hierarchy AS (
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
)
SELECT
    p.*,
    policy_roster_hierarchy.roster_hierarchy_json
FROM latest_policy AS p
LEFT JOIN policy_roster_hierarchy
  ON policy_roster_hierarchy._dlt_id = p._dlt_id
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
        p._dlt_load_id,
        p._dlt_id,
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
        _dlt_load_id,
        _dlt_id,
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
        END AS file_date
    FROM base
)
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
"""
