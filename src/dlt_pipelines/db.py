"""Database connectivity checks."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath

import psycopg2

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


def _connect():
    return psycopg2.connect(
        dbname=_postgres_setting("database"),
        user=_postgres_setting("username"),
        password=_postgres_setting("password"),
        host=_postgres_setting("host"),
        port=_postgres_setting("port"),
        connect_timeout=int(_postgres_setting("connect_timeout", default="15")),
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
