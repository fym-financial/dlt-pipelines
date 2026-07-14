"""Incremental raw-to-typed DLT pipelines."""

from __future__ import annotations

from typing import Any

import dlt


def run_unl_fym_policy_typed_pipeline(
    *,
    source_credentials: str,
    initial_load_id: str | None,
) -> Any:
    """Incrementally merge typed FYM policy rows from the raw PostgreSQL view."""
    from dlt.sources.sql_database import sql_table

    incremental = dlt.sources.incremental(
        "raw_dlt_load_id",
        initial_value=initial_load_id,
        range_start="open" if initial_load_id is not None else "closed",
        row_order="asc",
        on_cursor_value_missing="raise",
    )
    resource = sql_table(
        credentials=source_credentials,
        schema="raw",
        table="unl_fym_policy_typed_source",
        incremental=incremental,
        backend="pyarrow",
        chunk_size=50_000,
        write_disposition={"disposition": "merge", "strategy": "insert-only"},
        primary_key="raw_dlt_id",
    ).with_name("unl_fym_policy")

    pipeline = dlt.pipeline(
        pipeline_name="unl_typed",
        destination="postgres",
        dataset_name="typed",
    )
    return pipeline.run(resource)
