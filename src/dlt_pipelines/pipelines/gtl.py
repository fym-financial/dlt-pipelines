"""PostgreSQL statements for the GTL FYM policy typed dataset."""

from __future__ import annotations


GTL_TYPED_POLICY_COLUMNS = (
    "mga", "mga_name", "ga", "ga_name", "wa", "wa_name",
    "agent_ga_level_01", "agent_level_02", "agent_level_03", "agent_level_04",
    "agent_level_05", "agent_level_06", "agent_level_07", "agent_level_08",
    "agent_level_09", "agent_level_10", "plan_code", "issue_date", "cntrct_code",
    "app_recvd_date", "annual_premium", "issue_state", "policy_nbr", "paid_to_date",
    "billing_mode", "first_name", "last_name", "zip", "phone_nbr", "_source_file",
    "_source_modified_at", "raw_dlt_load_id", "raw_dlt_id", "cntrct_reason",
    "cntrct_date", "billing_form", "term_date", "file_date", "at_risk_policy",
    "_dlt_load_id", "_dlt_id",
)

_OPTIONAL_RAW_COLUMNS = (
    "ga", "ga_name", "agent_ga_level_01", "cntrct_reason", "cntrct_date",
    "billing_form", "term_date",
)


def gtl_fym_policy_typed_select() -> str:
    """Normalize a GTL extract using the same policy types and risk rule as UNL."""
    return r"""
WITH source_rows AS (
    SELECT
        nullif(trim(p.mga::text), '') AS mga,
        nullif(trim(p.mga_name::text), '') AS mga_name,
        nullif(trim(p.ga::text), '') AS ga,
        nullif(trim(p.ga_name::text), '') AS ga_name,
        nullif(trim(p.wa::text), '') AS wa,
        nullif(trim(p.wa_name::text), '') AS wa_name,
        nullif(trim(p.agent_ga_level_01::text), '') AS agent_ga_level_01,
        nullif(trim(p.agent_level_02::text), '') AS agent_level_02,
        nullif(trim(p.agent_level_03::text), '') AS agent_level_03,
        nullif(trim(p.agent_level_04::text), '') AS agent_level_04,
        nullif(trim(p.agent_level_05::text), '') AS agent_level_05,
        nullif(trim(p.agent_level_06::text), '') AS agent_level_06,
        nullif(trim(p.agent_level_07::text), '') AS agent_level_07,
        nullif(trim(p.agent_level_08::text), '') AS agent_level_08,
        nullif(trim(p.agent_level_09::text), '') AS agent_level_09,
        nullif(trim(p.agent_level_10::text), '') AS agent_level_10,
        nullif(trim(p.plan_code::text), '') AS plan_code,
        nullif(trim(p.issue_date::text), '') AS issue_date_text,
        nullif(trim(p.cntrct_code::text), '') AS cntrct_code,
        nullif(trim(p.app_recvd_date::text), '') AS app_recvd_date_text,
        nullif(trim(p.annual_premium::text), '') AS annual_premium_text,
        nullif(trim(p.issue_state::text), '') AS issue_state,
        nullif(trim(p.policy_nbr::text), '') AS policy_nbr,
        nullif(trim(p.paid_to_date::text), '') AS paid_to_date_text,
        nullif(trim(p.billing_mode::text), '') AS billing_mode_text,
        nullif(trim(p.first_name::text), '') AS first_name,
        nullif(trim(p.last_name::text), '') AS last_name,
        nullif(trim(p.zip::text), '') AS zip,
        nullif(trim(p.phone_nbr::text), '') AS phone_nbr,
        p._source_file::text AS _source_file,
        p._source_modified_at::timestamptz AS _source_modified_at,
        p._dlt_load_id::text AS raw_dlt_load_id,
        p._dlt_id::text AS raw_dlt_id,
        nullif(trim(p.cntrct_reason::text), '') AS cntrct_reason,
        nullif(split_part(p.cntrct_date::text, '.', 1), '') AS cntrct_date_text,
        nullif(trim(p.billing_form::text), '') AS billing_form,
        nullif(split_part(p.term_date::text, '.', 1), '') AS term_date_text,
        substring(split_part(p._source_file, '_Policy_', 2) from 1 for 8) AS file_date_text,
        p._dlt_load_id::text AS _dlt_load_id,
        p._dlt_id::text AS _dlt_id
    FROM raw.gtl_fym_policy AS p
),
typed_rows AS (
    SELECT
        mga, mga_name, ga, ga_name, wa, wa_name, agent_ga_level_01,
        agent_level_02, agent_level_03, agent_level_04, agent_level_05,
        agent_level_06, agent_level_07, agent_level_08, agent_level_09,
        agent_level_10, plan_code,
        CASE WHEN issue_date_text ~ '^\d{8}$' THEN to_date(issue_date_text, 'YYYYMMDD') END AS issue_date,
        cntrct_code,
        CASE WHEN app_recvd_date_text ~ '^\d{8}$' THEN to_date(app_recvd_date_text, 'YYYYMMDD') END AS app_recvd_date,
        CASE WHEN annual_premium_text ~ '^[+-]?\d+(\.\d+)?$' THEN annual_premium_text::numeric END AS annual_premium,
        issue_state, policy_nbr,
        CASE WHEN paid_to_date_text ~ '^\d{8}$' THEN to_date(paid_to_date_text, 'YYYYMMDD') END AS paid_to_date,
        CASE WHEN billing_mode_text ~ '^\d+$' THEN billing_mode_text::integer END AS billing_mode,
        first_name, last_name, zip, phone_nbr, _source_file, _source_modified_at,
        raw_dlt_load_id, raw_dlt_id, cntrct_reason,
        CASE WHEN cntrct_date_text ~ '^\d{8}$' THEN to_date(cntrct_date_text, 'YYYYMMDD') END AS cntrct_date,
        billing_form,
        CASE WHEN term_date_text ~ '^\d{8}$' THEN to_date(term_date_text, 'YYYYMMDD') END AS term_date,
        CASE WHEN file_date_text ~ '^\d{8}$' THEN to_date(file_date_text, 'YYYYMMDD') END AS file_date,
        _dlt_load_id, _dlt_id
    FROM source_rows
)
SELECT
    typed_rows.*,
    (cntrct_code = 'A' AND billing_form = 'DIR' AND billing_mode = 3
     AND paid_to_date IS NOT NULL AND file_date IS NOT NULL
     AND paid_to_date < file_date) AS at_risk_policy
FROM typed_rows
"""


def _gtl_change_history_select() -> str:
    from dlt_pipelines.db import _unl_fym_policy_change_history_select

    return _unl_fym_policy_change_history_select().replace(
        "typed.unl_fym_policy", "typed.gtl_fym_policy"
    )


def _gtl_at_risk_episode_select() -> str:
    from dlt_pipelines.db import _unl_fym_policy_at_risk_episode_select

    return _unl_fym_policy_at_risk_episode_select().replace(
        "typed.unl_fym_policy", "typed.gtl_fym_policy"
    )


def _gtl_roster_hierarchy_select() -> str:
    from dlt_pipelines.db import _unl_fym_policy_roster_hierarchy_select

    return (
        _unl_fym_policy_roster_hierarchy_select()
        .replace("typed.unl_fym_policy", "typed.gtl_fym_policy")
        .replace("fl.provider = 'unl'", "fl.provider = 'gtl'")
        .replace(
            "AND (fl.file_name LIKE 'UNLFYM_Policy_%.csv'\n"
            "           OR fl.file_name LIKE 'FYM_Policy_%.csv')",
            "AND fl.file_name LIKE 'GTLFYM_Policy_%.csv'",
        )
        .replace("carrier.name ILIKE '%unl%'", "carrier.name ILIKE '%gtl%'")
    )


def _gtl_latest_load_view_statement(schema_name: str) -> str:
    join_id = "p._dlt_id" if schema_name == "raw" else "coalesce(p.raw_dlt_id, p._dlt_id)"
    return f"""CREATE OR REPLACE VIEW {schema_name}.gtl_fym_policy_latest_load AS
WITH latest_file AS (
    SELECT file_name
    FROM audit.file_landings
    WHERE provider = 'gtl'
      AND status = 'loaded_to_postgres'
      AND file_name LIKE 'GTLFYM_Policy_%.csv'
    ORDER BY landed_at DESC, file_name DESC
    LIMIT 1
),
latest_policy AS (
    SELECT p.*
    FROM {schema_name}.gtl_fym_policy AS p
    JOIN latest_file AS lf ON lf.file_name = p._source_file
)
SELECT
    p.*,
    policy_roster_hierarchy.roster_hierarchy_json,
    'gtl'::text AS carrier,
    history.previous_contract_code,
    history.contract_code_last_change_date,
    history.previous_at_risk_status,
    history.at_risk_status_last_change_date
FROM latest_policy AS p
LEFT JOIN typed.gtl_fym_policy_roster_hierarchy AS policy_roster_hierarchy
  ON policy_roster_hierarchy._dlt_id = {join_id}
LEFT JOIN typed.gtl_fym_policy_change_history AS history
  ON history._dlt_id = {join_id}
"""


def gtl_typed_refresh_statements() -> tuple[str, ...]:
    """Append new GTL rows and rebuild matching derived policy objects."""
    typed_select = gtl_fym_policy_typed_select()
    columns = ", ".join(GTL_TYPED_POLICY_COLUMNS)
    history_select = _gtl_change_history_select()
    episode_select = _gtl_at_risk_episode_select()
    roster_hierarchy_select = _gtl_roster_hierarchy_select()
    compatibility = tuple(
        f"ALTER TABLE raw.gtl_fym_policy ADD COLUMN IF NOT EXISTS {column} text"
        for column in _OPTIONAL_RAW_COLUMNS
    )
    return (
        "CREATE SCHEMA IF NOT EXISTS typed",
        *compatibility,
        f"CREATE TABLE IF NOT EXISTS typed.gtl_fym_policy AS {typed_select} WITH NO DATA",
        "CREATE UNIQUE INDEX IF NOT EXISTS gtl_fym_policy_typed_dlt_id_idx ON typed.gtl_fym_policy (_dlt_id)",
        f"INSERT INTO typed.gtl_fym_policy ({columns}) SELECT {columns} FROM ({typed_select}) AS source ON CONFLICT (_dlt_id) DO NOTHING",
        f"CREATE TABLE IF NOT EXISTS typed.gtl_fym_policy_change_history AS {history_select} WITH NO DATA",
        "CREATE UNIQUE INDEX IF NOT EXISTS gtl_fym_policy_change_history_dlt_id_idx ON typed.gtl_fym_policy_change_history (_dlt_id)",
        "TRUNCATE TABLE typed.gtl_fym_policy_change_history",
        f"INSERT INTO typed.gtl_fym_policy_change_history {history_select}",
        "CREATE TABLE IF NOT EXISTS typed.gtl_fym_policy_roster_hierarchy (_dlt_id text PRIMARY KEY, roster_hierarchy_json jsonb)",
        "CREATE TEMP TABLE gtl_fym_policy_roster_hierarchy_refresh ON COMMIT DROP AS "
        f"{roster_hierarchy_select}",
        "INSERT INTO typed.gtl_fym_policy_roster_hierarchy (_dlt_id, roster_hierarchy_json) "
        "SELECT _dlt_id, roster_hierarchy_json "
        "FROM pg_temp.gtl_fym_policy_roster_hierarchy_refresh WHERE true "
        "ON CONFLICT (_dlt_id) DO UPDATE "
        "SET roster_hierarchy_json = EXCLUDED.roster_hierarchy_json",
        "DELETE FROM typed.gtl_fym_policy_roster_hierarchy AS existing "
        "WHERE NOT EXISTS (SELECT 1 "
        "FROM pg_temp.gtl_fym_policy_roster_hierarchy_refresh AS refreshed "
        "WHERE refreshed._dlt_id = existing._dlt_id)",
        f"CREATE TABLE IF NOT EXISTS typed.gtl_fym_policy_at_risk_episodes AS {episode_select} WITH NO DATA",
        "TRUNCATE TABLE typed.gtl_fym_policy_at_risk_episodes",
        f"INSERT INTO typed.gtl_fym_policy_at_risk_episodes {episode_select}",
        _gtl_latest_load_view_statement("raw"),
        _gtl_latest_load_view_statement("typed"),
        "CREATE INDEX IF NOT EXISTS gtl_fym_policy_file_date_idx ON typed.gtl_fym_policy (file_date)",
        "CREATE INDEX IF NOT EXISTS gtl_fym_policy_policy_nbr_idx ON typed.gtl_fym_policy (policy_nbr)",
        "ANALYZE typed.gtl_fym_policy",
        "ANALYZE typed.gtl_fym_policy_change_history",
        "ANALYZE typed.gtl_fym_policy_roster_hierarchy",
        "ANALYZE typed.gtl_fym_policy_at_risk_episodes",
    )
