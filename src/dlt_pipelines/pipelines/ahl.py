"""PostgreSQL statements for the AHL FYM policy typed dataset."""

from __future__ import annotations


AHL_TYPED_POLICY_COLUMNS = (
    "policy_number",
    "first_name",
    "last_name",
    "date_of_birth",
    "phone",
    "zip",
    "email",
    "resident_state",
    "issue_state",
    "issue_age",
    "base_plan_code",
    "rider_1_plan_code",
    "rider_2_plan_code",
    "rider_3_plan_code",
    "rider_4_plan_code",
    "rider_5_plan_code",
    "issue_date",
    "app_received_date",
    "paid_to_date",
    "annual_premium",
    "billing_mode",
    "billing_form",
    "contract_code",
    "contract_date",
    "contract_reason",
    "term_date",
    "at_risk",
    "writing_agent_number",
    "writing_agent_name",
    "ga_number",
    "ga_name",
    "mga_1_number",
    "mga_1_name",
    "mga_2_number",
    "mga_2_name",
    "_source_file",
    "_source_modified_at",
    "file_date",
    "at_risk_policy",
    "raw_dlt_load_id",
    "raw_dlt_id",
    "_dlt_load_id",
    "_dlt_id",
)


def ahl_fym_policy_typed_select() -> str:
    """Normalize AHL values while retaining every source field."""
    return r"""
WITH source_rows AS (
    SELECT
        nullif(trim(p.policy_number::text), '') AS policy_number,
        nullif(trim(p.first_name::text), '') AS first_name,
        nullif(trim(p.last_name::text), '') AS last_name,
        nullif(trim(p.date_of_birth::text), '') AS date_of_birth_text,
        nullif(trim(p.phone::text), '') AS phone,
        nullif(trim(p.zip::text), '') AS zip,
        nullif(trim(p.email::text), '') AS email,
        nullif(trim(p.resident_state::text), '') AS resident_state,
        nullif(trim(p.issue_state::text), '') AS issue_state,
        nullif(trim(p.issue_age::text), '') AS issue_age_text,
        nullif(trim(p.base_plan_code::text), '') AS base_plan_code,
        nullif(trim(p.rider_1_plan_code::text), '') AS rider_1_plan_code,
        nullif(trim(p.rider_2_plan_code::text), '') AS rider_2_plan_code,
        nullif(trim(p.rider_3_plan_code::text), '') AS rider_3_plan_code,
        nullif(trim(p.rider_4_plan_code::text), '') AS rider_4_plan_code,
        nullif(trim(p.rider_5_plan_code::text), '') AS rider_5_plan_code,
        nullif(trim(p.issue_date::text), '') AS issue_date_text,
        nullif(trim(p.app_received_date::text), '') AS app_received_date_text,
        nullif(trim(p.paid_to_date::text), '') AS paid_to_date_text,
        nullif(trim(p.annual_premium::text), '') AS annual_premium_text,
        nullif(trim(p.billing_mode::text), '') AS billing_mode_text,
        nullif(trim(p.billing_form::text), '') AS billing_form,
        nullif(trim(p.contract_code::text), '') AS contract_code,
        nullif(trim(p.contract_date::text), '') AS contract_date_text,
        nullif(trim(p.contract_reason::text), '') AS contract_reason,
        nullif(trim(p.term_date::text), '') AS term_date_text,
        nullif(trim(p.at_risk::text), '') AS at_risk,
        nullif(trim(p.writing_agent_number::text), '') AS writing_agent_number,
        nullif(trim(p.writing_agent_name::text), '') AS writing_agent_name,
        nullif(trim(p.ga_number::text), '') AS ga_number,
        nullif(trim(p.ga_name::text), '') AS ga_name,
        nullif(trim(p.mga_1_number::text), '') AS mga_1_number,
        nullif(trim(p.mga_1_name::text), '') AS mga_1_name,
        nullif(trim(p.mga_2_number::text), '') AS mga_2_number,
        nullif(trim(p.mga_2_name::text), '') AS mga_2_name,
        p._source_file::text AS _source_file,
        p._source_modified_at::timestamptz AS _source_modified_at,
        p._dlt_load_id::text AS raw_dlt_load_id,
        p._dlt_id::text AS raw_dlt_id,
        p._dlt_load_id::text AS _dlt_load_id,
        p._dlt_id::text AS _dlt_id
    FROM raw.ahl_fym_policy AS p
),
typed_rows AS (
    SELECT
        policy_number,
        first_name,
        last_name,
        CASE WHEN date_of_birth_text ~ '^\d{8}$'
              AND to_char(to_date(date_of_birth_text, 'YYYYMMDD'), 'YYYYMMDD') = date_of_birth_text
             THEN to_date(date_of_birth_text, 'YYYYMMDD') END AS date_of_birth,
        phone,
        zip,
        email,
        resident_state,
        issue_state,
        CASE WHEN issue_age_text ~ '^\d+$' THEN issue_age_text::smallint END AS issue_age,
        base_plan_code,
        rider_1_plan_code,
        rider_2_plan_code,
        rider_3_plan_code,
        rider_4_plan_code,
        rider_5_plan_code,
        CASE WHEN issue_date_text ~ '^\d{8}$'
              AND to_char(to_date(issue_date_text, 'YYYYMMDD'), 'YYYYMMDD') = issue_date_text
             THEN to_date(issue_date_text, 'YYYYMMDD') END AS issue_date,
        CASE WHEN app_received_date_text ~ '^\d{8}$'
              AND to_char(to_date(app_received_date_text, 'YYYYMMDD'), 'YYYYMMDD') = app_received_date_text
             THEN to_date(app_received_date_text, 'YYYYMMDD') END AS app_received_date,
        CASE WHEN paid_to_date_text ~ '^\d{8}$'
              AND to_char(to_date(paid_to_date_text, 'YYYYMMDD'), 'YYYYMMDD') = paid_to_date_text
             THEN to_date(paid_to_date_text, 'YYYYMMDD') END AS paid_to_date,
        CASE WHEN annual_premium_text ~ '^[+-]?\d+(\.\d+)?$'
             THEN annual_premium_text::numeric END AS annual_premium,
        CASE WHEN billing_mode_text ~ '^\d+$' THEN billing_mode_text::integer END AS billing_mode,
        billing_form,
        contract_code,
        CASE WHEN contract_date_text ~ '^\d{8}$'
              AND to_char(to_date(contract_date_text, 'YYYYMMDD'), 'YYYYMMDD') = contract_date_text
             THEN to_date(contract_date_text, 'YYYYMMDD') END AS contract_date,
        contract_reason,
        CASE WHEN term_date_text ~ '^\d{8}$'
              AND to_char(to_date(term_date_text, 'YYYYMMDD'), 'YYYYMMDD') = term_date_text
             THEN to_date(term_date_text, 'YYYYMMDD') END AS term_date,
        at_risk,
        writing_agent_number,
        writing_agent_name,
        ga_number,
        ga_name,
        mga_1_number,
        mga_1_name,
        mga_2_number,
        mga_2_name,
        _source_file,
        _source_modified_at,
        coalesce(
            _source_modified_at::date,
            CASE WHEN raw_dlt_load_id ~ '^\d+(\.\d+)?$'
                 THEN to_timestamp(raw_dlt_load_id::double precision)::date END
        ) AS file_date,
        false AS at_risk_policy,
        raw_dlt_load_id,
        raw_dlt_id,
        _dlt_load_id,
        _dlt_id
    FROM source_rows
)
SELECT * FROM typed_rows
"""


def ahl_typed_refresh_statements() -> tuple[str, ...]:
    typed_select = ahl_fym_policy_typed_select()
    columns = ", ".join(AHL_TYPED_POLICY_COLUMNS)
    return (
        "CREATE SCHEMA IF NOT EXISTS typed",
        f"CREATE TABLE IF NOT EXISTS typed.ahl_fym_policy AS {typed_select} WITH NO DATA",
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS ahl_fym_policy_typed_dlt_id_idx "
            "ON typed.ahl_fym_policy (_dlt_id)"
        ),
        (
            f"INSERT INTO typed.ahl_fym_policy ({columns}) "
            f"SELECT {columns} FROM ({typed_select}) AS source "
            "ON CONFLICT (_dlt_id) DO NOTHING"
        ),
        """CREATE TABLE IF NOT EXISTS typed.ahl_fym_policy_change_history (
            _dlt_id text PRIMARY KEY,
            previous_contract_code text,
            contract_code_last_change_date date,
            previous_at_risk_status boolean,
            at_risk_status_last_change_date date
        )""",
        "TRUNCATE TABLE typed.ahl_fym_policy_change_history",
        _ahl_change_history_insert_statement(),
        """CREATE TABLE IF NOT EXISTS typed.ahl_fym_policy_at_risk_episodes (
            policy_number text NOT NULL,
            episode_id bigint NOT NULL,
            ep_start date,
            ep_end date,
            cure_date date,
            term_date date,
            outcome text,
            PRIMARY KEY (policy_number, episode_id)
        )""",
        # AHL at-risk derivation is intentionally disabled until the business rule is confirmed.
        "TRUNCATE TABLE typed.ahl_fym_policy_at_risk_episodes",
        _ahl_latest_load_view_statement("raw"),
        _ahl_latest_load_view_statement("typed"),
        "CREATE INDEX IF NOT EXISTS ahl_fym_policy_file_date_idx ON typed.ahl_fym_policy (file_date)",
        "CREATE INDEX IF NOT EXISTS ahl_fym_policy_policy_number_idx ON typed.ahl_fym_policy (policy_number)",
        "ANALYZE typed.ahl_fym_policy",
        "ANALYZE typed.ahl_fym_policy_change_history",
    )


def _ahl_change_history_insert_statement() -> str:
    return """INSERT INTO typed.ahl_fym_policy_change_history (
        _dlt_id,
        previous_contract_code,
        contract_code_last_change_date,
        previous_at_risk_status,
        at_risk_status_last_change_date
    )
    WITH ordered AS (
        SELECT
            p.*,
            row_number() OVER policy_order AS observation_number,
            lag(contract_code) OVER policy_order AS prior_contract_code,
            lag(at_risk_policy) OVER policy_order AS prior_at_risk_status
        FROM typed.ahl_fym_policy AS p
        WINDOW policy_order AS (
            PARTITION BY policy_number
            ORDER BY file_date NULLS FIRST, _source_modified_at NULLS FIRST, _source_file, _dlt_id
        )
    ),
    marked AS (
        SELECT
            ordered.*,
            CASE WHEN observation_number > 1
                       AND prior_contract_code IS DISTINCT FROM contract_code
                 THEN observation_number END AS contract_change_number,
            CASE WHEN observation_number > 1
                       AND prior_at_risk_status IS DISTINCT FROM at_risk_policy
                 THEN observation_number END AS at_risk_change_number
        FROM ordered
    ),
    resolved AS (
        SELECT
            marked.*,
            max(contract_change_number) OVER policy_progress AS latest_contract_change_number,
            max(at_risk_change_number) OVER policy_progress AS latest_at_risk_change_number
        FROM marked
        WINDOW policy_progress AS (
            PARTITION BY policy_number
            ORDER BY observation_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    )
    SELECT
        current_row._dlt_id,
        contract_change.prior_contract_code,
        contract_change.file_date,
        at_risk_change.prior_at_risk_status,
        at_risk_change.file_date
    FROM resolved AS current_row
    LEFT JOIN resolved AS contract_change
      ON contract_change.policy_number IS NOT DISTINCT FROM current_row.policy_number
     AND contract_change.observation_number = current_row.latest_contract_change_number
    LEFT JOIN resolved AS at_risk_change
      ON at_risk_change.policy_number IS NOT DISTINCT FROM current_row.policy_number
     AND at_risk_change.observation_number = current_row.latest_at_risk_change_number
    """


def _ahl_latest_load_view_statement(schema_name: str) -> str:
    if schema_name == "typed":
        projection = ",\n        ".join(f"p.{column}" for column in AHL_TYPED_POLICY_COLUMNS)
        latest_file_order = """max(_source_modified_at) DESC NULLS LAST,
            max(file_date) DESC NULLS LAST,
            max(raw_dlt_load_id) DESC NULLS LAST,"""
    else:
        projection = "p.*"
        latest_file_order = """max(_source_modified_at) DESC NULLS LAST,
            max(_dlt_load_id) DESC NULLS LAST,"""

    return f"""CREATE OR REPLACE VIEW {schema_name}.ahl_fym_policy_latest_load AS
    WITH latest_file AS (
        SELECT _source_file
        FROM {schema_name}.ahl_fym_policy
        GROUP BY _source_file
        ORDER BY
            {latest_file_order}
            _source_file DESC
        LIMIT 1
    )
    SELECT
        {projection},
        NULL::jsonb AS roster_hierarchy_json,
        'ahl'::text AS carrier,
        history.previous_contract_code,
        history.contract_code_last_change_date,
        history.previous_at_risk_status,
        history.at_risk_status_last_change_date
    FROM {schema_name}.ahl_fym_policy AS p
    JOIN latest_file AS latest
      ON latest._source_file = p._source_file
    LEFT JOIN typed.ahl_fym_policy_change_history AS history
      ON history._dlt_id = p._dlt_id
    -- TODO: add AHL roster hierarchy enrichment after its carrier mapping is defined.
    """
