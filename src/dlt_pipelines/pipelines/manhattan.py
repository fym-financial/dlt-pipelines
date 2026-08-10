"""PostgreSQL statements for the Manhattan policy typed dataset."""

from __future__ import annotations


MANHATTAN_TYPED_POLICY_COLUMNS = (
    "company",
    "block",
    "policy",
    "plan",
    "plan_desc",
    "first_name",
    "last_name",
    "app_rcvd",
    "issue_date",
    "paid_to_date",
    "modal_premium",
    "annual_premium",
    "status",
    "writing_agent_1_number",
    "writing_agent_1_split",
    "writing_agent_1_name",
    "writing_agent_2_number",
    "writing_agent_2_split",
    "writing_agent_2_name",
    "writing_agent_3_number",
    "writing_agent_3_split",
    "writing_agent_3_name",
    "writing_agent_4_number",
    "writing_agent_4_split",
    "writing_agent_4_name",
    "group_no_name",
    "owner_phone",
    "owner_email",
    "owner_freq_cd",
    "owner_pay_freq",
    "insured_zip",
    "issue_state",
    "_source_file",
    "_source_modified_at",
    "file_date",
    "at_risk_policy",
    "raw_dlt_load_id",
    "raw_dlt_id",
    "_dlt_load_id",
    "_dlt_id",
)


def manhattan_policy_typed_select() -> str:
    """Normalize Manhattan values while retaining every source field."""
    return r"""
WITH source_rows AS (
    SELECT
        nullif(trim(p.company::text), '') AS company,
        nullif(trim(p.block::text), '') AS block,
        nullif(trim(p.policy::text), '') AS policy,
        nullif(trim(p.plan::text), '') AS plan,
        nullif(trim(p.plan_desc::text), '') AS plan_desc,
        nullif(trim(p.first_name::text), '') AS first_name,
        nullif(trim(p.last_name::text), '') AS last_name,
        nullif(trim(p.app_rcvd::text), '') AS app_rcvd_text,
        nullif(trim(p.issue_date::text), '') AS issue_date_text,
        nullif(trim(p.paid_to_date::text), '') AS paid_to_date_text,
        nullif(trim(p.modal_premium::text), '') AS modal_premium_text,
        nullif(trim(p.annual_premium::text), '') AS annual_premium_text,
        nullif(trim(p.status::text), '') AS status,
        nullif(trim(p.wrtng_agt_1_nox::text), '') AS writing_agent_1_number,
        nullif(trim(p.wrtng_agt_1_split::text), '') AS writing_agent_1_split_text,
        nullif(trim(p.wrtng_agt_1_name::text), '') AS writing_agent_1_name,
        nullif(trim(p.wrtng_agt_2_nox::text), '') AS writing_agent_2_number,
        nullif(trim(p.wrtng_agt_2_split::text), '') AS writing_agent_2_split_text,
        nullif(trim(p.wrtng_agt_2_name::text), '') AS writing_agent_2_name,
        nullif(trim(p.wrtng_agt_3_nox::text), '') AS writing_agent_3_number,
        nullif(trim(p.wrtng_agt_3_split::text), '') AS writing_agent_3_split_text,
        nullif(trim(p.wrtng_agt_3_name::text), '') AS writing_agent_3_name,
        nullif(trim(p.wrtng_agt_4_nox::text), '') AS writing_agent_4_number,
        nullif(trim(p.wrtng_agt_4_split::text), '') AS writing_agent_4_split_text,
        nullif(trim(p.wrtng_agt_4_name::text), '') AS writing_agent_4_name,
        nullif(trim(p.group_no_name::text), '') AS group_no_name,
        nullif(trim(p.owner_phonex::text), '') AS owner_phone,
        nullif(trim(p.owner_email::text), '') AS owner_email,
        nullif(trim(p.owner_freq_cd::text), '') AS owner_freq_cd,
        nullif(trim(p.owner_pay_freq::text), '') AS owner_pay_freq,
        nullif(trim(p.insured_zip::text), '') AS insured_zip,
        nullif(trim(p.issue_state::text), '') AS issue_state,
        p._source_file::text AS _source_file,
        p._source_modified_at::timestamptz AS _source_modified_at,
        p._dlt_load_id::text AS raw_dlt_load_id,
        p._dlt_id::text AS raw_dlt_id,
        p._dlt_load_id::text AS _dlt_load_id,
        p._dlt_id::text AS _dlt_id
    FROM raw.manhattan_policy AS p
),
typed_rows AS (
    SELECT
        company,
        block,
        policy,
        plan,
        plan_desc,
        first_name,
        last_name,
        CASE WHEN app_rcvd_text ~ '^\d{2}/\d{2}/\d{4}$'
              AND to_char(to_date(app_rcvd_text, 'MM/DD/YYYY'), 'MM/DD/YYYY') = app_rcvd_text
             THEN to_date(app_rcvd_text, 'MM/DD/YYYY') END AS app_rcvd,
        CASE WHEN issue_date_text ~ '^\d{2}/\d{2}/\d{4}$'
              AND to_char(to_date(issue_date_text, 'MM/DD/YYYY'), 'MM/DD/YYYY') = issue_date_text
             THEN to_date(issue_date_text, 'MM/DD/YYYY') END AS issue_date,
        CASE WHEN paid_to_date_text ~ '^\d{2}/\d{2}/\d{4}$'
              AND to_char(to_date(paid_to_date_text, 'MM/DD/YYYY'), 'MM/DD/YYYY') = paid_to_date_text
             THEN to_date(paid_to_date_text, 'MM/DD/YYYY') END AS paid_to_date,
        CASE WHEN modal_premium_text ~ '^[+-]?\d+(\.\d+)?$'
             THEN modal_premium_text::numeric END AS modal_premium,
        CASE WHEN annual_premium_text ~ '^[+-]?\d+(\.\d+)?$'
             THEN annual_premium_text::numeric END AS annual_premium,
        status,
        writing_agent_1_number,
        CASE WHEN writing_agent_1_split_text ~ '^[+-]?\d+(\.\d+)?$'
             THEN writing_agent_1_split_text::numeric END AS writing_agent_1_split,
        writing_agent_1_name,
        writing_agent_2_number,
        CASE WHEN writing_agent_2_split_text ~ '^[+-]?\d+(\.\d+)?$'
             THEN writing_agent_2_split_text::numeric END AS writing_agent_2_split,
        writing_agent_2_name,
        writing_agent_3_number,
        CASE WHEN writing_agent_3_split_text ~ '^[+-]?\d+(\.\d+)?$'
             THEN writing_agent_3_split_text::numeric END AS writing_agent_3_split,
        writing_agent_3_name,
        writing_agent_4_number,
        CASE WHEN writing_agent_4_split_text ~ '^[+-]?\d+(\.\d+)?$'
             THEN writing_agent_4_split_text::numeric END AS writing_agent_4_split,
        writing_agent_4_name,
        group_no_name,
        owner_phone,
        owner_email,
        owner_freq_cd,
        owner_pay_freq,
        insured_zip,
        issue_state,
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


def manhattan_typed_refresh_statements() -> tuple[str, ...]:
    typed_select = manhattan_policy_typed_select()
    columns = ", ".join(MANHATTAN_TYPED_POLICY_COLUMNS)
    return (
        "CREATE SCHEMA IF NOT EXISTS typed",
        *_all_null_raw_column_statements(),
        f"CREATE TABLE IF NOT EXISTS typed.manhattan_policy AS {typed_select} WITH NO DATA",
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS manhattan_policy_typed_dlt_id_idx "
            "ON typed.manhattan_policy (_dlt_id)"
        ),
        (
            f"INSERT INTO typed.manhattan_policy ({columns}) "
            f"SELECT {columns} FROM ({typed_select}) AS source "
            "ON CONFLICT (_dlt_id) DO NOTHING"
        ),
        """CREATE TABLE IF NOT EXISTS typed.manhattan_policy_status_history (
            _dlt_id text PRIMARY KEY,
            previous_status text,
            status_last_change_date date
        )""",
        "TRUNCATE TABLE typed.manhattan_policy_status_history",
        _manhattan_status_history_insert_statement(),
        """CREATE TABLE IF NOT EXISTS typed.manhattan_policy_at_risk_episodes (
            policy text NOT NULL,
            episode_id bigint NOT NULL,
            ep_start date,
            ep_end date,
            cure_date date,
            term_date date,
            outcome text,
            PRIMARY KEY (policy, episode_id)
        )""",
        "TRUNCATE TABLE typed.manhattan_policy_at_risk_episodes",
        _manhattan_latest_load_view_statement("raw"),
        _manhattan_latest_load_view_statement("typed"),
        "CREATE INDEX IF NOT EXISTS manhattan_policy_file_date_idx ON typed.manhattan_policy (file_date)",
        "CREATE INDEX IF NOT EXISTS manhattan_policy_policy_idx ON typed.manhattan_policy (policy)",
        "ANALYZE typed.manhattan_policy",
        "ANALYZE typed.manhattan_policy_status_history",
    )


def _all_null_raw_column_statements() -> tuple[str, ...]:
    columns = (
        "wrtng_agt_2_nox",
        "wrtng_agt_2_split",
        "wrtng_agt_2_name",
        "wrtng_agt_3_nox",
        "wrtng_agt_3_split",
        "wrtng_agt_3_name",
        "wrtng_agt_4_nox",
        "wrtng_agt_4_split",
        "wrtng_agt_4_name",
    )
    return tuple(
        f"ALTER TABLE raw.manhattan_policy ADD COLUMN IF NOT EXISTS {column} text"
        for column in columns
    )


def _manhattan_status_history_insert_statement() -> str:
    return """INSERT INTO typed.manhattan_policy_status_history (
        _dlt_id,
        previous_status,
        status_last_change_date
    )
    WITH ordered AS (
        SELECT
            p.*,
            row_number() OVER policy_order AS observation_number,
            lag(status) OVER policy_order AS prior_status
        FROM typed.manhattan_policy AS p
        WINDOW policy_order AS (
            PARTITION BY policy
            ORDER BY file_date NULLS FIRST, _source_modified_at NULLS FIRST, _source_file, _dlt_id
        )
    ),
    marked AS (
        SELECT
            ordered.*,
            CASE WHEN observation_number > 1
                       AND prior_status IS DISTINCT FROM status
                 THEN observation_number END AS status_change_number
        FROM ordered
    ),
    resolved AS (
        SELECT
            marked.*,
            max(status_change_number) OVER policy_progress AS latest_status_change_number
        FROM marked
        WINDOW policy_progress AS (
            PARTITION BY policy
            ORDER BY observation_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    )
    SELECT
        current_row._dlt_id,
        status_change.prior_status,
        status_change.file_date
    FROM resolved AS current_row
    LEFT JOIN resolved AS status_change
      ON status_change.policy IS NOT DISTINCT FROM current_row.policy
     AND status_change.observation_number = current_row.latest_status_change_number
    """


def _manhattan_latest_load_view_statement(schema_name: str) -> str:
    if schema_name == "typed":
        projection = ",\n        ".join(
            f"p.{column}" for column in MANHATTAN_TYPED_POLICY_COLUMNS
        )
        latest_file_order = """max(_source_modified_at) DESC NULLS LAST,
            max(file_date) DESC NULLS LAST,
            max(raw_dlt_load_id) DESC NULLS LAST,"""
    else:
        projection = "p.*"
        latest_file_order = """max(_source_modified_at) DESC NULLS LAST,
            max(_dlt_load_id) DESC NULLS LAST,"""

    return f"""CREATE OR REPLACE VIEW {schema_name}.manhattan_policy_latest_load AS
    WITH latest_file AS (
        SELECT _source_file
        FROM {schema_name}.manhattan_policy
        GROUP BY _source_file
        ORDER BY
            {latest_file_order}
            _source_file DESC
        LIMIT 1
    )
    SELECT
        {projection},
        NULL::jsonb AS roster_hierarchy_json,
        'manhattan'::text AS carrier,
        history.previous_status,
        history.status_last_change_date
    FROM {schema_name}.manhattan_policy AS p
    JOIN latest_file AS latest
      ON latest._source_file = p._source_file
    LEFT JOIN typed.manhattan_policy_status_history AS history
      ON history._dlt_id = p._dlt_id
    -- TODO: add Manhattan roster hierarchy enrichment after its mapping is defined.
    """
