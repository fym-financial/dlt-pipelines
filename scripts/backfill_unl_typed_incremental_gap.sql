-- One-time recovery for rows skipped when the UNL typed DLT pipeline advanced
-- its cursor without inserting into the legacy table. Run this only after
-- deploying the source projection that preserves _dlt_id and _dlt_load_id.

BEGIN;

INSERT INTO typed.unl_fym_policy (
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
    _dlt_load_id,
    _dlt_id,
    cntrct_reason,
    cntrct_date,
    billing_form,
    term_date,
    file_date,
    at_risk_policy,
    raw_dlt_id,
    raw_dlt_load_id
)
SELECT
    source.mga,
    source.mga_name,
    source.ga,
    source.ga_name,
    source.wa,
    source.wa_name,
    source.agent_ga_level_01,
    source.agent_level_02,
    source.agent_level_03,
    source.agent_level_04,
    source.agent_level_05,
    source.agent_level_06,
    source.agent_level_07,
    source.agent_level_08,
    source.agent_level_09,
    source.agent_level_10,
    source.plan_code,
    source.issue_date,
    source.cntrct_code,
    source.app_recvd_date,
    source.annual_premium,
    source.issue_state,
    source.policy_nbr,
    source.paid_to_date,
    source.billing_mode,
    source.first_name,
    source.last_name,
    source.zip,
    source.phone_nbr,
    source._source_file,
    source._dlt_load_id,
    source._dlt_id,
    source.cntrct_reason,
    source.cntrct_date,
    source.billing_form,
    source.term_date,
    source.file_date,
    source.at_risk_policy,
    source.raw_dlt_id,
    source.raw_dlt_load_id
FROM raw.unl_fym_policy_typed_source AS source
WHERE NOT EXISTS (
    SELECT 1
    FROM typed.unl_fym_policy AS existing
    WHERE existing._dlt_id = source._dlt_id
       OR existing.raw_dlt_id = source.raw_dlt_id
)
ON CONFLICT DO NOTHING;

ANALYZE typed.unl_fym_policy;

COMMIT;

-- This should return zero after the insert.
SELECT count(*) AS missing_typed_rows
FROM raw.unl_fym_policy AS source
LEFT JOIN typed.unl_fym_policy AS typed
  ON typed._dlt_id = source._dlt_id
WHERE typed._dlt_id IS NULL;
