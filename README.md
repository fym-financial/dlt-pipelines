# DLT Pipelines

ETL project for loading data from multiple sources into PostgreSQL with
[dlt](https://dlthub.com/). Secrets are expected to come from Infisical and be
injected into the process environment when a pipeline runs.

## Project Layout

```text
.
├── .dlt/
│   ├── config.toml
│   └── secrets.toml.example
├── data/
│   └── sample_customers.csv
├── src/dlt_pipelines/
│   ├── cli.py
│   ├── pipelines/load.py
│   ├── transfers.py
│   └── sources/
│       ├── api.py
│       ├── files.py
│       └── s3.py
├── tests/
│   └── test_sources.py
├── docker-compose.yml
└── pyproject.toml
```

## Setup

Install dependencies:

```bash
uv sync
```

Start a local PostgreSQL database:

```bash
docker compose up -d postgres
```

For local development, the baseline `docker-compose.yml` database uses:

- Database: `dlt_data`
- User: `loader`
- Password: `loader`
- Host: `localhost`
- Port: `5432`

## File Flow

The SFTP flow is intentionally split into two stages:

1. Copy raw files from SFTP into an S3 landing location.
2. Route landed files by path/name and load each file type into its configured
   PostgreSQL table with DLT.

Run both stages together:

```bash
infisical run --env=dev -- uv run dlt-pipeline run-sftp-flow unl
```

This now also refreshes the typed PostgreSQL table by default. To skip that refresh:

```bash
infisical run --env=dev -- uv run dlt-pipeline run-sftp-flow unl --no-refresh-typed
```

Copy files from SFTP to B2/S3 without deleting SFTP sources:

```bash
infisical run --env=dev -- uv run dlt-pipeline land-sftp unl
```

Move files from SFTP to B2/S3 after verifying target size:

```bash
infisical run --env=dev -- uv run dlt-pipeline move-sftp unl
```

Test one move with progress output:

```bash
infisical run --env=dev -- uv run dlt-pipeline move-sftp unl --limit 1
```

Preview which files would be copied and where they would land:

```bash
infisical run --env=dev -- uv run dlt-pipeline land-sftp unl --dry-run
```

List what the SFTP server returns under the configured root:

```bash
infisical run --env=dev -- uv run dlt-pipeline land-sftp unl --list
```

Run only the S3-to-Postgres load step:

```bash
infisical run --env=dev -- uv run dlt-pipeline load-s3 unl
```

This also refreshes the typed PostgreSQL table by default. To skip that refresh:

```bash
infisical run --env=dev -- uv run dlt-pipeline load-s3 unl --no-refresh-typed
```

Refresh the typed PostgreSQL table from `raw` without running a new file load:

```bash
infisical run --env=dev -- uv run dlt-pipeline refresh-typed unl
```

This runs a separate DLT SQL-source pipeline from `raw.unl_fym_policy` to
`typed.unl_fym_policy`. Its first run backfills the full raw table; later runs
use `raw_dlt_load_id` as a persisted incremental cursor and merge only new rows.
Roster snapshots and `typed.unl_weekly_advance_statements` are still rebuilt,
and the latest-load views are migrated before the data refresh.

The FYM policy typed history and both FYM policy latest-load views include:

- `previous_contract_code` and `contract_code_last_change_date`
- `previous_at_risk_status` and `at_risk_status_last_change_date`

The previous value is the value immediately before the most recent observed
change for that policy. Both fields in a pair are null until a change is
observed. Change history is appended only for typed rows that do not already
have a history record, so normal refreshes do not rewrite historical rows.
If a newly ingested raw load has an older file date, history is rebuilt only
for the policies affected by that out-of-order backfill.

After a successful `load-s3`, matching landed files are moved into an `Archive`
subdirectory beside their current B2/S3 location.

For the complete operational command sequence, see
[`docs/unl-runbook.md`](docs/unl-runbook.md).

## Infisical Secrets

The baseline pipeline uses DLT's PostgreSQL destination config. Store these
secrets in Infisical and inject them as environment variables at runtime:

| Infisical secret name | Required | Local baseline value | Notes |
| --- | --- | --- | --- |
| `DESTINATION__POSTGRES__CREDENTIALS__DATABASE` | Yes | `dlt_data` | PostgreSQL database name. |
| `DESTINATION__POSTGRES__CREDENTIALS__USERNAME` | Yes | `loader` | PostgreSQL user. |
| `DESTINATION__POSTGRES__CREDENTIALS__PASSWORD` | Yes | `loader` | Use the real password outside local dev. |
| `DESTINATION__POSTGRES__CREDENTIALS__HOST` | Yes | `localhost` | Use the service host in deployed environments. |
| `DESTINATION__POSTGRES__CREDENTIALS__PORT` | Yes | `5432` | PostgreSQL port. |
| `DESTINATION__POSTGRES__CREDENTIALS__CONNECT_TIMEOUT` | No | `15` | Connection timeout in seconds. |

The UNL typed refresh also snapshots roster tables from the application database
into `raw_roster` before rebuilding typed views. By default it reads
`fym_prod` on the same PostgreSQL instance and reuses the destination
host/user/password/port. Override these when refreshing from staging or when the
roster source uses different credentials:

| Infisical secret name | Required | Example | Notes |
| --- | --- | --- | --- |
| `ROSTER__POSTGRES__CREDENTIALS__DATABASE` | No | `fym_staging` | Defaults to `fym_prod`. Use `fym_prod` or `fym_staging`. |
| `ROSTER__POSTGRES__CREDENTIALS__USERNAME` | No | `loader` | Defaults to the destination username. |
| `ROSTER__POSTGRES__CREDENTIALS__PASSWORD` | No | `...` | Defaults to the destination password. |
| `ROSTER__POSTGRES__CREDENTIALS__HOST` | No | `localhost` | Defaults to the destination host. |
| `ROSTER__POSTGRES__CREDENTIALS__PORT` | No | `5432` | Defaults to the destination port. |
| `ROSTER__POSTGRES__CREDENTIALS__CONNECT_TIMEOUT` | No | `15` | Defaults to the destination timeout. |
| `ROSTER__POSTGRES__SCHEMA` | No | `public` | Source schema containing the `roster_*` tables. |

The SFTP-to-S3 flow uses provider-scoped secrets for the `unl` provider:

| Infisical secret name | Required | Example | Notes |
| --- | --- | --- | --- |
| `SFTP_PROVIDERS__UNL__SFTP__BUCKET_URL` | Yes | `sftp://sftp.example.com/inbound` | Base SFTP folder to scan. |
| `SFTP_PROVIDERS__UNL__SFTP__FILE_GLOBS` | No | `*.csv,CommissionStatements/*.csv` | Comma-separated file patterns to copy from SFTP. |
| `SFTP_PROVIDERS__UNL__SFTP__USERNAME` | Depends | `etl-user` | Required for username/password or key auth. |
| `SFTP_PROVIDERS__UNL__SFTP__PASSWORD` | Depends | `...` | Required for password auth. |
| `SFTP_PROVIDERS__UNL__SFTP__PORT` | No | `22` | SFTP port. |
| `SFTP_PROVIDERS__UNL__SFTP__KEY_FILENAME` | Depends | `/run/secrets/sftp_key` | Required for key auth if not using password auth. |
| `SFTP_PROVIDERS__UNL__SFTP__KEY_PASSPHRASE` | No | `...` | Private key passphrase, if applicable. |
| `SFTP_PROVIDERS__UNL__S3__LANDING__BUCKET_URL` | Yes | `s3://my-landing-bucket/raw` | S3 landing bucket or prefix root. |
| `SFTP_PROVIDERS__UNL__S3__LANDING__PREFIX` | No | `unl/inbound` | Provider-specific prefix under the landing URL. |
| `AWS_ACCESS_KEY_ID` | Yes | `...` | Backblaze Application Key ID. |
| `AWS_SECRET_ACCESS_KEY` | Yes | `...` | Backblaze Application Key. |
| `AWS_SESSION_TOKEN` | No | `...` | Required for temporary AWS credentials. |
| `AWS_REGION` | Yes | `us-west-004` | Backblaze bucket region. |
| `S3_ENDPOINT_URL` | Yes | `https://s3.us-west-004.backblazeb2.com` | Used by the SFTP-to-S3 copy step. |

For Backblaze B2, create an application key with access to the landing bucket.
Use the Backblaze **Application Key ID** as `AWS_ACCESS_KEY_ID` and the
Backblaze **Application Key** as `AWS_SECRET_ACCESS_KEY`. The endpoint format is
`https://s3.<region>.backblazeb2.com`. The loader mirrors these values into
DLT's filesystem-source credential variables before reading from B2.

## File Routes

Raw files are copied from SFTP to B2/S3 without parsing. Database loading is
controlled by explicit file routes in `.dlt/config.toml`.

Each route maps a landed file pattern to a parser and destination table:

```toml
[load]
workers = 1
parallelism_strategy = "sequential"

[[sftp_providers.unl.routes]]
name = "fym_policy"
file_glob = "unl/inbound/FYM_Policy_*.csv"
parser = "csv"
table_name = "unl_fym_policy"

[[sftp_providers.unl.routes]]
name = "life_professionals_policy"
file_glob = "unl/inbound/LifeProfessionals_Policy_*.csv"
parser = "csv"
table_name = "unl_life_professionals_policy"

[[sftp_providers.unl.routes]]
name = "weekly_commissions"
file_glob = "unl/inbound/CommissionStatements/WC_*.csv"
parser = "csv"
table_name = "unl_weekly_commissions"

[[sftp_providers.unl.routes]]
name = "monthly_commissions"
file_glob = "unl/inbound/CommissionStatements/MC_*.csv"
parser = "csv"
table_name = "unl_monthly_commissions"

[[sftp_providers.unl.routes]]
name = "weekly_advance_statements"
file_glob = "unl/inbound/CommissionStatements/WA_*.csv"
parser = "csv"
table_name = "unl_weekly_advance_statements"

[[sftp_providers.unl.routes]]
name = "monthly_advance_statements"
file_glob = "unl/inbound/CommissionStatements/MA_*.csv"
parser = "csv"
table_name = "unl_monthly_advance_statements"
```

The explicit `[load]` settings are intentional. With `dlt` 1.27.2, the load
stage otherwise defaults to a much higher worker count and can fan out several
parallel PostgreSQL `insert_values` jobs during one load package.

Supported baseline parsers are `csv`, `jsonl`, and `parquet`.

Do not use one catch-all table for mixed schemas unless the business decision is
to store raw records as semi-structured JSON. The default project behavior is to
load each known file type into its own table.

You can also set the PostgreSQL credentials as one connection string instead of
the individual fields:

```bash
DESTINATION__POSTGRES__CREDENTIALS=postgresql://loader:loader@localhost:5432/dlt_data
```

Prefer the split-field secrets when possible. They are easier to rotate and
less error-prone when host, user, or password changes independently.

Run a pipeline through Infisical:

```bash
infisical run --env=dev -- uv run dlt-pipeline load csv
```

Check PostgreSQL connectivity with the same credentials used by DLT:

```bash
infisical run --env=dev -- uv run dlt-pipeline check-db
```

Use the appropriate Infisical environment for the target deployment, such as
`dev`, `staging`, or `prod`.

## Local Secrets Fallback

For manual local runs without Infisical, copy the example TOML file:

```bash
cp .dlt/secrets.toml.example .dlt/secrets.toml
```

`.dlt/secrets.toml` is gitignored and should not be committed. Infisical
environment variables remain the preferred path.

## Run Pipelines

Load the local CSV sample:

```bash
uv run dlt-pipeline load csv
```

Load the REST API template source:

```bash
uv run dlt-pipeline load api
```

Load landed S3 CSV files into Postgres:

```bash
uv run dlt-pipeline load-s3 unl
```

The generic loader also supports the S3 source when a provider is supplied:

```bash
uv run dlt-pipeline load s3 unl
```

Use a different destination schema:

```bash
uv run dlt-pipeline load csv --dataset staging
```

## Adding Sources

Add new extractors under `src/dlt_pipelines/sources/`, then register them in
`SOURCE_FACTORIES` in `src/dlt_pipelines/pipelines/load.py`.

If a source needs credentials, define those as Infisical secrets using DLT's
double-underscore environment variable convention, then document them in this
README next to the Postgres baseline.
