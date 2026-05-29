# UNL Data Load Runbook

This runbook covers the UNL inbound file process:

```text
UNL SFTP -> Backblaze B2/S3 landing -> PostgreSQL raw schema -> B2/S3 Archive
```

All commands assume secrets are injected by Infisical.

## Expected Files

The UNL SFTP account may expose files in the root directory and in a
`CommissionStatements` subdirectory.

```text
./FYM_Policy_YYYYMMDDHHMMSS.csv
./LifeProfessionals_Policy_YYYYMMDDHHMMSS.csv
/CommissionStatements/WC_202JVV00_YYYY_MM_DD.csv
/CommissionStatements/MC_202JVV00_YYYY_MM_DD.csv
```

The configured B2/S3 landing prefix is expected to be:

```text
fym-inbound-files/unl/inbound
```

## Expected Tables

Loaded files are routed by `.dlt/config.toml` into the `raw` schema:

```text
raw.unl_fym_policy
raw.unl_life_professionals_policy
raw.unl_weekly_commissions
raw.unl_monthly_commissions
```

Only routes with matching landed files load rows.
Routes without matching landed files are skipped.
CSV files that fail parsing are skipped. The skipped filename and parser error
are written to stderr so the file can be inspected later.

File lifecycle events are recorded in:

```text
audit.file_landings
```

Current statuses are:

```text
moved_to_landing
loaded_to_postgres
archived
```

## 1. Check PostgreSQL Connectivity

```bash
infisical run --env=dev -- uv run dlt-pipeline check-db
```

Expected outcome:

```text
PostgreSQL connection OK.
Database: analytics
User: analytics_loader
Schemas:
  audit: OK (exists=True, usage=True, create=True)
  raw: OK (exists=True, usage=True, create=True)
Server: PostgreSQL ...
```

If a schema is `NOT READY`, fix grants for `analytics_loader` before loading.

## 2. Inspect SFTP

```bash
infisical run --env=dev -- uv run dlt-pipeline move-sftp unl --list
```

Expected outcome:

```text
SFTP root: .
SFTP patterns:
  ./*.csv
  /*.csv
  ./CommissionStatements/*.csv
  /CommissionStatements/*.csv
SFTP listing: <count> item(s) found.
file ./FYM_Policy_YYYYMMDDHHMMSS.csv
file /CommissionStatements/WC_202JVV00_YYYY_MM_DD.csv
```

This command does not copy or delete files. It confirms what the SFTP server
currently exposes.

## 3. Preview Move

```bash
infisical run --env=dev -- uv run dlt-pipeline move-sftp unl --dry-run
```

Expected outcome:

```text
SFTP root: .
SFTP patterns:
  ./*.csv
  /*.csv
  ./CommissionStatements/*.csv
  /CommissionStatements/*.csv
S3 target root: fym-inbound-files
S3 target prefix: unl/inbound
Dry run: <count> file(s) would be moved to S3.
./FYM_Policy_YYYYMMDDHHMMSS.csv -> fym-inbound-files/unl/inbound/FYM_Policy_YYYYMMDDHHMMSS.csv
/CommissionStatements/WC_202JVV00_YYYY_MM_DD.csv -> fym-inbound-files/unl/inbound/CommissionStatements/WC_202JVV00_YYYY_MM_DD.csv
```

This command does not copy or delete files. Use it to verify source-to-target
paths before moving files.

## 4. Test One File Move

```bash
infisical run --env=dev -- uv run dlt-pipeline move-sftp unl --limit 1
```

Expected outcome:

```text
Connecting to SFTP.
Connecting to S3-compatible target.
Move plan contains 1 file(s).
[1/1] Preparing ...
[1/1] Ensuring target directory ...
[1/1] Opening source ...
[1/1] Opening target ...
[1/1] Copying bytes.
[1/1] Verifying size source=... target=...
[1/1] Deleting source ...
[1/1] Deleted source ...
[1/1] Moved ...
Moved 1 file(s) to S3.
```

This verifies SFTP read access, B2/S3 write access, size verification, and SFTP
delete permission.

## 5. Move All Files

```bash
infisical run --env=dev -- uv run dlt-pipeline move-sftp unl
```

Expected outcome:

```text
Connecting to SFTP.
Connecting to S3-compatible target.
Move plan contains <count> file(s).
...
Moved <count> file(s) to S3.
```

This command copies files into the B2/S3 landing prefix, verifies target size,
and deletes the original SFTP files.
It records `moved_to_landing` audit rows.

## 6. Load Landed Files

```bash
infisical run --env=dev -- uv run dlt-pipeline load-s3 unl
```

Expected outcome:

1. DLT loads matching landed files into PostgreSQL.
2. DLT prints load package information.
3. `loaded_to_postgres` audit rows are recorded.
4. After a successful load, files are moved into `Archive` subdirectories.
5. `archived` audit rows are recorded.

Archive paths are created beside each file's current directory:

```text
unl/inbound/FYM_Policy_YYYYMMDDHHMMSS.csv
-> unl/inbound/Archive/FYM_Policy_YYYYMMDDHHMMSS.csv

unl/inbound/CommissionStatements/WC_202JVV00_YYYY_MM_DD.csv
-> unl/inbound/CommissionStatements/Archive/WC_202JVV00_YYYY_MM_DD.csv
```

To load without archiving:

```bash
infisical run --env=dev -- uv run dlt-pipeline load-s3 unl --no-archive
```

## 7. Archive Only

Preview archive moves:

```bash
infisical run --env=dev -- uv run dlt-pipeline archive-s3 unl --dry-run
```

Run archive moves:

```bash
infisical run --env=dev -- uv run dlt-pipeline archive-s3 unl
```

Expected outcome:

```text
Archive plan contains <count> file(s).
...
Archived <count> landed file(s).
```

Use this only for recovery when files were loaded with `--no-archive` or a prior
archive step did not complete.
It records `archived` audit rows.

## 8. Full Flow

```bash
infisical run --env=dev -- uv run dlt-pipeline run-sftp-flow unl
```

Expected outcome:

1. Matching SFTP files are moved into B2/S3.
2. SFTP source files are deleted only after B2/S3 target size verification.
3. Landed files are routed and loaded into PostgreSQL.
4. Successfully loaded files are moved into Archive subdirectories.
5. Audit rows are recorded for move, load, and archive stages.

Use the full flow only after the connectivity, SFTP inspection, dry run, and
one-file move test pass.

## Recovery Notes

If `move-sftp` succeeds but `load-s3` fails, files remain in the active B2/S3
landing prefix and can be loaded again:

```bash
infisical run --env=dev -- uv run dlt-pipeline load-s3 unl
```

If `load-s3 unl --no-archive` was used, archive files after confirming the load:

```bash
infisical run --env=dev -- uv run dlt-pipeline archive-s3 unl
```

If SFTP files are moved with `--limit 1`, rerun `move-sftp unl --dry-run` to see
the remaining SFTP files before continuing.
