#!/usr/bin/env python3
"""Export legacy UNL weekly advance rows from SQLite as DLT-loadable CSV files."""

from __future__ import annotations

import argparse
import csv
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


EXPORT_COLUMNS = (
    "policy_nbr",
    "desc",
    "agent_nbr",
    "first_name",
    "last_name",
    "agency",
    "trans_type",
    "plan",
    "prem_paid_amt",
    "comm_rate",
    "comm_prem_amt",
    "adv_per",
    "amount",
    "effective_date",
    "paid_to_date",
    "last_activity_date",
)

SOURCE_EXPRESSIONS = {
    "policy_nbr": "trim(policy_number) AS policy_nbr",
    "desc": "trim(insured_name) AS desc",
    "agent_nbr": "trim(agent_number) AS agent_nbr",
    "first_name": "trim(first_name) AS first_name",
    "last_name": "trim(last_name) AS last_name",
    "agency": "trim(agency) AS agency",
    "trans_type": "trim(transaction_type) AS trans_type",
    "plan": "trim(plan) AS plan",
    "prem_paid_amt": "premium_paid_amount AS prem_paid_amt",
    "comm_rate": "commission_rate AS comm_rate",
    "comm_prem_amt": "commission_premium_amount AS comm_prem_amt",
    "adv_per": "advance_percent AS adv_per",
    "amount": "amount",
    "effective_date": "effective_date",
    "paid_to_date": "paid_to_date",
    "last_activity_date": "last_activity_date",
}


@dataclass(frozen=True)
class ReportExport:
    source_report_id: int
    statement_date: str
    original_filename: str
    output_filename: str
    row_count: int


def main() -> None:
    args = _parse_args()
    sqlite_path = args.sqlite_path.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(sqlite_path)
    connection.row_factory = sqlite3.Row
    try:
        reports = _reports_to_export(connection, args.start_date, args.end_date)
        if args.dry_run:
            for report in reports:
                print(
                    f"{report.source_report_id}: {report.statement_date} "
                    f"-> {output_dir / report.output_filename} ({report.row_count} rows)"
                )
            print(f"Dry run: {len(reports)} file(s), {sum(r.row_count for r in reports)} row(s).")
            return

        manifest_path = output_dir / "manifest.csv"
        with manifest_path.open("w", newline="") as manifest_file:
            manifest = csv.writer(manifest_file)
            manifest.writerow(
                (
                    "source_report_id",
                    "statement_date",
                    "original_filename",
                    "output_filename",
                    "row_count",
                )
            )
            for report in reports:
                row_count = _write_report_csv(connection, output_dir / report.output_filename, report)
                manifest.writerow(
                    (
                        report.source_report_id,
                        report.statement_date,
                        report.original_filename,
                        report.output_filename,
                        row_count,
                    )
                )

        print(f"Wrote {len(reports)} file(s) to {output_dir}")
        print(f"Wrote manifest: {manifest_path}")
        print(f"Total rows: {sum(r.row_count for r in reports)}")
    finally:
        connection.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export reports_unlweeklyadvance rows into WA_*.csv files.",
    )
    parser.add_argument(
        "--sqlite-path",
        type=Path,
        default=Path("db.sqlite3"),
        help="Path to the legacy SQLite database. Defaults to ./db.sqlite3.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/unl-weekly-advance-backfill"),
        help="Directory where WA CSV files will be written.",
    )
    parser.add_argument(
        "--start-date",
        help="Optional inclusive statement date filter in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--end-date",
        help="Optional inclusive statement date filter in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the files that would be written without creating CSVs.",
    )
    return parser.parse_args()


def _reports_to_export(
    connection: sqlite3.Connection,
    start_date: str | None,
    end_date: str | None,
) -> list[ReportExport]:
    _validate_date_arg(start_date, "--start-date")
    _validate_date_arg(end_date, "--end-date")

    filters = []
    params: list[str] = []
    if start_date:
        filters.append("statement_date >= ?")
        params.append(start_date)
    if end_date:
        filters.append("statement_date <= ?")
        params.append(end_date)
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

    query = f"""
        WITH report_rows AS (
            SELECT
                u.report_ptr_id AS source_report_id,
                date(
                    substr(u.option_value, 7, 4) || '-' ||
                    substr(u.option_value, 1, 2) || '-' ||
                    substr(u.option_value, 4, 2)
                ) AS statement_date,
                rr.original_filename,
                count(a.id) AS row_count
            FROM reports_unlreport u
            JOIN reports_report rr ON rr.id = u.report_ptr_id
            JOIN reports_unlweeklyadvance a ON a.source_report_id = u.report_ptr_id
            WHERE u.commission_type = 'WA'
            GROUP BY u.report_ptr_id, u.option_value, rr.original_filename
        )
        SELECT *
        FROM report_rows
        {where_clause}
        ORDER BY statement_date, source_report_id
    """
    rows = connection.execute(query, params).fetchall()
    return [
        ReportExport(
            source_report_id=int(row["source_report_id"]),
            statement_date=row["statement_date"],
            original_filename=row["original_filename"],
            output_filename=_output_filename(row["statement_date"]),
            row_count=int(row["row_count"]),
        )
        for row in rows
    ]


def _write_report_csv(
    connection: sqlite3.Connection,
    output_path: Path,
    report: ReportExport,
) -> int:
    column_list = ", ".join(_select_expression(column) for column in EXPORT_COLUMNS)
    query = f"""
        SELECT {column_list}
        FROM reports_unlweeklyadvance
        WHERE source_report_id = ?
        ORDER BY row_number
    """
    rows = connection.execute(query, (report.source_report_id,))
    row_count = 0
    with output_path.open("w", newline="") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(EXPORT_COLUMNS)
        for row in rows:
            writer.writerow([row[column] for column in EXPORT_COLUMNS])
            row_count += 1

    if row_count != report.row_count:
        raise RuntimeError(
            f"Expected {report.row_count} row(s) for {report.output_filename}, wrote {row_count}."
        )
    return row_count


def _select_expression(column: str) -> str:
    return SOURCE_EXPRESSIONS[column]


def _output_filename(statement_date: str) -> str:
    parsed = datetime.strptime(statement_date, "%Y-%m-%d").date()
    return f"WA_202JVV00_{parsed:%Y_%m_%d}.csv"


def _validate_date_arg(value: str | None, arg_name: str) -> None:
    if value is None:
        return
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(f"{arg_name} must be formatted as YYYY-MM-DD.") from exc


if __name__ == "__main__":
    main()
