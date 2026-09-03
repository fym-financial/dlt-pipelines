"""Command-line entry point for running DLT pipelines."""

from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import PurePosixPath

from dlt_pipelines.db import (
    FileAuditEvent,
    check_unl_fym_policy_loaded,
    check_postgres_connection,
    prepare_ahl_rebuild,
    record_file_events,
    refresh_typed_dataset,
    verify_raw_file_loads,
)
from dlt_pipelines.pipelines.load import SOURCE_CHOICES, run_pipeline
from dlt_pipelines.sources.s3 import validate_landed_csv_contracts
from dlt_pipelines.transfers import (
    archive_landed_files,
    describe_sftp_scan,
    land_sftp_files_to_s3,
    list_sftp_files,
    move_sftp_files_to_s3,
    plan_landed_files_archive,
    plan_sftp_files_to_s3,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run DLT file pipelines.")
    subparsers = parser.add_subparsers(dest="command")

    load_parser = subparsers.add_parser("load", help="Load a source into PostgreSQL.")
    load_parser.add_argument(
        "source",
        choices=SOURCE_CHOICES,
        help="Source extractor to run.",
    )
    load_parser.add_argument(
        "provider",
        nargs="?",
        help="Provider key. Required when source is s3.",
    )
    load_parser.add_argument(
        "--dataset",
        default="raw",
        help="Destination dataset/schema name in PostgreSQL.",
    )
    load_parser.add_argument(
        "--pipeline-name",
        default="dlt_pipelines",
        help="DLT pipeline state name.",
    )

    load_heartland_parser = subparsers.add_parser(
        "load-heartland",
        help="Load Heartland inforced policies into raw and typed PostgreSQL tables.",
    )
    load_heartland_parser.add_argument(
        "--dataset",
        default="raw",
        help="Raw destination dataset/schema name in PostgreSQL.",
    )
    load_heartland_parser.add_argument(
        "--pipeline-name",
        default="heartland_api",
        help="DLT pipeline state name.",
    )
    load_heartland_parser.add_argument(
        "--no-refresh-typed",
        action="store_true",
        help="Skip promoting the snapshot into canonical raw and typed history.",
    )

    land_parser = subparsers.add_parser("land-sftp", help="Copy matching SFTP files into S3.")
    land_parser.add_argument("provider", help="Provider key, such as unl.")
    land_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List matching SFTP files and target S3 paths without copying.",
    )
    land_parser.add_argument(
        "--list",
        action="store_true",
        help="List files and directories under the configured SFTP root.",
    )
    land_parser.add_argument(
        "--limit",
        type=int,
        help="Copy at most this many files. Useful for testing.",
    )

    move_parser = subparsers.add_parser(
        "move-sftp",
        help="Copy matching SFTP files into S3, verify them, then delete SFTP sources.",
    )
    move_parser.add_argument("provider", help="Provider key, such as unl.")
    move_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List matching SFTP files and target S3 paths without moving.",
    )
    move_parser.add_argument(
        "--list",
        action="store_true",
        help="List files and directories under the configured SFTP root.",
    )
    move_parser.add_argument(
        "--limit",
        type=int,
        help="Move at most this many files. Useful for testing.",
    )

    load_s3_parser = subparsers.add_parser(
        "load-s3",
        help="Load one provider's landed S3 files into PostgreSQL.",
    )
    load_s3_parser.add_argument("provider", help="Provider key, such as unl.")
    load_s3_parser.add_argument(
        "--dataset",
        default="raw",
        help="Destination dataset/schema name in PostgreSQL.",
    )
    load_s3_parser.add_argument(
        "--pipeline-name",
        default="dlt_pipelines",
        help="DLT pipeline state name.",
    )
    load_s3_parser.add_argument(
        "--no-archive",
        action="store_true",
        help="Do not move landed files into Archive after a successful load.",
    )
    load_s3_parser.add_argument(
        "--no-refresh-typed",
        action="store_true",
        help="Skip refreshing the provider's typed PostgreSQL tables after a successful raw load.",
    )
    load_s3_parser.add_argument(
        "--rebuild-ahl",
        action="store_true",
        help=(
            "Destructively replace AHL raw data and DLT resource state, then rebuild "
            "AHL typed objects. Valid only with provider ahl."
        ),
    )

    archive_parser = subparsers.add_parser(
        "archive-s3",
        help="Move provider landed files into Archive subdirectories.",
    )
    archive_parser.add_argument("provider", help="Provider key, such as unl.")
    archive_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List landed files and Archive target paths without moving.",
    )

    flow_parser = subparsers.add_parser(
        "run-sftp-flow",
        help="Move SFTP files into S3, then load landed S3 files into PostgreSQL.",
    )
    flow_parser.add_argument("provider", help="Provider key, such as unl.")
    flow_parser.add_argument(
        "--dataset",
        default="raw",
        help="Destination dataset/schema name in PostgreSQL.",
    )
    flow_parser.add_argument(
        "--pipeline-name",
        default="dlt_pipelines",
        help="DLT pipeline state name.",
    )
    flow_parser.add_argument(
        "--no-refresh-typed",
        action="store_true",
        help="Skip refreshing the provider's typed PostgreSQL tables after a successful raw load.",
    )

    refresh_typed_parser = subparsers.add_parser(
        "refresh-typed",
        help="Refresh provider-specific typed PostgreSQL tables from raw data.",
    )
    refresh_typed_parser.add_argument("provider", help="Provider key, such as unl.")

    check_fym_policy_parser = subparsers.add_parser(
        "check-fym-policy-load",
        help="Check whether the latest UNL FYM policy load is recent enough.",
    )
    check_fym_policy_parser.add_argument(
        "--max-age-hours",
        type=float,
        default=30.0,
        help="Maximum age in hours for the latest loaded FYM policy file. Defaults to 30.",
    )

    subparsers.add_parser(
        "check-db",
        help="Verify PostgreSQL connectivity using configured destination credentials.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()

    if args.command == "land-sftp":
        if _handle_sftp_inspection(args, action_name="copied"):
            return

        copied_paths = land_sftp_files_to_s3(
            args.provider,
            limit=args.limit,
            progress=_print_progress,
        )
        _record_copied_files(args.provider, copied_paths)
        print(f"Copied {len(copied_paths)} file(s) to S3.")
        for path in copied_paths:
            print(path)
        return

    if args.command == "move-sftp":
        if _handle_sftp_inspection(args, action_name="moved"):
            return

        moved = move_sftp_files_to_s3(
            args.provider,
            limit=args.limit,
            progress=_print_progress,
        )
        _record_moved_files(args.provider, moved)
        print(f"Moved {len(moved)} file(s) to S3.")
        for result in moved:
            print(f"{result.source_path} -> {result.target_path}")
        return

    if args.command == "load-s3":
        if args.rebuild_ahl and args.provider != "ahl":
            raise RuntimeError("--rebuild-ahl is valid only for provider 'ahl'.")
        if args.rebuild_ahl and args.no_refresh_typed:
            raise RuntimeError("--rebuild-ahl cannot be combined with --no-refresh-typed.")
        if args.rebuild_ahl:
            validated_files = validate_landed_csv_contracts("ahl")
            print(f"Validated {validated_files} landed AHL file contract(s).")
            prepare_ahl_rebuild()

        if args.rebuild_ahl:
            load_info = run_pipeline(
                source_name="s3",
                provider=args.provider,
                dataset_name=args.dataset,
                pipeline_name=args.pipeline_name,
                refresh="drop_resources",
            )
        else:
            load_info = run_pipeline(
                source_name="s3",
                provider=args.provider,
                dataset_name=args.dataset,
                pipeline_name=args.pipeline_name,
            )
        print(load_info)
        archive_plan = plan_landed_files_archive(args.provider)
        _verify_raw_loads(archive_plan, schema_name=args.dataset)
        _record_loaded_files(args.provider, archive_plan)
        if not args.no_refresh_typed:
            _refresh_typed_and_print(args.provider)
        if not args.no_archive:
            archived = archive_landed_files(
                args.provider,
                progress=_print_progress,
                plan=archive_plan,
            )
            _record_archived_files(args.provider, archived)
            print(f"Archived {len(archived)} landed file(s).")
        return

    if args.command == "load-heartland":
        load_info = run_pipeline(
            source_name="heartland",
            dataset_name=args.dataset,
            pipeline_name=args.pipeline_name,
        )
        print(load_info)
        if not args.no_refresh_typed:
            _refresh_typed_and_print("heartland")
        return

    if args.command == "archive-s3":
        if args.dry_run:
            plan = plan_landed_files_archive(args.provider)
            print(f"Dry run: {len(plan)} landed file(s) would be archived.")
            for item in plan:
                print(f"{item.source_path} -> {item.archive_path}")
            return

        archived = archive_landed_files(args.provider, progress=_print_progress)
        _record_archived_files(args.provider, archived)
        print(f"Archived {len(archived)} landed file(s).")
        for item in archived:
            print(f"{item.source_path} -> {item.archive_path}")
        return

    if args.command == "run-sftp-flow":
        moved = move_sftp_files_to_s3(args.provider, progress=_print_progress)
        _record_moved_files(args.provider, moved)
        print(f"Moved {len(moved)} file(s) to S3.")
        if not moved:
            print(f"No files moved for provider {args.provider}; skipping S3 load.")
            return
        load_info = run_pipeline(
            source_name="s3",
            provider=args.provider,
            dataset_name=args.dataset,
            pipeline_name=args.pipeline_name,
        )
        print(load_info)
        archive_plan = plan_landed_files_archive(args.provider)
        _verify_raw_loads(archive_plan, schema_name=args.dataset)
        _record_loaded_files(args.provider, archive_plan)
        if not args.no_refresh_typed:
            _refresh_typed_and_print(args.provider)
        archived = archive_landed_files(
            args.provider,
            progress=_print_progress,
            plan=archive_plan,
        )
        _record_archived_files(args.provider, archived)
        print(f"Archived {len(archived)} landed file(s).")
        return

    if args.command == "refresh-typed":
        _refresh_typed_and_print(args.provider)
        return

    if args.command == "check-fym-policy-load":
        result = check_unl_fym_policy_loaded(max_age=timedelta(hours=args.max_age_hours))
        if result.is_recent:
            print(
                f"Latest loaded UNL FYM policy file is recent: {result.file_name} "
                f"({result.status} at {result.landed_at}, age {_format_timedelta(result.age)})."
            )
            return
        if result.found:
            print(
                f"Latest loaded UNL FYM policy file is stale: {result.file_name} "
                f"({result.status} at {result.landed_at}, age {_format_timedelta(result.age)}; "
                f"max age {_format_timedelta(result.max_age)})."
            )
            raise SystemExit(2)
        print(
            "Missing loaded UNL FYM policy file: expected latest audit row with "
            f"file_name matching {result.file_pattern} and status loaded_to_postgres."
        )
        raise SystemExit(2)

    if args.command == "check-db":
        result = check_postgres_connection()
        print("PostgreSQL connection OK.")
        print(f"Database: {result.database}")
        print(f"User: {result.user}")
        print("Schemas:")
        for schema in result.schemas:
            status = "OK" if schema.exists and schema.has_usage and schema.has_create else "NOT READY"
            print(
                f"  {schema.name}: {status} "
                f"(exists={schema.exists}, usage={schema.has_usage}, create={schema.has_create})"
            )
        print(f"Server: {result.server_version}")
        return

    if args.command != "load":
        build_parser().print_help()
        return

    load_info = run_pipeline(
        source_name=args.source,
        provider=args.provider,
        dataset_name=args.dataset,
        pipeline_name=args.pipeline_name,
    )
    print(load_info)


def _print_progress(message: str) -> None:
    print(message, flush=True)


def _refresh_typed_and_print(provider: str) -> None:
    result = refresh_typed_dataset(provider)
    print(
        f"Refreshed {result.schema_name}.{result.table_name} for provider "
        f"{result.provider} with {result.row_count} row(s)."
    )


def _format_timedelta(value: timedelta | None) -> str:
    if value is None:
        return "unknown"
    total_seconds = max(0, int(value.total_seconds()))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    return f"{hours}h {minutes}m"


def _record_copied_files(provider: str, copied_paths: list[str]) -> None:
    events = [
        FileAuditEvent(
            provider=provider,
            source_path=path,
            target_path=path,
            status="copied_to_landing",
        )
        for path in copied_paths
    ]
    count = record_file_events(events)
    print(f"Recorded {count} copied file audit event(s).")


def _record_moved_files(provider: str, moved) -> None:
    events = [
        FileAuditEvent(
            provider=provider,
            source_path=result.source_path,
            target_path=result.target_path,
            file_size_bytes=result.target_size or result.source_size,
            status="moved_to_landing",
        )
        for result in moved
    ]
    count = record_file_events(events)
    print(f"Recorded {count} moved file audit event(s).")


def _record_loaded_files(provider: str, archive_plan) -> None:
    events = [
        FileAuditEvent(
            provider=provider,
            source_path=item.source_path,
            target_path=item.source_path,
            status="loaded_to_postgres",
        )
        for item in archive_plan
    ]
    count = record_file_events(events)
    print(f"Recorded {count} loaded file audit event(s).")


def _verify_raw_loads(archive_plan, *, schema_name: str) -> None:
    expected_files = [
        (item.table_name, PurePosixPath(item.source_path).name)
        for item in archive_plan
    ]
    verify_raw_file_loads(expected_files, schema_name=schema_name)
    print(f"Verified {len(expected_files)} loaded file(s) in {schema_name}.")


def _record_archived_files(provider: str, archived) -> None:
    events = [
        FileAuditEvent(
            provider=provider,
            source_path=item.source_path,
            target_path=item.archive_path,
            status="archived",
        )
        for item in archived
    ]
    count = record_file_events(events)
    print(f"Recorded {count} archived file audit event(s).")


def _handle_sftp_inspection(args: argparse.Namespace, *, action_name: str) -> bool:
    if args.list:
        context = describe_sftp_scan(args.provider)
        items = list_sftp_files(args.provider)
        print(f"SFTP root: {context.source_root}")
        print("SFTP patterns:")
        for pattern in context.source_patterns:
            print(f"  {pattern}")
        print(f"SFTP listing: {len(items)} item(s) found.")
        for item in items:
            item_type = "dir " if item.is_dir else "file"
            print(f"{item_type} {item.path}")
        return True

    if args.dry_run:
        context = describe_sftp_scan(args.provider)
        plan = plan_sftp_files_to_s3(args.provider)
        print(f"SFTP root: {context.source_root}")
        print("SFTP patterns:")
        for pattern in context.source_patterns:
            print(f"  {pattern}")
        print(f"S3 target root: {context.target_root}")
        print(f"S3 target prefix: {context.target_prefix}")
        print(f"Dry run: {len(plan)} file(s) would be {action_name} to S3.")
        for item in plan:
            print(f"{item.source_path} -> {item.target_path}")
        return True

    return False


if __name__ == "__main__":
    main()
