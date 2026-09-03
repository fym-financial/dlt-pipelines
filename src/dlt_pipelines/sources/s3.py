"""S3 landing-zone sources."""

from __future__ import annotations

import os
import posixpath
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any

import dlt
import fsspec
from dlt.sources import TDataItems
from dlt.sources.filesystem import FileItemDict, filesystem, read_jsonl, read_parquet

from dlt_pipelines.config import (
    ensure_env_from_setting,
    get_provider_routes,
    get_provider_s3_landing_bucket_url,
    get_provider_setting,
)
from dlt_pipelines.transfers import s3_options_from_env


@dataclass(frozen=True)
class FileRoute:
    name: str
    file_glob: str
    table_name: str
    parser: str
    parser_options: dict[str, object]

    @classmethod
    def from_mapping(cls, route: dict[str, object]) -> "FileRoute":
        name = _required_route_value(route, "name")
        return cls(
            name=name,
            file_glob=_required_route_value(route, "file_glob"),
            table_name=str(route.get("table_name") or name),
            parser=str(route.get("parser") or "csv"),
            parser_options=_parser_options_from_mapping(route),
        )


def landed_csv_files(provider: str | None = None) -> object:
    """Read routed files from the S3 landing zone into DLT tables."""
    if not provider:
        raise RuntimeError("A provider name is required to load landed S3 files.")

    bucket_url = get_provider_s3_landing_bucket_url(provider)
    routes = _routes_for_provider(provider)
    _ensure_aws_environment()
    matching_routes = _routes_with_matches(bucket_url, routes)
    if not matching_routes:
        raise RuntimeError(f"No landed files matched configured routes for provider '{provider}'.")

    resources = []
    for route in matching_routes:
        files = filesystem(
            bucket_url=bucket_url,
            file_glob=route.file_glob,
            incremental=dlt.sources.incremental("modification_date"),
        )
        resources.append(
            (files | _reader_for_route(route)(**route.parser_options)).with_name(route.table_name)
        )
    return resources


def _routes_with_matches(bucket_url: str, routes: list[FileRoute]) -> list[FileRoute]:
    fs, bucket_root = fsspec.core.url_to_fs(bucket_url, **s3_options_from_env())
    bucket_root = bucket_root.rstrip("/")
    matching_routes: list[FileRoute] = []

    for route in routes:
        pattern = posixpath.join(bucket_root, route.file_glob)
        if any(not fs.isdir(path) for path in fs.glob(pattern)):
            matching_routes.append(route)

    return matching_routes


def validate_landed_csv_contracts(provider: str) -> int:
    """Validate configured CSV headers before a destructive provider rebuild."""
    bucket_url = get_provider_s3_landing_bucket_url(provider)
    routes = _routes_for_provider(provider)
    _ensure_aws_environment()
    fs, bucket_root = fsspec.core.url_to_fs(bucket_url, **s3_options_from_env())
    bucket_root = bucket_root.rstrip("/")
    validated_files = 0

    for route in routes:
        expected_columns = route.parser_options.get("expected_columns")
        if route.parser != "csv" or expected_columns is None:
            continue
        if not isinstance(expected_columns, (list, tuple)):
            raise RuntimeError(f"Route '{route.name}' expected_columns must be a list.")
        pattern = posixpath.join(bucket_root, route.file_glob)
        for path in fs.glob(pattern):
            if fs.isdir(path):
                continue
            with fs.open(path, "rb") as file:
                _validate_csv_columns(
                    file,
                    expected_columns=expected_columns,
                    file_name=posixpath.basename(path),
                )
            validated_files += 1

    if validated_files == 0:
        raise RuntimeError(
            f"No landed CSV files with configured column contracts matched provider '{provider}'."
        )
    return validated_files


def _routes_for_provider(provider: str) -> list[FileRoute]:
    routes = [FileRoute.from_mapping(route) for route in get_provider_routes(provider)]
    if routes:
        return routes

    file_glob = get_provider_setting(
        provider,
        ("s3", "landing", "file_glob"),
        default="**/*.csv",
    )
    return [
        FileRoute(
            name=f"{provider}_default_csv",
            file_glob=file_glob or "**/*.csv",
            table_name=f"{provider}_records",
            parser="csv",
            parser_options={},
        )
    ]


def _reader_for_route(route: FileRoute):
    readers = {
        "csv": read_csv_with_file_errors,
        "jsonl": read_jsonl,
        "parquet": read_parquet,
    }
    try:
        return readers[route.parser]
    except KeyError as exc:
        supported = ", ".join(sorted(readers))
        raise RuntimeError(
            f"Unsupported parser '{route.parser}' for route '{route.name}'. "
            f"Use one of: {supported}."
        ) from exc


def _required_route_value(route: dict[str, object], key: str) -> str:
    value = route.get(key)
    if not value:
        raise RuntimeError(f"File route is missing required key: {key}")
    return str(value)


def _parser_options_from_mapping(route: dict[str, object]) -> dict[str, object]:
    value = route.get("parser_options")
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise RuntimeError(f"Route '{route.get('name')}' parser_options must be a table.")
    return dict(value)


def _ensure_aws_environment() -> None:
    ensure_env_from_setting(
        "AWS_ACCESS_KEY_ID",
        ("s3", "credentials", "aws_access_key_id"),
    )
    ensure_env_from_setting(
        "AWS_SECRET_ACCESS_KEY",
        ("s3", "credentials", "aws_secret_access_key"),
    )
    ensure_env_from_setting(
        "AWS_SESSION_TOKEN",
        ("s3", "credentials", "aws_session_token"),
    )
    ensure_env_from_setting(
        "AWS_REGION",
        ("s3", "credentials", "region_name"),
    )
    ensure_env_from_setting(
        "ENDPOINT_URL",
        ("s3", "credentials", "endpoint_url"),
    )
    ensure_env_from_setting(
        "SOURCES__FILESYSTEM__CREDENTIALS__ENDPOINT_URL",
        ("s3", "credentials", "endpoint_url"),
    )
    ensure_env_from_setting(
        "SOURCES__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID",
        ("s3", "credentials", "aws_access_key_id"),
    )
    ensure_env_from_setting(
        "SOURCES__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY",
        ("s3", "credentials", "aws_secret_access_key"),
    )
    ensure_env_from_setting(
        "SOURCES__FILESYSTEM__CREDENTIALS__REGION_NAME",
        ("s3", "credentials", "region_name"),
    )
    _copy_env_if_present(
        "AWS_ACCESS_KEY_ID",
        "SOURCES__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID",
    )
    _copy_env_if_present(
        "AWS_SECRET_ACCESS_KEY",
        "SOURCES__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY",
    )
    _copy_env_if_present(
        "AWS_SESSION_TOKEN",
        "SOURCES__FILESYSTEM__CREDENTIALS__AWS_SESSION_TOKEN",
    )
    _copy_env_if_present(
        "AWS_REGION",
        "SOURCES__FILESYSTEM__CREDENTIALS__REGION_NAME",
    )
    _copy_env_if_present(
        "S3_ENDPOINT_URL",
        "SOURCES__FILESYSTEM__CREDENTIALS__ENDPOINT_URL",
    )


def _copy_env_if_present(source_name: str, target_name: str) -> None:
    if target_name not in os.environ and source_name in os.environ:
        os.environ[target_name] = os.environ[source_name]


@dlt.transformer()
def read_csv_with_file_errors(
    items: Iterator[FileItemDict],
    chunksize: int = 10000,
    expected_columns: list[str] | tuple[str, ...] | None = None,
    **pandas_kwargs: Any,
) -> Iterator[TDataItems]:
    yield from _read_csv_with_file_errors(
        items,
        chunksize=chunksize,
        expected_columns=expected_columns,
        **pandas_kwargs,
    )


def _read_csv_with_file_errors(
    items: Iterator[FileItemDict],
    chunksize: int = 10000,
    expected_columns: list[str] | tuple[str, ...] | None = None,
    **pandas_kwargs: Any,
) -> Iterator[TDataItems]:
    import pandas as pd

    kwargs = {"header": "infer", "chunksize": chunksize, **pandas_kwargs}
    for file_obj in items:
        file_name = str(file_obj.get("file_name") or file_obj.get("file_url") or file_obj)
        source_modified_at = file_obj.get("modification_date")
        try:
            with file_obj.open() as file:
                for df in pd.read_csv(file, **kwargs):
                    if expected_columns is not None:
                        _validate_column_names(
                            df.columns,
                            expected_columns=expected_columns,
                            file_name=file_name,
                        )
                    df["_source_file"] = file_name
                    df["_source_modified_at"] = source_modified_at
                    yield df.to_dict(orient="records")
        except Exception as exc:
            raise RuntimeError(f"Failed to read CSV file {file_name}: {exc}") from exc


def _validate_csv_columns(
    file, *, expected_columns: Sequence[str], file_name: str
) -> None:
    import pandas as pd

    columns = pd.read_csv(file, nrows=0).columns
    _validate_column_names(columns, expected_columns=expected_columns, file_name=file_name)


def _validate_column_names(
    columns, *, expected_columns: Sequence[str], file_name: str
) -> None:
    expected = list(expected_columns)
    actual = list(columns)
    if actual != expected:
        raise ValueError(
            f"CSV columns do not match the configured contract for {file_name} "
            f"(expected {len(expected)}, got {len(actual)})"
        )
