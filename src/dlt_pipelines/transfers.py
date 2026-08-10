"""Raw file transfer utilities."""

from __future__ import annotations

from collections.abc import Callable
import os
import posixpath
import shutil
from dataclasses import dataclass

import fsspec

from dlt_pipelines.config import (
    get_provider_s3_landing_bucket_url,
    get_provider_setting,
    get_provider_setting_list,
    get_setting,
)

COPY_BUFFER_SIZE = 8 * 1024 * 1024


@dataclass(frozen=True)
class SftpToS3Config:
    provider: str
    sftp_bucket_url: str
    sftp_file_globs: tuple[str, ...]
    s3_landing_bucket_url: str
    s3_landing_prefix: str

    @classmethod
    def for_provider(cls, provider: str) -> "SftpToS3Config":
        return cls(
            provider=provider,
            sftp_bucket_url=get_provider_setting(
                provider,
                ("sftp", "bucket_url"),
                required=True,
            ),
            sftp_file_globs=tuple(
                get_provider_setting_list(
                    provider,
                    ("sftp", "file_globs"),
                    default=[
                        "*.csv",
                        "CommissionStatements/*.csv",
                    ],
                )
            ),
            s3_landing_bucket_url=get_provider_s3_landing_bucket_url(provider),
            s3_landing_prefix=(
                get_provider_setting(
                    provider,
                    ("s3", "landing", "prefix"),
                    default="",
                )
                or ""
            ).strip("/"),
        )


@dataclass(frozen=True)
class TransferPlanItem:
    source_path: str
    target_path: str


@dataclass(frozen=True)
class TransferResult:
    source_path: str
    target_path: str
    source_size: int | None
    target_size: int | None
    deleted_source: bool


@dataclass(frozen=True)
class ArchivePlanItem:
    source_path: str
    archive_path: str


@dataclass(frozen=True)
class SftpListingItem:
    path: str
    is_dir: bool


@dataclass(frozen=True)
class SftpScanContext:
    source_root: str
    source_patterns: tuple[str, ...]
    target_root: str
    target_prefix: str


def describe_sftp_scan(
    provider: str,
    config: SftpToS3Config | None = None,
) -> SftpScanContext:
    """Describe the resolved SFTP scan and S3 target settings."""
    config = config or SftpToS3Config.for_provider(provider)
    _, source_root = fsspec.core.url_to_fs(
        config.sftp_bucket_url,
        **_sftp_options_for_provider(config.provider),
    )
    _, target_root = fsspec.core.url_to_fs(
        config.s3_landing_bucket_url,
        **_s3_options_from_env(),
    )
    source_root = _normalize_source_root(source_root)
    target_root = target_root.rstrip("/")
    return SftpScanContext(
        source_root=source_root,
        source_patterns=tuple(
            pattern
            for file_glob in config.sftp_file_globs
            for pattern in _remote_glob_patterns(source_root, file_glob)
        ),
        target_root=target_root,
        target_prefix=config.s3_landing_prefix,
    )


def list_sftp_files(
    provider: str,
    config: SftpToS3Config | None = None,
) -> list[SftpListingItem]:
    """List files and directories under the configured SFTP root."""
    config = config or SftpToS3Config.for_provider(provider)
    source_fs, source_root = fsspec.core.url_to_fs(
        config.sftp_bucket_url,
        **_sftp_options_for_provider(config.provider),
    )
    source_root = _normalize_source_root(source_root)
    paths: list[str] = []
    for path in _candidate_listing_roots(source_root):
        try:
            listing = source_fs.ls(path, detail=False)
        except (FileNotFoundError, OSError):
            continue
        paths.extend(listing)

    return _dedupe_listing([
        SftpListingItem(path=path, is_dir=source_fs.isdir(path))
        for path in sorted(paths)
    ])


def plan_sftp_files_to_s3(
    provider: str,
    config: SftpToS3Config | None = None,
) -> list[TransferPlanItem]:
    """List matching SFTP files and their target S3 paths without copying."""
    config = config or SftpToS3Config.for_provider(provider)
    source_fs, source_root = fsspec.core.url_to_fs(
        config.sftp_bucket_url,
        **_sftp_options_for_provider(config.provider),
    )
    _, target_root = fsspec.core.url_to_fs(
        config.s3_landing_bucket_url,
        **_s3_options_from_env(),
    )

    source_root = _normalize_source_root(source_root)
    target_root = target_root.rstrip("/")
    plan: list[TransferPlanItem] = []

    for source_pattern in (
        pattern
        for file_glob in config.sftp_file_globs
        for pattern in _remote_glob_patterns(source_root, file_glob)
    ):
        for source_path in source_fs.glob(source_pattern):
            if source_fs.isdir(source_path):
                continue

            relative_path = _relative_remote_path(source_path, source_root)
            target_path = posixpath.join(target_root, config.s3_landing_prefix, relative_path)
            plan.append(TransferPlanItem(source_path=source_path, target_path=target_path))

    return _dedupe_plan(plan)


ProgressCallback = Callable[[str], None]


def land_sftp_files_to_s3(
    provider: str,
    config: SftpToS3Config | None = None,
    *,
    limit: int | None = None,
    progress: ProgressCallback | None = None,
) -> list[str]:
    """Copy matching files from SFTP into the S3 landing zone."""
    results = _transfer_sftp_files_to_s3(
        provider,
        config,
        limit=limit,
        progress=progress,
        delete_source=False,
    )
    return [result.target_path for result in results]


def move_sftp_files_to_s3(
    provider: str,
    config: SftpToS3Config | None = None,
    *,
    limit: int | None = None,
    progress: ProgressCallback | None = None,
) -> list[TransferResult]:
    """Copy matching SFTP files to S3, verify size, then delete SFTP files."""
    return _transfer_sftp_files_to_s3(
        provider,
        config,
        limit=limit,
        progress=progress,
        delete_source=True,
    )


def plan_landed_files_archive(provider: str) -> list[ArchivePlanItem]:
    """List landed B2/S3 files and their Archive target paths."""
    from dlt_pipelines.sources.s3 import _routes_for_provider

    bucket_url = get_provider_s3_landing_bucket_url(provider)
    fs, bucket_root = fsspec.core.url_to_fs(bucket_url, **_s3_options_from_env())
    bucket_root = bucket_root.rstrip("/")
    plan: list[ArchivePlanItem] = []

    for route in _routes_for_provider(provider):
        pattern = posixpath.join(bucket_root, route.file_glob)
        for source_path in fs.glob(pattern):
            if fs.isdir(source_path):
                continue
            archive_path = _archive_path_for(source_path)
            plan.append(ArchivePlanItem(source_path=source_path, archive_path=archive_path))

    return _dedupe_archive_plan(plan)


def archive_landed_files(provider: str, *, progress: ProgressCallback | None = None) -> list[ArchivePlanItem]:
    """Move landed B2/S3 files into Archive subdirectories after database load."""
    bucket_url = get_provider_s3_landing_bucket_url(provider)
    _report(progress, "Connecting to S3-compatible target for archiving.")
    fs, _ = fsspec.core.url_to_fs(bucket_url, **_s3_options_from_env())
    plan = plan_landed_files_archive(provider)
    _report(progress, f"Archive plan contains {len(plan)} file(s).")

    archived: list[ArchivePlanItem] = []
    for index, item in enumerate(plan, start=1):
        archive_parent = posixpath.dirname(item.archive_path)
        _report(progress, f"[{index}/{len(plan)}] Ensuring archive directory {archive_parent}.")
        fs.makedirs(archive_parent, exist_ok=True)
        _report(progress, f"[{index}/{len(plan)}] Moving {item.source_path} -> {item.archive_path}.")
        fs.mv(item.source_path, item.archive_path)
        archived.append(item)
        _report(progress, f"[{index}/{len(plan)}] Archived {item.archive_path}.")

    return archived


def _transfer_sftp_files_to_s3(
    provider: str,
    config: SftpToS3Config | None,
    *,
    limit: int | None,
    progress: ProgressCallback | None,
    delete_source: bool,
) -> list[TransferResult]:
    config = config or SftpToS3Config.for_provider(provider)
    _report(progress, "Connecting to SFTP.")
    source_fs, source_root = fsspec.core.url_to_fs(
        config.sftp_bucket_url,
        **_sftp_options_for_provider(config.provider),
    )
    _report(progress, "Connecting to S3-compatible target.")
    target_fs, target_root = fsspec.core.url_to_fs(
        config.s3_landing_bucket_url,
        **_s3_options_from_env(),
    )

    source_root = _normalize_source_root(source_root)
    target_root = target_root.rstrip("/")
    results: list[TransferResult] = []
    ensured_target_dirs: set[str] = set()
    plan = plan_sftp_files_to_s3(provider, config)
    if limit is not None:
        plan = plan[:limit]
    action_name = "Move" if delete_source else "Copy"
    _report(progress, f"{action_name} plan contains {len(plan)} file(s).")

    for index, item in enumerate(plan, start=1):
        _report(progress, f"[{index}/{len(plan)}] Preparing {item.target_path}.")
        target_parent = posixpath.dirname(item.target_path)
        if target_parent:
            _ensure_target_directory(
                target_fs,
                target_parent,
                ensured_target_dirs,
                progress=progress,
                progress_prefix=f"[{index}/{len(plan)}]",
            )

        _report(progress, f"[{index}/{len(plan)}] Opening source {item.source_path}.")
        with source_fs.open(item.source_path, "rb") as source_file:
            _report(progress, f"[{index}/{len(plan)}] Opening target {item.target_path}.")
            with target_fs.open(item.target_path, "wb") as target_file:
                _report(progress, f"[{index}/{len(plan)}] Copying bytes.")
                shutil.copyfileobj(source_file, target_file, length=COPY_BUFFER_SIZE)

        source_size = _file_size(source_fs, item.source_path)
        target_size = _file_size(target_fs, item.target_path)
        _report(
            progress,
            f"[{index}/{len(plan)}] Verifying size source={source_size} target={target_size}.",
        )
        if source_size is not None and target_size is not None and source_size != target_size:
            raise RuntimeError(
                f"Size verification failed for {item.source_path}: "
                f"source={source_size}, target={target_size}."
            )

        deleted_source = False
        if delete_source:
            _report(progress, f"[{index}/{len(plan)}] Deleting source {item.source_path}.")
            source_fs.rm(item.source_path)
            deleted_source = True
            _report(progress, f"[{index}/{len(plan)}] Deleted source {item.source_path}.")

        results.append(
            TransferResult(
                source_path=item.source_path,
                target_path=item.target_path,
                source_size=source_size,
                target_size=target_size,
                deleted_source=deleted_source,
            )
        )
        _report(progress, f"[{index}/{len(plan)}] {'Moved' if delete_source else 'Copied'} {item.target_path}.")

    return results


def _sftp_options_for_provider(provider: str) -> dict[str, object]:
    options: dict[str, object] = {}
    mappings = {
        ("sftp", "username"): "username",
        ("sftp", "password"): "password",
        ("sftp", "key_filename"): "key_filename",
        ("sftp", "key_passphrase"): "passphrase",
    }
    for path, option_name in mappings.items():
        value = get_provider_setting(provider, path)
        if value:
            options[option_name] = value

    port = get_provider_setting(provider, ("sftp", "port"))
    if port:
        options["port"] = int(port)

    return options


def _report(progress: ProgressCallback | None, message: str) -> None:
    if progress:
        progress(message)


def _ensure_target_directory(
    target_fs,
    target_parent: str,
    ensured_target_dirs: set[str],
    *,
    progress: ProgressCallback | None,
    progress_prefix: str,
) -> None:
    if target_parent in ensured_target_dirs:
        return

    _report(progress, f"{progress_prefix} Ensuring target directory {target_parent}.")
    target_fs.makedirs(target_parent, exist_ok=True)
    ensured_target_dirs.add(target_parent)


def _file_size(fs, path: str) -> int | None:
    try:
        return fs.size(path)
    except (AttributeError, FileNotFoundError, OSError, NotImplementedError):
        return None


def _normalize_source_root(source_root: str) -> str:
    source_root = source_root.rstrip("/")
    return source_root or "."


def _join_remote_path(root: str, path: str) -> str:
    if root == "":
        return path
    if root == ".":
        return f"./{path.lstrip('./')}"
    return posixpath.join(root, path)


def _relative_remote_path(path: str, root: str) -> str:
    if root in ("", "."):
        return _canonical_remote_path(path)
    return posixpath.relpath(path, root)


def _dedupe_plan(plan: list[TransferPlanItem]) -> list[TransferPlanItem]:
    seen: set[str] = set()
    deduped: list[TransferPlanItem] = []
    for item in plan:
        if item.target_path in seen:
            continue
        seen.add(item.target_path)
        deduped.append(item)
    return deduped


def _dedupe_archive_plan(plan: list[ArchivePlanItem]) -> list[ArchivePlanItem]:
    seen: set[str] = set()
    deduped: list[ArchivePlanItem] = []
    for item in plan:
        if item.source_path in seen:
            continue
        seen.add(item.source_path)
        deduped.append(item)
    return deduped


def _archive_path_for(source_path: str) -> str:
    parent = posixpath.dirname(source_path)
    file_name = posixpath.basename(source_path)
    return posixpath.join(parent, "Archive", file_name)


def _candidate_listing_roots(source_root: str) -> tuple[str, ...]:
    roots = [source_root]
    if source_root == ".":
        roots.extend(["./CommissionStatements", "/CommissionStatements"])
    else:
        roots.append(_join_remote_path(source_root, "CommissionStatements"))
    return tuple(dict.fromkeys(roots))


def _dedupe_listing(items: list[SftpListingItem]) -> list[SftpListingItem]:
    seen: set[str] = set()
    deduped: list[SftpListingItem] = []
    for item in items:
        canonical_path = _canonical_remote_path(item.path)
        if canonical_path in seen:
            continue
        seen.add(canonical_path)
        deduped.append(item)
    return deduped


def _remote_glob_patterns(root: str, file_glob: str) -> tuple[str, ...]:
    pattern = _join_remote_path(root, file_glob)
    if root == ".":
        absolute_pattern = "/" + file_glob.lstrip("./")
        return tuple(dict.fromkeys((pattern, absolute_pattern)))
    return (pattern,)


def _canonical_remote_path(path: str) -> str:
    return path.removeprefix("./").lstrip("/")


def _s3_options_from_env() -> dict[str, object]:
    return s3_options_from_env()


def s3_options_from_env() -> dict[str, object]:
    options: dict[str, object] = {}
    mappings = {
        "AWS_ACCESS_KEY_ID": ("key", ("s3", "credentials", "aws_access_key_id")),
        "AWS_SECRET_ACCESS_KEY": ("secret", ("s3", "credentials", "aws_secret_access_key")),
        "AWS_SESSION_TOKEN": ("token", ("s3", "credentials", "aws_session_token")),
    }
    for env_name, (option_name, toml_path) in mappings.items():
        value = get_setting(env_name, toml_path)
        if value:
            options[option_name] = value

    client_kwargs: dict[str, object] = {}
    region_name = get_setting(
        "AWS_REGION",
        ("s3", "credentials", "region_name"),
    ) or os.getenv("AWS_DEFAULT_REGION")
    if region_name:
        client_kwargs["region_name"] = region_name
    endpoint_url = get_setting(
        "S3_ENDPOINT_URL",
        ("s3", "credentials", "endpoint_url"),
    ) or os.getenv("ENDPOINT_URL")
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url
    if client_kwargs:
        options["client_kwargs"] = client_kwargs

    return options
