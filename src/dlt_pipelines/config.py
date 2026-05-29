"""Configuration helpers for environment variables and local TOML fallback."""

from __future__ import annotations

import os
import re
import tomllib
from functools import lru_cache
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def get_setting(
    env_name: str,
    toml_path: tuple[str, ...],
    *,
    default: str | None = None,
    required: bool = False,
) -> str | None:
    env_value = os.getenv(env_name)
    if env_value:
        return env_value

    toml_value = _lookup_toml_value(toml_path)
    if toml_value is not None:
        return str(toml_value)

    if required:
        raise RuntimeError(f"Missing required setting: {env_name} or {'.'.join(toml_path)}")
    return default


def get_provider_setting(
    provider: str,
    path: tuple[str, ...],
    *,
    default: str | None = None,
    required: bool = False,
) -> str | None:
    env_name = "__".join(("SFTP_PROVIDERS", _provider_env_key(provider), *path)).upper()
    toml_path = ("sftp_providers", provider, *path)
    return get_setting(env_name, toml_path, default=default, required=required)


def get_provider_setting_list(
    provider: str,
    path: tuple[str, ...],
    *,
    default: list[str] | None = None,
) -> list[str]:
    env_name = "__".join(("SFTP_PROVIDERS", _provider_env_key(provider), *path)).upper()
    env_value = os.getenv(env_name)
    if env_value:
        return [value.strip() for value in env_value.split(",") if value.strip()]

    toml_value = _lookup_toml_value(("sftp_providers", provider, *path))
    if isinstance(toml_value, list):
        return [str(value) for value in toml_value]
    if isinstance(toml_value, str):
        return [toml_value]
    return default or []


def get_provider_routes(provider: str) -> list[dict[str, Any]]:
    routes = _lookup_toml_value(("sftp_providers", provider, "routes"))
    if routes is None:
        return []
    if not isinstance(routes, list):
        raise RuntimeError(f"sftp_providers.{provider}.routes must be a TOML array.")
    return routes


def ensure_env_from_setting(env_name: str, toml_path: tuple[str, ...]) -> None:
    if os.getenv(env_name):
        return

    toml_value = _lookup_toml_value(toml_path)
    if toml_value is not None:
        os.environ[env_name] = str(toml_value)


def _provider_env_key(provider: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", provider).strip("_")
    if not normalized:
        raise ValueError("Provider name must contain at least one letter or number.")
    return normalized


def _lookup_toml_value(path: tuple[str, ...]) -> Any | None:
    for config_file in (PROJECT_ROOT / ".dlt" / "secrets.toml", PROJECT_ROOT / ".dlt" / "config.toml"):
        data = _load_toml(config_file)
        value: Any = data
        for key in path:
            if not isinstance(value, dict) or key not in value:
                break
            value = value[key]
        else:
            return value
    return None


@lru_cache(maxsize=2)
def _load_toml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("rb") as file:
        return tomllib.load(file)
