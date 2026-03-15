from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import tomllib


@dataclass(frozen=True)
class StorageSettings:
    sqlite_path: Path
    output_dir: Path


@dataclass(frozen=True)
class ActivitySourceSettings:
    endpoint_url: str
    api_key_env: str
    page_size: int = 200
    rate_limit_per_second: float = 2.0
    include_deposits_withdrawals: bool = True


@dataclass(frozen=True)
class AccountRegistrySettings:
    path: Path | None


@dataclass(frozen=True)
class RuntimeLogSettings:
    paths: tuple[Path, ...]
    midpoint_offset_seconds: int = 450
    midpoint_window_seconds: int = 180


@dataclass(frozen=True)
class MarketRegistrySettings:
    canonical_dir: Path | None
    legacy_dir: Path | None


@dataclass(frozen=True)
class MarketResolutionSettings:
    path: Path | None


@dataclass(frozen=True)
class QualitySettings:
    freshness_sla_seconds: int = 1800
    unknown_attribution_threshold: float = 0.05
    suspicious_gap_seconds: int = 86400


@dataclass(frozen=True)
class ApiSettings:
    host: str = "127.0.0.1"
    port: int = 8080


@dataclass(frozen=True)
class Settings:
    storage: StorageSettings
    activity: ActivitySourceSettings
    account_registry: AccountRegistrySettings
    runtime_log: RuntimeLogSettings
    market_registry: MarketRegistrySettings
    market_resolution: MarketResolutionSettings
    quality: QualitySettings
    api: ApiSettings
    config_path: Path


def _resolve_path(base_dir: Path, raw: str | None) -> Path | None:
    if not raw:
        return None
    path = Path(raw)
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def _get(mapping: dict[str, Any], key: str, default: Any) -> Any:
    value = mapping.get(key, default)
    return default if value is None else value


def load_settings(config_path: str | Path) -> Settings:
    config_path = Path(config_path).resolve()
    with config_path.open("rb") as handle:
        data = tomllib.load(handle)

    base_dir = config_path.parent.parent if config_path.parent.name == "config" else config_path.parent
    storage_data = data.get("storage", {})
    sources = data.get("sources", {})
    activity_data = sources.get("activity", {})
    registry_data = sources.get("account_registry", {})
    runtime_data = sources.get("runtime_log", {})
    market_registry_data = sources.get("market_registry", {})
    resolution_data = sources.get("market_resolution", {})
    quality_data = data.get("quality", {})
    api_data = data.get("api", {})

    return Settings(
        storage=StorageSettings(
            sqlite_path=_resolve_path(base_dir, _get(storage_data, "sqlite_path", "var/authoritative_pnl.sqlite3")) or base_dir / "var/authoritative_pnl.sqlite3",
            output_dir=_resolve_path(base_dir, _get(storage_data, "output_dir", "out")) or base_dir / "out",
        ),
        activity=ActivitySourceSettings(
            endpoint_url=_get(activity_data, "endpoint_url", ""),
            api_key_env=_get(activity_data, "api_key_env", "POLYMARKET_API_KEY"),
            page_size=int(_get(activity_data, "page_size", 200)),
            rate_limit_per_second=float(_get(activity_data, "rate_limit_per_second", 2.0)),
            include_deposits_withdrawals=bool(_get(activity_data, "include_deposits_withdrawals", True)),
        ),
        account_registry=AccountRegistrySettings(
            path=_resolve_path(base_dir, registry_data.get("path")),
        ),
        runtime_log=RuntimeLogSettings(
            paths=tuple(_resolve_path(base_dir, raw_path) for raw_path in runtime_data.get("paths", []) if _resolve_path(base_dir, raw_path) is not None),
            midpoint_offset_seconds=int(_get(runtime_data, "midpoint_offset_seconds", 450)),
            midpoint_window_seconds=int(_get(runtime_data, "midpoint_window_seconds", 180)),
        ),
        market_registry=MarketRegistrySettings(
            canonical_dir=_resolve_path(base_dir, market_registry_data.get("canonical_dir")),
            legacy_dir=_resolve_path(base_dir, market_registry_data.get("legacy_dir")),
        ),
        market_resolution=MarketResolutionSettings(
            path=_resolve_path(base_dir, resolution_data.get("path")),
        ),
        quality=QualitySettings(
            freshness_sla_seconds=int(_get(quality_data, "freshness_sla_seconds", 1800)),
            unknown_attribution_threshold=float(_get(quality_data, "unknown_attribution_threshold", 0.05)),
            suspicious_gap_seconds=int(_get(quality_data, "suspicious_gap_seconds", 86400)),
        ),
        api=ApiSettings(
            host=_get(api_data, "host", "127.0.0.1"),
            port=int(_get(api_data, "port", 8080)),
        ),
        config_path=config_path,
    )
