from __future__ import annotations

from authoritative_pnl.connectors.market_registry import MarketRegistryConnector
from authoritative_pnl.connectors.market_resolution import MarketResolutionConnector
from authoritative_pnl.connectors.runtime_log import RuntimeLogConnector
from authoritative_pnl.settings import Settings
from authoritative_pnl.store.sqlite_store import SqliteStore


def run(settings: Settings, store: SqliteStore) -> dict[str, int]:
    runtime_claims = RuntimeLogConnector(settings.runtime_log).extract_claims()
    registry = MarketRegistryConnector(
        settings.market_registry.canonical_dir,
        settings.market_registry.legacy_dir,
    )
    canonical_claims = registry.extract_canonical_claims()
    legacy_claims = registry.extract_legacy_claims()
    resolutions = MarketResolutionConnector(settings.market_resolution.path).load()

    store.replace_claims("runtime_midpoint_claims", runtime_claims)
    store.replace_claims("canonical_market_claims", canonical_claims)
    store.replace_claims("legacy_lookup_claims", legacy_claims)
    store.replace_resolution_records(resolutions)

    return {
        "runtime_claims": len(runtime_claims),
        "canonical_claims": len(canonical_claims),
        "legacy_claims": len(legacy_claims),
        "resolution_records": len(resolutions),
    }
