from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
import json
import time

from authoritative_pnl.models.events import to_decimal
from authoritative_pnl.settings import Settings
from authoritative_pnl.store.sqlite_store import SqliteStore


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: str
    details: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def run(settings: Settings, store: SqliteStore) -> list[CheckResult]:
    now = int(time.time())
    coverage = store.list_wallet_coverage()
    raw_events = store.get_raw_events()
    attributed_events = store.get_attributed_events()
    market_facts = store.get_fact_rows("pnl_market_facts")
    strategy_facts = store.get_fact_rows("pnl_strategy_facts")
    wallet_facts = store.get_fact_rows("pnl_wallet_facts")

    freshness_stale = [
        row["wallet_address"]
        for row in coverage
        if now - int(row["last_refresh_ts"]) > settings.quality.freshness_sla_seconds
    ]

    missing_slug_count = sum(1 for row in raw_events if not row["slug"])
    unknown_count = sum(1 for row in attributed_events if row["strategy_name"] == "UNKNOWN")
    unknown_rate = Decimal("0")
    if attributed_events:
        unknown_rate = Decimal(unknown_count) / Decimal(len(attributed_events))

    gap_wallets = []
    timestamps_by_wallet: dict[str, list[int]] = {}
    for row in raw_events:
        timestamps_by_wallet.setdefault(str(row["wallet_address"]), []).append(int(row["timestamp_unix"]))
    for wallet, timestamps in timestamps_by_wallet.items():
        timestamps.sort()
        if any((current - previous) > settings.quality.suspicious_gap_seconds for previous, current in zip(timestamps, timestamps[1:])):
            gap_wallets.append(wallet)

    market_total = sum((to_decimal(row["net_cashflow_usdc"]) for row in market_facts), Decimal("0"))
    strategy_total = sum((to_decimal(row["net_cashflow_usdc"]) for row in strategy_facts), Decimal("0"))
    wallet_total = sum((to_decimal(row["net_cashflow_usdc"]) for row in wallet_facts), Decimal("0"))

    return [
        CheckResult(
            name="freshness",
            status="fail" if freshness_stale else "pass",
            details={"stale_wallets": freshness_stale},
        ),
        CheckResult(
            name="gap_check",
            status="fail" if gap_wallets else "pass",
            details={"wallets": gap_wallets},
        ),
        CheckResult(
            name="missing_slug",
            status="fail" if missing_slug_count else "pass",
            details={"missing_slug_count": missing_slug_count},
        ),
        CheckResult(
            name="unknown_attribution",
            status="fail" if unknown_rate > Decimal(str(settings.quality.unknown_attribution_threshold)) else "pass",
            details={"unknown_count": unknown_count, "unknown_rate": str(unknown_rate)},
        ),
        CheckResult(
            name="cross_report_consistency",
            status="fail" if market_total != strategy_total or market_total != wallet_total else "pass",
            details={
                "market_total": str(market_total),
                "strategy_total": str(strategy_total),
                "wallet_total": str(wallet_total),
            },
        ),
    ]


def to_json(results: list[CheckResult]) -> str:
    return json.dumps([result.as_dict() for result in results], indent=2, sort_keys=True)
