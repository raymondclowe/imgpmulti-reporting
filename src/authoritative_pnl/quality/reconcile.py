from __future__ import annotations

from decimal import Decimal

from authoritative_pnl.models.events import to_decimal
from authoritative_pnl.store.sqlite_store import SqliteStore


def build_summary(store: SqliteStore) -> dict[str, object]:
    market_facts = store.get_fact_rows("pnl_market_facts")
    strategy_facts = store.get_fact_rows("pnl_strategy_facts")
    wallet_facts = store.get_fact_rows("pnl_wallet_facts")

    market_total = sum((to_decimal(row["net_cashflow_usdc"]) for row in market_facts), Decimal("0"))
    strategy_total = sum((to_decimal(row["net_cashflow_usdc"]) for row in strategy_facts), Decimal("0"))
    wallet_total = sum((to_decimal(row["net_cashflow_usdc"]) for row in wallet_facts), Decimal("0"))

    return {
        "market_total": str(market_total),
        "strategy_total": str(strategy_total),
        "wallet_total": str(wallet_total),
        "consistent": market_total == strategy_total == wallet_total,
    }
