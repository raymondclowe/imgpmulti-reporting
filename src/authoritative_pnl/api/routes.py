from __future__ import annotations

from urllib.parse import parse_qs

from authoritative_pnl.quality.reconcile import build_summary
from authoritative_pnl.store.sqlite_store import SqliteStore


def dispatch(store: SqliteStore, path: str, query_string: str) -> tuple[int, dict[str, object]]:
    params = {key: values[0] for key, values in parse_qs(query_string).items() if values}
    limit = int(params.pop("limit", "100"))

    if path == "/health":
        return 200, {"status": "ok"}
    if path == "/coverage/wallets":
        return 200, {"items": store.list_wallet_coverage()}
    if path == "/events/raw":
        return 200, {"items": store.query_rows("raw_activity_events", filters=params, limit=limit)}
    if path == "/events/attributed":
        return 200, {"items": store.query_rows("attributed_events", filters=params, limit=limit)}
    if path == "/pnl/strategy":
        return 200, {"items": store.query_rows("pnl_strategy_facts", filters=params, limit=limit)}
    if path == "/pnl/market":
        return 200, {"items": store.query_rows("pnl_market_facts", filters=params, limit=limit)}
    if path == "/pnl/wallet":
        return 200, {"items": store.query_rows("pnl_wallet_facts", filters=params, limit=limit)}
    if path == "/pnl/daily":
        return 200, {"items": store.query_rows("pnl_daily_facts", filters=params, limit=limit)}
    if path == "/reconciliation/summary":
        return 200, build_summary(store)
    return 404, {"error": "not_found", "path": path}
