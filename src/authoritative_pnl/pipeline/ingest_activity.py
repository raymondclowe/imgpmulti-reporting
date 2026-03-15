from __future__ import annotations

import logging

from authoritative_pnl.connectors.account_registry import AccountRegistryConnector
from authoritative_pnl.connectors.polymarket_activity import PolymarketActivityClient
from authoritative_pnl.settings import Settings
from authoritative_pnl.store.sqlite_store import SqliteStore


LOGGER = logging.getLogger(__name__)


def run(settings: Settings, store: SqliteStore) -> dict[str, int]:
    registry = AccountRegistryConnector(settings.account_registry.path)
    wallets = registry.load_wallets()
    client = PolymarketActivityClient(settings.activity)
    inserted = 0
    refreshed = 0

    for wallet in wallets:
        cursor: str | None = None
        try:
            while True:
                page = client.fetch_wallet_activity(wallet.wallet_address, cursor=cursor)
                inserted += store.insert_raw_events(page.events)
                cursor = page.next_cursor
                if not cursor or not page.events:
                    break
            store.upsert_wallet_coverage(wallet.wallet_address, last_cursor=cursor, status="ok")
            refreshed += 1
        except Exception as exc:
            LOGGER.exception("failed to ingest activity for %s", wallet.wallet_address)
            store.upsert_wallet_coverage(wallet.wallet_address, last_cursor=cursor, status="error", error_text=str(exc))

    return {"wallets": len(wallets), "refreshed": refreshed, "inserted": inserted}
