from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from pathlib import Path
from typing import Any
import json
import sqlite3
import time

from authoritative_pnl.connectors.market_resolution import MarketResolutionRecord
from authoritative_pnl.models.attribution import AttributionClaim, AttributedEvent
from authoritative_pnl.models.events import RawActivityEvent, to_decimal
from authoritative_pnl.models.pnl import PnlFact


def _json_loads(value: str | None) -> Any:
    return json.loads(value) if value else None


class SqliteStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS raw_activity_events (
                    event_uid TEXT PRIMARY KEY,
                    wallet_address TEXT NOT NULL,
                    timestamp_unix INTEGER NOT NULL,
                    activity_type TEXT NOT NULL,
                    side TEXT,
                    outcome TEXT,
                    size TEXT NOT NULL,
                    usdc_size TEXT NOT NULL,
                    price TEXT,
                    fees_paid TEXT NOT NULL,
                    rebates_earned TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    condition_id TEXT,
                    transaction_hash TEXT,
                    asset TEXT,
                    payload_json TEXT NOT NULL,
                    ingested_at_unix INTEGER NOT NULL,
                    source_cursor TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_raw_activity_wallet_slug ON raw_activity_events(wallet_address, slug);
                CREATE INDEX IF NOT EXISTS idx_raw_activity_wallet_ts ON raw_activity_events(wallet_address, timestamp_unix);
                CREATE INDEX IF NOT EXISTS idx_raw_activity_slug ON raw_activity_events(slug);
                CREATE INDEX IF NOT EXISTS idx_raw_activity_type ON raw_activity_events(activity_type);

                CREATE TABLE IF NOT EXISTS runtime_midpoint_claims (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    wallet_address TEXT,
                    slug TEXT NOT NULL,
                    strategy_name TEXT NOT NULL,
                    strategy_class TEXT,
                    machine_id TEXT,
                    config_file TEXT,
                    source TEXT NOT NULL,
                    source_file TEXT NOT NULL,
                    extracted_at_unix INTEGER NOT NULL,
                    raw_payload TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS canonical_market_claims (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    wallet_address TEXT,
                    slug TEXT NOT NULL,
                    strategy_name TEXT NOT NULL,
                    strategy_class TEXT,
                    machine_id TEXT,
                    config_file TEXT,
                    source TEXT NOT NULL,
                    source_file TEXT NOT NULL,
                    extracted_at_unix INTEGER NOT NULL,
                    raw_payload TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS legacy_lookup_claims (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    wallet_address TEXT,
                    slug TEXT NOT NULL,
                    strategy_name TEXT NOT NULL,
                    strategy_class TEXT,
                    machine_id TEXT,
                    config_file TEXT,
                    source TEXT NOT NULL,
                    source_file TEXT NOT NULL,
                    extracted_at_unix INTEGER NOT NULL,
                    raw_payload TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS wallet_coverage (
                    wallet_address TEXT PRIMARY KEY,
                    last_cursor TEXT,
                    last_refresh_ts INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    error_text TEXT
                );

                CREATE TABLE IF NOT EXISTS resolution_records (
                    slug TEXT PRIMARY KEY,
                    is_resolved INTEGER NOT NULL,
                    winning_outcome TEXT,
                    resolved_at_unix INTEGER,
                    raw_payload TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS attributed_events (
                    attributed_event_uid TEXT PRIMARY KEY,
                    raw_event_uid TEXT NOT NULL,
                    wallet_address TEXT NOT NULL,
                    timestamp_unix INTEGER NOT NULL,
                    slug TEXT NOT NULL,
                    activity_type TEXT NOT NULL,
                    side TEXT,
                    outcome TEXT,
                    strategy_name TEXT NOT NULL,
                    strategy_class TEXT,
                    machine_id TEXT,
                    config_file TEXT,
                    attribution_source TEXT NOT NULL,
                    attributed_fraction TEXT NOT NULL,
                    size TEXT NOT NULL,
                    usdc_size TEXT NOT NULL,
                    fees_paid TEXT NOT NULL,
                    rebates_earned TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_attributed_wallet_slug_ts ON attributed_events(wallet_address, slug, timestamp_unix);
                CREATE INDEX IF NOT EXISTS idx_attributed_strategy ON attributed_events(strategy_name);

                CREATE TABLE IF NOT EXISTS pnl_market_facts (
                    key TEXT PRIMARY KEY,
                    wallet_address TEXT,
                    slug TEXT,
                    strategy_name TEXT,
                    fact_date TEXT,
                    gross_inflow_usdc TEXT NOT NULL,
                    gross_outflow_usdc TEXT NOT NULL,
                    fees_paid TEXT NOT NULL,
                    rebates_earned TEXT NOT NULL,
                    net_cashflow_usdc TEXT NOT NULL,
                    open_share_balance TEXT NOT NULL,
                    open_cost_usdc TEXT NOT NULL,
                    redeemable_value_usdc TEXT NOT NULL,
                    realized_pnl_usdc TEXT,
                    unrealized_exposure_usdc TEXT,
                    computation_version TEXT NOT NULL,
                    input_window TEXT NOT NULL,
                    source_snapshot_id TEXT NOT NULL,
                    unresolved_flags TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pnl_strategy_facts AS SELECT * FROM pnl_market_facts WHERE 0;
                CREATE TABLE IF NOT EXISTS pnl_wallet_facts AS SELECT * FROM pnl_market_facts WHERE 0;
                CREATE TABLE IF NOT EXISTS pnl_daily_facts AS SELECT * FROM pnl_market_facts WHERE 0;
                """
            )

    def insert_raw_events(self, events: Iterable[RawActivityEvent]) -> int:
        rows = [event.as_record() for event in events]
        if not rows:
            return 0
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT OR IGNORE INTO raw_activity_events (
                    event_uid, wallet_address, timestamp_unix, activity_type, side, outcome,
                    size, usdc_size, price, fees_paid, rebates_earned, slug, condition_id,
                    transaction_hash, asset, payload_json, ingested_at_unix, source_cursor
                ) VALUES (
                    :event_uid, :wallet_address, :timestamp_unix, :activity_type, :side, :outcome,
                    :size, :usdc_size, :price, :fees_paid, :rebates_earned, :slug, :condition_id,
                    :transaction_hash, :asset, :payload_json, :ingested_at_unix, :source_cursor
                )
                """,
                rows,
            )
            return connection.total_changes

    def replace_claims(self, table_name: str, claims: Iterable[AttributionClaim]) -> None:
        rows = [claim.as_record() for claim in claims]
        with self.connect() as connection:
            connection.execute(f"DELETE FROM {table_name}")
            if rows:
                connection.executemany(
                    f"""
                    INSERT INTO {table_name} (
                        wallet_address, slug, strategy_name, strategy_class, machine_id,
                        config_file, source, source_file, extracted_at_unix, raw_payload
                    ) VALUES (
                        :wallet_address, :slug, :strategy_name, :strategy_class, :machine_id,
                        :config_file, :source, :source_file, :extracted_at_unix, :raw_payload
                    )
                    """,
                    rows,
                )

    def upsert_wallet_coverage(
        self,
        wallet_address: str,
        *,
        last_cursor: str | None,
        status: str,
        error_text: str | None = None,
        last_refresh_ts: int | None = None,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO wallet_coverage(wallet_address, last_cursor, last_refresh_ts, status, error_text)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(wallet_address) DO UPDATE SET
                    last_cursor=excluded.last_cursor,
                    last_refresh_ts=excluded.last_refresh_ts,
                    status=excluded.status,
                    error_text=excluded.error_text
                """,
                (wallet_address.lower(), last_cursor, last_refresh_ts or int(time.time()), status, error_text),
            )

    def replace_resolution_records(self, records: Iterable[MarketResolutionRecord]) -> None:
        rows = [
            {
                "slug": record.slug,
                "is_resolved": 1 if record.is_resolved else 0,
                "winning_outcome": record.winning_outcome,
                "resolved_at_unix": record.resolved_at_unix,
                "raw_payload": json.dumps(record.raw_payload, separators=(",", ":"), sort_keys=True),
            }
            for record in records
            if record.slug
        ]
        with self.connect() as connection:
            connection.execute("DELETE FROM resolution_records")
            if rows:
                connection.executemany(
                    """
                    INSERT INTO resolution_records(slug, is_resolved, winning_outcome, resolved_at_unix, raw_payload)
                    VALUES (:slug, :is_resolved, :winning_outcome, :resolved_at_unix, :raw_payload)
                    """,
                    rows,
                )

    def replace_attributed_events(self, events: Iterable[AttributedEvent]) -> None:
        rows = [event.as_record() for event in events]
        with self.connect() as connection:
            connection.execute("DELETE FROM attributed_events")
            if rows:
                connection.executemany(
                    """
                    INSERT INTO attributed_events (
                        attributed_event_uid, raw_event_uid, wallet_address, timestamp_unix, slug,
                        activity_type, side, outcome, strategy_name, strategy_class, machine_id,
                        config_file, attribution_source, attributed_fraction, size, usdc_size,
                        fees_paid, rebates_earned, payload_json
                    ) VALUES (
                        :attributed_event_uid, :raw_event_uid, :wallet_address, :timestamp_unix, :slug,
                        :activity_type, :side, :outcome, :strategy_name, :strategy_class, :machine_id,
                        :config_file, :attribution_source, :attributed_fraction, :size, :usdc_size,
                        :fees_paid, :rebates_earned, :payload_json
                    )
                    """,
                    rows,
                )

    def replace_pnl_facts(self, table_name: str, facts: Iterable[PnlFact]) -> None:
        rows = [fact.as_record() for fact in facts]
        with self.connect() as connection:
            connection.execute(f"DELETE FROM {table_name}")
            if rows:
                connection.executemany(
                    f"""
                    INSERT INTO {table_name} (
                        key, wallet_address, slug, strategy_name, fact_date, gross_inflow_usdc,
                        gross_outflow_usdc, fees_paid, rebates_earned, net_cashflow_usdc,
                        open_share_balance, open_cost_usdc, redeemable_value_usdc,
                        realized_pnl_usdc, unrealized_exposure_usdc, computation_version,
                        input_window, source_snapshot_id, unresolved_flags
                    ) VALUES (
                        :key, :wallet_address, :slug, :strategy_name, :fact_date, :gross_inflow_usdc,
                        :gross_outflow_usdc, :fees_paid, :rebates_earned, :net_cashflow_usdc,
                        :open_share_balance, :open_cost_usdc, :redeemable_value_usdc,
                        :realized_pnl_usdc, :unrealized_exposure_usdc, :computation_version,
                        :input_window, :source_snapshot_id, :unresolved_flags
                    )
                    """,
                    rows,
                )

    def list_wallet_coverage(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute("SELECT * FROM wallet_coverage ORDER BY wallet_address").fetchall()
        return [dict(row) for row in rows]

    def get_raw_events(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute("SELECT * FROM raw_activity_events ORDER BY wallet_address, slug, timestamp_unix, event_uid").fetchall()
        return [dict(row) for row in rows]

    def get_claims(self, table_name: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(f"SELECT * FROM {table_name} ORDER BY extracted_at_unix, id").fetchall()
        return [dict(row) for row in rows]

    def get_resolution_map(self) -> dict[str, dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute("SELECT * FROM resolution_records").fetchall()
        return {row["slug"]: dict(row) for row in rows}

    def get_attributed_events(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute("SELECT * FROM attributed_events ORDER BY wallet_address, slug, timestamp_unix, attributed_event_uid").fetchall()
        return [dict(row) for row in rows]

    def get_fact_rows(self, table_name: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(f"SELECT * FROM {table_name} ORDER BY key").fetchall()
        return [dict(row) for row in rows]

    def query_rows(self, table_name: str, *, filters: dict[str, str] | None = None, limit: int = 100) -> list[dict[str, Any]]:
        filters = filters or {}
        sql = f"SELECT * FROM {table_name}"
        params: list[Any] = []
        if filters:
            clauses = []
            for key, value in filters.items():
                clauses.append(f"{key} = ?")
                params.append(value)
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY 1 LIMIT ?"
        params.append(limit)
        with self.connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def latest_snapshot_metadata(self) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT MIN(timestamp_unix) AS min_ts, MAX(timestamp_unix) AS max_ts, COUNT(*) AS count FROM raw_activity_events"
            ).fetchone()
        min_ts = row["min_ts"] or 0
        max_ts = row["max_ts"] or 0
        count = row["count"] or 0
        return {
            "input_window": f"{min_ts}:{max_ts}",
            "source_snapshot_id": f"raw-{count}-{max_ts}",
        }

    @staticmethod
    def decimal_from_row(row: dict[str, Any], key: str) -> Decimal:
        return to_decimal(row.get(key))

    @staticmethod
    def json_from_row(row: dict[str, Any], key: str) -> Any:
        return _json_loads(row.get(key))
