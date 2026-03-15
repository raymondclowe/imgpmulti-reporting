from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Callable
import json

from authoritative_pnl.models.events import ActivityType, to_decimal
from authoritative_pnl.models.pnl import PnlFact
from authoritative_pnl.settings import Settings
from authoritative_pnl.store.sqlite_store import SqliteStore


COMPUTATION_VERSION = "v1"


@dataclass
class OutcomeLedger:
    shares: Decimal = Decimal("0")
    cost_basis: Decimal = Decimal("0")


@dataclass
class AggregateState:
    wallet_address: str | None = None
    slug: str | None = None
    strategy_name: str | None = None
    fact_date: str | None = None
    gross_inflow_usdc: Decimal = Decimal("0")
    gross_outflow_usdc: Decimal = Decimal("0")
    fees_paid: Decimal = Decimal("0")
    rebates_earned: Decimal = Decimal("0")
    ledgers: dict[str, OutcomeLedger] = field(default_factory=lambda: defaultdict(OutcomeLedger))
    unresolved_flags: set[str] = field(default_factory=set)

    def apply_trade(self, row: dict[str, object]) -> None:
        outcome = str(row.get("outcome") or "")
        ledger = self.ledgers[outcome]
        side = str(row.get("side") or "").lower()
        size = to_decimal(row.get("size"))
        usdc_size = to_decimal(row.get("usdc_size"))
        fees = to_decimal(row.get("fees_paid"))
        rebates = to_decimal(row.get("rebates_earned"))
        self.fees_paid += fees
        self.rebates_earned += rebates

        if side == "buy":
            self.gross_outflow_usdc += usdc_size
            ledger.shares += size
            ledger.cost_basis += usdc_size + fees - rebates
            return

        self.gross_inflow_usdc += usdc_size
        if ledger.shares > 0 and size > 0:
            reduction_ratio = min(Decimal("1"), size / ledger.shares)
            ledger.cost_basis -= ledger.cost_basis * reduction_ratio
        ledger.shares -= size

    def apply_non_trade(self, row: dict[str, object]) -> None:
        activity_type = ActivityType(str(row["activity_type"]))
        usdc_size = to_decimal(row.get("usdc_size"))
        fees = to_decimal(row.get("fees_paid"))
        rebates = to_decimal(row.get("rebates_earned"))
        size = to_decimal(row.get("size"))
        outcome = str(row.get("outcome") or "")
        self.fees_paid += fees
        self.rebates_earned += rebates

        if activity_type in {ActivityType.REDEEM, ActivityType.MERGE, ActivityType.WITHDRAWAL}:
            self.gross_inflow_usdc += usdc_size
        elif activity_type == ActivityType.DEPOSIT:
            self.gross_outflow_usdc += usdc_size

        if activity_type == ActivityType.REDEEM and outcome:
            ledger = self.ledgers[outcome]
            if ledger.shares > 0 and size > 0:
                reduction_ratio = min(Decimal("1"), size / ledger.shares)
                ledger.cost_basis -= ledger.cost_basis * reduction_ratio
            ledger.shares -= size
        elif activity_type == ActivityType.MERGE:
            self.unresolved_flags.add("inventory_not_adjusted_for_merge")

    def finalize(
        self,
        *,
        key: str,
        input_window: str,
        source_snapshot_id: str,
        resolution: dict[str, object] | None,
    ) -> PnlFact:
        open_share_balance = sum((ledger.shares for ledger in self.ledgers.values()), Decimal("0"))
        open_cost_usdc = sum((ledger.cost_basis for ledger in self.ledgers.values()), Decimal("0"))
        redeemable_value_usdc = Decimal("0")
        realized_pnl_usdc: Decimal | None = None
        unrealized_exposure_usdc: Decimal | None = open_cost_usdc

        if resolution and int(resolution["is_resolved"]) == 1:
            winning_outcome = resolution.get("winning_outcome")
            if winning_outcome:
                winning_ledger = self.ledgers.get(str(winning_outcome), OutcomeLedger())
                if winning_ledger.shares > 0:
                    redeemable_value_usdc = winning_ledger.shares
                    self.unresolved_flags.add("unredeemed_winning_shares")
            if any(ledger.shares > 0 for ledger in self.ledgers.values()):
                losers = [name for name, ledger in self.ledgers.items() if ledger.shares > 0 and name != winning_outcome]
                if losers:
                    self.unresolved_flags.add("resolved_market_open_losing_shares")
            realized_pnl_usdc = self.gross_inflow_usdc + redeemable_value_usdc + self.rebates_earned - self.gross_outflow_usdc - self.fees_paid
            unrealized_exposure_usdc = None

        net_cashflow_usdc = self.gross_inflow_usdc + self.rebates_earned - self.gross_outflow_usdc - self.fees_paid
        return PnlFact(
            key=key,
            wallet_address=self.wallet_address,
            slug=self.slug,
            strategy_name=self.strategy_name,
            fact_date=self.fact_date,
            gross_inflow_usdc=self.gross_inflow_usdc,
            gross_outflow_usdc=self.gross_outflow_usdc,
            fees_paid=self.fees_paid,
            rebates_earned=self.rebates_earned,
            net_cashflow_usdc=net_cashflow_usdc,
            open_share_balance=open_share_balance,
            open_cost_usdc=open_cost_usdc,
            redeemable_value_usdc=redeemable_value_usdc,
            realized_pnl_usdc=realized_pnl_usdc,
            unrealized_exposure_usdc=unrealized_exposure_usdc,
            computation_version=COMPUTATION_VERSION,
            input_window=input_window,
            source_snapshot_id=source_snapshot_id,
            unresolved_flags=tuple(sorted(self.unresolved_flags)),
        )


def _build_fact_table(
    rows: list[dict[str, object]],
    resolution_map: dict[str, dict[str, object]],
    metadata: dict[str, object],
    key_builder: Callable[[dict[str, object]], tuple[str, AggregateState]],
) -> list[PnlFact]:
    states: dict[str, AggregateState] = {}
    for row in rows:
        key, template = key_builder(row)
        state = states.setdefault(key, template)
        activity_type = ActivityType(str(row["activity_type"]))
        if activity_type == ActivityType.TRADE:
            state.apply_trade(row)
        else:
            state.apply_non_trade(row)

    facts: list[PnlFact] = []
    for key, state in states.items():
        resolution = resolution_map.get(state.slug) if state.slug else None
        facts.append(
            state.finalize(
                key=key,
                input_window=str(metadata["input_window"]),
                source_snapshot_id=str(metadata["source_snapshot_id"]),
                resolution=resolution,
            )
        )
    return sorted(facts, key=lambda fact: fact.key)


def run(settings: Settings, store: SqliteStore) -> dict[str, int]:
    del settings
    rows = store.get_attributed_events()
    resolution_map = store.get_resolution_map()
    metadata = store.latest_snapshot_metadata()

    def market_key_builder(row: dict[str, object]) -> tuple[str, AggregateState]:
        key = f"{row['wallet_address']}:{row['slug']}"
        return key, AggregateState(wallet_address=str(row["wallet_address"]), slug=str(row["slug"]))

    def strategy_key_builder(row: dict[str, object]) -> tuple[str, AggregateState]:
        key = f"{row['wallet_address']}:{row['strategy_name']}"
        return key, AggregateState(wallet_address=str(row["wallet_address"]), strategy_name=str(row["strategy_name"]))

    def wallet_key_builder(row: dict[str, object]) -> tuple[str, AggregateState]:
        key = str(row["wallet_address"])
        return key, AggregateState(wallet_address=key)

    def daily_key_builder(row: dict[str, object]) -> tuple[str, AggregateState]:
        day = _date_from_timestamp(int(row["timestamp_unix"]))
        key = f"{row['wallet_address']}:{day}"
        return key, AggregateState(wallet_address=str(row["wallet_address"]), fact_date=day)

    market_facts = _build_fact_table(rows, resolution_map, metadata, market_key_builder)
    strategy_facts = _build_fact_table(rows, resolution_map, metadata, strategy_key_builder)
    wallet_facts = _build_fact_table(rows, resolution_map, metadata, wallet_key_builder)
    daily_facts = _build_fact_table(rows, resolution_map, metadata, daily_key_builder)

    store.replace_pnl_facts("pnl_market_facts", market_facts)
    store.replace_pnl_facts("pnl_strategy_facts", strategy_facts)
    store.replace_pnl_facts("pnl_wallet_facts", wallet_facts)
    store.replace_pnl_facts("pnl_daily_facts", daily_facts)

    return {
        "market_facts": len(market_facts),
        "strategy_facts": len(strategy_facts),
        "wallet_facts": len(wallet_facts),
        "daily_facts": len(daily_facts),
    }


def _date_from_timestamp(timestamp_unix: int) -> str:
    from datetime import UTC, datetime

    return datetime.fromtimestamp(timestamp_unix, tz=UTC).date().isoformat()
