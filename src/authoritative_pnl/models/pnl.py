from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any
import json

from authoritative_pnl.models.events import decimal_to_str


@dataclass(frozen=True)
class PnlFact:
    key: str
    wallet_address: str | None
    slug: str | None
    strategy_name: str | None
    fact_date: str | None
    gross_inflow_usdc: Decimal
    gross_outflow_usdc: Decimal
    fees_paid: Decimal
    rebates_earned: Decimal
    net_cashflow_usdc: Decimal
    open_share_balance: Decimal
    open_cost_usdc: Decimal
    redeemable_value_usdc: Decimal
    realized_pnl_usdc: Decimal | None
    unrealized_exposure_usdc: Decimal | None
    computation_version: str
    input_window: str
    source_snapshot_id: str
    unresolved_flags: tuple[str, ...]

    def as_record(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "wallet_address": self.wallet_address,
            "slug": self.slug,
            "strategy_name": self.strategy_name,
            "fact_date": self.fact_date,
            "gross_inflow_usdc": decimal_to_str(self.gross_inflow_usdc),
            "gross_outflow_usdc": decimal_to_str(self.gross_outflow_usdc),
            "fees_paid": decimal_to_str(self.fees_paid),
            "rebates_earned": decimal_to_str(self.rebates_earned),
            "net_cashflow_usdc": decimal_to_str(self.net_cashflow_usdc),
            "open_share_balance": decimal_to_str(self.open_share_balance),
            "open_cost_usdc": decimal_to_str(self.open_cost_usdc),
            "redeemable_value_usdc": decimal_to_str(self.redeemable_value_usdc),
            "realized_pnl_usdc": None if self.realized_pnl_usdc is None else decimal_to_str(self.realized_pnl_usdc),
            "unrealized_exposure_usdc": None if self.unrealized_exposure_usdc is None else decimal_to_str(self.unrealized_exposure_usdc),
            "computation_version": self.computation_version,
            "input_window": self.input_window,
            "source_snapshot_id": self.source_snapshot_id,
            "unresolved_flags": json.dumps(list(self.unresolved_flags)),
        }
