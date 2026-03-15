from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
from enum import Enum
from hashlib import sha256
from typing import Any
import json
import time


class ActivityType(str, Enum):
    TRADE = "TRADE"
    MERGE = "MERGE"
    REDEEM = "REDEEM"
    SPLIT = "SPLIT"
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"


def decimal_to_str(value: Decimal) -> str:
    return format(value.normalize(), "f") if value != 0 else "0"


def to_decimal(value: Any, default: str = "0") -> Decimal:
    if value is None or value == "":
        return Decimal(default)
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def normalize_payload_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def build_event_uid(identity: dict[str, Any]) -> str:
    blob = json.dumps(identity, separators=(",", ":"), sort_keys=True)
    return sha256(blob.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RawActivityEvent:
    event_uid: str
    wallet_address: str
    timestamp_unix: int
    activity_type: ActivityType
    side: str | None
    outcome: str | None
    size: Decimal
    usdc_size: Decimal
    price: Decimal | None
    fees_paid: Decimal
    rebates_earned: Decimal
    slug: str
    condition_id: str | None
    transaction_hash: str | None
    asset: str | None
    payload_json: dict[str, Any]
    ingested_at_unix: int
    source_cursor: dict[str, Any]

    @classmethod
    def from_payload(
        cls,
        *,
        payload: dict[str, Any],
        wallet_address: str,
        source_cursor: dict[str, Any] | None = None,
        ingested_at_unix: int | None = None,
    ) -> "RawActivityEvent":
        activity_type = ActivityType(str(payload.get("activity_type") or payload.get("type") or "TRADE").upper())
        timestamp_unix = int(payload.get("timestamp_unix") or payload.get("timestamp") or payload.get("ts") or payload.get("created_at_epoch") or 0)
        side = payload.get("side")
        outcome = payload.get("outcome")
        size = to_decimal(payload.get("size") or payload.get("shares") or payload.get("quantity"))
        usdc_size = to_decimal(payload.get("usdc_size") or payload.get("amount") or payload.get("notional"))
        price_value = payload.get("price")
        price = to_decimal(price_value) if price_value not in (None, "") else None
        fees_paid = to_decimal(payload.get("fees_paid") or payload.get("fee") or payload.get("fees"))
        rebates_earned = to_decimal(payload.get("rebates_earned") or payload.get("rebate"))
        slug = str(payload.get("slug") or payload.get("market_slug") or "").strip()
        if not slug:
            raise ValueError("activity payload missing slug")
        condition_id = payload.get("condition_id")
        transaction_hash = payload.get("transaction_hash") or payload.get("tx_hash")
        asset = payload.get("asset")
        source_cursor = source_cursor or {}
        ingested_at_unix = int(ingested_at_unix or time.time())

        identity = {
            "wallet_address": wallet_address.lower(),
            "timestamp_unix": timestamp_unix,
            "activity_type": activity_type.value,
            "side": side,
            "outcome": outcome,
            "size": decimal_to_str(size),
            "usdc_size": decimal_to_str(usdc_size),
            "price": decimal_to_str(price) if price is not None else None,
            "fees_paid": decimal_to_str(fees_paid),
            "rebates_earned": decimal_to_str(rebates_earned),
            "slug": slug,
            "condition_id": condition_id,
            "transaction_hash": transaction_hash,
            "asset": asset,
            "provider_event_id": payload.get("id") or payload.get("activity_id"),
        }

        return cls(
            event_uid=build_event_uid(identity),
            wallet_address=wallet_address.lower(),
            timestamp_unix=timestamp_unix,
            activity_type=activity_type,
            side=side,
            outcome=outcome,
            size=size,
            usdc_size=usdc_size,
            price=price,
            fees_paid=fees_paid,
            rebates_earned=rebates_earned,
            slug=slug,
            condition_id=condition_id,
            transaction_hash=transaction_hash,
            asset=asset,
            payload_json=payload,
            ingested_at_unix=ingested_at_unix,
            source_cursor=source_cursor,
        )

    def as_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["activity_type"] = self.activity_type.value
        record["size"] = decimal_to_str(self.size)
        record["usdc_size"] = decimal_to_str(self.usdc_size)
        record["price"] = decimal_to_str(self.price) if self.price is not None else None
        record["fees_paid"] = decimal_to_str(self.fees_paid)
        record["rebates_earned"] = decimal_to_str(self.rebates_earned)
        record["payload_json"] = normalize_payload_json(self.payload_json)
        record["source_cursor"] = normalize_payload_json(self.source_cursor)
        return record
