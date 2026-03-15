from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
from enum import Enum
from typing import Any
import json

from authoritative_pnl.models.events import ActivityType, decimal_to_str


class AttributionSource(str, Enum):
    RUNTIME_LOG_MIDPOINT = "runtime_log_midpoint"
    CANONICAL_REGISTRY = "canonical_registry"
    LEGACY_LOOKUP = "legacy_lookup"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class AttributionClaim:
    wallet_address: str | None
    slug: str
    strategy_name: str
    strategy_class: str | None
    machine_id: str | None
    config_file: str | None
    source: AttributionSource
    source_file: str
    extracted_at_unix: int
    raw_payload: dict[str, Any]

    def as_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["source"] = self.source.value
        record["raw_payload"] = json.dumps(self.raw_payload, separators=(",", ":"), sort_keys=True)
        return record


@dataclass(frozen=True)
class AttributedEvent:
    attributed_event_uid: str
    raw_event_uid: str
    wallet_address: str
    timestamp_unix: int
    slug: str
    activity_type: ActivityType
    side: str | None
    outcome: str | None
    strategy_name: str
    strategy_class: str | None
    machine_id: str | None
    config_file: str | None
    attribution_source: AttributionSource
    attributed_fraction: Decimal
    size: Decimal
    usdc_size: Decimal
    fees_paid: Decimal
    rebates_earned: Decimal
    payload_json: dict[str, Any]

    def as_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["activity_type"] = self.activity_type.value
        record["attribution_source"] = self.attribution_source.value
        record["attributed_fraction"] = decimal_to_str(self.attributed_fraction)
        record["size"] = decimal_to_str(self.size)
        record["usdc_size"] = decimal_to_str(self.usdc_size)
        record["fees_paid"] = decimal_to_str(self.fees_paid)
        record["rebates_earned"] = decimal_to_str(self.rebates_earned)
        record["payload_json"] = json.dumps(self.payload_json, separators=(",", ":"), sort_keys=True)
        return record
