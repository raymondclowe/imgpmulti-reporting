from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
from typing import Iterable
import json

from authoritative_pnl.connectors.account_registry import AccountRegistryConnector, WalletAccount
from authoritative_pnl.models.attribution import AttributionClaim, AttributedEvent, AttributionSource
from authoritative_pnl.models.events import ActivityType, RawActivityEvent, decimal_to_str, to_decimal
from authoritative_pnl.settings import Settings
from authoritative_pnl.store.sqlite_store import SqliteStore


UNKNOWN_STRATEGY = "UNKNOWN"


@dataclass
class StrategyExposure:
    strategy_name: str
    strategy_class: str | None
    machine_id: str | None
    config_file: str | None
    attribution_source: AttributionSource
    shares: Decimal = Decimal("0")


def _raw_event_from_row(row: dict[str, object]) -> RawActivityEvent:
    return RawActivityEvent(
        event_uid=str(row["event_uid"]),
        wallet_address=str(row["wallet_address"]),
        timestamp_unix=int(row["timestamp_unix"]),
        activity_type=ActivityType(str(row["activity_type"])),
        side=row["side"],
        outcome=row["outcome"],
        size=to_decimal(row["size"]),
        usdc_size=to_decimal(row["usdc_size"]),
        price=to_decimal(row["price"]) if row["price"] is not None else None,
        fees_paid=to_decimal(row["fees_paid"]),
        rebates_earned=to_decimal(row["rebates_earned"]),
        slug=str(row["slug"]),
        condition_id=row["condition_id"],
        transaction_hash=row["transaction_hash"],
        asset=row["asset"],
        payload_json=json.loads(str(row["payload_json"])),
        ingested_at_unix=int(row["ingested_at_unix"]),
        source_cursor=json.loads(str(row["source_cursor"])),
    )


def _claim_from_row(row: dict[str, object]) -> AttributionClaim:
    return AttributionClaim(
        wallet_address=str(row["wallet_address"]).lower() if row["wallet_address"] else None,
        slug=str(row["slug"]),
        strategy_name=str(row["strategy_name"]),
        strategy_class=row["strategy_class"],
        machine_id=row["machine_id"],
        config_file=row["config_file"],
        source=AttributionSource(str(row["source"])),
        source_file=str(row["source_file"]),
        extracted_at_unix=int(row["extracted_at_unix"]),
        raw_payload=json.loads(str(row["raw_payload"])),
    )


def _wallet_lookup(settings: Settings) -> dict[str, WalletAccount]:
    accounts = AccountRegistryConnector(settings.account_registry.path).load_wallets()
    return {account.wallet_address: account for account in accounts}


def _select_claim(
    claims: Iterable[AttributionClaim],
    *,
    wallet_address: str,
    slug: str,
    timestamp_unix: int,
    machine_id: str | None = None,
) -> AttributionClaim | None:
    candidates = [claim for claim in claims if claim.slug == slug and (claim.wallet_address in (None, wallet_address))]
    if machine_id is not None:
        candidates = [claim for claim in candidates if claim.machine_id in (None, machine_id)]
    if not candidates:
        return None
    prior = [claim for claim in candidates if claim.extracted_at_unix <= timestamp_unix]
    source = prior if prior else candidates
    return max(source, key=lambda claim: claim.extracted_at_unix)


def _build_uid(raw_event_uid: str, strategy_name: str, fraction: Decimal) -> str:
    material = f"{raw_event_uid}:{strategy_name}:{decimal_to_str(fraction)}"
    return sha256(material.encode("utf-8")).hexdigest()


def _make_attributed_event(
    raw_event: RawActivityEvent,
    *,
    strategy_name: str,
    strategy_class: str | None,
    machine_id: str | None,
    config_file: str | None,
    attribution_source: AttributionSource,
    fraction: Decimal,
) -> AttributedEvent:
    return AttributedEvent(
        attributed_event_uid=_build_uid(raw_event.event_uid, strategy_name, fraction),
        raw_event_uid=raw_event.event_uid,
        wallet_address=raw_event.wallet_address,
        timestamp_unix=raw_event.timestamp_unix,
        slug=raw_event.slug,
        activity_type=raw_event.activity_type,
        side=raw_event.side,
        outcome=raw_event.outcome,
        strategy_name=strategy_name,
        strategy_class=strategy_class,
        machine_id=machine_id,
        config_file=config_file,
        attribution_source=attribution_source,
        attributed_fraction=fraction,
        size=raw_event.size * fraction,
        usdc_size=raw_event.usdc_size * fraction,
        fees_paid=raw_event.fees_paid * fraction,
        rebates_earned=raw_event.rebates_earned * fraction,
        payload_json=raw_event.payload_json,
    )


def run(settings: Settings, store: SqliteStore) -> dict[str, int]:
    raw_events = [_raw_event_from_row(row) for row in store.get_raw_events()]
    runtime_claims = [_claim_from_row(row) for row in store.get_claims("runtime_midpoint_claims")]
    canonical_claims = [_claim_from_row(row) for row in store.get_claims("canonical_market_claims")]
    legacy_claims = [_claim_from_row(row) for row in store.get_claims("legacy_lookup_claims")]
    wallets = _wallet_lookup(settings)

    exposures: dict[tuple[str, str], dict[str, StrategyExposure]] = defaultdict(dict)
    attributed_events: list[AttributedEvent] = []
    unknown_count = 0

    for raw_event in raw_events:
        wallet_account = wallets.get(raw_event.wallet_address)
        machine_id = wallet_account.machine_id if wallet_account else None
        exposure_key = (raw_event.wallet_address, raw_event.slug)

        if raw_event.activity_type == ActivityType.TRADE:
            claim = _select_claim(runtime_claims, wallet_address=raw_event.wallet_address, slug=raw_event.slug, timestamp_unix=raw_event.timestamp_unix, machine_id=machine_id)
            if claim is None:
                claim = _select_claim(canonical_claims, wallet_address=raw_event.wallet_address, slug=raw_event.slug, timestamp_unix=raw_event.timestamp_unix)
            if claim is None:
                claim = _select_claim(legacy_claims, wallet_address=raw_event.wallet_address, slug=raw_event.slug, timestamp_unix=raw_event.timestamp_unix)

            if claim is None:
                claim = AttributionClaim(
                    wallet_address=raw_event.wallet_address,
                    slug=raw_event.slug,
                    strategy_name=UNKNOWN_STRATEGY,
                    strategy_class=None,
                    machine_id=machine_id,
                    config_file=None,
                    source=AttributionSource.UNKNOWN,
                    source_file="",
                    extracted_at_unix=raw_event.timestamp_unix,
                    raw_payload={},
                )
                unknown_count += 1

            attributed = _make_attributed_event(
                raw_event,
                strategy_name=claim.strategy_name,
                strategy_class=claim.strategy_class,
                machine_id=claim.machine_id or machine_id,
                config_file=claim.config_file,
                attribution_source=claim.source,
                fraction=Decimal("1"),
            )
            attributed_events.append(attributed)
            signed_shares = raw_event.size if str(raw_event.side).lower() == "buy" else raw_event.size * Decimal("-1")
            strategy_exposure = exposures[exposure_key].setdefault(
                claim.strategy_name,
                StrategyExposure(
                    strategy_name=claim.strategy_name,
                    strategy_class=claim.strategy_class,
                    machine_id=claim.machine_id or machine_id,
                    config_file=claim.config_file,
                    attribution_source=claim.source,
                ),
            )
            strategy_exposure.shares += signed_shares
            continue

        positive_exposure = [item for item in exposures[exposure_key].values() if item.shares > 0]
        total_positive = sum((item.shares for item in positive_exposure), Decimal("0"))
        if total_positive > 0:
            for item in positive_exposure:
                fraction = item.shares / total_positive
                attributed_events.append(
                    _make_attributed_event(
                        raw_event,
                        strategy_name=item.strategy_name,
                        strategy_class=item.strategy_class,
                        machine_id=item.machine_id,
                        config_file=item.config_file,
                        attribution_source=item.attribution_source,
                        fraction=fraction,
                    )
                )
        else:
            unknown_count += 1
            attributed_events.append(
                _make_attributed_event(
                    raw_event,
                    strategy_name=UNKNOWN_STRATEGY,
                    strategy_class=None,
                    machine_id=machine_id,
                    config_file=None,
                    attribution_source=AttributionSource.UNKNOWN,
                    fraction=Decimal("1"),
                )
            )

    store.replace_attributed_events(attributed_events)
    return {"attributed_events": len(attributed_events), "unknown_events": unknown_count}
