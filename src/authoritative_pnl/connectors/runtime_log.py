from __future__ import annotations

from pathlib import Path
import json

from authoritative_pnl.models.attribution import AttributionClaim, AttributionSource
from authoritative_pnl.settings import RuntimeLogSettings


class RuntimeLogConnector:
    def __init__(self, settings: RuntimeLogSettings) -> None:
        self.settings = settings

    def extract_claims(self) -> list[AttributionClaim]:
        claims: list[AttributionClaim] = []
        for path in self.settings.paths:
            if not path.exists():
                raise FileNotFoundError(f"runtime log not found: {path}")
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    payload = json.loads(line)
                    claim = self._claim_from_payload(payload, path)
                    if claim is not None:
                        claims.append(claim)
        claims.sort(key=lambda item: (item.wallet_address or "", item.slug, item.extracted_at_unix))
        return claims

    def _claim_from_payload(self, payload: dict[str, object], path: Path) -> AttributionClaim | None:
        slug = str(payload.get("slug") or payload.get("market_slug") or "").strip()
        strategy_name = str(payload.get("strategy_name") or payload.get("strategy") or "").strip()
        if not slug or not strategy_name:
            return None

        machine_id = payload.get("machine_id")
        wallet_address = payload.get("wallet_address") or payload.get("wallet")
        wallet_address = str(wallet_address).lower().strip() if wallet_address else None
        raw_ts = payload.get("midpoint_ts") or payload.get("timestamp_unix") or payload.get("ts")
        if raw_ts is None:
            market_start = payload.get("market_start_ts") or payload.get("market_open_ts") or payload.get("start_ts")
            if market_start is None:
                return None
            raw_ts = int(market_start) + self.settings.midpoint_offset_seconds

        return AttributionClaim(
            wallet_address=wallet_address,
            slug=slug,
            strategy_name=strategy_name,
            strategy_class=str(payload.get("strategy_class") or "").strip() or None,
            machine_id=str(machine_id).strip() if machine_id else None,
            config_file=str(payload.get("config_file") or "").strip() or None,
            source=AttributionSource.RUNTIME_LOG_MIDPOINT,
            source_file=str(path),
            extracted_at_unix=int(raw_ts),
            raw_payload=payload,
        )
