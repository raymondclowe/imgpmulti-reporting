from __future__ import annotations

from pathlib import Path
import json

from authoritative_pnl.models.attribution import AttributionClaim, AttributionSource


class MarketRegistryConnector:
    def __init__(self, canonical_dir: Path | None, legacy_dir: Path | None) -> None:
        self.canonical_dir = canonical_dir
        self.legacy_dir = legacy_dir

    def extract_canonical_claims(self) -> list[AttributionClaim]:
        return self._extract_claims(self.canonical_dir, AttributionSource.CANONICAL_REGISTRY)

    def extract_legacy_claims(self) -> list[AttributionClaim]:
        return self._extract_claims(self.legacy_dir, AttributionSource.LEGACY_LOOKUP)

    def _extract_claims(self, directory: Path | None, source: AttributionSource) -> list[AttributionClaim]:
        if directory is None:
            return []
        if not directory.exists():
            raise FileNotFoundError(f"market registry directory not found: {directory}")

        claims: list[AttributionClaim] = []
        for path in sorted(directory.rglob("*.json")):
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
            items = payload.get("claims") if isinstance(payload, dict) else payload
            if not isinstance(items, list):
                items = [payload]
            for item in items:
                claim = self._claim_from_item(item, path, source)
                if claim is not None:
                    claims.append(claim)

        claims.sort(key=lambda item: (item.wallet_address or "", item.slug, item.extracted_at_unix))
        return claims

    def _claim_from_item(
        self,
        payload: dict[str, object],
        path: Path,
        source: AttributionSource,
    ) -> AttributionClaim | None:
        slug = str(payload.get("slug") or payload.get("market_slug") or "").strip()
        strategy_name = str(payload.get("strategy_name") or payload.get("strategy") or "").strip()
        if not slug or not strategy_name:
            return None
        wallet_address = payload.get("wallet_address") or payload.get("wallet")
        wallet_address = str(wallet_address).lower().strip() if wallet_address else None
        extracted_at_unix = int(payload.get("extracted_at_unix") or payload.get("timestamp_unix") or 0)
        return AttributionClaim(
            wallet_address=wallet_address,
            slug=slug,
            strategy_name=strategy_name,
            strategy_class=str(payload.get("strategy_class") or "").strip() or None,
            machine_id=str(payload.get("machine_id") or "").strip() or None,
            config_file=str(payload.get("config_file") or "").strip() or None,
            source=source,
            source_file=str(path),
            extracted_at_unix=extracted_at_unix,
            raw_payload=payload,
        )
