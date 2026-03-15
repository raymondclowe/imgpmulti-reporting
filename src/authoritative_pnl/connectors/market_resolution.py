from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import csv
import json


@dataclass(frozen=True)
class MarketResolutionRecord:
    slug: str
    is_resolved: bool
    winning_outcome: str | None
    resolved_at_unix: int | None
    raw_payload: dict[str, object]


class MarketResolutionConnector:
    def __init__(self, path: Path | None) -> None:
        self.path = path

    def load(self) -> list[MarketResolutionRecord]:
        if self.path is None:
            return []
        if not self.path.exists():
            raise FileNotFoundError(f"market resolution file not found: {self.path}")
        if self.path.suffix == ".csv":
            return self._load_csv()
        return self._load_json()

    def _load_csv(self) -> list[MarketResolutionRecord]:
        rows: list[MarketResolutionRecord] = []
        with self.path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                rows.append(
                    MarketResolutionRecord(
                        slug=str(row.get("slug") or "").strip(),
                        is_resolved=str(row.get("is_resolved") or "false").lower() == "true",
                        winning_outcome=str(row.get("winning_outcome") or "").strip() or None,
                        resolved_at_unix=int(row["resolved_at_unix"]) if row.get("resolved_at_unix") else None,
                        raw_payload=row,
                    )
                )
        return rows

    def _load_json(self) -> list[MarketResolutionRecord]:
        with self.path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        items = payload.get("markets") if isinstance(payload, dict) else payload
        records: list[MarketResolutionRecord] = []
        for item in items:
            records.append(
                MarketResolutionRecord(
                    slug=str(item.get("slug") or "").strip(),
                    is_resolved=bool(item.get("is_resolved")),
                    winning_outcome=str(item.get("winning_outcome") or "").strip() or None,
                    resolved_at_unix=int(item["resolved_at_unix"]) if item.get("resolved_at_unix") else None,
                    raw_payload=item,
                )
            )
        return records
