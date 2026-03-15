from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json
import tomllib


@dataclass(frozen=True)
class WalletAccount:
    wallet_address: str
    machine_id: str | None
    wallet_type: str | None
    raw_payload: dict[str, Any]


class AccountRegistryConnector:
    def __init__(self, path: Path | None) -> None:
        self.path = path

    def load_wallets(self) -> list[WalletAccount]:
        if self.path is None:
            return []
        if not self.path.exists():
            raise FileNotFoundError(f"account registry not found: {self.path}")

        if self.path.suffix == ".toml":
            with self.path.open("rb") as handle:
                payload = tomllib.load(handle)
        else:
            with self.path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)

        wallets = payload.get("wallets", payload)
        records: list[WalletAccount] = []
        for item in wallets:
            wallet_address = str(item.get("wallet_address") or item.get("address") or "").lower().strip()
            if not wallet_address:
                continue
            records.append(
                WalletAccount(
                    wallet_address=wallet_address,
                    machine_id=item.get("machine_id"),
                    wallet_type=item.get("wallet_type") or item.get("kind"),
                    raw_payload=item,
                )
            )
        return records
