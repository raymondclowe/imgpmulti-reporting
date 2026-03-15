from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json
import os
import time

from authoritative_pnl.models.events import RawActivityEvent
from authoritative_pnl.settings import ActivitySourceSettings


@dataclass(frozen=True)
class ActivityPage:
    events: list[RawActivityEvent]
    next_cursor: str | None


class PolymarketActivityClient:
    def __init__(self, settings: ActivitySourceSettings) -> None:
        self.settings = settings

    def fetch_wallet_activity(self, wallet_address: str, cursor: str | None = None) -> ActivityPage:
        if not self.settings.endpoint_url:
            raise ValueError("activity endpoint_url is not configured")

        query = {
            "wallet": wallet_address,
            "limit": self.settings.page_size,
            "includeDepositsWithdrawals": str(self.settings.include_deposits_withdrawals).lower(),
        }
        if cursor:
            query["cursor"] = cursor
        request = Request(f"{self.settings.endpoint_url}?{urlencode(query)}")
        api_key = os.getenv(self.settings.api_key_env)
        if api_key:
            request.add_header("Authorization", f"Bearer {api_key}")
        request.add_header("Accept", "application/json")

        response_data = self._execute(request)
        items = response_data.get("items") or response_data.get("data") or response_data.get("events") or []
        next_cursor = response_data.get("next_cursor") or response_data.get("nextCursor") or response_data.get("cursor")
        events = [
            RawActivityEvent.from_payload(
                payload=item,
                wallet_address=wallet_address,
                source_cursor={"cursor": cursor, "next_cursor": next_cursor},
            )
            for item in items
        ]
        return ActivityPage(events=events, next_cursor=next_cursor)

    def _execute(self, request: Request) -> dict[str, Any]:
        delay = 1.0 / max(self.settings.rate_limit_per_second, 0.1)
        time.sleep(delay)
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
