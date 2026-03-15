from decimal import Decimal
import unittest

from authoritative_pnl.models.events import RawActivityEvent


class RawActivityEventTests(unittest.TestCase):
    def test_builds_deterministic_uid(self) -> None:
        payload = {
            "id": "evt-1",
            "type": "trade",
            "timestamp": 1700000000,
            "side": "buy",
            "outcome": "YES",
            "shares": "10",
            "amount": "4.1",
            "fee": "0.1",
            "slug": "example-market",
            "transaction_hash": "0xabc",
        }
        first = RawActivityEvent.from_payload(payload=payload, wallet_address="0xwallet")
        second = RawActivityEvent.from_payload(payload=payload, wallet_address="0xwallet")

        self.assertEqual(first.event_uid, second.event_uid)
        self.assertEqual(first.activity_type.value, "TRADE")
        self.assertEqual(first.size, Decimal("10"))
        self.assertEqual(first.usdc_size, Decimal("4.1"))


if __name__ == "__main__":
    unittest.main()
