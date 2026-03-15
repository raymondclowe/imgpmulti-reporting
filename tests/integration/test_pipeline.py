from pathlib import Path
from tempfile import TemporaryDirectory
import json
import unittest

from authoritative_pnl.models.events import RawActivityEvent
from authoritative_pnl.pipeline import build_attribution, compute_pnl, ingest_attribution_context
from authoritative_pnl.settings import load_settings
from authoritative_pnl.store.sqlite_store import SqliteStore


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class PipelineIntegrationTests(unittest.TestCase):
    def test_attribution_precedence_and_pro_rata(self) -> None:
        with TemporaryDirectory() as temp_dir:
            tmp_path = Path(temp_dir)
            config_path = tmp_path / "config" / "reporting.toml"
            registry_path = tmp_path / "wallets.json"
            runtime_log_path = tmp_path / "runtime.jsonl"
            canonical_dir = tmp_path / "canonical"
            legacy_dir = tmp_path / "legacy"
            resolution_path = tmp_path / "resolutions.json"
            sqlite_path = tmp_path / "var" / "authoritative.sqlite3"

            _write(
                registry_path,
                json.dumps({"wallets": [{"wallet_address": "0xwallet", "machine_id": "machine-a"}]}),
            )
            _write(
                runtime_log_path,
                "\n".join(
                    [
                        json.dumps({
                            "wallet_address": "0xwallet",
                            "machine_id": "machine-a",
                            "slug": "market-1",
                            "strategy_name": "alpha",
                            "strategy_class": "maker",
                            "midpoint_ts": 100,
                        }),
                        json.dumps({
                            "wallet_address": "0xwallet",
                            "machine_id": "machine-a",
                            "slug": "market-1",
                            "strategy_name": "beta",
                            "strategy_class": "maker",
                            "midpoint_ts": 200,
                        }),
                    ]
                ),
            )
            _write(
                canonical_dir / "canonical.json",
                json.dumps({"claims": [{"wallet_address": "0xwallet", "slug": "market-1", "strategy_name": "canonical", "extracted_at_unix": 1}]}),
            )
            _write(
                legacy_dir / "legacy.json",
                json.dumps({"claims": [{"wallet_address": "0xwallet", "slug": "market-1", "strategy_name": "legacy", "extracted_at_unix": 1}]}),
            )
            _write(
                resolution_path,
                json.dumps({"markets": [{"slug": "market-1", "is_resolved": True, "winning_outcome": "YES", "resolved_at_unix": 500}]}),
            )
            _write(
                config_path,
                f"""
[storage]
sqlite_path = \"{sqlite_path}\"
output_dir = \"{tmp_path / 'out'}\"

[sources.activity]
endpoint_url = \"\"

[sources.account_registry]
path = \"{registry_path}\"

[sources.runtime_log]
paths = [\"{runtime_log_path}\"]

[sources.market_registry]
canonical_dir = \"{canonical_dir}\"
legacy_dir = \"{legacy_dir}\"

[sources.market_resolution]
path = \"{resolution_path}\"
""",
            )

            settings = load_settings(config_path)
            store = SqliteStore(settings.storage.sqlite_path)
            store.initialize()

            store.insert_raw_events(
                [
                    RawActivityEvent.from_payload(
                        payload={
                            "id": "evt-1",
                            "type": "trade",
                            "timestamp": 110,
                            "side": "buy",
                            "outcome": "YES",
                            "shares": "10",
                            "amount": "4.0",
                            "slug": "market-1",
                        },
                        wallet_address="0xwallet",
                    ),
                    RawActivityEvent.from_payload(
                        payload={
                            "id": "evt-2",
                            "type": "trade",
                            "timestamp": 210,
                            "side": "buy",
                            "outcome": "YES",
                            "shares": "20",
                            "amount": "12.0",
                            "slug": "market-1",
                        },
                        wallet_address="0xwallet",
                    ),
                    RawActivityEvent.from_payload(
                        payload={
                            "id": "evt-3",
                            "type": "redeem",
                            "timestamp": 300,
                            "outcome": "YES",
                            "shares": "15",
                            "amount": "15.0",
                            "slug": "market-1",
                        },
                        wallet_address="0xwallet",
                    ),
                ]
            )

            ingest_attribution_context.run(settings, store)
            attribution_result = build_attribution.run(settings, store)
            pnl_result = compute_pnl.run(settings, store)

            self.assertEqual(attribution_result["attributed_events"], 4)
            attributed = store.get_attributed_events()
            strategies = [(row["strategy_name"], row["activity_type"], row["attributed_fraction"]) for row in attributed]
            self.assertIn(("alpha", "TRADE", "1"), strategies)
            self.assertIn(("beta", "TRADE", "1"), strategies)
            self.assertTrue(any(row["strategy_name"] == "alpha" and row["activity_type"] == "REDEEM" for row in attributed))
            self.assertTrue(any(row["strategy_name"] == "beta" and row["activity_type"] == "REDEEM" for row in attributed))

            market_facts = store.get_fact_rows("pnl_market_facts")
            self.assertEqual(pnl_result["market_facts"], 1)
            self.assertEqual(market_facts[0]["realized_pnl_usdc"], "14")
            self.assertEqual(json.loads(market_facts[0]["unresolved_flags"]), ["unredeemed_winning_shares"])


if __name__ == "__main__":
    unittest.main()
