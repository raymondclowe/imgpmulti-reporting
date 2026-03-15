from __future__ import annotations

import argparse
import json

from authoritative_pnl.api.app import serve
from authoritative_pnl.logging import configure_logging
from authoritative_pnl.pipeline import build_attribution, compute_pnl, ingest_activity, ingest_attribution_context, materialize_reports
from authoritative_pnl.quality import checks
from authoritative_pnl.quality.reconcile import build_summary
from authoritative_pnl.settings import Settings, load_settings
from authoritative_pnl.store.sqlite_store import SqliteStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Authoritative P&L reporting CLI")
    parser.add_argument("--config", default="config/reporting.example.toml")
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name in [
        "init-db",
        "ingest-activity",
        "ingest-attribution",
        "build-attribution",
        "compute-pnl",
        "materialize",
        "quality-checks",
        "reconcile",
        "run-all",
        "serve-api",
    ]:
        subparsers.add_parser(name)

    return parser


def _store_from_settings(settings: Settings) -> SqliteStore:
    store = SqliteStore(settings.storage.sqlite_path)
    store.initialize()
    return store


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    settings = load_settings(args.config)
    store = _store_from_settings(settings)

    if args.command == "init-db":
        print(json.dumps({"status": "ok", "sqlite_path": str(settings.storage.sqlite_path)}, indent=2))
        return 0
    if args.command == "ingest-activity":
        print(json.dumps(ingest_activity.run(settings, store), indent=2, sort_keys=True))
        return 0
    if args.command == "ingest-attribution":
        print(json.dumps(ingest_attribution_context.run(settings, store), indent=2, sort_keys=True))
        return 0
    if args.command == "build-attribution":
        print(json.dumps(build_attribution.run(settings, store), indent=2, sort_keys=True))
        return 0
    if args.command == "compute-pnl":
        print(json.dumps(compute_pnl.run(settings, store), indent=2, sort_keys=True))
        return 0
    if args.command == "materialize":
        print(json.dumps(materialize_reports.run(settings, store), indent=2, sort_keys=True))
        return 0
    if args.command == "quality-checks":
        print(checks.to_json(checks.run(settings, store)))
        return 0
    if args.command == "reconcile":
        print(json.dumps(build_summary(store), indent=2, sort_keys=True))
        return 0
    if args.command == "run-all":
        result = {
            "ingest_attribution": ingest_attribution_context.run(settings, store),
            "build_attribution": build_attribution.run(settings, store),
            "compute_pnl": compute_pnl.run(settings, store),
            "materialize": materialize_reports.run(settings, store),
            "quality": [item.as_dict() for item in checks.run(settings, store)],
            "reconciliation": build_summary(store),
        }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if args.command == "serve-api":
        serve(settings, store)
        return 0

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
