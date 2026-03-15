from __future__ import annotations

from pathlib import Path
import csv
import json

from authoritative_pnl.settings import Settings
from authoritative_pnl.store.sqlite_store import SqliteStore


def run(settings: Settings, store: SqliteStore) -> dict[str, int]:
    output_dir = settings.storage.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    tables = {
        "events_raw": store.get_raw_events(),
        "events_attributed": store.get_attributed_events(),
        "pnl_market": store.get_fact_rows("pnl_market_facts"),
        "pnl_strategy": store.get_fact_rows("pnl_strategy_facts"),
        "pnl_wallet": store.get_fact_rows("pnl_wallet_facts"),
        "pnl_daily": store.get_fact_rows("pnl_daily_facts"),
    }

    written = 0
    for name, rows in tables.items():
        _write_json(output_dir / f"{name}.json", rows)
        _write_csv(output_dir / f"{name}.csv", rows)
        written += 2

    _write_parquet_if_available(output_dir, tables)
    return {"artifacts": written}


def _write_json(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(rows, handle, indent=2, sort_keys=True)


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        if not rows:
            handle.write("")
            return
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_parquet_if_available(output_dir: Path, tables: dict[str, list[dict[str, object]]]) -> None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ModuleNotFoundError:
        return

    for name, rows in tables.items():
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, output_dir / f"{name}.parquet")
