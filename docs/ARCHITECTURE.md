# Architecture

This implementation enforces three boundaries:

1. Raw financial events come only from the configured authoritative activity endpoint.
2. Attribution context comes only from raw runtime logs and registry claim files.
3. Derived outputs are rebuilt from persisted raw payloads stored in SQLite.

The runtime flow is:

1. Ingest raw activity events into `raw_activity_events` without mutation of `payload_json`.
2. Ingest raw attribution context into dedicated claim tables.
3. Build attributed event slices with deterministic precedence: runtime midpoint, canonical registry, legacy lookup, then `UNKNOWN`.
4. Compute market, strategy, wallet, and daily P&L fact tables from attributed events.
5. Materialize CSV/JSON outputs and serve them through a minimal HTTP API.

Non-negotiable behavior:

- No journal files are read anywhere in this repository.
- Unknown attribution is surfaced explicitly rather than guessed.
- Resolved markets with unredeemed winning shares are flagged instead of being forced into a loss assumption.
