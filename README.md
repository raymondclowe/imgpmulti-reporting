# imgpmulti-reporting
Reporting only on the imgpmulti node fleet data

## Implementation Status

This repository now contains a working first implementation of the standalone authoritative P&L system described below.

Implemented components:

- Raw activity ingestion into SQLite from a configured authoritative activity endpoint
- Raw runtime-log, canonical-registry, legacy-lookup, and resolution ingestion
- Deterministic attribution with precedence: runtime midpoint, canonical registry, legacy lookup, then `UNKNOWN`
- Pro-rata allocation of non-trade events across existing strategy share exposure within `(wallet_address, slug)`
- P&L fact generation for market, strategy, wallet, and daily views
- Quality checks, reconciliation summary, CSV/JSON materialization, and a minimal HTTP API

Non-negotiable rule enforced in code:

- No journal data is read for financial calculations

Quick start:

```bash
python3 -m venv .venv
PYTHONPATH=src .venv/bin/python -m authoritative_pnl.cli --config config/reporting.example.toml init-db
PYTHONPATH=src .venv/bin/python -m authoritative_pnl.cli --config config/reporting.example.toml run-all
```

# Standalone Authoritative P&L Repository - Design Document

Version: 1.0
Date: 2026-03-15
Status: Proposed
Owner: Data/Reporting

## 1. Purpose

Design a standalone repository that extracts lowest-level source information from authoritative systems in imgpmulti and produces trustworthy P&L reporting.

This repository exists to make one thing non-negotiable:

- Financial reporting must be built from what actually happened on-chain, not from in-process intent logs.

## 2. Problem Statement

imgpmulti currently has multiple data paths (activity store, registries, runtime logs, journals, cached history). Some are authoritative, some are diagnostic, and some are legacy.

Without a strict source hierarchy, reporting can drift:

- Different endpoints can disagree.
- Legacy journal data can leak into financial calculations.
- Strategy attribution can be guessed from current runtime state instead of historical state.

The standalone repo must hardcode a source-of-truth model and make data lineage auditable end-to-end.

## 3. Scope

In scope:

- Extract authoritative, lowest-level event data needed for P&L.
- Attribute events to strategy/machine/config with deterministic precedence.
- Compute wallet-level and strategy-level P&L from canonical activity events.
- Publish reproducible reporting outputs (API, CSV, parquet views).
- Provide data quality checks and reconciliation reports.

Out of scope:

- Order placement, strategy execution, or trading orchestration.
- Re-implementing imgpmulti web UI.
- Using journals for any financial computation.

## 4. Authoritative Data Hierarchy

### 4.1 Financial Truth (what happened)

Priority order for financial events:

1. Polymarket Activity API mirrored into local Activity Store (authoritative)
2. No fallback to journals for financial values

Financial fields covered by this rule:

- fills, shares, cost, proceeds, fees, rebates, merge/redeem/split cashflow
- realized P&L inputs
- position quantity/cost basis inputs

### 4.2 Attribution Truth (who did it)

Priority order for strategy attribution:

1. Strategy runtime log midpoint entry (market midpoint around +450s)
2. Canonical market registry claim
3. Immutable legacy lookup bundle
4. UNKNOWN (explicitly labeled; never guessed)

Important constraint:

- Runtime-log lookup must be machine-scoped. Never scan all machines blindly for a matching slug.

### 4.3 Join Keys

Canonical join for attribution:

- (wallet_address, slug)

Not canonical:

- condition_id alone

Rationale:

- Journals and registry are slug-oriented.
- Activity events include both slug and condition_id.
- slug is the stable bridge across attribution data sets.

## 5. Lowest-Level Source Inputs

The standalone repo ingests only raw or near-raw authoritative sources:

1. Polymarket Activity API payloads (raw JSON per event)
2. Runtime log raw lines (strategy_runtime_log.jsonl)
3. Canonical market registry raw claims (canonical.json and claim files)
4. Wallet/account registry (api-keys and instance mapping files)
5. Market resolution records (database/cache-backed resolution table and source CSV)

The repo stores source payloads as first-class data so every derived value can be re-built.

## 6. Standalone Repository Architecture

Proposed repository name:

- authoritative-pnl

Proposed structure:

```text
authoritative-pnl/
  README.md
  pyproject.toml
  config/
    reporting.example.toml
  src/
    authoritative_pnl/
      __init__.py
      cli.py
      settings.py
      logging.py
      models/
        events.py
        attribution.py
        pnl.py
      connectors/
        polymarket_activity.py
        runtime_log.py
        market_registry.py
        account_registry.py
        market_resolution.py
      store/
        sqlite_store.py
        migrations/
      pipeline/
        ingest_activity.py
        ingest_attribution_context.py
        build_attribution.py
        compute_pnl.py
        materialize_reports.py
      quality/
        checks.py
        reconcile.py
      api/
        app.py
        routes.py
  tests/
    unit/
    integration/
    golden/
  docs/
    ARCHITECTURE.md
    DATA_CONTRACTS.md
    OPERATIONS.md
```

## 7. Data Model

### 7.1 Raw Activity Events

Table: raw_activity_events

Required fields:

- event_uid (deterministic hash)
- wallet_address
- timestamp_unix
- activity_type (TRADE, MERGE, REDEEM, SPLIT, DEPOSIT, WITHDRAWAL)
- side (nullable)
- outcome (nullable)
- size
- usdc_size
- price
- fees_paid
- rebates_earned
- slug (not null)
- condition_id (nullable)
- transaction_hash (nullable)
- asset (nullable)
- payload_json (full original payload)
- ingested_at_unix
- source_cursor (offset/page/attempt metadata)

Constraints:

- unique(event_uid)
- index(wallet_address, slug)
- index(wallet_address, timestamp_unix)
- index(slug)
- index(activity_type)

### 7.2 Attribution Context Tables

Tables:

- runtime_midpoint_claims
- canonical_market_claims
- legacy_lookup_claims

Each row must include source system, source file, and extracted timestamp for lineage.

### 7.3 Attributed Events

Table: attributed_events

Adds:

- strategy_name
- strategy_class
- machine_id
- config_file
- attribution_source (runtime_log_midpoint | canonical_registry | legacy_lookup | unknown)
- attributed_fraction (for pro-rata non-trade allocation)

### 7.4 P&L Fact Tables

Tables:

- pnl_market_facts
- pnl_strategy_facts
- pnl_wallet_facts
- pnl_daily_facts

Each fact row must include:

- computation_version
- input_window
- source_snapshot_id
- unresolved_flags (if any)

## 8. Pipeline Design

### 8.1 Stage A - Ingest Activity (Authoritative Financial Input)

Inputs:

- wallet registry (trade and hedge wallets)
- Polymarket activity endpoint with deposits/withdrawals included

Rules:

- Do not drop raw records.
- Persist raw payload_json exactly.
- Use incremental cursor per wallet with periodic full audit pass.
- Respect API rate limits with backoff and bounded concurrency.

Output:

- raw_activity_events
- wallet_coverage status

### 8.2 Stage B - Ingest Attribution Context

Inputs:

- strategy runtime logs
- canonical registry
- legacy lookup bundle

Rules:

- Build market midpoint index from runtime logs.
- Preserve duplicate/competing claims but resolve using deterministic precedence during attribution.

Output:

- runtime_midpoint_claims
- canonical_market_claims
- legacy_lookup_claims

### 8.3 Stage C - Build Attributed Events

For each raw activity event:

1. Resolve candidate machine context (from explicit mapping or canonical claim)
2. Lookup runtime midpoint claim scoped to machine and slug
3. Fallback to canonical claim
4. Fallback to legacy lookup
5. Else mark UNKNOWN

Allocation rule:

- TRADE: direct attribution
- MERGE/REDEEM/SPLIT: pro-rata by net contributed shares per strategy within (wallet, slug)

### 8.4 Stage D - Compute P&L

P&L computed from attributed events, not from journals.

Core components per market:

- trade outflow
- trade inflow
- merge inflow
- redeem inflow
- split effects
- fees/rebates

Resolution-aware behavior:

- Closed/resolved markets: compute realized P&L from full lifecycle cashflow.
- Open markets: report unrealized/open-cost fields separately.

No silent assumptions:

- If a market outcome is unresolved and no redeem happened, keep it explicitly open/unrealized.

## 9. Reporting Outputs

### 9.1 API

Minimal API endpoints:

- GET /health
- GET /coverage/wallets
- GET /events/raw
- GET /events/attributed
- GET /pnl/strategy
- GET /pnl/market
- GET /pnl/wallet
- GET /pnl/daily
- GET /reconciliation/summary

### 9.2 Files

Materializations:

- parquet snapshots for analytics
- CSV exports for operations
- JSON summary reports for CI checks

### 9.3 Lineage Fields

Every report row should include:

- source_snapshot_id
- attribution_source
- computation_version
- generated_at

## 10. Data Quality and Controls

Mandatory checks:

1. Freshness check per wallet (last_refresh_ts SLA)
2. Gap check (non-monotonic or suspicious timestamp jumps)
3. Duplicate check (event_uid collisions)
4. Missing slug check
5. Unknown-attribution rate threshold
6. Cross-report consistency check (market sums equal strategy sums)
7. Reconciliation check against wallet-level known totals

Quality gates:

- If freshness or unknown-attribution exceeds threshold, mark outputs degraded.
- Never auto-correct by using journals.

## 11. Operational Model

Execution modes:

- daemon mode (continuous incremental ingest + periodic recompute)
- batch mode (point-in-time rebuild)

Scheduling defaults:

- activity ingest every 5 minutes
- attribution rebuild every 5 minutes
- pnl recompute every 5 minutes
- full reconciliation every hour

Persistence:

- SQLite for standalone single-node deployment
- optional Postgres backend for multi-user/service deployment

## 12. Security and Compliance

Principles:

- Read-only access to source registries where possible
- Secrets never written to report tables
- Environment-based secret injection for API credentials
- Audit logs for fetch, transform, and publish steps

## 13. Migration Plan

Phase 1:

- Build standalone ingestion + attribution + strategy P&L CLI
- Validate on one wallet and one date range

Phase 2:

- Expand to all active trade/hedge wallets
- Add API and materialized exports
- Add quality gates and daily reconciliation

Phase 3:

- Integrate downstream dashboards to consume standalone outputs
- Deprecate legacy journal-backed reporting paths for financial views

## 14. Acceptance Criteria

The design is accepted when all are true:

1. All financial numbers in standalone outputs are derivable from activity events only.
2. Attribution source is explicit for every attributed event.
3. UNKNOWN attribution is measurable and alerting.
4. Strategy, market, and wallet aggregates reconcile within tolerance.
5. Re-running the same snapshot yields identical outputs.

## 15. Risks and Mitigations

Risk: API rate limits or temporary outages

- Mitigation: incremental fetch, retry/backoff, bounded concurrency, cached last-good outputs.

Risk: attribution ambiguity for old/historical data

- Mitigation: strict precedence, UNKNOWN labeling, no guessed strategy assignment.

Risk: drift between standalone and existing dashboards during migration

- Mitigation: parallel run with daily diff report and explicit exception list.

Risk: unresolved markets misread as losses

- Mitigation: enforce realized vs unrealized split and surface unredeemed exposure explicitly.

## 16. Non-Negotiable Rules

1. Never use trade journals for financial calculations.
2. Never overwrite authoritative raw payloads with transformed values.
3. Never hide attribution uncertainty; label unknowns explicitly.
4. Never mix open and closed market P&L without flags.
5. Never ship a report row without lineage metadata.

## 17. Initial Implementation Backlog

1. Bootstrap repository and CI skeleton.
2. Implement activity connector and raw event store.
3. Implement runtime log midpoint indexer.
4. Implement canonical registry connector.
5. Implement attribution engine with precedence chain.
6. Implement pro-rata allocation for MERGE/REDEEM/SPLIT.
7. Implement P&L fact builders (market, strategy, wallet, daily).
8. Implement quality checks and reconciliation summaries.
9. Implement REST API and CSV/parquet materializers.
10. Add golden tests based on fixed historical snapshots.

## 18. Future Extensions

- Multi-chain support if market universe expands.
- Event streaming sink for near-real-time P&L.
- Automatic anomaly detection on strategy performance drift.
