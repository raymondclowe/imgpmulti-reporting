# Operations

## Bootstrap

1. Copy `config/reporting.example.toml` to a real config file.
2. Point the config at authoritative raw sources only.
3. Run `authoritative-pnl --config <path> init-db`.

## Pipeline

Run stages independently:

```bash
authoritative-pnl --config config/reporting.example.toml ingest-activity
authoritative-pnl --config config/reporting.example.toml ingest-attribution
authoritative-pnl --config config/reporting.example.toml build-attribution
authoritative-pnl --config config/reporting.example.toml compute-pnl
authoritative-pnl --config config/reporting.example.toml materialize
authoritative-pnl --config config/reporting.example.toml quality-checks
```

Or run the local end-to-end workflow:

```bash
authoritative-pnl --config config/reporting.example.toml run-all
```

## API

Run:

```bash
authoritative-pnl --config config/reporting.example.toml serve-api
```

Endpoints:

- `/health`
- `/coverage/wallets`
- `/events/raw`
- `/events/attributed`
- `/pnl/strategy`
- `/pnl/market`
- `/pnl/wallet`
- `/pnl/daily`
- `/reconciliation/summary`
