# Data Contracts

## Activity Payloads

Accepted raw activity fields are normalized from near-raw provider payloads using these aliases:

- `activity_type` or `type`
- `timestamp_unix`, `timestamp`, `ts`, or `created_at_epoch`
- `size`, `shares`, or `quantity`
- `usdc_size`, `amount`, or `notional`
- `slug` or `market_slug`

The full raw payload is preserved exactly in `payload_json`.

## Attribution Claims

Each claim row must contain:

- `slug`
- `strategy_name`
- `source_file`
- `extracted_at_unix`

Runtime claims should also include machine scoping through `machine_id` where available.

## Resolution Records

Resolution data must provide:

- `slug`
- `is_resolved`
- `winning_outcome` when known
