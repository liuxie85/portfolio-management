# Feishu Bitable Schema (truth source)

This doc defines the canonical field names expected by the code.
Field **names must match exactly** (case-sensitive, underscore-sensitive).

## Tables

### holdings

Key: `(asset_id, account, market)`

Required fields:
- `asset_id` (text)
- `asset_name` (text)
- `asset_type` (text/select)
- `account` (text)
- `market` (text)
- `quantity` (number)
- `currency` (text)

Optional fields:
- `avg_cost` (number)
- `industry` (text)
- `asset_class` (text)

### transactions

Required fields:
- `tx_date` (date)
- `tx_type` (text/select)
- `asset_id` (text)
- `asset_name` (text)
- `asset_type` (text/select)
- `market` (text)
- `account` (text)
- `quantity` (number)
- `price` (number)
- `amount` (number)
- `currency` (text)
- `fee` (number)
- `remark` (text)
- `request_id` (text) — idempotency key (auto-generated if omitted)
- `dedup_key` (text) — content fingerprint

### cash_flow

Required fields:
- `flow_date` (date)
- `account` (text)
- `direction` (text/select)
- `amount` (number)
- `currency` (text)
- `market` (text)
- `dedup_key` (text)

### nav_history

Required fields:
- `date` (date)
- `account` (text)
- `nav` (number)
- `shares` (number)

### price_cache

Required fields:
- `asset_id` (text)
- `asset_name` (text)
- `price` (number)
- `currency` (text)
- `cny_price` (number)
- `expires_at` (text/datetime)
- `data_source` (text)

Optional:
- `change` (number)
- `change_pct` (number)
- `exchange_rate` (number)

### holdings_snapshot

Used for audit/replay.

Required fields:
- `as_of` (date)
- `account` (text)
- `asset_id` (text)
- `asset_name` (text)
- `asset_type` (text/select)
- `market` (text)
- `quantity` (number)
- `currency` (text)
- `price` (number)
- `cny_price` (number)
- `market_value_cny` (number)
- `dedup_key` (text)
