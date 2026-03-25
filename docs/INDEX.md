# Portfolio Management — Project Map

This repo is designed to be operated by an agent (Skill) and a human.
Keep docs short, executable, and reality-checked.

## Core entrypoints

- Skill API (primary surface): `skill_api.py`
- Storage backend (Feishu only): `src/feishu_storage.py` + `src/feishu_client.py`
- Portfolio logic: `src/portfolio.py`
- Pricing + caching: `src/price_fetcher.py` + `src/market_time.py`
- Timezone helpers (Beijing time semantics): `src/time_utils.py`

## Daily report

- Publisher: `scripts/publish_daily_report.py`
- Notes: `scripts/README_daily_report.md`

## Diagnostics / runbooks

- Pricing diagnosis: `scripts/diagnose_pricing.py`

## Schema truth source

- Feishu Bitable schema reference (fields/names/types): `docs/schema.md`

## Non-negotiable invariants

- Beijing time semantics for all business dates (tx_date, flow_date, nav date, snapshot as_of)
- Pricing cache policy:
  - cache valid → MUST NOT call realtime sources (unless `force_refresh`)
  - cache expired → try realtime; if fails, may fallback to stale cache but MUST mark `is_stale=true`
- Valuation identity (within tolerance): `total ≈ cash + stock + fund`

## Common commands

- Diagnose pricing behavior (cache vs realtime vs fallback):
  - `python scripts/diagnose_pricing.py --account lx`
  - `python scripts/diagnose_pricing.py --account lx --json`

- Publish daily report:
  - `python scripts/publish_daily_report.py`

- (Planned) Schema doctor:
  - `python scripts/schema_doctor.py`
