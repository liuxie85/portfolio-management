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

- Environment doctor: `scripts/doctor.py`
- Schema doctor (Feishu fields vs docs/schema.md): `scripts/schema_doctor.py`
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

- Environment doctor (deps + network + Feishu sanity):
  - `python scripts/doctor.py`

- Schema doctor (compare live Feishu fields with docs/schema.md):
  - `python scripts/schema_doctor.py`
  - `python scripts/schema_doctor.py --json`

- Diagnose pricing behavior (cache vs realtime vs fallback):
  - `python scripts/diagnose_pricing.py --account lx`
  - `python scripts/diagnose_pricing.py --account lx --json`

- Publish daily report:
  - `python scripts/publish_daily_report.py`

## Natural language adapter (stub)

- `scripts/nl.py "..."` converts natural language into a structured intent JSON.
- It has **no side effects**; execution must be done by calling `scripts/pm.py` or `skill_api` explicitly.
