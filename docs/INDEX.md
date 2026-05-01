# Portfolio Management — Project Map

This repo is designed to be operated as an HTTP service, with Skill/MCP/CLI
adapters kept for automation and compatibility.
Keep docs short, executable, and reality-checked.

## Core entrypoints

- HTTP service (primary surface): `src/service/http.py`
- Service runner: `scripts/serve.py`
- Service API notes: `docs/service.md`
- Skill API (compatibility adapter): `skill_api.py`
- Storage backend (Feishu only): `src/feishu_storage.py` + `src/feishu_client.py`
- Portfolio logic: `src/portfolio.py`
- Pricing + caching: `src/price_fetcher.py` + `src/market_time.py`
- Timezone helpers (Beijing time semantics): `src/time_utils.py`

## Daily report

- Publisher: `scripts/publish_daily_report.py`
- Notes: `scripts/README_daily_report.md`

## Diagnostics / runbooks

- Environment doctor: `scripts/doctor.py`
- Schema doctor (Feishu fields vs docs/schema.md): `scripts/migrate_schema.py check-live`
- Pricing diagnosis: `scripts/diagnose_pricing.py`

## Schema truth source

- Feishu Bitable schema reference (fields/names/types): `docs/schema.md`
- Schema migration notes: `docs/migrations.md`

## Non-negotiable invariants

- Beijing time semantics for all business dates (tx_date, flow_date, nav date, snapshot as_of)
- Pricing cache policy:
  - cache valid → MUST NOT call realtime sources (unless `force_refresh`)
  - cache expired → try realtime; if fails, may fallback to stale cache but MUST mark `is_stale=true`
- Valuation identity (within tolerance): `total ≈ cash + stock + fund`
- NAV write surfaces:
  - full write → `FeishuStorage.write_nav_record()` / `write_nav_records()`
  - derived-field patch → `FeishuStorage.patch_nav_derived_fields()`
  - legacy `save_nav()` / `upsert_nav_bulk()` / `update_nav_fields()` are removed

## Common commands

- Environment doctor (deps + network + Feishu sanity):
  - `python scripts/doctor.py`

- Run HTTP service:
  - `python scripts/service.py start`
  - `python scripts/service.py status`
  - Non-loopback binds require `--allow-remote`; the service is otherwise local-only and unauthenticated.

- Schema checks:
  - `python scripts/migrate_schema.py check-live`
  - `python scripts/migrate_schema.py expectations`

- Diagnose pricing behavior (cache vs realtime vs fallback):
  - `python scripts/diagnose_pricing.py --account lx`
  - `python scripts/diagnose_pricing.py --account lx --json`

- Publish daily report:
  - `python scripts/publish_daily_report.py`

## Natural language adapter (stub)

- `scripts/nl.py "..."` converts natural language into a structured intent JSON.
- It has **no side effects**; execution must be done by calling `scripts/pm.py` or `skill_api` explicitly.
