# Daily report publisher

`publish_daily_report.py` records the current NAV, renders the daily HTML report, and publishes it into a static directory.

## What it does

1. Call `skill_api.record_nav(...)`
2. Call `skill_api.generate_report(report_type="daily", ...)`
3. Call `skill_api.get_nav()` to enrich the page with return / snapshot fields
4. Render HTML
5. Write the HTML into:
   - `reports/investment-daily-YYYY-MM-DD.html`
   - `<publish-root>/investment-daily-YYYY-MM-DD/index.html`

## Usage

```bash
cd /home/node/.openclaw/workspace/portfolio-management
. .venv/bin/activate
python scripts/publish_daily_report.py
```

## Useful options

```bash
python scripts/publish_daily_report.py \
  --account-label lx \
  --reports-dir ./reports \
  --publish-root ../prototypes \
  --publish-base-url https://openclaw-pub-<instance>.imlgz.com
```

## Notes

- `--account-label` is display-only.
- `--publish-base-url` can also be provided via `OPENCLAW_PUBLISH_BASE_URL`.
- If `OPENCLAW_PUBLISH_BASE_URL` is not set, the script will derive a default OpenClaw publish URL from `OPENCLAW_INSTANCE_ID` when available.
- This script is intentionally split into three layers:
  - data collection: `build_report_data(...)`
  - HTML rendering: `render_daily_report_html(...)`
  - file publishing: `publish_report(...)`
