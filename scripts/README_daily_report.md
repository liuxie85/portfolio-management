# Daily report publisher

`publish_daily_report.py` records the current NAV, renders the daily HTML report, and publishes it into a static directory.
It is the only entry point that may collect daily-report data. `generate_daily_report_html.py` is renderer-only and must receive a prepared JSON bundle.

## What it does

1. Call `skill_api.record_nav(...)`
2. Call `skill_api.generate_report(report_type="daily", ...)`
3. Call `skill_api.get_nav()` to enrich the page with return / snapshot fields
4. Render HTML
5. Write the HTML into:
   - `reports/investment-daily-YYYY-MM-DD.html`
   - `reports/latest.html`
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
  --publish-root ../prototypes

# 如果需要生成可访问的 URL，请在运行环境里设置：
#   export OPENCLAW_PUBLISH_BASE_URL="https://<your-private-domain-or-gateway>"
# 注意：不要把真实 URL 写进仓库（避免泄漏）。
```

## Notes

- `--account-label` is display-only.
- `OPENCLAW_PUBLISH_BASE_URL` 用于生成 `public_url` 字段（可选）。
- 出于安全考虑：脚本不会再从 `OPENCLAW_INSTANCE_ID` 推导默认 URL；没有配置则不输出可访问 URL（仅输出 slug）。
- This script is intentionally split into three layers:
  - data collection: `build_report_data(...)`
  - HTML rendering: `render_daily_report_html(...)`
  - file publishing: `publish_report(...)`
- Legacy HTML helpers must not instantiate `PortfolioSkill` or call `build_snapshot()` / `generate_report()` directly.
