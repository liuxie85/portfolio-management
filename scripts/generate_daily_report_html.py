#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from skill_api import PortfolioSkill

PUBLIC_DIR = REPO_ROOT / "public"
OUTPUT_PATH = PUBLIC_DIR / "index.html"


def fmt_money(v: Any) -> str:
    if v is None:
        return "--"
    return f"¥{float(v):,.2f}"


def fmt_pct_ratio(v: Any) -> str:
    if v is None:
        return "--"
    return f"{float(v) * 100:.2f}%"


def fmt_pct_change(v: Any) -> str:
    if v is None:
        return "--"
    return f"{float(v):+.2%}"


def kpi_class(v: Any) -> str:
    if v is None:
        return ""
    return "up" if float(v) >= 0 else "down"


def html_escape(s: Any) -> str:
    text = "" if s is None else str(s)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def build_market_breakdown(holdings: List[Dict[str, Any]], total_value: float) -> List[Dict[str, Any]]:
    by_market: Dict[str, float] = {}
    for h in holdings:
        market = h.get("market") or "未分类"
        mv = float(h.get("market_value") or 0)
        by_market[market] = by_market.get(market, 0.0) + mv

    rows = []
    for market, value in sorted(by_market.items(), key=lambda x: x[1], reverse=True):
        ratio = (value / total_value) if total_value > 0 else 0.0
        rows.append({"market": market, "value": value, "ratio": ratio})
    return rows


def render_html(bundle: Dict[str, Any]) -> str:
    report = bundle["report"]
    full = bundle["full"]
    snapshot = bundle["snapshot"]

    overview = report.get("overview") or {}
    holdings = (snapshot.get("holdings_data") or {}).get("holdings") or []
    top_holdings = report.get("top_holdings") or holdings[:10]

    total_value = float(report.get("total_value") or 0)
    nav = report.get("nav")
    cash_ratio = overview.get("cash_ratio")
    cash_flow = report.get("cash_flow")
    pnl = report.get("pnl")
    mtd_nav_change = report.get("mtd_nav_change")
    ytd_nav_change = report.get("ytd_nav_change")
    mtd_pnl = report.get("mtd_pnl")
    ytd_pnl = report.get("ytd_pnl")
    snapshot_time = report.get("snapshot_time") or snapshot.get("snapshot_time")
    date_text = report.get("date") or datetime.now().strftime("%Y-%m-%d")

    market_breakdown = build_market_breakdown(holdings, total_value)

    top_rows = "\n".join(
        f"<tr>"
        f"<td>{html_escape(h.get('code'))}</td>"
        f"<td>{html_escape(h.get('name'))}</td>"
        f"<td>{html_escape(h.get('market') or '--')}</td>"
        f"<td>{html_escape(h.get('quantity'))}</td>"
        f"<td>{fmt_money(h.get('market_value'))}</td>"
        f"<td>{fmt_pct_ratio(h.get('weight'))}</td>"
        f"</tr>"
        for h in top_holdings[:10]
    )

    market_rows = "\n".join(
        f"<tr>"
        f"<td>{html_escape(r['market'])}</td>"
        f"<td>{fmt_money(r['value'])}</td>"
        f"<td>{fmt_pct_ratio(r['ratio'])}</td>"
        f"</tr>"
        for r in market_breakdown
    )

    warnings = full.get("warnings") or []
    warnings_html = ""
    if warnings:
        items = "".join(f"<li>{html_escape(w)}</li>" for w in warnings)
        warnings_html = f"<div class='card'><h2>Warnings</h2><ul>{items}</ul></div>"

    return f"""<!doctype html>
<html lang='zh-CN'>
<head>
  <meta charset='utf-8' />
  <meta name='viewport' content='width=device-width, initial-scale=1' />
  <title>Daily Portfolio Report - {html_escape(date_text)}</title>
  <style>
    :root {{
      --bg: #f6f8fa;
      --card: #ffffff;
      --text: #24292f;
      --muted: #57606a;
      --border: #d0d7de;
      --up: #1a7f37;
      --down: #cf222e;
      --accent: #0969da;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: -apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,"PingFang SC","Microsoft YaHei",sans-serif; background: var(--bg); color: var(--text); }}
    .wrap {{ max-width: 1120px; margin: 28px auto 56px; padding: 0 16px; }}
    .header {{ margin-bottom: 16px; }}
    .title {{ font-size: 30px; font-weight: 700; margin: 0; }}
    .sub {{ color: var(--muted); font-size: 14px; margin-top: 6px; }}
    .grid {{ display: grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap: 12px; }}
    .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 14px; margin-top: 12px; }}
    .kpi-title {{ color: var(--muted); font-size: 12px; margin-bottom: 8px; }}
    .kpi-value {{ font-size: 24px; font-weight: 700; line-height: 1.2; }}
    .up {{ color: var(--up); }}
    .down {{ color: var(--down); }}
    h2 {{ font-size: 18px; margin: 0 0 12px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ border-top: 1px solid var(--border); padding: 10px 8px; text-align: left; }}
    th {{ color: var(--muted); font-weight: 600; background: #f6f8fa; }}
    .table-wrap {{ overflow-x: auto; }}
    .footer {{ margin-top: 18px; color: var(--muted); font-size: 12px; }}
    @media (max-width: 980px) {{ .grid {{ grid-template-columns: repeat(2, minmax(0,1fr)); }} }}
    @media (max-width: 560px) {{ .grid {{ grid-template-columns: 1fr; }} .title {{ font-size: 24px; }} }}
  </style>
</head>
<body>
  <div class='wrap'>
    <div class='header'>
      <h1 class='title'>Portfolio Daily Report · {html_escape(date_text)}</h1>
      <div class='sub'>snapshot_time: {html_escape(snapshot_time)}</div>
    </div>

    <div class='grid'>
      <div class='card'><div class='kpi-title'>Total Value</div><div class='kpi-value'>{fmt_money(total_value)}</div></div>
      <div class='card'><div class='kpi-title'>NAV</div><div class='kpi-value'>{'--' if nav is None else f'{float(nav):.6f}'}</div></div>
      <div class='card'><div class='kpi-title'>Cash ratio</div><div class='kpi-value'>{fmt_pct_ratio(cash_ratio)}</div></div>
      <div class='card'><div class='kpi-title'>Cash flow</div><div class='kpi-value {kpi_class(cash_flow)}'>{fmt_money(cash_flow)}</div></div>

      <div class='card'><div class='kpi-title'>Daily PnL</div><div class='kpi-value {kpi_class(pnl)}'>{fmt_money(pnl)}</div></div>
      <div class='card'><div class='kpi-title'>MTD return</div><div class='kpi-value {kpi_class(mtd_nav_change)}'>{fmt_pct_change(mtd_nav_change)}</div></div>
      <div class='card'><div class='kpi-title'>YTD return</div><div class='kpi-value {kpi_class(ytd_nav_change)}'>{fmt_pct_change(ytd_nav_change)}</div></div>
      <div class='card'><div class='kpi-title'>MTD PnL / YTD PnL</div><div class='kpi-value {kpi_class((ytd_pnl if ytd_pnl is not None else mtd_pnl))}'>{fmt_money(mtd_pnl)} / {fmt_money(ytd_pnl)}</div></div>
    </div>

    <div class='card'>
      <h2>Top Holdings</h2>
      <div class='table-wrap'>
        <table>
          <thead>
            <tr><th>Code</th><th>Name</th><th>Market</th><th>Quantity</th><th>Market Value</th><th>Weight</th></tr>
          </thead>
          <tbody>
            {top_rows}
          </tbody>
        </table>
      </div>
    </div>

    <div class='card'>
      <h2>Market Breakdown</h2>
      <div class='table-wrap'>
        <table>
          <thead>
            <tr><th>Market</th><th>Value</th><th>Ratio</th></tr>
          </thead>
          <tbody>
            {market_rows}
          </tbody>
        </table>
      </div>
    </div>

    {warnings_html}

    <div class='footer'>Generated by scripts/generate_daily_report_html.py</div>
  </div>
</body>
</html>
"""


def main() -> None:
    skill = PortfolioSkill()
    snapshot = skill.build_snapshot()
    navs = skill.storage.get_nav_history(skill.account, days=9999)
    full = skill.full_report(snapshot=snapshot, navs=navs)
    if not full.get("success"):
        raise RuntimeError(full.get("error") or "full_report failed")

    report = skill.generate_report(
        report_type="daily",
        record_nav=False,
        snapshot=snapshot,
        navs=navs,
    )
    if not report.get("success"):
        raise RuntimeError(report.get("error") or "generate_report(daily) failed")

    html = render_html({"snapshot": snapshot, "full": full, "report": report})

    PUBLIC_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(str(OUTPUT_PATH))


if __name__ == "__main__":
    main()
