#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = REPO_ROOT.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from skill_api import record_nav, generate_report, get_nav


@dataclass
class PublishConfig:
    repo_root: Path
    workspace: Path
    reports_dir: Path
    publish_root: Path
    account_label: str
    publish_base_url: Optional[str] = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Record NAV, render daily report HTML, and publish it to a static directory.")
    parser.add_argument("--account-label", default=os.environ.get("PM_REPORT_ACCOUNT_LABEL", "lx"), help="Display-only account label shown in the HTML report.")
    parser.add_argument("--reports-dir", default=str(REPO_ROOT / "reports"), help="Directory for generated HTML report files.")
    parser.add_argument("--publish-root", default=str(WORKSPACE / "prototypes"), help="Root directory for published static pages.")
    parser.add_argument("--publish-base-url", default=os.environ.get("OPENCLAW_PUBLISH_BASE_URL"), help="Base public URL. Example: https://openclaw-pub-xxxx.imlgz.com")
    parser.add_argument("--price-timeout", type=int, default=30, help="Price fetch timeout in seconds.")
    parser.add_argument("--dry-run", action="store_true", help="Do not persist NAV writes.")
    return parser.parse_args()


def resolve_publish_base_url(explicit: Optional[str]) -> Optional[str]:
    if explicit:
        return explicit.rstrip("/")
    instance_id = os.environ.get("OPENCLAW_INSTANCE_ID", "").strip()
    if not instance_id:
        return None
    return f"https://openclaw-pub-{instance_id}.imlgz.com"


def build_config(args: argparse.Namespace) -> PublishConfig:
    return PublishConfig(
        repo_root=REPO_ROOT,
        workspace=WORKSPACE,
        reports_dir=Path(args.reports_dir),
        publish_root=Path(args.publish_root),
        account_label=args.account_label,
        publish_base_url=resolve_publish_base_url(args.publish_base_url),
    )


def fmt_money(v: float) -> str:
    return f"¥{v:,.2f}"


def fmt_pct(v: float) -> str:
    return f"{v * 100:.2f}%"


def fmt_opt_pct(v: Any) -> str:
    if v is None:
        return "--"
    return f"{float(v) * 100:.2f}%"


def fmt_opt_money(v: Any) -> str:
    if v is None:
        return "--"
    return fmt_money(float(v))


def type_label(v: str) -> str:
    return {
        "a_stock": "A股",
        "hk_stock": "港股",
        "us_stock": "美股",
        "fund": "基金",
        "cash": "现金",
        "mmf": "货基",
        "bond": "债券",
    }.get(v, v or "--")


def build_report_data(price_timeout: int, dry_run: bool = False) -> dict[str, Any]:
    if dry_run:
        raise RuntimeError("当前 skill_api.record_nav 便捷入口不支持 dry_run 参数，请改用 PortfolioSkill.record_nav(...) 或移除 --dry-run")
    nav_result = record_nav(price_timeout=price_timeout)
    if not nav_result.get("success"):
        raise RuntimeError(json.dumps(nav_result, ensure_ascii=False))

    report = generate_report(report_type="daily", record_nav=False, price_timeout=price_timeout)
    if not report.get("success"):
        raise RuntimeError(json.dumps(report, ensure_ascii=False))

    nav_snapshot = get_nav()
    if not nav_snapshot.get("success"):
        raise RuntimeError(json.dumps(nav_snapshot, ensure_ascii=False))

    return {
        "nav_result": nav_result,
        "report": report,
        "nav_snapshot": nav_snapshot,
    }


def render_daily_report_html(report_bundle: dict[str, Any], config: PublishConfig) -> tuple[str, str]:
    report = report_bundle["report"]
    nav_result = report_bundle["nav_result"]
    nav_snapshot = report_bundle["nav_snapshot"]

    overview = report.get("overview") or {}
    top = report.get("top_holdings") or []
    latest = nav_snapshot.get("latest") or {}
    history = nav_snapshot.get("history") or []

    total_value = float(report.get("total_value") or 0)
    nav = float(report.get("nav") or 0)
    cash_flow = float(report.get("cash_flow") or 0)
    cash_ratio = float(overview.get("cash_ratio") or 0)
    stock_ratio = float(overview.get("stock_ratio") or 0)
    fund_ratio = float(overview.get("fund_ratio") or 0)
    cagr_pct = float(report.get("cagr_pct") or 0)
    mtd_nav_change = latest.get("mtd_nav_change")
    ytd_nav_change = latest.get("ytd_nav_change")
    mtd_pnl = latest.get("mtd_pnl")
    ytd_pnl = latest.get("ytd_pnl")
    shares = latest.get("shares") or nav_result.get("shares")
    stock_value = latest.get("stock_value")
    cash_value = latest.get("cash_value")
    fund_value = total_value - (float(stock_value) if stock_value is not None else 0.0) - (float(cash_value) if cash_value is not None else 0.0)
    equity_value = (float(stock_value) if stock_value is not None else 0.0) + fund_value
    equity_ratio = stock_ratio + fund_ratio
    dt = report.get("date") or date.today().isoformat()
    prev_nav = history[-2].get("nav") if len(history) >= 2 else None
    daily_change = (nav - float(prev_nav)) if prev_nav not in (None, 0) else None
    daily_return = ((nav / float(prev_nav)) - 1) if prev_nav not in (None, 0) else None

    rows = []
    for h in top[:10]:
        market = h.get("market", "--")
        rows.append(
            f"<tr><td>{h['code']}</td><td>{h['name']}</td><td>{type_label(h.get('type'))}</td><td>{market}</td><td>{h['quantity']}</td><td>{fmt_money(h['market_value'])}</td><td>{h['weight'] * 100:.2f}%</td></tr>"
        )
    rows_html = "\n".join(rows)
    warnings = report.get("warnings") or nav_result.get("warnings") or []
    warnings_html = "" if not warnings else "<section class='section card'><h2>提示</h2><ul>" + "".join(f"<li>{w}</li>" for w in warnings) + "</ul></section>"

    html = f"""<!doctype html>
<html lang='zh-CN'>
<head>
<meta charset='utf-8' />
<meta name='viewport' content='width=device-width, initial-scale=1' />
<title>投资日报 - {dt}</title>
<style>
:root{{--bg:#0b1020;--panel:#121933;--text:#eef2ff;--muted:#9aa4c7;--line:#2a3563;--accent:#60a5fa;--up:#22c55e;--down:#ef4444;}}
*{{box-sizing:border-box}}
body{{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"PingFang SC","Microsoft YaHei",sans-serif;background:linear-gradient(180deg,#0b1020,#0f1630);color:var(--text);padding:32px}}
.wrap{{max-width:1100px;margin:0 auto}}
.hero,.card{{background:rgba(18,25,51,.92);border:1px solid var(--line);border-radius:18px;padding:22px}}
.hero{{padding:28px}}
.title{{font-size:34px;font-weight:800;margin:0 0 6px}}
.sub{{color:var(--muted);font-size:13px}}
.big{{font-size:42px;font-weight:800;margin:16px 0 4px}}
.grid{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:16px;margin-top:22px}}
.grid-2{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px;margin-top:18px}}
.label{{color:var(--muted);font-size:13px;margin-bottom:6px}}
.value{{font-size:24px;font-weight:700}}
.value-sm{{font-size:20px;font-weight:700}}
.section{{margin-top:18px}}
.kpi-up{{color:var(--up)}}
.kpi-down{{color:var(--down)}}
.table-scroll{{overflow-x:auto;-webkit-overflow-scrolling:touch;border-radius:16px;border:1px solid var(--line)}}
.table-scroll table{{min-width:760px;border-collapse:collapse;width:100%}}
th,td{{padding:12px 14px;border-bottom:1px solid var(--line);text-align:left;font-size:14px;white-space:nowrap}}
th{{color:var(--muted)}}
tr:last-child td{{border-bottom:none}}
ul{{margin:0;padding-left:20px;color:var(--muted)}}
@media (max-width:900px){{.grid{{grid-template-columns:repeat(2,minmax(0,1fr))}} .grid-2{{grid-template-columns:1fr}} body{{padding:18px}} .big{{font-size:34px}} .title{{font-size:28px}}}}
</style>
</head>
<body>
<div class='wrap'>
  <section class='hero'>
    <div class='sub'>Portfolio Management · 投资日报</div>
    <h1 class='title'>投资日报｜{dt}</h1>
    <div class='sub'>快照时间 {report.get('snapshot_time')} · 账户 {config.account_label}</div>
    <div class='big'>{fmt_money(total_value)}</div>
    <div class='sub'>今日净值 NAV {nav:.6f} · 份额 {shares if shares is not None else '--'}</div>
    <div class='grid'>
      <div class='card'><div class='label'>昨日对比</div><div class='value {'kpi-up' if (daily_return or 0) >= 0 else 'kpi-down'}>{fmt_opt_pct(daily_return)}</div><div class='sub'>净值变动 {fmt_opt_money(daily_change)}</div></div>
      <div class='card'><div class='label'>当日资金变动</div><div class='value'>{fmt_money(cash_flow)}</div></div>
      <div class='card'><div class='label'>权益仓位</div><div class='value'>{fmt_pct(equity_ratio)}</div><div class='sub'>现金 {fmt_pct(cash_ratio)}</div></div>
      <div class='card'><div class='label'>成立以来年化</div><div class='value'>{cagr_pct:.2f}%</div></div>
    </div>
  </section>

  <section class='section grid-2'>
    <div class='card'>
      <div class='label'>收益概览</div>
      <div class='grid' style='margin-top:8px'>
        <div><div class='label'>本月收益率</div><div class='value-sm {'kpi-up' if (mtd_nav_change or 0) >= 0 else 'kpi-down'}>{fmt_opt_pct(mtd_nav_change)}</div></div>
        <div><div class='label'>年内收益率</div><div class='value-sm {'kpi-up' if (ytd_nav_change or 0) >= 0 else 'kpi-down'}>{fmt_opt_pct(ytd_nav_change)}</div></div>
        <div><div class='label'>本月收益额</div><div class='value-sm {'kpi-up' if (mtd_pnl or 0) >= 0 else 'kpi-down'}>{fmt_opt_money(mtd_pnl)}</div></div>
        <div><div class='label'>年内收益额</div><div class='value-sm {'kpi-up' if (ytd_pnl or 0) >= 0 else 'kpi-down'}>{fmt_opt_money(ytd_pnl)}</div></div>
      </div>
    </div>
    <div class='card'>
      <div class='label'>资产概览</div>
      <div class='grid' style='margin-top:8px'>
        <div><div class='label'>权益资产（股票+基金）</div><div class='value-sm'>{fmt_opt_money(equity_value)}</div></div>
        <div><div class='label'>现金资产</div><div class='value-sm'>{fmt_opt_money(cash_value)}</div></div>
        <div><div class='label'>权益仓位</div><div class='value-sm'>{fmt_pct(equity_ratio)}</div></div>
        <div><div class='label'>现金仓位</div><div class='value-sm'>{fmt_pct(cash_ratio)}</div></div>
      </div>
      <div class='sub' style='margin-top:12px'>{nav_result.get('message', '')}</div>
    </div>
  </section>

  {warnings_html}

  <section class='section card'>
    <h2>前十大持仓</h2>
    <div class='table-scroll'>
      <table>
        <tr><th>代码</th><th>名称</th><th>类型</th><th>账户</th><th>数量</th><th>市值</th><th>权重</th></tr>
        {rows_html}
      </table>
    </div>
  </section>
</div>
</body>
</html>"""
    return dt, html


def publish_report(report_date: str, html: str, config: PublishConfig) -> dict[str, Any]:
    slug = f"investment-daily-{report_date}"
    report_path = config.reports_dir / f"{slug}.html"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(html, encoding="utf-8")

    out_dir = config.publish_root / slug
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "index.html").write_text(html, encoding="utf-8")

    public_url = f"{config.publish_base_url}/{slug}/" if config.publish_base_url else slug
    return {
        "date": report_date,
        "slug": slug,
        "report_file": str(report_path),
        "publish_dir": str(out_dir),
        "public_url": public_url,
    }


def main() -> None:
    args = parse_args()
    config = build_config(args)
    report_bundle = build_report_data(price_timeout=args.price_timeout, dry_run=args.dry_run)
    report_date, html = render_daily_report_html(report_bundle, config)
    publish_result = publish_report(report_date, html, config)
    result = {
        "success": True,
        **publish_result,
        "nav_result": report_bundle["nav_result"],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
