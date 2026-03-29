#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
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

# NOTE: use skill instance to reuse a single snapshot (avoid repeated price fetch).


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
    # SECURITY: do not embed real publish URLs in repo history. Use env var only.
    parser.add_argument("--publish-base-url", default=os.environ.get("OPENCLAW_PUBLISH_BASE_URL"), help="Base publish URL (set via env OPENCLAW_PUBLISH_BASE_URL).")
    parser.add_argument("--price-timeout", type=int, default=30, help="Price fetch timeout in seconds.")
    parser.add_argument("--dry-run", action="store_true", help="Do not persist NAV writes.")
    parser.add_argument("--no-html", action="store_true", help="Do not render HTML; only record NAV + generate JSON bundle.")
    parser.add_argument("--no-publish", action="store_true", help="Do not write HTML files into reports/publish dirs.")
    parser.add_argument("--quiet", action="store_true", help="No stdout on success (scheduled mode).")
    parser.add_argument("--debug-internal", action="store_true", help="Do not suppress internal stdout prints (debug only).")
    return parser.parse_args()


def resolve_publish_base_url(explicit: Optional[str]) -> Optional[str]:
    # SECURITY: Never derive or hardcode publish URLs in the repo.
    # If you want public URLs, set OPENCLAW_PUBLISH_BASE_URL in runtime secrets.
    if explicit:
        return explicit.rstrip("/")
    return None




def _suppress_internal_stdout(enabled: bool):
    """Context manager to suppress noisy internal stdout prints."""
    if not enabled:
        return contextlib.nullcontext()
    return contextlib.redirect_stdout(open(os.devnull, 'w'))


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


def fmt_opt_nav_delta(v: Any) -> str:
    """Format NAV delta as a plain number (not money)."""
    if v is None:
        return "--"
    return f"{float(v):+.6f}"


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
    """Build a consistent bundle for publishing.

    Performance notes:
    - Avoid fetching holdings/prices more than once.
    - Use one snapshot for both record_nav and report generation.
    """
    # Build a single snapshot first (heavy operation: holdings + price fetch).
    # Import lazily to keep script startup fast.
    from skill_api import _get_default_skill

    import time
    def _ms():
        return int(time.time()*1000)

    skill = _get_default_skill()

    t_snapshot = _ms()
    snapshot = skill.build_snapshot()

    # Debug: show price meta if present
    try:
        valuation = snapshot.get('valuation')
        pm = getattr(valuation, 'price_meta', None)
        if pm is not None:
            print('[price_meta]', pm)
    except Exception:
        pass
    snapshot_ms = _ms() - t_snapshot

    t_navs = _ms()
    navs_all = skill.storage.get_nav_history(skill.account, days=9999)
    navs_ms = _ms() - t_navs


    # Fetch full NAV history once and reuse it across record_nav/report.
    # This avoids duplicate Feishu reads in a single publish run.
    navs_all = skill.storage.get_nav_history(skill.account, days=9999)

    t_record_nav = _ms()
    # NOTE: skill_api.record_nav() 默认 dry_run=True（安全约束）。
    # 作为定时任务，我们在非 dry_run 模式下显式写入：dry_run=False 且 confirm=True。
    if dry_run:
        nav_result = skill.record_nav(price_timeout=price_timeout, dry_run=True, confirm=False, snapshot=snapshot)
    else:
        nav_result = skill.record_nav(price_timeout=price_timeout, dry_run=False, confirm=True, snapshot=snapshot)

    record_nav_ms = _ms() - t_record_nav

    if not nav_result.get("success"):
        raise RuntimeError(json.dumps(nav_result, ensure_ascii=False))

    t_report = _ms()
    # Generate report using the same snapshot (no extra price fetch).
    report = skill.generate_report(report_type="daily", record_nav=False, price_timeout=price_timeout, snapshot=snapshot, navs=navs_all)
    report_ms = _ms() - t_report

    if not report.get("success"):
        raise RuntimeError(json.dumps(report, ensure_ascii=False))

    t_get_nav = _ms()
    # For daily report, we only need recent 2 days of NAV history.
    nav_snapshot = skill.get_nav(days=2)
    get_nav_ms = _ms() - t_get_nav
    if not nav_snapshot.get("success"):
        raise RuntimeError(json.dumps(nav_snapshot, ensure_ascii=False))

    return {
        "nav_result": nav_result,
        "report": report,
        "nav_snapshot": nav_snapshot,
        "stage_timings": {
            "snapshot_ms": snapshot_ms,
            "navs_all_ms": navs_ms,
            "record_nav_ms": record_nav_ms,
            "generate_report_ms": report_ms,
            "get_nav_ms": get_nav_ms,
        },
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
    # Gap definition: compare "today" against the previous NAV record (not necessarily yesterday).
    prev_nav = history[-1].get("nav") if len(history) >= 1 else None
    prev_total_value = history[-1].get("total_value") if len(history) >= 1 else None

    daily_change = (nav - float(prev_nav)) if prev_nav not in (None, 0) else None
    daily_return = ((nav / float(prev_nav)) - 1) if prev_nav not in (None, 0) else None

    # Gap PnL is defined as NAV-history.pnl (gap vs previous record), not "today vs yesterday".
    # We do not store gap nav_change; we only display it.
    # Prefer using the stored pnl if present; otherwise fall back to an estimated gap pnl.
    gap_pnl = latest.get("pnl")
    if gap_pnl is None and (prev_total_value not in (None, 0)):
        # Fallback estimate: Δtotal_value - cash_flow (cash_flow is already gap vs previous record)
        gap_pnl = float(total_value) - float(prev_total_value) - float(cash_flow)

    est_daily_pnl = gap_pnl

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
      <div class='card'><div class='label'>较昨日</div><div class='value {'kpi-up' if (daily_return or 0) >= 0 else 'kpi-down'}'>{fmt_opt_pct(daily_return)}</div><div class='sub'>ΔNAV {fmt_opt_nav_delta(daily_change)} · 当日盈亏(估) {fmt_opt_money(est_daily_pnl)}</div></div>
      <div class='card'><div class='label'>当日资金变动</div><div class='value'>{fmt_money(cash_flow)}</div></div>
      <div class='card'><div class='label'>权益仓位</div><div class='value'>{fmt_pct(equity_ratio)}</div><div class='sub'>现金 {fmt_pct(cash_ratio)}</div></div>
      <div class='card'><div class='label'>成立以来年化</div><div class='value'>{cagr_pct:.2f}%</div></div>
    </div>
  </section>

  <section class='section grid-2'>
    <div class='card'>
      <div class='label'>收益概览</div>
      <div class='grid' style='margin-top:8px'>
        <div><div class='label'>本月收益率</div><div class='value-sm {'kpi-up' if (mtd_nav_change or 0) >= 0 else 'kpi-down'}'>{fmt_opt_pct(mtd_nav_change)}</div></div>
        <div><div class='label'>年内收益率</div><div class='value-sm {'kpi-up' if (ytd_nav_change or 0) >= 0 else 'kpi-down'}'>{fmt_opt_pct(ytd_nav_change)}</div></div>
        <div><div class='label'>本月收益额</div><div class='value-sm {'kpi-up' if (mtd_pnl or 0) >= 0 else 'kpi-down'}'>{fmt_opt_money(mtd_pnl)}</div></div>
        <div><div class='label'>年内收益额</div><div class='value-sm {'kpi-up' if (ytd_pnl or 0) >= 0 else 'kpi-down'}'>{fmt_opt_money(ytd_pnl)}</div></div>
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


def _now_ms() -> int:
    import time
    return int(time.time() * 1000)


def main() -> None:
    args = parse_args()
    config = build_config(args)

    # Speed: scheduled daily report can skip expensive NAV runtime validation.
    # Enable only for this script via env var to avoid impacting other entry points.
    if os.environ.get("PM_DISABLE_NAV_RUNTIME_VALIDATION") in ("1", "true", "TRUE", "yes", "YES"):
        os.environ["PORTFOLIO_NAV_DISABLE_RUNTIME_VALIDATION"] = "1"

    timings: dict[str, int] = {}
    t0 = _now_ms()

    with _suppress_internal_stdout(enabled=(not bool(args.debug_internal))):
        t1 = _now_ms()
        report_bundle = build_report_data(price_timeout=args.price_timeout, dry_run=args.dry_run)
        timings['build_report_data_ms'] = _now_ms() - t1

        # Fast mode: only compute bundle (record_nav + generate_report + get_nav)
        if bool(args.no_html):
            timings['total_ms'] = _now_ms() - t0
            out = {
                "success": True,
                "nav_result": report_bundle.get("nav_result"),
                "report": report_bundle.get("report"),
                "nav_snapshot": report_bundle.get("nav_snapshot"),
                "stage_timings": report_bundle.get("stage_timings"),
                "timings": timings,
            }
            if not bool(args.quiet):
                print(json.dumps(out, ensure_ascii=False, indent=2))
            return

        t2 = _now_ms()
        report_date, html = render_daily_report_html(report_bundle, config)
        timings['render_html_ms'] = _now_ms() - t2

        publish_result = None
        if not bool(args.no_publish):
            t3 = _now_ms()
            publish_result = publish_report(report_date, html, config)
            timings['publish_ms'] = _now_ms() - t3

        timings['total_ms'] = _now_ms() - t0

        result = {
            "success": True,
            "date": report_date,
            "nav_result": report_bundle["nav_result"],
            "publish": publish_result,
            "timings": timings,
        }
        if not bool(args.quiet):
            print(json.dumps(result, ensure_ascii=False, indent=2))
    
    
if __name__ == "__main__":
    main()
