#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime
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


def env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value in ("1", "true", "TRUE", "yes", "YES")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Record NAV, render daily report HTML, and publish it to a static directory.")
    parser.add_argument("--account", default=None, help="Account to operate on. Defaults to config/PORTFOLIO_ACCOUNT.")
    parser.add_argument("--account-label", default=os.environ.get("PM_REPORT_ACCOUNT_LABEL", "lx"), help="Display-only account label shown in the HTML report.")
    parser.add_argument("--reports-dir", default=str(REPO_ROOT / "reports"), help="Directory for generated HTML report files.")
    parser.add_argument("--publish-root", default=str(WORKSPACE / "prototypes"), help="Root directory for published static pages.")
    # SECURITY: do not embed real publish URLs in repo history. Use env var only.
    parser.add_argument("--publish-base-url", default=os.environ.get("OPENCLAW_PUBLISH_BASE_URL"), help="Base publish URL (set via env OPENCLAW_PUBLISH_BASE_URL).")
    parser.add_argument("--price-timeout", type=int, default=30, help="Price fetch timeout in seconds.")
    parser.add_argument("--dry-run", action="store_true", help="Do not persist NAV writes.")
    parser.add_argument("--use-bulk-nav-upsert", action="store_true", help="Persist NAV through storage.upsert_nav_bulk (single-row use is optional).")
    parser.add_argument("--no-html", action="store_true", help="Do not render HTML; only record NAV + generate JSON bundle.")
    parser.add_argument("--no-publish", action="store_true", help="Do not write HTML files into reports/publish dirs.")
    parser.add_argument("--quiet", action="store_true", help="No stdout on success (scheduled mode).")
    parser.add_argument("--debug-internal", action="store_true", help="Do not suppress internal stdout prints (debug only).")
    parser.add_argument(
        "--sync-futu-cash-mmf",
        action="store_true",
        default=os.environ.get("PM_SYNC_FUTU_CASH_MMF") in ("1", "true", "TRUE", "yes", "YES"),
        help="Sync Futu cash/MMF balances into holdings before building the report snapshot.",
    )
    parser.add_argument(
        "--sync-futu-dry-run",
        dest="sync_futu_dry_run",
        action="store_true",
        help="Preview Futu cash/MMF sync without writing holdings (default).",
    )
    parser.add_argument(
        "--sync-futu-write",
        dest="sync_futu_dry_run",
        action="store_false",
        help="Actually write Futu cash/MMF holdings when --sync-futu-cash-mmf is set.",
    )
    parser.set_defaults(sync_futu_dry_run=env_flag("PM_SYNC_FUTU_DRY_RUN", True))
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


def build_report_data(
    price_timeout: int,
    dry_run: bool = False,
    use_bulk_nav_upsert: bool = False,
    sync_futu_cash_mmf: bool = False,
    sync_futu_dry_run: bool = True,
    account: Optional[str] = None,
) -> dict[str, Any]:
    """Build a consistent bundle for publishing.

    Performance notes:
    - Avoid fetching holdings/prices more than once.
    - Use one snapshot for both record_nav and report generation.
    """
    # Build a single snapshot first (heavy operation: holdings + price fetch).
    # Import lazily to keep script startup fast.
    from skill_api import get_skill

    import time
    def _ms():
        return int(time.time()*1000)

    skill = get_skill(account)

    futu_sync_result = None
    if sync_futu_cash_mmf:
        futu_sync_result = skill.sync_futu_cash_mmf(dry_run=sync_futu_dry_run)
        if not futu_sync_result.get("success"):
            raise RuntimeError(json.dumps(futu_sync_result, ensure_ascii=False))

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
        nav_result = skill.record_nav(
            price_timeout=price_timeout,
            dry_run=True,
            confirm=False,
            snapshot=snapshot,
            use_bulk_persist=use_bulk_nav_upsert,
        )
    else:
        nav_result = skill.record_nav(
            price_timeout=price_timeout,
            dry_run=False,
            confirm=True,
            snapshot=snapshot,
            use_bulk_persist=use_bulk_nav_upsert,
        )

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
        "account": skill.account,
        "snapshot": snapshot,
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
        "futu_sync_result": futu_sync_result,
    }


def render_daily_report_html(report_bundle: dict[str, Any], config: PublishConfig) -> tuple[str, str]:
    """Render daily report HTML using the single GitHub-style template.

    We keep only ONE template to reduce maintenance cost and avoid style drift.
    """
    # Reuse the GitHub-style renderer from generate_daily_report_html.py
    from scripts import generate_daily_report_html as gh

    # build_snapshot() is created in build_report_data(); reuse it to avoid extra price fetch.
    snapshot = report_bundle.get('snapshot') or {}

    # gh.render_html expects a bundle with keys: report/full/snapshot
    report = report_bundle.get('report') or {}
    nav_result = report_bundle.get('nav_result') or {}

    full = {
        'warnings': (report.get('warnings') or nav_result.get('warnings') or []),
    }

    dt = report.get('date') or date.today().isoformat()
    html = gh.render_html({'report': report, 'full': full, 'snapshot': snapshot})
    return dt, html


def _ensure_publish_server_running() -> None:
    """Best-effort: avoid external 502 by ensuring :3000 publish server is up."""
    try:
        ensure_script = WORKSPACE / "tools" / "ensure_publish_server.py"
        if ensure_script.exists():
            __import__("subprocess").run([sys.executable, str(ensure_script), "--quiet"], check=False)
    except Exception:
        # Publishing still writes files; server health is handled separately.
        pass


def publish_report(report_date: str, html: str, config: PublishConfig) -> dict[str, Any]:
    _ensure_publish_server_running()

    slug = f"investment-daily-{report_date}"
    report_path = config.reports_dir / f"{slug}.html"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(html, encoding="utf-8")
    latest_path = config.reports_dir / "latest.html"
    latest_path.write_text(html, encoding="utf-8")

    out_dir = config.publish_root / slug
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "index.html").write_text(html, encoding="utf-8")

    public_url = f"{config.publish_base_url}/{slug}/" if config.publish_base_url else slug
    return {
        "date": report_date,
        "slug": slug,
        "report_file": str(report_path),
        "latest_file": str(latest_path),
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
        report_bundle = build_report_data(
            price_timeout=args.price_timeout,
            dry_run=args.dry_run,
            use_bulk_nav_upsert=bool(args.use_bulk_nav_upsert),
            sync_futu_cash_mmf=bool(args.sync_futu_cash_mmf),
            sync_futu_dry_run=bool(args.sync_futu_dry_run),
            account=args.account,
        )
        timings['build_report_data_ms'] = _now_ms() - t1

        # Fast mode: only compute bundle (record_nav + generate_report + get_nav)
        if bool(args.no_html):
            timings['total_ms'] = _now_ms() - t0
            out = {
                "success": True,
                "account": report_bundle.get("account"),
                "nav_result": report_bundle.get("nav_result"),
                "report": report_bundle.get("report"),
                "nav_snapshot": report_bundle.get("nav_snapshot"),
                "stage_timings": report_bundle.get("stage_timings"),
                "futu_sync_result": report_bundle.get("futu_sync_result"),
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
            "account": report_bundle.get("account"),
            "date": report_date,
            "nav_result": report_bundle["nav_result"],
            "futu_sync_result": report_bundle.get("futu_sync_result"),
            "publish": publish_result,
            "timings": timings,
        }
        if not bool(args.quiet):
            print(json.dumps(result, ensure_ascii=False, indent=2))
    
    
if __name__ == "__main__":
    main()
