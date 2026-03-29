#!/usr/bin/env python3
"""Bulk recompute + backfill nav_history derived fields.

Design:
- Recompute target dates with existing PortfolioManager.record_nav(...) logic (persist=False)
- Persist in one/batched call via FeishuStorage.upsert_nav_bulk(...)
- Supports input JSON (audit/recompute output) OR date range over existing nav_history

Examples:
  # Dry-run from date range
  ./.venv/bin/python scripts/backfill_nav_history_bulk.py --account lx --from 2025-01-01 --to 2025-12-31 --dry-run

  # Apply from audit output
  ./.venv/bin/python scripts/backfill_nav_history_bulk.py --account lx --input audit/rebuild_strong_consistency_lx.json --apply
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from skill_api import PortfolioSkill
from src.models import NAVHistory, PortfolioValuation


@dataclass
class BaseNavPoint:
    d: date
    total_value: float
    cash_value: Optional[float] = None
    stock_value: Optional[float] = None
    fund_value: Optional[float] = None
    cn_stock_value: Optional[float] = None
    us_stock_value: Optional[float] = None
    hk_stock_value: Optional[float] = None


def _to_date(v: Any) -> date:
    if isinstance(v, date):
        return v
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(float(v) / 1000).date()
    return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()


def _to_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    if v is None:
        return default
    try:
        return float(v)
    except Exception:
        return default


def _load_input_rows(path: str) -> List[Dict[str, Any]]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(data, list):
        return data
    for key in ("rebuilt", "rows", "navs", "items"):
        rows = data.get(key)
        if isinstance(rows, list):
            return rows
    raise ValueError("input json must be a list or contain one of keys: rebuilt/rows/navs/items")


def _rows_to_points(rows: List[Dict[str, Any]]) -> List[BaseNavPoint]:
    out: List[BaseNavPoint] = []
    for r in rows:
        d_raw = r.get("date")
        if not d_raw:
            continue
        d = _to_date(d_raw)

        total_value = _to_float(r.get("total_value"))
        if total_value is None:
            # best-effort fallback
            stock_v = _to_float(r.get("stock_value"), 0.0) or 0.0
            cash_v = _to_float(r.get("cash_value"), 0.0) or 0.0
            fund_v = _to_float(r.get("fund_value"), 0.0) or 0.0
            total_value = stock_v + cash_v + fund_v

        out.append(
            BaseNavPoint(
                d=d,
                total_value=float(total_value),
                cash_value=_to_float(r.get("cash_value")),
                stock_value=_to_float(r.get("stock_value")),
                fund_value=_to_float(r.get("fund_value")),
                cn_stock_value=_to_float(r.get("cn_stock_value")),
                us_stock_value=_to_float(r.get("us_stock_value")),
                hk_stock_value=_to_float(r.get("hk_stock_value")),
            )
        )

    # de-dup by date (keep last)
    m = {p.d: p for p in out}
    return [m[d] for d in sorted(m.keys())]


def _existing_points_from_range(skill: PortfolioSkill, d_from: date, d_to: date) -> List[BaseNavPoint]:
    navs = skill.storage.get_nav_history(skill.account, days=9999)
    points: List[BaseNavPoint] = []
    for n in navs:
        if n.date < d_from or n.date > d_to:
            continue
        points.append(
            BaseNavPoint(
                d=n.date,
                total_value=float(n.total_value or 0.0),
                cash_value=n.cash_value,
                stock_value=n.stock_value,
                fund_value=n.fund_value,
                cn_stock_value=n.cn_stock_value,
                us_stock_value=n.us_stock_value,
                hk_stock_value=n.hk_stock_value,
            )
        )
    points.sort(key=lambda x: x.d)
    return points


def _build_valuation(account: str, p: BaseNavPoint) -> PortfolioValuation:
    total = float(p.total_value or 0.0)
    stock = p.stock_value
    cash = p.cash_value
    fund = p.fund_value

    if stock is None and cash is not None:
        stock = total - float(cash)
    if cash is None and stock is not None:
        cash = total - float(stock)
    if stock is None and cash is None:
        cash = total
        stock = 0.0
    if fund is None:
        fund = 0.0

    return PortfolioValuation(
        account=account,
        total_value_cny=total,
        cash_value_cny=float(cash or 0.0),
        stock_value_cny=float(stock or 0.0),
        fund_value_cny=float(fund or 0.0),
        cn_asset_value=float(p.cn_stock_value or 0.0),
        us_asset_value=float(p.us_stock_value or 0.0),
        hk_asset_value=float(p.hk_stock_value or 0.0),
        holdings=[],
        warnings=[],
    )


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Bulk recompute/backfill nav_history derived fields")
    ap.add_argument("--account", default="lx")
    ap.add_argument("--input", help="Input JSON from audit/recompute output")
    ap.add_argument("--from", dest="d_from", help="YYYY-MM-DD (required if --input absent)")
    ap.add_argument("--to", dest="d_to", help="YYYY-MM-DD (required if --input absent)")
    ap.add_argument("--mode", choices=["replace", "upsert"], default="replace")
    ap.add_argument("--allow-partial", action="store_true")
    ap.add_argument("--apply", action="store_true", help="Actually write to Feishu")
    ap.add_argument("--dry-run", action="store_true", help="Force dry-run (explicit no-write)")
    ap.add_argument("--limit", type=int, default=0, help="Only process first N dates (debug)")
    args = ap.parse_args()
    if args.apply and args.dry_run:
        raise ValueError("--apply and --dry-run are mutually exclusive")
    return args


def main() -> None:
    args = parse_args()
    skill = PortfolioSkill(account=args.account)

    # Build base points
    if args.input:
        rows = _load_input_rows(args.input)
        points = _rows_to_points(rows)
    else:
        if not args.d_from or not args.d_to:
            raise ValueError("either --input or (--from and --to) is required")
        d_from = _to_date(args.d_from)
        d_to = _to_date(args.d_to)
        points = _existing_points_from_range(skill, d_from, d_to)

    if args.limit and args.limit > 0:
        points = points[: args.limit]

    if not points:
        print(json.dumps({"success": True, "count": 0, "message": "no points"}, ensure_ascii=False))
        return

    # Preload once; then mutate in-memory nav index incrementally so later dates depend on recomputed earlier dates.
    skill.storage.preload_nav_index(skill.account)
    idx = skill.storage.get_nav_index(skill.account)
    working_navs: List[NAVHistory] = sorted(list(idx.get("_nav_objects") or []), key=lambda n: n.date)

    def _upsert_working(nav: NAVHistory):
        replaced = False
        for i, n in enumerate(working_navs):
            if n.date == nav.date:
                working_navs[i] = nav
                replaced = True
                break
        if not replaced:
            working_navs.append(nav)
            working_navs.sort(key=lambda x: x.date)

    recomputed: List[NAVHistory] = []
    for p in points:
        # inject working nav history into in-memory index for this iteration
        if isinstance(skill.storage._nav_index_mem_cache.get(skill.account), dict):
            skill.storage._nav_index_mem_cache[skill.account]["_nav_objects"] = list(working_navs)

        valuation = _build_valuation(skill.account, p)
        nav = skill.portfolio.record_nav(
            skill.account,
            valuation=valuation,
            nav_date=p.d,
            persist=False,
            overwrite_existing=True,
            dry_run=True,
        )

        # Keep market-value decomposition from input/existing base where available.
        nav.total_value = float(p.total_value)
        if p.cash_value is not None:
            nav.cash_value = float(p.cash_value)
        if p.stock_value is not None:
            nav.stock_value = float(p.stock_value)
        if p.fund_value is not None:
            nav.fund_value = float(p.fund_value)
        if p.cn_stock_value is not None:
            nav.cn_stock_value = float(p.cn_stock_value)
        if p.us_stock_value is not None:
            nav.us_stock_value = float(p.us_stock_value)
        if p.hk_stock_value is not None:
            nav.hk_stock_value = float(p.hk_stock_value)

        recomputed.append(nav)
        _upsert_working(nav)

    payload = {
        "success": True,
        "account": skill.account,
        "count": len(recomputed),
        "date_from": recomputed[0].date.isoformat(),
        "date_to": recomputed[-1].date.isoformat(),
        "mode": args.mode,
        "dry_run": not args.apply,
        "sample": [
            {
                "date": n.date.isoformat(),
                "nav": n.nav,
                "shares": n.shares,
                "cash_flow": n.cash_flow,
                "pnl": n.pnl,
                "mtd_nav_change": n.mtd_nav_change,
                "ytd_nav_change": n.ytd_nav_change,
                "mtd_pnl": n.mtd_pnl,
                "ytd_pnl": n.ytd_pnl,
            }
            for n in recomputed[:5]
        ],
    }

    if args.apply:
        write_result = skill.storage.upsert_nav_bulk(
            recomputed,
            mode=args.mode,
            allow_partial=bool(args.allow_partial),
        )
        payload["write"] = write_result
    else:
        payload["write"] = {
            "mode": args.mode,
            "would_write": len(recomputed),
            "note": "run with --apply to persist",
        }

    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
