#!/usr/bin/env python3
"""Patch Feishu nav_history safely (merge + validate + dry-run).

Compatibility wrapper target: prefer ``scripts/nav_history_repair.py patch``
for new automation.

Design goals
- Never overwrite non-target fields with model defaults (e.g., cash_value/stock_value becoming 0).
- Two-phase workflow: dry-run diff -> apply.
- Optional mathematical validations; abort apply if any invariant fails.

Typical usage
  ./.venv/bin/python scripts/nav_history_patch.py \
    --account lx \
    --patch-file audit/rebuild_strong_consistency_lx.json \
    --mode strong-consistency-gap \
    --dry-run

  ./.venv/bin/python scripts/nav_history_patch.py \
    --account lx \
    --patch-file audit/rebuild_strong_consistency_lx.json \
    --mode strong-consistency-gap \
    --apply

Patch file format
- Accepts JSON with either:
  - {"rebuilt": [ {"date": "YYYY-MM-DD", ... } ]}
  - {"rows": [ ... ]}
- For strong-consistency-gap mode we look for keys:
  gap_cash_flow, gap_share_change, shares, nav, pnl,
  mtd_nav_change, ytd_nav_change, mtd_pnl, ytd_pnl

Notes
- We intentionally patch only a whitelist of fields.
- We always read existing record first and merge patch fields.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Ensure repo root is importable when running from scripts/
import sys
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from skill_api import PortfolioSkill
from src.models import NAVHistory


MONEY_EPS = 0.06  # tolerate rounding/quantization noise
NAV_EPS = 1e-6
WEIGHT_EPS = 1e-4


def _iso_to_date(s: str) -> date:
    return datetime.strptime(s[:10], "%Y-%m-%d").date()


def _money_equal(a: Optional[float], b: Optional[float], eps: float = MONEY_EPS) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(float(a) - float(b)) <= eps


def _nav_equal(a: Optional[float], b: Optional[float], eps: float = 2e-6) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(float(a) - float(b)) <= eps


def _weight_equal(a: Optional[float], b: Optional[float], eps: float = WEIGHT_EPS) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(float(a) - float(b)) <= eps


@dataclass
class PatchRow:
    d: date
    # desired replacements (None means "do not patch")
    cash_flow: Optional[float] = None
    share_change: Optional[float] = None
    shares: Optional[float] = None
    nav: Optional[float] = None
    pnl: Optional[float] = None
    mtd_nav_change: Optional[float] = None
    ytd_nav_change: Optional[float] = None
    mtd_pnl: Optional[float] = None
    ytd_pnl: Optional[float] = None


def load_patch_rows(patch_file: str, mode: str) -> List[PatchRow]:
    data = json.loads(Path(patch_file).read_text(encoding="utf-8"))
    rows = data.get("rebuilt") or data.get("rows")
    if not isinstance(rows, list):
        raise ValueError("patch-file must contain a list under 'rebuilt' or 'rows'")

    out: List[PatchRow] = []
    for r in rows:
        d = _iso_to_date(r["date"]) if isinstance(r.get("date"), str) else _iso_to_date(str(r.get("date")))

        if mode == "strong-consistency-gap":
            out.append(
                PatchRow(
                    d=d,
                    cash_flow=float(r["gap_cash_flow"]) if r.get("gap_cash_flow") is not None else None,
                    share_change=float(r["gap_share_change"]) if r.get("gap_share_change") is not None else None,
                    shares=float(r["shares"]) if r.get("shares") is not None else None,
                    nav=float(r["nav"]) if r.get("nav") is not None else None,
                    pnl=float(r["pnl"]) if r.get("pnl") is not None else None,
                    mtd_nav_change=float(r["mtd_nav_change"]) if r.get("mtd_nav_change") is not None else None,
                    ytd_nav_change=float(r["ytd_nav_change"]) if r.get("ytd_nav_change") is not None else None,
                    mtd_pnl=float(r["mtd_pnl"]) if r.get("mtd_pnl") is not None else None,
                    ytd_pnl=float(r["ytd_pnl"]) if r.get("ytd_pnl") is not None else None,
                )
            )
        else:
            raise ValueError(f"unsupported mode: {mode}")

    # de-dup by date (keep last)
    m = {p.d: p for p in out}
    return [m[d] for d in sorted(m.keys())]


def merge_existing(existing: NAVHistory, patch: PatchRow, patch_none: bool = False) -> NAVHistory:
    """Return a new NAVHistory object, preserving all non-target fields."""

    def pick(old: Any, new: Any) -> Any:
        if new is None and not patch_none:
            return old
        return new

    return NAVHistory(
        record_id=existing.record_id,
        date=existing.date,
        account=existing.account,
        # keep breakdown + weights + total_value as-is
        total_value=existing.total_value,
        cash_value=existing.cash_value,
        stock_value=existing.stock_value,
        fund_value=existing.fund_value,
        cn_stock_value=existing.cn_stock_value,
        us_stock_value=existing.us_stock_value,
        hk_stock_value=existing.hk_stock_value,
        stock_weight=existing.stock_weight,
        cash_weight=existing.cash_weight,
        # patch target fields
        shares=pick(existing.shares, patch.shares),
        nav=pick(existing.nav, patch.nav),
        cash_flow=pick(existing.cash_flow, patch.cash_flow),
        share_change=pick(existing.share_change, patch.share_change),
        pnl=pick(existing.pnl, patch.pnl),
        mtd_nav_change=pick(existing.mtd_nav_change, patch.mtd_nav_change),
        ytd_nav_change=pick(existing.ytd_nav_change, patch.ytd_nav_change),
        mtd_pnl=pick(existing.mtd_pnl, patch.mtd_pnl),
        ytd_pnl=pick(existing.ytd_pnl, patch.ytd_pnl),
        # preserve details unless we explicitly patch it elsewhere
        details=existing.details,
    )


def validate_math(
    *,
    pm: PortfolioSkill,
    navs_sorted: List[NAVHistory],
    idx: int,
    candidate: NAVHistory,
    mode: str,
    validate_level: str = "basic",  # basic|full
) -> List[str]:
    """Return list of violations for one candidate record.

    validate_level:
      - basic: invariants that should always hold for patched fields (safe, low false positives)
      - full: include breakdown weights + mtd/ytd derivations (stricter; may flag legacy-history inconsistencies)
    """
    errs: List[str] = []

    # Invariant A/B: breakdown consistency
    # We only enforce when breakdown appears to be populated (non-trivial values or weights present),
    # because legacy history may not store these fields.
    breakdown_present = (
        (candidate.cash_value is not None and abs(candidate.cash_value) > MONEY_EPS)
        or (candidate.stock_value is not None and abs(candidate.stock_value) > MONEY_EPS)
        or (candidate.fund_value is not None and abs(candidate.fund_value) > MONEY_EPS)
        or (candidate.stock_weight is not None)
        or (candidate.cash_weight is not None)
    )

    # total_value == stock_value + cash_value is a basic accounting identity (when breakdown exists)
    if breakdown_present:
        expected_total = (candidate.stock_value or 0.0) + (candidate.cash_value or 0.0)
        if candidate.total_value is not None and not _money_equal(candidate.total_value, expected_total):
            errs.append(f"total_value != stock_value + cash_value ({candidate.total_value} != {expected_total})")

    # weights checks are stricter; keep them in full mode
    if validate_level == "full" and breakdown_present:
        if candidate.total_value and candidate.total_value > 0 and candidate.stock_weight is not None and candidate.cash_weight is not None:
            exp_stock_w = (candidate.stock_value or 0.0) / candidate.total_value
            exp_cash_w = (candidate.cash_value or 0.0) / candidate.total_value
            if not _weight_equal(candidate.stock_weight, exp_stock_w):
                errs.append(f"stock_weight mismatch ({candidate.stock_weight} != {exp_stock_w})")
            if not _weight_equal(candidate.cash_weight, exp_cash_w):
                errs.append(f"cash_weight mismatch ({candidate.cash_weight} != {exp_cash_w})")
            if not _weight_equal(candidate.stock_weight + candidate.cash_weight, 1.0):
                errs.append(f"weights sum != 1 ({candidate.stock_weight + candidate.cash_weight})")

    # Invariant C: nav = total_value / shares
    if candidate.shares is not None and candidate.nav is not None and candidate.shares > 0 and candidate.total_value is not None:
        exp_nav = candidate.total_value / candidate.shares
        if not _nav_equal(candidate.nav, exp_nav):
            errs.append(f"nav != total_value/shares ({candidate.nav} != {exp_nav})")

    # Invariant D: recurrence + share_change relation (strong-consistency-gap)
    if mode == "strong-consistency-gap":
        if idx > 0:
            prev = navs_sorted[idx - 1]
            if prev.nav is not None and prev.nav > 0 and prev.shares is not None and candidate.shares is not None:
                if candidate.cash_flow is not None and candidate.share_change is not None:
                    # Share change relation is sensitive to rounding of prev.nav.
                    # Validate via cash terms: share_change * prev_nav ~= cash_flow.
                    exp_cash = candidate.share_change * prev.nav
                    # allow small drift due to rounding/quantization
                    if not _money_equal(candidate.cash_flow, exp_cash, eps=10.0):
                        errs.append(f"cash_flow != share_change*prev_nav ({candidate.cash_flow} != {exp_cash})")

                    exp_shares = prev.shares + candidate.share_change
                    # shares quantized to 0.01; tolerate several rounding units
                    if not _money_equal(candidate.shares, exp_shares, eps=0.30):
                        errs.append(f"shares != prev_shares + share_change ({candidate.shares} != {exp_shares})")

                # pnl constraint only for consecutive day
                if (candidate.date - prev.date).days == 1:
                    if candidate.pnl is None:
                        errs.append("pnl should not be None for consecutive day")
                    else:
                        exp_pnl = candidate.total_value - prev.total_value - (candidate.cash_flow or 0.0)
                        if not _money_equal(candidate.pnl, exp_pnl):
                            errs.append(f"pnl mismatch ({candidate.pnl} != {exp_pnl})")
                else:
                    # for non-consecutive, pnl should be None (project convention)
                    if candidate.pnl is not None:
                        errs.append("pnl should be None when not consecutive day")

        # MTD/YTD are "full" checks; legacy stored values may follow older conventions.
        if validate_level == "full":
            all_navs = navs_sorted
            p = pm.portfolio
            nav_index = p._build_nav_lookup(all_navs)

            pm_base = p._find_prev_month_end_nav(all_navs, candidate.date.year, candidate.date.month, nav_index=nav_index)
            py_base = p._find_year_end_nav(all_navs, str(candidate.date.year - 1), nav_index=nav_index)

            if candidate.nav is not None:
                exp_mtd = p._calc_mtd_nav_change(candidate.nav, pm_base) if pm_base else None
                exp_ytd = p._calc_ytd_nav_change(candidate.nav, py_base) if py_base else None
                exp_mtd_r = round(exp_mtd, 6) if exp_mtd is not None else None
                exp_ytd_r = round(exp_ytd, 6) if exp_ytd is not None else None
                if candidate.mtd_nav_change is not None:
                    if exp_mtd_r is None:
                        errs.append("mtd_nav_change patched but month base missing")
                    elif not _nav_equal(candidate.mtd_nav_change, exp_mtd_r):
                        errs.append(f"mtd_nav_change mismatch ({candidate.mtd_nav_change} != {exp_mtd_r})")
                if candidate.ytd_nav_change is not None:
                    if exp_ytd_r is None:
                        errs.append("ytd_nav_change patched but year base missing")
                    elif not _nav_equal(candidate.ytd_nav_change, exp_ytd_r):
                        errs.append(f"ytd_nav_change mismatch ({candidate.ytd_nav_change} != {exp_ytd_r})")

            monthly_cf = p._get_monthly_cash_flow(candidate.account, candidate.date.year, candidate.date.month) if pm_base else None
            yearly_cf = p._get_yearly_cash_flow(candidate.account, str(candidate.date.year)) if py_base else None

            if candidate.mtd_pnl is not None:
                if not (pm_base and monthly_cf is not None):
                    errs.append("mtd_pnl patched but month base/cash_flow missing")
                else:
                    exp_mtd_pnl = p._calc_mtd_pnl(candidate.total_value, pm_base, monthly_cf)
                    exp_mtd_pnl_r = round(exp_mtd_pnl, 2) if exp_mtd_pnl is not None else None
                    if exp_mtd_pnl_r is not None and not _money_equal(candidate.mtd_pnl, exp_mtd_pnl_r):
                        errs.append(f"mtd_pnl mismatch ({candidate.mtd_pnl} != {exp_mtd_pnl_r})")

            if candidate.ytd_pnl is not None:
                if not (py_base and yearly_cf is not None):
                    errs.append("ytd_pnl patched but year base/cash_flow missing")
                else:
                    exp_ytd_pnl = p._calc_ytd_pnl(candidate.total_value, py_base, yearly_cf)
                    exp_ytd_pnl_r = round(exp_ytd_pnl, 2) if exp_ytd_pnl is not None else None
                    if exp_ytd_pnl_r is not None and not _money_equal(candidate.ytd_pnl, exp_ytd_pnl_r):
                        errs.append(f"ytd_pnl mismatch ({candidate.ytd_pnl} != {exp_ytd_pnl_r})")

    return errs


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", default=None)
    ap.add_argument("--patch-file", required=True)
    ap.add_argument("--mode", choices=["strong-consistency-gap"], default="strong-consistency-gap")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--backup-file", default=None, help="where to write backup JSON before apply")
    ap.add_argument("--no-validate", action="store_true")
    ap.add_argument("--validate-level", choices=["basic","full"], default="basic")
    ap.add_argument(
        "--validate-scope",
        choices=["changed", "patched", "all"],
        default="changed",
        help=(
            "changed: validate only records that will actually change (default). "
            "patched: validate all dates present in the patch file. "
            "all: validate the entire series."
        ),
    )
    args = ap.parse_args(argv)

    if not args.dry_run and not args.apply:
        raise SystemExit("must pass --dry-run or --apply")
    if args.dry_run and args.apply:
        raise SystemExit("choose only one of --dry-run / --apply")

    ps = PortfolioSkill()
    if args.account:
        ps.account = args.account

    patches = load_patch_rows(args.patch_file, args.mode)

    navs = ps.storage.get_nav_history(ps.account, days=9999)
    navs = sorted(navs, key=lambda n: n.date)
    nav_by_date = {n.date: n for n in navs}

    # build candidate merged list
    merged: List[NAVHistory] = []
    diffs: List[Dict[str, Any]] = []
    violations: List[Dict[str, Any]] = []

    patch_fields = [
        "cash_flow",
        "share_change",
        "shares",
        "nav",
        "pnl",
        "mtd_nav_change",
        "ytd_nav_change",
        "mtd_pnl",
        "ytd_pnl",
    ]

    for p in patches:
        existing = nav_by_date.get(p.d)
        if not existing:
            diffs.append({"date": p.d.isoformat(), "status": "missing_existing"})
            continue

        cand = merge_existing(existing, p)

        # diff only target fields
        change = {"date": p.d.isoformat(), "record_id": existing.record_id, "changes": {}}
        for f in patch_fields:
            old = getattr(existing, f)
            new = getattr(cand, f)
            if old != new:
                change["changes"][f] = {"old": old, "new": new}

        diffs.append(change)
        merged.append(cand)

    # validate in full-date order: create a combined series (original, with patches applied)
    patched_by_date = {m.date: m for m in merged}
    series = [patched_by_date.get(n.date, n) for n in navs]

    if not args.no_validate:
        # Dates to validate depend on scope
        dates_in_patch = set(patched_by_date.keys())
        dates_changed = set()
        for d in dates_in_patch:
            old = nav_by_date.get(d)
            new = patched_by_date.get(d)
            if old and new:
                # if any target field differs, mark as changed
                for f in [
                    "cash_flow",
                    "share_change",
                    "shares",
                    "nav",
                    "pnl",
                    "mtd_nav_change",
                    "ytd_nav_change",
                    "mtd_pnl",
                    "ytd_pnl",
                ]:
                    if getattr(old, f) != getattr(new, f):
                        dates_changed.add(d)
                        break

        for i, n in enumerate(series):
            if args.validate_scope == "changed" and n.date not in dates_changed:
                continue
            if args.validate_scope == "patched" and n.date not in dates_in_patch:
                continue
            errs = validate_math(pm=ps, navs_sorted=series, idx=i, candidate=n, mode=args.mode, validate_level=args.validate_level)
            if errs:
                violations.append({"date": n.date.isoformat(), "record_id": n.record_id, "errors": errs})

    out_dir = Path("audit")
    out_dir.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    diff_path = out_dir / f"nav_history_patch_diff_{ps.account}_{stamp}.json"
    diff_path.write_text(json.dumps({"account": ps.account, "mode": args.mode, "diffs": diffs, "violations": violations}, ensure_ascii=False, indent=2), encoding="utf-8")
    print("wrote", diff_path)

    if violations:
        print("VALIDATION FAILED; first 5 violations:")
        for v in violations[:5]:
            print(v["date"], v["errors"][:3])
        if args.apply:
            raise SystemExit("abort apply due to validation errors")

    if args.dry_run:
        # summary
        changed = sum(1 for d in diffs if d.get("changes"))
        print("dry-run ok; records with changes:", changed, "of", len(diffs))
        return

    # apply
    backup_file = args.backup_file or str(out_dir / f"nav_history_patch_backup_{ps.account}_{stamp}.json")
    backup = []
    for p in patches:
        existing = nav_by_date.get(p.d)
        if not existing:
            continue
        backup.append(ps.storage.save_nav(existing, overwrite_existing=True, dry_run=True))
    Path(backup_file).write_text(json.dumps(backup, ensure_ascii=False, indent=2), encoding="utf-8")
    print("backup wrote", backup_file)

    # perform updates
    updated = 0
    for p in patches:
        existing = nav_by_date.get(p.d)
        if not existing:
            continue
        cand = merge_existing(existing, p)
        # hard safety: do not allow breakdown fields to change in this patch tool
        for f in ["cash_value", "stock_value", "fund_value", "cn_stock_value", "us_stock_value", "hk_stock_value", "total_value"]:
            if getattr(existing, f) != getattr(cand, f):
                raise SystemExit(f"safety abort: non-target field changed: {p.d} {f}")
        ps.storage.save_nav(cand, overwrite_existing=True, dry_run=False)
        updated += 1

    print("applied patches; updated", updated, "records")


if __name__ == "__main__":
    main()
