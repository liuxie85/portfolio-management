#!/usr/bin/env python3
"""Cleanup local artifacts to control disk usage.

Policy (requested by user):
- holdings_snapshot: keep last 180 days (by filename date)
- audit: keep last 90 days (by mtime)

This script only deletes files under repo-local directories.
Default is dry-run. Use --apply to actually delete.

Usage:
  ./.venv/bin/python scripts/cleanup_local_artifacts.py --dry-run
  ./.venv/bin/python scripts/cleanup_local_artifacts.py --apply

Notes:
- holdings_snapshot files are expected to be JSON named YYYY-MM-DD.json under .data/holdings_snapshot/<account>/
- audit files are typically JSON and can be many; we delete by mtime.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / ".data"
SNAPSHOT_DIR = DATA_DIR / "holdings_snapshot"
AUDIT_DIR = REPO_ROOT / "audit"


@dataclass
class PlanItem:
    path: Path
    reason: str
    mtime: float


def parse_snapshot_date(p: Path) -> date | None:
    # Expect filename like 2026-03-29.json
    name = p.name
    if not name.endswith('.json'):
        return None
    stem = name[:-5]
    try:
        return datetime.strptime(stem, "%Y-%m-%d").date()
    except Exception:
        return None


def build_holdings_snapshot_plan(keep_days: int, now_d: date) -> List[PlanItem]:
    plan: List[PlanItem] = []
    if not SNAPSHOT_DIR.exists():
        return plan
    cutoff = now_d - timedelta(days=keep_days)

    for p in SNAPSHOT_DIR.rglob("*.json"):
        if not p.is_file():
            continue
        d = parse_snapshot_date(p)
        if d is None:
            continue
        if d < cutoff:
            plan.append(PlanItem(path=p, reason=f"holdings_snapshot older than {keep_days}d (date={d.isoformat()}, cutoff={cutoff.isoformat()})", mtime=p.stat().st_mtime))
    return plan


def build_audit_plan(keep_days: int, now_ts: float) -> List[PlanItem]:
    plan: List[PlanItem] = []
    if not AUDIT_DIR.exists():
        return plan
    cutoff_ts = now_ts - keep_days * 86400

    for p in AUDIT_DIR.rglob("*"):
        if not p.is_file():
            continue
        st = p.stat()
        if st.st_mtime < cutoff_ts:
            plan.append(PlanItem(path=p, reason=f"audit older than {keep_days}d by mtime", mtime=st.st_mtime))
    return plan


def delete_files(items: List[PlanItem], apply: bool) -> Tuple[int, int]:
    count = 0
    bytes_freed = 0
    for it in items:
        try:
            size = it.path.stat().st_size
        except Exception:
            size = 0
        if apply:
            try:
                it.path.unlink(missing_ok=True)
                count += 1
                bytes_freed += size
            except Exception as e:
                print(f"[WARN] failed to delete {it.path}: {e}")
        else:
            count += 1
            bytes_freed += size
    return count, bytes_freed


def fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.1f}{unit}" if unit != "B" else f"{n}{unit}"
        n /= 1024
    return f"{n:.1f}GB"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="Actually delete files")
    ap.add_argument("--dry-run", action="store_true", help="Dry run (default)")
    ap.add_argument("--snapshot-keep-days", type=int, default=180)
    ap.add_argument("--audit-keep-days", type=int, default=90)
    args = ap.parse_args()

    apply = bool(args.apply)
    if args.apply and args.dry_run:
        raise SystemExit("--apply and --dry-run cannot both be set")

    now = datetime.now()
    now_d = now.date()
    now_ts = now.timestamp()

    snapshot_items = build_holdings_snapshot_plan(args.snapshot_keep_days, now_d)
    audit_items = build_audit_plan(args.audit_keep_days, now_ts)

    all_items = snapshot_items + audit_items
    all_items.sort(key=lambda x: (x.path.as_posix()))

    print(f"Plan: {len(snapshot_items)} holdings_snapshot files, {len(audit_items)} audit files")
    for it in all_items[:200]:
        mt = datetime.fromtimestamp(it.mtime).isoformat(timespec='seconds')
        print(f"- {it.path.relative_to(REPO_ROOT)}\tmtime={mt}\t{it.reason}")
    if len(all_items) > 200:
        print(f"... ({len(all_items)-200} more)")

    cnt, bytes_freed = delete_files(all_items, apply=apply)
    action = "Deleted" if apply else "Would delete"
    print(f"{action}: {cnt} files, approx {fmt_bytes(bytes_freed)}")

    # Best-effort: remove empty directories under holdings_snapshot and audit.
    if apply:
        for d in [SNAPSHOT_DIR, AUDIT_DIR]:
            if not d.exists():
                continue
            # bottom-up
            for sub in sorted([p for p in d.rglob('*') if p.is_dir()], key=lambda x: len(x.as_posix()), reverse=True):
                try:
                    sub.rmdir()
                except Exception:
                    pass


if __name__ == "__main__":
    main()
