#!/usr/bin/env python3
"""Audit: every nav_history point must have holdings_snapshot rows.

Rule:
- For each nav_history record (account, date), there must exist at least one
  holdings_snapshot row where (account==account && as_of==YYYY-MM-DD(date)).

This is a *read-only* audit script.
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

# Ensure project root on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from skill_api import PortfolioSkill


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument('--account', default=None)
    ap.add_argument(
        '--since',
        default=None,
        help='Only enforce snapshot presence for nav_history dates >= since (YYYY-MM-DD).',
    )
    args = ap.parse_args()

    ps = PortfolioSkill()
    account = args.account or ps.account
    storage = ps.storage

    navs = storage.get_nav_history(account, days=9999)

    since_d: date | None = None
    if args.since:
        since_d = date.fromisoformat(args.since)

    missing = []
    checked = 0
    for nav in navs:
        if not nav.date:
            continue
        if since_d and nav.date < since_d:
            continue
        checked += 1
        as_of = nav.date.strftime('%Y-%m-%d')
        # Use a narrow filter; Feishu uses && for AND.
        f = f'CurrentValue.[as_of] = "{as_of}" && CurrentValue.[account] = "{account}"'
        recs = storage.client.list_records('holdings_snapshot', filter_str=f, page_size=1)
        if not recs:
            missing.append({'account': account, 'date': as_of, 'nav_record_id': nav.record_id})

    out = {
        'account': account,
        'since': args.since,
        'nav_count': len(navs),
        'checked_count': checked,
        'missing_count': len(missing),
        'missing': missing,
    }
    out_path = ROOT / 'audit' / f'nav_history_missing_snapshot_{account}.json'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2) + '\n', 'utf-8')

    print(f"nav_count={len(navs)} missing_snapshot={len(missing)}")
    if missing:
        print("sample_missing=", missing[:5])
        print("wrote", out_path)
        return 2
    print("wrote", out_path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
