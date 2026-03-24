#!/usr/bin/env python3
"""Audit Feishu nav_history records whose UTC date differs from Beijing business date.

This is a *guardrail* script: it does not modify any data.
It helps detect cross-day drift when the underlying date field is stored as a timestamp.

Usage:
  ./.venv/bin/python scripts/audit_feishu_date_tz_mismatch.py --account lx

Output:
  - prints summary + samples
  - writes audit/feishu_date_tz_mismatch_<account>.json
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import sys
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from skill_api import PortfolioSkill


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--account', default='lx')
    ap.add_argument('--table', default='nav_history')
    ap.add_argument('--limit', type=int, default=500)
    args = ap.parse_args()

    ps = PortfolioSkill(); ps.account = args.account
    client = ps.storage.client

    records = client.list_records(args.table, filter_str=f'CurrentValue.[account] = "{args.account}"')

    bj = timezone(timedelta(hours=8))
    mismatches = []
    for r in records:
        f = r.get('fields') or {}
        ms = f.get('date')
        if not isinstance(ms, int):
            continue
        utc_d = datetime.fromtimestamp(ms/1000, tz=timezone.utc).date().isoformat()
        bj_d = datetime.fromtimestamp(ms/1000, tz=bj).date().isoformat()
        if utc_d != bj_d:
            mismatches.append({
                'record_id': r.get('record_id'),
                'utc_date': utc_d,
                'bj_date': bj_d,
                'date_ms': ms,
            })

    out = {
        'table': args.table,
        'account': args.account,
        'record_count': len(records),
        'mismatch_count': len(mismatches),
        'mismatches': mismatches,
    }

    Path('audit').mkdir(exist_ok=True)
    out_path = Path('audit') / f'feishu_date_tz_mismatch_{args.account}.json'
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding='utf-8')

    print(f"records={len(records)} mismatches={len(mismatches)}")
    for m in mismatches[:10]:
        print(m)
    print('wrote', out_path)


if __name__ == '__main__':
    main()
