#!/usr/bin/env python3
"""portfolio-management CLI (thin wrapper around skill_api).

Design goals:
- Provide a few common read-only commands.
- Fast defaults (no writes; avoid slow realtime price fetch unless asked).
- Human-readable by default; `--json` for automation.

Usage examples:
  . .venv/bin/activate
  python scripts/pm.py cash
  python scripts/pm.py holdings
  python scripts/pm.py holdings --include-price --timeout 25
  python scripts/pm.py nav
  python scripts/pm.py report daily
  python scripts/pm.py report daily --timeout 25 --json

Safety:
- This CLI intentionally does NOT expose write paths by default.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Ensure repo root is on sys.path so `import skill_api` works.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _dump(obj, as_json: bool):
    if as_json:
        print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))
    else:
        # simple human-readable
        if isinstance(obj, dict):
            print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))
        else:
            print(obj)


def cmd_holdings(args):
    from skill_api import get_holdings

    res = get_holdings(include_price=bool(args.include_price))
    _dump(res, args.json)


def cmd_cash(args):
    from skill_api import get_cash

    res = get_cash()
    _dump(res, args.json)


def cmd_nav(args):
    from skill_api import get_nav

    res = get_nav()
    _dump(res, args.json)


def cmd_report(args):
    from skill_api import generate_report

    res = generate_report(report_type=args.type, record_nav=False, price_timeout=args.timeout)
    _dump(res, args.json)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pm", description="portfolio-management CLI")
    p.add_argument("--json", action="store_true", help="output JSON")

    sp = p.add_subparsers(dest="cmd", required=True)

    # Allow putting global flags after the subcommand (e.g. `pm cash --json`).
    # argparse doesn't support this natively; we implement it by also adding --json
    # to each subparser.

    p_hold = sp.add_parser("holdings", help="list holdings")
    p_hold.add_argument("--include-price", action="store_true", help="include price fields (may be slow)")
    p_hold.add_argument("--json", action="store_true", help="output JSON")
    p_hold.set_defaults(func=cmd_holdings)

    p_cash = sp.add_parser("cash", help="show cash positions")
    p_cash.add_argument("--json", action="store_true", help="output JSON")
    p_cash.set_defaults(func=cmd_cash)

    p_nav = sp.add_parser("nav", help="show latest nav")
    p_nav.add_argument("--json", action="store_true", help="output JSON")
    p_nav.set_defaults(func=cmd_nav)

    p_rep = sp.add_parser("report", help="generate report (read-only)")
    p_rep.add_argument("type", choices=["daily", "monthly", "yearly"], help="report type")
    p_rep.add_argument("--timeout", type=int, default=30, help="price timeout seconds (default 30)")
    p_rep.add_argument("--json", action="store_true", help="output JSON")
    p_rep.set_defaults(func=cmd_report)

    return p


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.func(args)
        return 0
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
