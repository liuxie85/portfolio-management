#!/usr/bin/env python3
"""Canonical schema migration/check entrypoint.

Default mode is dry-run planning. ``--apply`` records migrations in local state;
actual Feishu table/field creation is still manual until write-safe migration
operations are implemented.

Use this script for schema-related checks. ``schema_doctor.py`` and
``audit_feishu_schema_vs_code.py`` remain compatibility helpers.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.migrations import MigrationRunner
from src.migrations.feishu import get_migrations
from src.feishu_client import FeishuClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan/apply schema migration state.")
    parser.add_argument(
        "command",
        nargs="?",
        choices=["plan", "apply", "expectations", "check-live"],
        default="plan",
        help="schema action (default: plan)",
    )
    parser.add_argument("--apply", action="store_true", help="Mark pending migrations as applied in local migration state.")
    parser.add_argument("--strict", action="store_true", help="For check-live, exit non-zero if required fields are missing.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.apply:
        args.command = "apply"

    if args.command == "expectations":
        result = schema_expectations()
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    if args.command == "check-live":
        from scripts.schema_doctor import run_schema_check

        result = run_schema_check(strict=args.strict)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result.get("ok", True) else 1

    runner = MigrationRunner(get_migrations())
    result = runner.apply() if args.command == "apply" else runner.plan()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("success", True) else 1


def schema_expectations() -> dict:
    client = FeishuClient()
    expects = {
        "holdings": {
            "required": client.REQUIRED_FIELDS["holdings"],
            "numeric_fields": ["quantity", "avg_cost"],
        },
        "transactions": {
            "required": client.REQUIRED_FIELDS["transactions"],
            "numeric_fields": ["quantity", "price", "amount", "fee"],
        },
        "cash_flow": {
            "required": client.REQUIRED_FIELDS["cash_flow"],
            "numeric_fields": ["amount", "cny_amount", "exchange_rate"],
        },
        "nav_history": {
            "required": client.REQUIRED_FIELDS["nav_history"],
            "numeric_fields": [
                "total_value", "cash_value", "stock_value", "fund_value",
                "cn_stock_value", "us_stock_value", "hk_stock_value",
                "stock_weight", "cash_weight", "shares", "nav",
                "cash_flow", "share_change", "mtd_nav_change",
                "ytd_nav_change", "pnl", "mtd_pnl", "ytd_pnl",
            ],
        },
        "holdings_snapshot": {
            "required": client.REQUIRED_FIELDS.get("holdings_snapshot", []),
            "numeric_fields": ["quantity", "avg_cost", "price", "cny_price", "market_value_cny"],
        },
    }
    return {"success": True, "tables": expects}


if __name__ == "__main__":
    raise SystemExit(main())
