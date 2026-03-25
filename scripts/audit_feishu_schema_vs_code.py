#!/usr/bin/env python3
"""Audit Feishu Bitable schema vs code assumptions.

This script pulls Bitable field lists via the official Feishu API (through the
OpenClaw feishu_bitable_list_fields tool in assistant runs) is not available
from pure local execution. So this script is a lightweight *code-side* checker:
- It loads the configured table mappings from src/config.py
- It prints what the code believes are required fields and numeric-field typing
- It highlights known mismatches discovered during review

Use in the agent workflow:
1) Use feishu_bitable_list_fields to fetch real fields (tool-side)
2) Compare with the printed expectations from this script

The goal is to keep a single, repeatable checklist and to prevent schema drift.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import sys
from pathlib import Path

# Ensure project root is on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.feishu_client import FeishuClient
from src.feishu_storage import FeishuStorage


@dataclass
class TableExpect:
    required: List[str]
    numeric_fields: List[str]


def main() -> None:
    client = FeishuClient()
    storage = FeishuStorage(client=client)

    # Extract numeric typing config by calling the private method on a sample.
    # We keep the canonical list here to avoid reaching into internals.
    expects: Dict[str, TableExpect] = {
        'holdings': TableExpect(
            required=client.REQUIRED_FIELDS['holdings'],
            numeric_fields=['quantity', 'avg_cost'],
        ),
        'transactions': TableExpect(
            required=client.REQUIRED_FIELDS['transactions'],
            numeric_fields=['quantity', 'price', 'amount', 'fee'],
        ),
        'cash_flow': TableExpect(
            required=client.REQUIRED_FIELDS['cash_flow'],
            numeric_fields=['amount', 'cny_amount', 'exchange_rate'],
        ),
        'nav_history': TableExpect(
            required=client.REQUIRED_FIELDS['nav_history'],
            numeric_fields=[
                'total_value', 'cash_value', 'stock_value', 'fund_value',
                'cn_stock_value', 'us_stock_value', 'hk_stock_value',
                'stock_weight', 'cash_weight',
                'shares', 'nav', 'cash_flow', 'share_change',
                'mtd_nav_change', 'ytd_nav_change',
                'pnl', 'mtd_pnl', 'ytd_pnl',
            ],
        ),
        'holdings_snapshot': TableExpect(
            required=client.REQUIRED_FIELDS.get('holdings_snapshot', []),
            numeric_fields=['quantity', 'avg_cost', 'price', 'cny_price', 'market_value_cny'],
        ),
    }

    print("# Feishu schema expectations (code-side)\n")
    for t, exp in expects.items():
        cfg = client.table_configs.get(t)
        if not cfg:
            print(f"- {t}: [NOT CONFIGURED]\n")
            continue
        app_token, table_id = client._get_table_config(t)
        print(f"- {t}: app_token={app_token} table_id={table_id}")
        print(f"  required={exp.required}")
        print(f"  numeric_fields={exp.numeric_fields}\n")

    print("# Known schema pitfalls to verify")
    print("- transactions.tx_date should be Text in Feishu; code writes YYYY-MM-DD")
    print("- cash_flow.remark is SingleSelect in Feishu (recommend converting to Text or mapping)")


if __name__ == '__main__':
    main()
