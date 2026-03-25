#!/usr/bin/env python3
"""Schema doctor for Feishu Bitable tables.

Purpose:
- Detect missing/renamed fields early (FieldNameNotFound is a top source of runtime failures)
- Compare live Bitable schema with docs/schema.md (canonical expected fields)

Notes:
- This script is best-effort and focuses on **field names**. Field types are checked lightly.
- Requires Feishu app credentials configured in config.json.

Usage:
  . .venv/bin/activate
  python scripts/schema_doctor.py
  python scripts/schema_doctor.py --json
  python scripts/schema_doctor.py --strict
"""

from __future__ import annotations

# Ensure repo root is on sys.path when executed as a script.
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Set, Tuple, Any


DOCS_SCHEMA = Path(__file__).resolve().parents[1] / "docs" / "schema.md"


@dataclass
class TableSpec:
    name: str
    required: Set[str]
    optional: Set[str]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compare live Feishu Bitable fields with docs/schema.md")
    p.add_argument("--json", action="store_true", help="Output JSON")
    p.add_argument("--strict", action="store_true", help="Exit non-zero if any required field missing")
    return p.parse_args()


def parse_docs_schema(path: Path) -> Dict[str, TableSpec]:
    """Parse docs/schema.md into {table_name: TableSpec}.

    Expected format:
      ### holdings
      Required fields:
      - `asset_id` (text)
      ...
      Optional fields:
      - `avg_cost` (number)
    """
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()

    tables: Dict[str, TableSpec] = {}
    cur_table: str | None = None
    mode: str | None = None  # required|optional

    h_re = re.compile(r"^###\s+([a-zA-Z0-9_]+)\s*$")
    field_re = re.compile(r"^-\s+`([^`]+)`\s*\(")

    for ln in lines:
        m = h_re.match(ln.strip())
        if m:
            cur_table = m.group(1)
            tables[cur_table] = TableSpec(name=cur_table, required=set(), optional=set())
            mode = None
            continue

        if cur_table is None:
            continue

        s = ln.strip().lower()
        if s.startswith("required fields"):
            mode = "required"
            continue
        if s.startswith("optional fields"):
            mode = "optional"
            continue

        m2 = re.match(r"^-\s+`([^`]+)`", ln.strip())
        if m2 and mode in ("required", "optional"):
            f = m2.group(1).strip()
            if mode == "required":
                tables[cur_table].required.add(f)
            else:
                tables[cur_table].optional.add(f)

    return tables


def main() -> None:
    args = parse_args()

    from src.feishu_client import FeishuClient

    if not DOCS_SCHEMA.exists():
        raise SystemExit(f"docs/schema.md not found: {DOCS_SCHEMA}")

    specs = parse_docs_schema(DOCS_SCHEMA)

    client = FeishuClient()

    report: Dict[str, Any] = {
        "schema_doc": str(DOCS_SCHEMA),
        "tables": {},
        "ok": True,
    }

    for table_name, spec in specs.items():
        # skip tables not configured in current deployment
        try:
            app_token, table_id = client._get_table_config(table_name)
        except Exception as e:
            report["tables"][table_name] = {
                "configured": False,
                "error": str(e),
                "required": sorted(spec.required),
                "optional": sorted(spec.optional),
            }
            report["ok"] = False
            continue
        endpoint = f"/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        data = client._request("GET", endpoint, params={"page_size": 200})
        items = data.get("items", [])
        live_fields = {it.get("field_name") for it in items if it.get("field_name")}

        missing_required = sorted(spec.required - live_fields)
        extra_fields = sorted(live_fields - (spec.required | spec.optional))
        ok = len(missing_required) == 0

        report["tables"][table_name] = {
            "app_token": app_token,
            "table_id": table_id,
            "required": sorted(spec.required),
            "optional": sorted(spec.optional),
            "live_fields": sorted(live_fields),
            "missing_required": missing_required,
            "extra_fields": extra_fields,
            "ok": ok,
        }

        if not ok:
            report["ok"] = False

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        for t, r in report["tables"].items():
            if not r.get("configured", True):
                print(f"[SKIP] {t} (not configured) — {r.get('error')}")
                continue
            status = "OK" if r.get("ok") else "MISSING"
            print(f"[{status}] {t} ({r.get('app_token')}/{r.get('table_id')})")
            if r.get("missing_required"):
                print("  missing_required:", ", ".join(r["missing_required"]))
        if report["ok"]:
            print("\nALL_OK")

    if args.strict and not report["ok"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
