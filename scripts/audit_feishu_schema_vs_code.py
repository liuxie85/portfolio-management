#!/usr/bin/env python3
"""Compatibility wrapper for code-side schema expectations.

Prefer:
  python scripts/migrate_schema.py expectations
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.migrate_schema import schema_expectations


def main() -> None:
    print(json.dumps(schema_expectations(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
