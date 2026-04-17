#!/usr/bin/env python3
"""Plan or mark schema migrations.

Default mode is dry-run planning. ``--apply`` records migrations in local state;
actual Feishu table/field creation is still manual until write-safe migration
operations are implemented.
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan/apply schema migration state.")
    parser.add_argument("--apply", action="store_true", help="Mark pending migrations as applied in local migration state.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runner = MigrationRunner(get_migrations())
    result = runner.apply() if args.apply else runner.plan()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("success", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
