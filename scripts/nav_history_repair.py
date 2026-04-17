#!/usr/bin/env python3
"""Canonical nav_history repair entrypoint.

Subcommands:
- backfill: recompute derived NAV fields and persist through bulk upsert.
- patch: apply a validated field patch file.

Legacy scripts remain as compatibility wrappers, but new automation should use
this entrypoint so all nav_history writes are easy to audit.
"""
from __future__ import annotations

import argparse
import sys


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Canonical nav_history repair entrypoint.")
    sub = parser.add_subparsers(dest="command", required=True)

    backfill = sub.add_parser("backfill", help="recompute/backfill nav_history derived fields")
    backfill.add_argument("--account", default="lx")
    backfill.add_argument("--input", help="Input JSON from audit/recompute output")
    backfill.add_argument("--from", dest="d_from", help="YYYY-MM-DD (required if --input absent)")
    backfill.add_argument("--to", dest="d_to", help="YYYY-MM-DD (required if --input absent)")
    backfill.add_argument("--mode", choices=["replace", "upsert"], default="replace")
    backfill.add_argument("--allow-partial", action="store_true")
    backfill.add_argument("--apply", action="store_true", help="Actually write to Feishu")
    backfill.add_argument("--dry-run", action="store_true", help="Force dry-run (explicit no-write)")
    backfill.add_argument("--limit", type=int, default=0, help="Only process first N dates (debug)")

    patch = sub.add_parser("patch", help="apply validated nav_history patch file")
    patch.add_argument("--account", default=None)
    patch.add_argument("--patch-file", required=True)
    patch.add_argument("--mode", choices=["strong-consistency-gap"], default="strong-consistency-gap")
    patch.add_argument("--dry-run", action="store_true")
    patch.add_argument("--apply", action="store_true")
    patch.add_argument("--backup-file", default=None)
    patch.add_argument("--no-validate", action="store_true")
    patch.add_argument("--validate-level", choices=["basic", "full"], default="basic")
    patch.add_argument("--validate-scope", choices=["changed", "patched", "all"], default="changed")

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args, _unknown = parser.parse_known_args(argv)

    if args.command == "backfill":
        from scripts import backfill_nav_history_bulk

        forwarded = _args_to_argv(args, skip={"command"})
        backfill_nav_history_bulk.main(forwarded)
        return 0

    if args.command == "patch":
        from scripts import nav_history_patch

        forwarded = _args_to_argv(args, skip={"command"})
        nav_history_patch.main(forwarded)
        return 0

    parser.error(f"unsupported command: {args.command}")
    return 2


def _args_to_argv(args: argparse.Namespace, *, skip: set[str]) -> list[str]:
    aliases = {"d_from": "from", "d_to": "to"}
    out: list[str] = []
    for key, value in vars(args).items():
        if key in skip:
            continue
        flag_name = aliases.get(key, key).replace("_", "-")
        flag = f"--{flag_name}"
        if key == "limit" and value == 0:
            continue
        if isinstance(value, bool):
            if value:
                out.append(flag)
        elif value is not None:
            out.extend([flag, str(value)])
    return out


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
