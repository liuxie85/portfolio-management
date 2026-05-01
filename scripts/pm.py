#!/usr/bin/env python3
"""portfolio-management CLI (thin wrapper around skill_api).

Design goals:
- Provide a few common read-only commands.
- Prefer the local HTTP service, with direct skill_api fallback.
- Fast defaults (no writes; avoid slow realtime price fetch unless asked).
- Human-readable by default; `--json` for automation.

Usage examples:
  . .venv/bin/activate
  python scripts/pm.py cash
  python scripts/pm.py cash --account alice
  python scripts/pm.py accounts
  python scripts/pm.py overview --accounts alice,bob --json
  python scripts/pm.py holdings
  python scripts/pm.py holdings --include-price --timeout 25
  python scripts/pm.py nav
  python scripts/pm.py report daily --preview
  python scripts/pm.py report daily --preview --timeout 25 --json

Safety:
- This CLI intentionally does NOT expose write paths by default.
- `report` is preview-only. Official daily data/HTML publishing must use
  `scripts/publish_daily_report.py`.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import json
import sys
from pathlib import Path

# Ensure repo root is on sys.path so `import skill_api` works.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))




def suppress_internal(enabled: bool):
    if not enabled:
        return contextlib.nullcontext()
    return contextlib.redirect_stdout(open(os.devnull, 'w'))
def _dump(obj, as_json: bool):
    if as_json:
        print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))
    else:
        # simple human-readable
        if isinstance(obj, dict):
            print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))
        else:
            print(obj)


def _service_or_fallback(args, service_call, fallback_call):
    if not bool(getattr(args, "no_service", False)):
        from src.service.client import PortfolioServiceClient, PortfolioServiceUnavailable

        try:
            client = PortfolioServiceClient(
                base_url=getattr(args, "service_url", None),
                timeout=float(getattr(args, "service_timeout", 0.5)),
            )
            return service_call(client)
        except PortfolioServiceUnavailable:
            if bool(getattr(args, "require_service", False)):
                raise SystemExit("local service is unavailable and --require-service was set")
            pass
    return fallback_call()


def _default_account(account):
    if account:
        return account
    from src import config

    return config.get_account()


def cmd_holdings(args):
    def via_service(client):
        return client.get_holdings(
            account=_default_account(args.account),
            include_price=bool(args.include_price),
        )

    def direct():
        from skill_api import get_holdings

        return get_holdings(include_price=bool(args.include_price), account=args.account)

    res = _service_or_fallback(args, via_service, direct)
    _dump(res, args.json)


def cmd_cash(args):
    def via_service(client):
        return client.get_cash(account=_default_account(args.account))

    def direct():
        from skill_api import get_cash

        return get_cash(account=args.account)

    res = _service_or_fallback(args, via_service, direct)
    _dump(res, args.json)


def cmd_accounts(args):
    def via_service(client):
        return client.list_accounts(include_default=not bool(args.exclude_default))

    def direct():
        from skill_api import list_accounts

        return list_accounts(include_default=not bool(args.exclude_default))

    res = _service_or_fallback(args, via_service, direct)
    _dump(res, args.json)


def cmd_overview(args):
    def via_service(client):
        return client.multi_account_overview(
            accounts=args.accounts,
            price_timeout=args.timeout,
            include_details=bool(args.details),
        )

    def direct():
        from skill_api import multi_account_overview

        return multi_account_overview(
            accounts=args.accounts,
            price_timeout=args.timeout,
            include_details=bool(args.details),
        )

    res = _service_or_fallback(args, via_service, direct)
    _dump(res, args.json)


def cmd_nav(args):
    def via_service(client):
        return client.get_nav(account=_default_account(args.account))

    def direct():
        from skill_api import get_nav

        return get_nav(account=args.account)

    res = _service_or_fallback(args, via_service, direct)
    _dump(res, args.json)


def cmd_report(args):
    if not bool(args.preview):
        raise SystemExit(
            "pm report is preview-only. Re-run with --preview, or use "
            "scripts/publish_daily_report.py for the official daily report."
        )

    def via_service(client):
        return client.generate_report(
            account=_default_account(args.account),
            report_type=args.type,
            price_timeout=args.timeout,
        )

    def direct():
        from skill_api import generate_report

        return generate_report(
            report_type=args.type,
            record_nav=False,
            price_timeout=args.timeout,
            account=args.account,
        )

    res = _service_or_fallback(args, via_service, direct)
    if isinstance(res, dict):
        res.setdefault("preview_only", True)
        res.setdefault("canonical_entrypoint", "scripts/publish_daily_report.py")
    _dump(res, args.json)


def cmd_init_nav(args):
    if not bool(args.confirm) and not bool(args.dry_run):
        raise SystemExit("init-nav write requires --confirm. Re-run with --dry-run or add --confirm.")

    from skill_api import init_nav_history

    res = init_nav_history(
        date_str=args.date,
        price_timeout=args.timeout,
        dry_run=bool(args.dry_run),
        confirm=bool(args.confirm),
        use_bulk_persist=bool(args.use_bulk_persist),
        account=args.account,
    )
    _dump(res, args.json)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pm", description="portfolio-management CLI")
    p.add_argument("--json", action="store_true", help="output JSON")
    p.add_argument("--account", default=None, help="account to operate on; defaults to config/PORTFOLIO_ACCOUNT")
    p.add_argument("--service-url", default=None, help="local service URL; defaults to config/PORTFOLIO_SERVICE_URL")
    p.add_argument("--service-timeout", type=float, default=0.5, help="local service timeout seconds before fallback")
    p.add_argument("--no-service", action="store_true", help="bypass local service and call skill_api directly")
    p.add_argument("--require-service", action="store_true", help="fail instead of falling back when local service is unavailable")
    p.add_argument("--debug-internal", action="store_true", help="Do not suppress internal stdout prints (debug only).")

    sp = p.add_subparsers(dest="cmd", required=True)

    # Allow putting global flags after the subcommand (e.g. `pm cash --json`).
    # argparse doesn't support this natively; we implement it by also adding --json
    # to each subparser.
    def add_service_args(subparser):
        subparser.add_argument("--service-url", default=argparse.SUPPRESS, help="local service URL")
        subparser.add_argument("--service-timeout", type=float, default=argparse.SUPPRESS, help="local service timeout seconds before fallback")
        subparser.add_argument("--no-service", action="store_true", default=argparse.SUPPRESS, help="bypass local service and call skill_api directly")
        subparser.add_argument("--require-service", action="store_true", default=argparse.SUPPRESS, help="fail instead of falling back when local service is unavailable")

    p_hold = sp.add_parser("holdings", help="list holdings")
    p_hold.add_argument("--include-price", action="store_true", help="include price fields (may be slow)")
    p_hold.add_argument("--account", default=None, help="account to operate on; defaults to config/PORTFOLIO_ACCOUNT")
    p_hold.add_argument("--json", action="store_true", help="output JSON")
    add_service_args(p_hold)
    p_hold.set_defaults(func=cmd_holdings)

    p_cash = sp.add_parser("cash", help="show cash positions")
    p_cash.add_argument("--account", default=None, help="account to operate on; defaults to config/PORTFOLIO_ACCOUNT")
    p_cash.add_argument("--json", action="store_true", help="output JSON")
    add_service_args(p_cash)
    p_cash.set_defaults(func=cmd_cash)

    p_accounts = sp.add_parser("accounts", help="list discovered accounts")
    p_accounts.add_argument("--exclude-default", action="store_true", help="do not include the configured default account when it has no data")
    p_accounts.add_argument("--json", action="store_true", help="output JSON")
    add_service_args(p_accounts)
    p_accounts.set_defaults(func=cmd_accounts)

    p_overview = sp.add_parser("overview", help="show read-only multi-account overview")
    p_overview.add_argument("--accounts", default=None, help="comma-separated accounts; defaults to discovered accounts")
    p_overview.add_argument("--timeout", type=int, default=30, help="price timeout seconds (default 30)")
    p_overview.add_argument("--details", action="store_true", help="include each account's full report payload")
    p_overview.add_argument("--json", action="store_true", help="output JSON")
    add_service_args(p_overview)
    p_overview.set_defaults(func=cmd_overview)

    p_nav = sp.add_parser("nav", help="show latest nav")
    p_nav.add_argument("--account", default=None, help="account to operate on; defaults to config/PORTFOLIO_ACCOUNT")
    p_nav.add_argument("--json", action="store_true", help="output JSON")
    add_service_args(p_nav)
    p_nav.set_defaults(func=cmd_nav)

    p_rep = sp.add_parser("report", help="preview report data (read-only; not the official daily entry)")
    p_rep.add_argument("type", choices=["daily", "monthly", "yearly"], help="report type")
    p_rep.add_argument("--preview", action="store_true", help="acknowledge this command is preview-only")
    p_rep.add_argument("--timeout", type=int, default=30, help="price timeout seconds (default 30)")
    p_rep.add_argument("--account", default=None, help="account to operate on; defaults to config/PORTFOLIO_ACCOUNT")
    p_rep.add_argument("--json", action="store_true", help="output JSON")
    add_service_args(p_rep)
    p_rep.set_defaults(func=cmd_report)

    p_init_nav = sp.add_parser("init-nav", help="initialize first nav_history row for a new account")
    p_init_nav.add_argument("--date", default=None, help="nav date (YYYY-MM-DD); defaults to today")
    p_init_nav.add_argument("--timeout", type=int, default=30, help="price timeout seconds (default 30)")
    p_init_nav.add_argument("--dry-run", action="store_true", default=True, help="preview only (default)")
    p_init_nav.add_argument("--write", dest="dry_run", action="store_false", help="actually write nav_history")
    p_init_nav.add_argument("--confirm", action="store_true", help="required with --write")
    p_init_nav.add_argument("--use-bulk-persist", action="store_true", help="use nav_history bulk upsert path")
    p_init_nav.add_argument("--account", default=None, help="account to operate on; defaults to config/PORTFOLIO_ACCOUNT")
    p_init_nav.add_argument("--json", action="store_true", help="output JSON")
    p_init_nav.set_defaults(func=cmd_init_nav)

    return p


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.func(args)
        return 0
    except Exception as exc:
        from src.service.client import PortfolioServiceError

        if isinstance(exc, PortfolioServiceError):
            raise SystemExit(str(exc)) from exc
        raise
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
