#!/usr/bin/env python3
"""Diagnose pricing for a batch of holdings.

Goal: make price fetching behavior observable.

Outputs:
- per-asset decision: cache hit / expired / realtime fetch / fallback / missing
- per-source counts
- a compact summary that can be pasted into Feishu/PR comments

Notes:
- This script is intentionally lightweight and does not modify any state.
- It relies on existing storage/price_fetcher abstractions.

Usage:
  python scripts/diagnose_pricing.py --account lx
  python scripts/diagnose_pricing.py --account lx --json
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Diagnose pricing (cache vs realtime vs fallback) for current holdings")
    p.add_argument("--account", default="lx", help="Account to diagnose")
    p.add_argument("--json", action="store_true", help="Output JSON")
    p.add_argument("--force-refresh", action="store_true", help="Force refresh prices (ignore cache)")
    p.add_argument("--skip-us", action="store_true", help="Skip US quotes")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # Lazy imports so `--help` works even if dependencies are missing in some environments
    from src.feishu_storage import FeishuStorage
    from src.price_fetcher import PriceFetcher
    from src.time_utils import bj_now

    storage = FeishuStorage()
    fetcher = PriceFetcher(storage=storage)

    holdings = storage.get_holdings(account=args.account)
    codes = [h.asset_id for h in holdings]
    name_map = {h.asset_id: h.asset_name for h in holdings}

    prices = fetcher.fetch_batch(
        codes,
        name_map=name_map,
        force_refresh=args.force_refresh,
        use_concurrent=True,
        skip_us=args.skip_us,
        use_cache_only=False,
    )

    rows = []
    source_counter = Counter()
    state_counter = Counter()

    for h in holdings:
        p = prices.get(h.asset_id)
        if not p:
            state = "missing"
            source = None
        else:
            source = p.get("source")
            is_cache = bool(p.get("is_from_cache"))
            # cache_fallback is a special-case marker used in fetch_batch fallbacks
            is_fallback = source == "cache_fallback" or bool(p.get("is_stale"))
            if is_cache and is_fallback:
                state = "stale_fallback"
            elif is_cache:
                state = "cache"
            else:
                state = "realtime"

        state_counter[state] += 1
        if source:
            source_counter[source] += 1

        rows.append({
            "asset_id": h.asset_id,
            "asset_name": h.asset_name,
            "asset_type": h.asset_type.value if h.asset_type else None,
            "currency": h.currency,
            "state": state,
            "source": source,
            "expires_at": p.get("expires_at") if p else None,
            "fetched_at": p.get("fetched_at") if p else None,
            "price": p.get("price") if p else None,
            "cny_price": p.get("cny_price") if p else None,
        })

    payload = {
        "account": args.account,
        "as_of_bj": bj_now().isoformat(),
        "summary": {
            "total": len(holdings),
            **state_counter,
        },
        "sources": dict(source_counter),
        "rows": rows,
    }

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    # text output
    print(f"[pricing-diagnose] account={args.account} as_of={payload['as_of_bj']}")
    print("summary:", payload["summary"])
    if payload["sources"]:
        print("sources:", payload["sources"])
    missing = [r for r in rows if r["state"] == "missing"]
    if missing:
        print("missing:")
        for r in missing[:20]:
            print(f"  - {r['asset_id']} {r['asset_name']}")


if __name__ == "__main__":
    main()
