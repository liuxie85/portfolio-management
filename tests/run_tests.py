#!/usr/bin/env python3
"""Minimal test runner (no pytest dependency).

Usage:
  . .venv/bin/activate
  python tests/run_tests.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Ensure repo root is on sys.path so `import src.*` works when running `python tests/run_tests.py`.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def test_currency_from_us_ticker_suffix():
    from src.broker_message_parser import parse_futu_fill_message
    msg = "成交提醒: 【成交提醒】成功买入20股$富途控股 (FUTU.US)$，成交价格：147，此笔订单委托已全部成交，2026/03/12 21:59:45 (香港)。【富途证券(香港)】"
    p = parse_futu_fill_message(msg)
    assert p.ok
    assert p.currency == "USD"
    assert "currency_reason=ticker_suffix:.US" in p.raw


def test_currency_from_hk_ticker_suffix():
    from src.broker_message_parser import parse_futu_fill_message
    msg = "成交提醒: 【成交提醒】成功卖出200股$腾讯控股 (00700.HK)$，成交价格：610，此笔订单委托已全部成交，2025/11/27 14:42:11 (香港)。【富途证券(香港)】"
    p = parse_futu_fill_message(msg)
    assert p.ok
    assert p.currency == "HKD"
    assert "currency_reason=ticker_suffix:.HK" in p.raw


def test_currency_fallback_venue_hint():
    from src.broker_message_parser import parse_futu_fill_message
    msg = "成交提醒: 【成交提醒】成功买入10股$某未知标的$，成交价格：10，此笔订单委托已全部成交，2026/03/12 21:59:45 (香港)。【富途证券(香港)】"
    p = parse_futu_fill_message(msg)
    assert p.ok
    assert p.currency == "HKD"
    assert "currency_reason=venue_hint:HK" in p.raw


def main() -> None:
    from tests.test_asset_utils_market_suffix import (
        test_validate_code_strips_market_suffix_and_normalizes_hk,
        test_detect_market_type_respects_suffix,
    )
    from tests.test_price_fetcher_single_fetch_cache_only import (
        test_single_fetch_cache_only_and_stale_fallback,
    )
    from tests.test_holdings_preload_minimal import (
        test_preload_builds_index_and_projection_and_avoids_refetch,
        test_upsert_uses_preloaded_cache_for_batch_updates,
        test_upsert_create_after_preload_missing_key_without_refetch,
    )
    from tests.test_holdings_bulk_upsert_minimal import (
        test_bulk_upsert_additive_preloads_once_per_account_and_batches_updates,
        test_bulk_upsert_replace_mixed_update_create_updates_caches,
    )
    from tests.test_nav_cashflow_perf_minimal import (
        test_nav_base_cache_month_boundary_and_invalidation_flag,
        test_cash_flow_agg_cache_updates_on_new_record,
        test_record_nav_avoids_get_nav_history_full_scan_when_preloaded,
    )
    from tests.test_nav_bulk_upsert_minimal import (
        test_nav_bulk_upsert_uses_single_preload_and_batch_ops_for_n_le_500,
        test_nav_bulk_upsert_upsert_mode_keeps_existing_cache_values_for_none_fields,
        test_nav_bulk_upsert_updates_nav_index_cache_incrementally,
    )

    tests = [
        test_currency_from_us_ticker_suffix,
        test_currency_from_hk_ticker_suffix,
        test_currency_fallback_venue_hint,
        test_validate_code_strips_market_suffix_and_normalizes_hk,
        test_detect_market_type_respects_suffix,
        test_single_fetch_cache_only_and_stale_fallback,
        test_preload_builds_index_and_projection_and_avoids_refetch,
        test_upsert_uses_preloaded_cache_for_batch_updates,
        test_upsert_create_after_preload_missing_key_without_refetch,
        test_bulk_upsert_additive_preloads_once_per_account_and_batches_updates,
        test_bulk_upsert_replace_mixed_update_create_updates_caches,
        test_nav_base_cache_month_boundary_and_invalidation_flag,
        test_cash_flow_agg_cache_updates_on_new_record,
        test_record_nav_avoids_get_nav_history_full_scan_when_preloaded,
        test_nav_bulk_upsert_uses_single_preload_and_batch_ops_for_n_le_500,
        test_nav_bulk_upsert_upsert_mode_keeps_existing_cache_values_for_none_fields,
        test_nav_bulk_upsert_updates_nav_index_cache_incrementally,
    ]
    for t in tests:
        t()
    print(f"OK ({len(tests)} tests)")


if __name__ == "__main__":
    main()
