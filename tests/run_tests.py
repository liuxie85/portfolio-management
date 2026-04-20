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
    from tests.test_daily_top_holdings_merge_minimal import (
        test_full_report_top_holdings_merge_duplicates_and_cash_mmf_bucket,
    )
    from tests.test_audit_fixes import (
        test_round_none_guard_in_nav_record_fields,
        test_zero_is_not_none_in_truthiness_check,
        test_sort_with_none_dates_uses_date_min,
        test_deduct_cash_prevalidates_insufficient_funds,
        test_deduct_cash_succeeds_when_sufficient,
        test_nav_calculator_warns_when_shares_zero_but_value_positive,
        test_name_update_compares_content_not_length,
        test_del_does_not_raise,
        test_singleton_lock_exists,
        test_rate_limiter_has_lock,
        test_prev_close_not_overwritten_when_valid,
        test_escape_filter_value_handles_quotes,
    )
    from tests.test_feishu_efficiency import (
        test_get_holdings_uses_cache_when_loaded,
        test_get_holdings_includes_empty_when_requested,
        test_get_holdings_falls_through_when_cache_not_loaded,
        test_get_holdings_with_asset_type_bypasses_cache,
        test_get_transactions_pushes_date_filter_to_server,
        test_get_total_cash_flow_cny_uses_agg_cache,
    )
    from tests.test_pm_cli import (
        test_pm_report_requires_preview_flag,
        test_pm_report_preview_marks_noncanonical_output,
        test_pm_cash_passes_account,
        test_pm_init_nav_passes_account_and_write_flags,
        test_pm_init_nav_write_requires_confirm,
    )
    from tests.test_daily_report_entrypoints import (
        test_generate_daily_report_html_is_renderer_only,
        test_publish_daily_report_returns_renderer_bundle_shape,
        test_publish_daily_report_build_report_data_passes_account,
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
        test_full_report_top_holdings_merge_duplicates_and_cash_mmf_bucket,
        # audit fix regression tests
        test_round_none_guard_in_nav_record_fields,
        test_zero_is_not_none_in_truthiness_check,
        test_sort_with_none_dates_uses_date_min,
        test_deduct_cash_prevalidates_insufficient_funds,
        test_deduct_cash_succeeds_when_sufficient,
        test_nav_calculator_warns_when_shares_zero_but_value_positive,
        test_name_update_compares_content_not_length,
        test_del_does_not_raise,
        test_singleton_lock_exists,
        test_rate_limiter_has_lock,
        test_prev_close_not_overwritten_when_valid,
        test_escape_filter_value_handles_quotes,
        # feishu efficiency tests
        test_get_holdings_uses_cache_when_loaded,
        test_get_holdings_includes_empty_when_requested,
        test_get_holdings_falls_through_when_cache_not_loaded,
        test_get_holdings_with_asset_type_bypasses_cache,
        test_get_transactions_pushes_date_filter_to_server,
        test_get_total_cash_flow_cny_uses_agg_cache,
        # CLI / entrypoint account coverage
        test_pm_report_requires_preview_flag,
        test_pm_report_preview_marks_noncanonical_output,
        test_pm_cash_passes_account,
        test_pm_init_nav_passes_account_and_write_flags,
        test_pm_init_nav_write_requires_confirm,
        test_generate_daily_report_html_is_renderer_only,
        test_publish_daily_report_returns_renderer_bundle_shape,
        test_publish_daily_report_build_report_data_passes_account,
    ]
    for t in tests:
        t()
    print(f"OK ({len(tests)} tests)")


if __name__ == "__main__":
    main()
