from __future__ import annotations

from src.price_fetcher import PriceFetcher
from src.pricing.classifier import (
    get_exchange_prefix,
    get_type_hints_from_name,
    is_etf,
    is_otc_fund,
    normalize_code_with_name,
)


def test_pricing_classifier_matches_legacy_price_fetcher_wrappers():
    fetcher = PriceFetcher(storage=None, use_cache=False)

    assert normalize_code_with_name("600519", "贵州茅台股份") == fetcher._normalize_code_with_name("600519", "贵州茅台股份")
    assert get_type_hints_from_name("华夏成长混合基金") == fetcher._get_type_hints_from_name("华夏成长混合基金")
    assert is_etf("510300") == fetcher._is_etf("510300")
    assert is_otc_fund("004001") == fetcher._is_otc_fund("004001")
    assert get_exchange_prefix("510300") == fetcher._get_exchange_prefix("510300")


def test_pricing_classifier_keeps_ambiguous_a_stock_codes_out_of_funds():
    assert is_otc_fund("000001") is False
    assert is_otc_fund("300750") is False
    assert is_otc_fund("004001") is True
    assert is_etf("510300") is True
