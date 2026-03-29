"""Regression tests for PriceFetcher.fetch() cache-only + stale fallback semantics.

We intentionally avoid pytest dependency; tests are called from tests/run_tests.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta


@dataclass
class _StubStorage:
    """Minimal storage stub for PriceFetcher.

    Only implements get_price/save_price used by PriceFetcher.fetch().
    """

    cached: object
    saved: object = None

    def get_price(self, asset_id: str, *, allow_expired: bool = False, max_stale_after_expiry_sec: int = 0):
        # The stale-window filtering happens in LocalPriceCache/FeishuStorage.
        # For fetch() semantics tests, we return the cached object as-is.
        return self.cached

    def save_price(self, price):
        self.saved = price


def test_single_fetch_cache_only_and_stale_fallback() -> None:
    """fetch() should return cache_fallback payload when:

    - cached record exists but is expired
    - accept_stale_when_closed=True and within stale window
    - use_cache_only=True OR realtime fetch fails
    """

    from src.price_fetcher import PriceFetcher
    from src.models import PriceCache, AssetType
    from src.time_utils import bj_now_naive

    expired = PriceCache(
        asset_id="TEST.US",
        asset_name="Test",
        asset_type=AssetType.US_STOCK,
        price=1.0,
        currency="USD",
        cny_price=7.0,
        expires_at=bj_now_naive() - timedelta(hours=1),
    )

    storage = _StubStorage(expired)
    pf = PriceFetcher(storage=storage, use_cache=True)

    # Force realtime path to fail.
    pf._fetch_realtime = lambda code, asset_name=None: None

    # cache-only mode should return stale fallback.
    r1 = pf.fetch(
        "TEST.US",
        "Test",
        accept_stale_when_closed=True,
        max_stale_after_expiry_sec=7200,
        use_cache_only=True,
    )
    assert isinstance(r1, dict)
    assert r1.get("is_from_cache") is True
    assert r1.get("is_stale") is True
    assert r1.get("source") == "cache_fallback"

    # non-cache-only should try realtime, then fallback to stale cache when realtime fails.
    r2 = pf.fetch(
        "TEST.US",
        "Test",
        accept_stale_when_closed=True,
        max_stale_after_expiry_sec=7200,
        use_cache_only=False,
    )
    assert isinstance(r2, dict)
    assert r2.get("is_from_cache") is True
    assert r2.get("is_stale") is True
    assert r2.get("source") == "cache_fallback"
