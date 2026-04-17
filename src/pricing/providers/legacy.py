"""Adapter that exposes the legacy PriceFetcher source methods as a provider.

This is intentionally thin: it lets us introduce PriceService without moving
the  existing source-specific methods in one risky change.
"""
from __future__ import annotations

import time
from typing import Optional

from ..classifier import is_etf, is_otc_fund
from ..types import PriceRequest, ProviderResult


class LegacyRoutingProvider:
    """Route requests to the existing ``PriceFetcher._fetch_*`` methods."""

    name = "legacy-routing"

    def __init__(self, fetcher):
        self.fetcher = fetcher

    def supports(self, request: PriceRequest) -> bool:
        return True

    def fetch_one(self, request: PriceRequest) -> ProviderResult:
        started = time.time()
        code = request.normalized_code or request.code
        try:
            payload = self._fetch_by_legacy_rules(code, request)
            return ProviderResult(
                payload=payload,
                provider=self.name,
                latency_ms=int((time.time() - started) * 1000),
            )
        except Exception as exc:
            return ProviderResult(
                payload=None,
                provider=self.name,
                error=f"{type(exc).__name__}: {exc}",
                latency_ms=int((time.time() - started) * 1000),
            )

    def _fetch_by_legacy_rules(self, code: str, request: PriceRequest) -> Optional[dict]:
        hints = request.hints or {}

        if is_etf(code):
            return self.fetcher._fetch_etf(code)

        if code.startswith(("SH", "SZ")) or (
            code.isdigit()
            and len(code) == 6
            and code.startswith(("6", "0", "3", "1", "2"))
        ):
            is_likely_fund = hints.get("is_fund", False) or is_otc_fund(code)
            if is_likely_fund and not hints.get("is_stock", False):
                return self.fetcher._fetch_fund(code)
            return self.fetcher._fetch_a_stock(code)

        if code.startswith("HK") or (code.isdigit() and 4 <= len(code) <= 5):
            return self.fetcher._fetch_hk_stock(code)

        return self.fetcher._fetch_us_stock(code)
