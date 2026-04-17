"""Provider-based realtime pricing service."""
from __future__ import annotations

from typing import Iterable, List, Optional

from .provider import PriceProvider
from .types import PriceRequest


class PriceService:
    """Coordinate realtime providers and keep diagnostic metadata."""

    def __init__(self, providers: Iterable[PriceProvider]):
        self.providers: List[PriceProvider] = list(providers)
        self.last_diagnostics: list[dict] = []

    @classmethod
    def for_legacy_fetcher(cls, fetcher) -> "PriceService":
        from .providers import CNStockProvider, ETFProvider, FundProvider, HKStockProvider, USStockProvider

        return cls(
            [
                ETFProvider(fetcher),
                FundProvider(fetcher),
                CNStockProvider(fetcher),
                HKStockProvider(fetcher),
                USStockProvider(fetcher),
            ]
        )

    def fetch_realtime(self, request: PriceRequest) -> Optional[dict]:
        self.last_diagnostics = []
        for provider in self.providers:
            if not provider.supports(request):
                continue

            result = provider.fetch_one(request)
            self.last_diagnostics.append(
                {
                    "provider": result.provider,
                    "ok": result.ok,
                    "error": result.error,
                    "latency_ms": result.latency_ms,
                }
            )
            if result.ok:
                payload = dict(result.payload or {})
                payload.setdefault("provider", result.provider)
                payload.setdefault("source_chain", [d["provider"] for d in self.last_diagnostics])
                return payload

        return None
