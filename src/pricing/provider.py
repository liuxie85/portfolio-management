"""Provider contract for realtime pricing sources."""
from __future__ import annotations

from typing import Protocol

from .types import PriceRequest, ProviderResult


class PriceProvider(Protocol):
    """Protocol implemented by realtime quote providers."""

    name: str

    def supports(self, request: PriceRequest) -> bool:
        """Return True when this provider can attempt the request."""

    def fetch_one(self, request: PriceRequest) -> ProviderResult:
        """Fetch one realtime quote."""
