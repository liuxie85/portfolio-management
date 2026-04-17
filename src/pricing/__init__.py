"""Pricing service package.

The legacy public entrypoint remains ``src.price_fetcher.PriceFetcher``.
New provider-based code lives here and is wired in gradually.
"""

from .provider import PriceProvider
from .service import PriceService
from .types import PriceRequest, ProviderResult

__all__ = ["PriceProvider", "PriceRequest", "ProviderResult", "PriceService"]
