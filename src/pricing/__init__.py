"""Pricing service package.

The legacy public entrypoint remains ``src.price_fetcher.PriceFetcher``.
New provider-based code lives here and is wired in gradually.
"""

from .provider import PriceProvider
from .service import PriceService
from .types import PriceRequest, ProviderResult
from .classifier import get_type_hints_from_name, is_etf, is_otc_fund, normalize_code_with_name
from .payload import normalize_price_payload, quantize_money, quantize_pct, quantize_rate, to_decimal

__all__ = [
    "PriceProvider",
    "PriceRequest",
    "ProviderResult",
    "PriceService",
    "get_type_hints_from_name",
    "is_etf",
    "is_otc_fund",
    "normalize_code_with_name",
    "normalize_price_payload",
    "quantize_money",
    "quantize_pct",
    "quantize_rate",
    "to_decimal",
]
