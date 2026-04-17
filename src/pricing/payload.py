"""Shared price payload normalization helpers."""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Mapping

from src.time_utils import bj_now_naive

MONEY_QUANT = Decimal("0.01")
RATE_QUANT = Decimal("0.000001")
PCT_QUANT = Decimal("0.01")


def to_decimal(value) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def quantize_money(value) -> float:
    return float(to_decimal(value).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP))


def quantize_rate(value) -> float:
    return float(to_decimal(value).quantize(RATE_QUANT, rounding=ROUND_HALF_UP))


def quantize_pct(value) -> float:
    return float(to_decimal(value).quantize(PCT_QUANT, rounding=ROUND_HALF_UP))


def normalize_price_payload(payload: Mapping) -> dict:
    """Normalize provider price payloads to the project-wide precision contract."""
    result = dict(payload)
    if not result.get("is_from_cache"):
        result.setdefault("fetched_at", bj_now_naive().isoformat())

    for key in ("price", "prev_close", "open", "high", "low", "change", "cny_price"):
        if key in result and result[key] is not None:
            result[key] = quantize_money(result[key])
    if "change_pct" in result and result["change_pct"] is not None:
        result["change_pct"] = quantize_pct(result["change_pct"])
    if "exchange_rate" in result and result["exchange_rate"] is not None:
        result["exchange_rate"] = quantize_rate(result["exchange_rate"])
    return result
