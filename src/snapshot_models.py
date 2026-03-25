"""Snapshot models for auditability.

This module intentionally keeps the snapshot schema minimal and stable.
Snapshots are written at NAV record time to make each NAV point reproducible.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field, field_validator

from .models import _quantize_decimal, MONEY_QUANT


class HoldingSnapshot(BaseModel):
    """A per-day holding snapshot row (one asset per row).

    Business key (recommended): (as_of, account, asset_id, market)
    """

    record_id: Optional[str] = None

    as_of: str = Field(..., description="Business date (Asia/Shanghai) as YYYY-MM-DD")
    account: str
    asset_id: str
    market: str = ""

    quantity: float
    currency: str

    # Pricing used for valuation
    price: Optional[float] = None
    cny_price: Optional[float] = None
    market_value_cny: Optional[float] = None

    # Optional metadata
    dedup_key: str
    asset_name: Optional[str] = None
    avg_cost: Optional[float] = None
    source: Optional[str] = None
    remark: Optional[str] = None

    @field_validator('quantity', 'price', 'cny_price', 'market_value_cny', 'avg_cost', mode='before')
    @classmethod
    def _quantize_money_fields(cls, v):
        if v is None:
            return None
        return _quantize_decimal(v, MONEY_QUANT)
