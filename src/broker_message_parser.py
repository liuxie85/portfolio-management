"""Broker fill message parser.

Currently supports Futu (HK) fill notifications.

Example:
  成交提醒: 【成交提醒】成功买入20股$富途控股 (FUTU.US)$，成交价格：147，...，2026/03/12 21:59:45 (香港)。【富途证券(香港)】

Outputs a dict suitable for creating a Transaction.

Design goals (harness style):
- deterministic parsing
- explicit failures (do not guess silently)
- idempotency-friendly request_id derived from message content
"""

from __future__ import annotations

import re
import hashlib
from dataclasses import dataclass
from typing import Optional, Dict, Any


@dataclass
class ParsedFill:
    ok: bool
    error: Optional[str] = None
    tx_type: Optional[str] = None  # BUY/SELL
    asset_name: Optional[str] = None
    asset_id: Optional[str] = None
    quantity: Optional[float] = None
    price: Optional[float] = None
    currency: Optional[str] = None
    market: Optional[str] = None
    tx_date: Optional[str] = None  # YYYY-MM-DD
    tx_time: Optional[str] = None  # HH:MM:SS
    request_id: Optional[str] = None
    raw: Optional[str] = None


def _mk_request_id(raw: str) -> str:
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"futu_fill_{h}"


def parse_futu_fill_message(message: str, default_market: str = "富途") -> ParsedFill:
    raw = (message or "").strip()
    if not raw:
        return ParsedFill(ok=False, error="empty message")

    # 买入/卖出 + 数量
    m = re.search(r"成功(买入|卖出)(\d+(?:\.\d+)?)股\$([^$]+?)\$", raw)
    if not m:
        return ParsedFill(ok=False, error="pattern not matched", raw=raw)

    action = m.group(1)
    qty = float(m.group(2))
    asset_chunk = m.group(3)

    # asset chunk like: 富途控股 (FUTU.US)
    m2 = re.search(r"^(.*?)\s*\(([A-Z0-9\.\-]+)\)\s*$", asset_chunk.strip())
    if m2:
        asset_name = m2.group(1).strip()
        asset_id = m2.group(2).strip()
    else:
        asset_name = asset_chunk.strip()
        asset_id = None

    # price
    m3 = re.search(r"成交价格\s*[:：]\s*([0-9]+(?:\.[0-9]+)?)", raw)
    if not m3:
        return ParsedFill(ok=False, error="missing price", raw=raw)
    price = float(m3.group(1))

    # time: 2026/03/12 21:59:45 (香港)
    m4 = re.search(r"(20\d{2})/(\d{2})/(\d{2})\s+(\d{2}:\d{2}:\d{2})\s*\(香港\)", raw)
    tx_date = None
    tx_time = None
    currency = None
    if m4:
        y, mo, d, t = m4.group(1), m4.group(2), m4.group(3), m4.group(4)
        tx_date = f"{y}-{mo}-{d}"
        tx_time = t
        currency = "HKD"  # Futu HK timestamp implies HK channel; price currency depends on asset

    # Prefer currency from ticker suffix if present
    # e.g. FUTU.US -> USD
    if asset_id and asset_id.endswith('.US'):
        currency = 'USD'
    elif asset_id and asset_id.endswith('.HK'):
        currency = 'HKD'

    tx_type = 'BUY' if action == '买入' else 'SELL'

    return ParsedFill(
        ok=True,
        tx_type=tx_type,
        asset_name=asset_name,
        asset_id=asset_id,
        quantity=qty,
        price=price,
        currency=currency or 'HKD',
        market=default_market,
        tx_date=tx_date,
        tx_time=tx_time,
        request_id=_mk_request_id(raw),
        raw=raw,
    )
