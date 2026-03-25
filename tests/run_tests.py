#!/usr/bin/env python3
"""Minimal test runner (no pytest dependency).

Usage:
  . .venv/bin/activate
  python tests/run_tests.py
"""

from __future__ import annotations


def test_currency_from_us_ticker_suffix():
    from src.broker_message_parser import parse_futu_fill_message
    msg = "成交提醒: 【成交提醒】成功买入20股$富途控股 (FUTU.US)$，成交价格：147，此笔订单委托已全部成交，2026/03/12 21:59:45 (香港)。【富途证券(香港)】"
    p = parse_futu_fill_message(msg)
    assert p.ok
    assert p.currency == "USD"
    assert "currency_reason=ticker_suffix:.US" in p.raw


def test_currency_from_hk_ticker_suffix():
    from src.broker_message_parser import parse_futu_fill_message
    msg = "成交提醒: 【成交提醒】成功卖出200股$腾讯控股 (00700.HK)$，成交价格：610，此笔订单委托已全部成交，2025/11/27 14:42:11 (香港)。【富途证券(香港)】"
    p = parse_futu_fill_message(msg)
    assert p.ok
    assert p.currency == "HKD"
    assert "currency_reason=ticker_suffix:.HK" in p.raw


def test_currency_fallback_venue_hint():
    from src.broker_message_parser import parse_futu_fill_message
    msg = "成交提醒: 【成交提醒】成功买入10股$某未知标的$，成交价格：10，此笔订单委托已全部成交，2026/03/12 21:59:45 (香港)。【富途证券(香港)】"
    p = parse_futu_fill_message(msg)
    assert p.ok
    assert p.currency == "HKD"
    assert "currency_reason=venue_hint:HK" in p.raw


def main() -> None:
    tests = [
        test_currency_from_us_ticker_suffix,
        test_currency_from_hk_ticker_suffix,
        test_currency_fallback_venue_hint,
    ]
    for t in tests:
        t()
    print(f"OK ({len(tests)} tests)")


if __name__ == "__main__":
    main()
