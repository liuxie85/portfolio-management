"""Minimal (no pytest) test for daily report top_holdings merge behavior."""

from __future__ import annotations

from types import SimpleNamespace

from skill_api import PortfolioSkill


def test_full_report_top_holdings_merge_duplicates_and_cash_mmf_bucket():
    skill = PortfolioSkill(account="lx")

    holdings = [
        {"code": "AAPL", "name": "Apple", "quantity": 1, "type": "us_stock", "normalized_type": "stock", "market": "futu", "currency": "USD", "market_value": 100.0},
        {"code": "AAPL", "name": "Apple", "quantity": 2, "type": "us_stock", "normalized_type": "stock", "market": "ib", "currency": "USD", "market_value": 200.0},
        {"code": "00700", "name": "腾讯控股", "quantity": 100, "type": "hk_stock", "normalized_type": "stock", "market": "futu", "currency": "HKD", "market_value": 150.0},
        {"code": "00700", "name": "腾讯控股", "quantity": 20, "type": "hk_stock", "normalized_type": "stock", "market": "pingan", "currency": "HKD", "market_value": 50.0},
        {"code": "CNY-CASH", "name": "人民币现金", "quantity": 80, "type": "cash", "normalized_type": "cash", "market": "futu", "currency": "CNY", "market_value": 80.0},
        {"code": "USD-CASH", "name": "美元现金", "quantity": 20, "type": "cash", "normalized_type": "cash", "market": "ib", "currency": "USD", "market_value": 20.0},
        {"code": "CNY-MMF", "name": "货基", "quantity": 50, "type": "mmf", "normalized_type": "cash", "market": "pingan", "currency": "CNY", "market_value": 50.0},
    ]

    total_value = 650.0
    snapshot = {
        "snapshot_time": "2026-03-29T10:00:00",
        "valuation": SimpleNamespace(
            total_value_cny=total_value,
            cash_value_cny=150.0,
            stock_value_cny=500.0,
            fund_value_cny=0.0,
            cn_asset_value=0.0,
            us_asset_value=300.0,
            hk_asset_value=200.0,
        ),
        "holdings_data": {
            "success": True,
            "holdings": holdings,
            "count": len(holdings),
            "total_value": total_value,
            "cash_value": 150.0,
            "stock_value": 500.0,
            "cash_ratio": 150.0 / total_value,
            "warnings": [],
        },
        "position_data": {
            "cash_ratio": 150.0 / total_value,
            "stock_ratio": 500.0 / total_value,
            "fund_ratio": 0.0,
        },
    }

    full = skill.full_report(snapshot=snapshot, navs=[])

    assert full.get("success") is True
    top = full.get("top_holdings") or []
    assert len(top) == 3, top

    by_code = {row.get("code"): row for row in top}

    aapl = by_code.get("AAPL")
    assert aapl is not None
    assert abs(float(aapl["market_value"]) - 300.0) < 1e-9
    assert abs(float(aapl["weight"]) - (300.0 / total_value)) < 1e-9

    tencent = by_code.get("00700")
    assert tencent is not None
    assert abs(float(tencent["market_value"]) - 200.0) < 1e-9
    assert abs(float(tencent["weight"]) - (200.0 / total_value)) < 1e-9

    cash_row = next((r for r in top if r.get("normalized_type") == "cash"), None)
    assert cash_row is not None
    assert abs(float(cash_row["market_value"]) - 150.0) < 1e-9
    assert abs(float(cash_row["weight"]) - (150.0 / total_value)) < 1e-9
    assert cash_row.get("code") == "CASH+MMF"
