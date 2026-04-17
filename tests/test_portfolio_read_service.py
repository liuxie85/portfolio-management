from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

from src.app.portfolio_read_service import PortfolioReadService
from src.models import AssetType, Holding


def test_get_holdings_with_price_reuses_snapshot_contract():
    valuation = SimpleNamespace(
        holdings=[
            SimpleNamespace(
                asset_id="AAPL",
                asset_name="Apple",
                quantity=1,
                asset_type=AssetType.US_STOCK,
                market="富途",
                currency="USD",
                current_price=100,
                cny_price=720,
                market_value_cny=720,
                weight=0.9,
            ),
            SimpleNamespace(
                asset_id="CNY-CASH",
                asset_name="人民币现金",
                quantity=80,
                asset_type=AssetType.CASH,
                market="富途",
                currency="CNY",
                current_price=1,
                cny_price=1,
                market_value_cny=80,
                weight=0.1,
            ),
        ],
        total_value_cny=800,
        cash_value_cny=80,
        stock_value_cny=720,
        fund_value_cny=0,
        cash_ratio=0.1,
        stock_ratio=0.9,
        fund_ratio=0,
        warnings=["w"],
    )
    portfolio = SimpleNamespace(calculate_valuation=Mock(return_value=valuation))
    storage = Mock()
    service = PortfolioReadService(
        account="lx",
        storage=storage,
        portfolio=portfolio,
        reporting_service=Mock(),
    )

    result = service.get_holdings(include_price=True, include_cash=False, group_by_market=True)

    portfolio.calculate_valuation.assert_called_once_with("lx")
    storage.get_holdings.assert_not_called()
    assert result["total_value"] == 800
    assert result["cash_value"] == 80
    assert result["warnings"] == ["w"]
    assert list(result["by_market"]) == ["富途"]
    assert result["market_values"] == {"富途": 720}
    assert [h["code"] for h in result["by_market"]["富途"]] == ["AAPL"]


def test_get_holdings_without_price_keeps_light_storage_read():
    portfolio = SimpleNamespace(calculate_valuation=Mock())
    storage = SimpleNamespace(
        get_holdings=Mock(return_value=[
            Holding(
                asset_id="CNY-CASH",
                asset_name="人民币现金",
                asset_type=AssetType.CASH,
                account="lx",
                market="富途",
                quantity=10,
                currency="CNY",
            ),
            Holding(
                asset_id="AAPL",
                asset_name="Apple",
                asset_type=AssetType.US_STOCK,
                account="lx",
                market="富途",
                quantity=1,
                currency="USD",
            ),
        ])
    )
    service = PortfolioReadService(
        account="lx",
        storage=storage,
        portfolio=portfolio,
        reporting_service=Mock(),
    )

    result = service.get_holdings(include_price=False, include_cash=False)

    portfolio.calculate_valuation.assert_not_called()
    storage.get_holdings.assert_called_once_with(account="lx")
    assert result == {
        "success": True,
        "count": 1,
        "holdings": [
            {
                "code": "AAPL",
                "name": "Apple",
                "quantity": 1.0,
                "type": "us_stock",
                "normalized_type": "stock",
                "market": "富途",
                "currency": "USD",
            }
        ],
    }


def test_get_position_delegates_to_reporting_service_snapshot():
    snapshot = {"valuation": object(), "holdings_data": {}}
    service = PortfolioReadService(
        account="lx",
        storage=Mock(),
        portfolio=SimpleNamespace(calculate_valuation=Mock()),
        reporting_service=Mock(),
    )
    service.build_snapshot = Mock(return_value=snapshot)
    service.reporting_service.build_position.return_value = {"success": True, "total_value": 1}

    result = service.get_position()

    assert result == {"success": True, "total_value": 1}
    service.build_snapshot.assert_called_once_with()
    service.reporting_service.build_position.assert_called_once_with(snapshot)


def test_get_distribution_accepts_prebuilt_holdings_data_without_refetch():
    holdings_data = {
        "success": True,
        "holdings": [{"code": "AAPL", "normalized_type": "stock", "market_value": 1}],
        "total_value": 1,
    }
    service = PortfolioReadService(
        account="lx",
        storage=Mock(),
        portfolio=SimpleNamespace(calculate_valuation=Mock()),
        reporting_service=Mock(),
    )
    service.build_snapshot = Mock()
    service.reporting_service.build_distribution.return_value = {"success": True, "total_value": 1}

    result = service.get_distribution(holdings_data=holdings_data)

    assert result == {"success": True, "total_value": 1}
    service.build_snapshot.assert_not_called()
    service.reporting_service.build_distribution.assert_called_once_with({"holdings_data": holdings_data})
