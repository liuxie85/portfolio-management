from unittest.mock import Mock

from src.app.reporting_service import ReportingService
from src.models import AssetType, Holding, Industry, PortfolioValuation
from src.portfolio import PortfolioManager


def test_reporting_service_asset_distribution_uses_manager_valuation():
    storage = Mock()
    manager = Mock()
    manager.calculate_valuation.return_value = PortfolioValuation(
        account="a",
        total_value_cny=200.0,
        cash_value_cny=50.0,
        stock_value_cny=100.0,
        fund_value_cny=50.0,
        cn_asset_value=100.0,
        us_asset_value=75.0,
        hk_asset_value=25.0,
    )
    service = ReportingService(manager=manager, storage=storage)

    result = service.get_asset_distribution("a")

    assert result == {
        "现金": 0.25,
        "股票": 0.5,
        "基金": 0.25,
        "中国资产": 0.5,
        "美国资产": 0.375,
        "港股资产": 0.125,
    }


def test_reporting_service_asset_distribution_returns_empty_for_zero_value():
    storage = Mock()
    manager = Mock()
    manager.calculate_valuation.return_value = PortfolioValuation(account="a", total_value_cny=0.0)
    service = ReportingService(manager=manager, storage=storage)

    assert service.get_asset_distribution("a") == {}


def test_reporting_service_industry_distribution_with_price_and_cny_fallback():
    storage = Mock()
    manager = Mock()
    manager.price_fetcher = Mock()
    storage.get_holdings.return_value = [
        Holding(
            asset_id="000001",
            asset_name="平安银行",
            asset_type=AssetType.A_STOCK,
            account="a",
            quantity=100,
            currency="CNY",
            industry=Industry.FINANCE,
        ),
        Holding(
            asset_id="CNY-CASH",
            asset_name="人民币现金",
            asset_type=AssetType.CASH,
            account="a",
            quantity=50,
            currency="CNY",
            industry=None,
        ),
    ]
    manager.price_fetcher.fetch_batch.return_value = {
        "000001": {"cny_price": 10.0},
    }
    service = ReportingService(manager=manager, storage=storage)

    result = service.get_industry_distribution("a")

    assert result["金融"] == 1000.0 / 1050.0
    assert result["其他"] == 50.0 / 1050.0
    manager.price_fetcher.fetch_batch.assert_called_once()


def test_reporting_service_industry_distribution_returns_empty_without_value():
    storage = Mock()
    manager = Mock()
    manager.price_fetcher = None
    storage.get_holdings.return_value = [
        Holding(
            asset_id="AAPL",
            asset_name="Apple",
            asset_type=AssetType.US_STOCK,
            account="a",
            quantity=1,
            currency="USD",
            industry=Industry.TECH,
        )
    ]
    service = ReportingService(manager=manager, storage=storage)

    assert service.get_industry_distribution("a") == {}


def test_portfolio_distribution_methods_delegate_to_reporting_service():
    storage = Mock()
    manager = PortfolioManager(storage=storage, price_fetcher=Mock())
    manager.reporting_service = Mock()
    manager.reporting_service.get_asset_distribution.return_value = {"现金": 1.0}
    manager.reporting_service.get_industry_distribution.return_value = {"其他": 1.0}

    assert manager.get_asset_distribution("a") == {"现金": 1.0}
    assert manager.get_industry_distribution("a") == {"其他": 1.0}
    manager.reporting_service.get_asset_distribution.assert_called_once_with("a")
    manager.reporting_service.get_industry_distribution.assert_called_once_with("a")
