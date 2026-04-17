from datetime import date
from unittest.mock import Mock

from src.app.share_service import ShareService
from src.models import NAVHistory
from src.portfolio import PortfolioManager


def test_share_service_gets_total_shares():
    storage = Mock()
    storage.get_total_shares.return_value = 1000.0
    service = ShareService(storage=storage)

    assert service.get_shares("a") == 1000.0
    storage.get_total_shares.assert_called_once_with("a")


def test_share_service_calculates_shares_change_from_latest_nav():
    storage = Mock()
    storage.get_latest_nav.return_value = NAVHistory(date=date(2026, 3, 19), account="a", total_value=1000.0, nav=1.25)
    service = ShareService(storage=storage)

    assert service.calculate_shares_change("a", 125.0) == 100.0


def test_share_service_defaults_invalid_nav_to_one():
    storage = Mock()
    storage.get_latest_nav.return_value = NAVHistory(date=date(2026, 3, 19), account="a", total_value=0.0, nav=0)
    service = ShareService(storage=storage)

    assert service.calculate_shares_change("a", 125.0) == 125.0
    assert service.calculate_shares_change("a", 125.0, nav=-1.0) == 125.0


def test_portfolio_share_methods_delegate_to_share_service():
    storage = Mock()
    manager = PortfolioManager(storage=storage, price_fetcher=Mock())
    manager.share_service = Mock()
    manager.share_service.get_shares.return_value = 1000.0
    manager.share_service.calculate_shares_change.return_value = 50.0

    assert manager.get_shares("a") == 1000.0
    assert manager.calculate_shares_change("a", 100.0, nav=2.0) == 50.0
    manager.share_service.get_shares.assert_called_once_with("a")
    manager.share_service.calculate_shares_change.assert_called_once_with("a", 100.0, nav=2.0)
