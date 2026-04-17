from datetime import date
from unittest.mock import Mock, patch

from src.app.nav_baseline_service import NavBaselineService
from src.models import NAVHistory
from src.portfolio import PortfolioManager


def test_nav_baseline_service_gets_last_day_nav():
    storage = Mock()
    expected = NAVHistory(date=date(2026, 3, 18), account="a", total_value=1000.0, nav=1.0)
    storage.get_nav_on_date.return_value = expected
    service = NavBaselineService(storage=storage)

    assert service.get_last_day_nav("a", date(2026, 3, 19)) is expected
    storage.get_nav_on_date.assert_called_once_with("a", date(2026, 3, 18))


def test_nav_baseline_service_uses_earliest_near_one_nav_as_initial_value():
    storage = Mock()
    service = NavBaselineService(storage=storage)
    navs = [
        NAVHistory(date=date(2026, 3, 2), account="a", total_value=1200.0, nav=1.2),
        NAVHistory(date=date(2026, 3, 1), account="a", total_value=1000.0, nav=1.0),
    ]

    assert service.get_initial_value("a", all_navs=navs) == 1000.0


def test_nav_baseline_service_falls_back_to_config_initial_value():
    storage = Mock()
    storage.get_nav_history.return_value = []
    service = NavBaselineService(storage=storage)

    with patch("src.app.nav_baseline_service.config.get_initial_value", return_value=1234.56):
        assert service.get_initial_value("a") == 1234.56

    storage.get_nav_history.assert_called_once_with("a", days=365 * 2)


def test_portfolio_nav_baseline_methods_delegate_to_service():
    manager = PortfolioManager(storage=Mock(), price_fetcher=Mock())
    manager.nav_baseline_service = Mock()
    manager.nav_baseline_service.get_last_day_nav.return_value = "last"
    manager.nav_baseline_service.get_initial_value.return_value = 1000.0

    assert manager._get_last_day_nav("a", date(2026, 3, 19)) == "last"
    assert manager._get_initial_value("a", all_navs=[]) == 1000.0
    manager.nav_baseline_service.get_last_day_nav.assert_called_once_with("a", date(2026, 3, 19))
    manager.nav_baseline_service.get_initial_value.assert_called_once_with("a", all_navs=[])
