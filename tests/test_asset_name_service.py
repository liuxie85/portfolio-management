import time
from unittest.mock import Mock

from src.app.asset_name_service import AssetNameService
from src.models import AssetType
from src.portfolio import PortfolioManager


def test_asset_name_service_returns_fetched_name():
    manager = Mock()
    manager.price_fetcher.fetch.return_value = {"name": "平安银行"}
    service = AssetNameService(manager=manager)

    assert service.get_asset_name("000001", AssetType.A_STOCK, "平安") == "平安银行"


def test_asset_name_service_falls_back_to_user_name_on_error():
    manager = Mock()
    manager.price_fetcher.fetch.side_effect = RuntimeError("boom")
    service = AssetNameService(manager=manager)

    assert service.get_asset_name("000001", AssetType.A_STOCK, "平安") == "平安"


def test_asset_name_service_falls_back_to_asset_id_without_user_name():
    manager = Mock()
    manager.price_fetcher.fetch.return_value = {}
    service = AssetNameService(manager=manager)

    assert service.get_asset_name("000001", AssetType.A_STOCK) == "000001"


def test_asset_name_service_timeout_uses_fallback_name():
    manager = Mock()

    def slow_fetch(_asset_id):
        time.sleep(0.05)
        return {"name": "late"}

    manager.price_fetcher.fetch.side_effect = slow_fetch
    service = AssetNameService(manager=manager)

    assert service.get_asset_name("000001", AssetType.A_STOCK, "平安", timeout=0.001) == "平安"


def test_portfolio_get_asset_name_delegates_to_service():
    storage = Mock()
    manager = PortfolioManager(storage=storage, price_fetcher=Mock())
    manager.asset_name_service = Mock()
    manager.asset_name_service.get_asset_name.return_value = "平安银行"

    assert manager._get_asset_name("000001", AssetType.A_STOCK, "平安", timeout=1.0) == "平安银行"
    manager.asset_name_service.get_asset_name.assert_called_once_with(
        asset_id="000001",
        asset_type=AssetType.A_STOCK,
        user_provided_name="平安",
        timeout=1.0,
    )
