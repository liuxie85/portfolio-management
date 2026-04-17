from __future__ import annotations

from unittest.mock import Mock

from skill_api import PortfolioSkill


def test_get_holdings_delegates_to_read_service():
    skill = PortfolioSkill.__new__(PortfolioSkill)
    read_service = Mock()
    read_service.get_holdings.return_value = {"success": True, "count": 1}
    skill._read_service = Mock(return_value=read_service)

    result = skill.get_holdings(include_price=True, include_cash=False, group_by_market=True)

    assert result == {"success": True, "count": 1}
    skill._read_service.assert_called_once_with()
    read_service.get_holdings.assert_called_once_with(
        include_cash=False,
        group_by_market=True,
        include_price=True,
    )


def test_get_holdings_returns_error_when_read_service_fails():
    skill = PortfolioSkill.__new__(PortfolioSkill)
    skill._read_service = Mock(side_effect=RuntimeError("boom"))

    result = skill.get_holdings()

    assert result == {"success": False, "error": "boom"}


def test_get_position_delegates_to_read_service():
    skill = PortfolioSkill.__new__(PortfolioSkill)
    read_service = Mock()
    read_service.get_position.return_value = {"success": True, "total_value": 1}
    skill._read_service = Mock(return_value=read_service)

    result = skill.get_position()

    assert result == {"success": True, "total_value": 1}
    read_service.get_position.assert_called_once_with(holdings_data=None)


def test_get_distribution_delegates_to_read_service_with_holdings_data():
    skill = PortfolioSkill.__new__(PortfolioSkill)
    holdings_data = {
        "success": True,
        "holdings": [{"code": "AAPL", "normalized_type": "stock", "market_value": 1}],
        "total_value": 1,
    }
    read_service = Mock()
    read_service.get_distribution.return_value = {"success": True, "total_value": 1}
    skill._read_service = Mock(return_value=read_service)

    result = skill.get_distribution(holdings_data=holdings_data)

    assert result == {"success": True, "total_value": 1}
    read_service.get_distribution.assert_called_once_with(holdings_data=holdings_data)
