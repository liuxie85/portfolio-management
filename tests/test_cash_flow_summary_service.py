from datetime import date
from unittest.mock import Mock

from src.app.cash_flow_summary_service import CashFlowSummaryService
from src.models import CashFlow, NAVHistory


def test_cash_flow_summary_service_summarizes_aggregate_cache():
    storage = Mock()
    storage.get_cash_flow_aggs.return_value = {
        "daily": {
            "2024-12-31": 10000,
            "2025-03-01": 20000,
            "2025-03-14": 5000,
            "bad-date": 999,
            "2025-03-15": 123,
        },
        "monthly": {"2025-03": 25000},
        "yearly": {"2024": 10000, "2025": 25000},
    }
    service = CashFlowSummaryService(storage=storage)
    last_nav = NAVHistory(date=date(2025, 3, 13), account="a", total_value=1000.0, nav=1.0, shares=1000.0)

    result = service.summarize("a", date(2025, 3, 14), 2024, last_nav=last_nav)

    assert result == {
        "daily": 5000.0,
        "monthly": 25000.0,
        "yearly": {"2024": 10000.0, "2025": 25000.0},
        "cumulative": 35000.0,
        "gap": 5000.0,
    }
    storage.preload_cash_flow_aggs.assert_called_with("a")


def test_cash_flow_summary_service_period_and_point_queries():
    storage = Mock()
    storage.get_cash_flow_aggs.return_value = {
        "daily": {"2025-03-01": 100, "2025-03-14": 50},
        "monthly": {"2025-03": 150},
        "yearly": {"2025": 150},
    }
    service = CashFlowSummaryService(storage=storage)

    assert service.daily("a", date(2025, 3, 14)) == 50.0
    assert service.monthly("a", 2025, 3) == 150.0
    assert service.yearly("a", "2025") == 150.0
    assert service.period("a", date(2025, 3, 2), date(2025, 3, 14)) == 50.0


def test_cash_flow_summary_service_sums_cash_flow_objects():
    flows = [
        CashFlow(flow_date=date(2025, 3, 1), account="a", amount=100, currency="CNY", cny_amount=100, flow_type="DEPOSIT"),
        CashFlow(flow_date=date(2025, 3, 2), account="a", amount=50, currency="CNY", cny_amount=None, flow_type="DEPOSIT"),
        CashFlow(flow_date=date(2025, 3, 3), account="a", amount=-20, currency="CNY", cny_amount=-20, flow_type="WITHDRAW"),
    ]

    assert CashFlowSummaryService.sum_cash_flows(flows) == 80.0
