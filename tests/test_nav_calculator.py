from datetime import date

import pytest

from src.domain.nav_calculator import NavCalculator
from src.models import NAVHistory, PortfolioValuation


def test_nav_calculator_period_metrics_and_yearly_mutation():
    prev_year = NAVHistory(date=date(2024, 12, 31), account="a", total_value=1000.0, nav=1.0, shares=1000.0)
    prev_month = NAVHistory(date=date(2025, 2, 28), account="a", total_value=1100.0, nav=1.1, shares=1000.0)
    yesterday = NAVHistory(date=date(2025, 3, 13), account="a", total_value=1200.0, nav=1.2, shares=1000.0)
    yearly_data = {
        "2025": {
            "prev_end": prev_year,
            "end": NAVHistory(date=date(2025, 12, 31), account="a", total_value=1300.0, nav=1.3),
            "cash_flow": 50.0,
        }
    }

    result = NavCalculator.calc_nav_metrics(
        today=date(2025, 3, 14),
        total_value=1320.0,
        yesterday_nav=yesterday,
        prev_year_end_nav=prev_year,
        prev_month_end_nav=prev_month,
        last_nav=yesterday,
        yearly_data=yearly_data,
        daily_cash_flow=120.0,
        monthly_cash_flow=120.0,
        yearly_cash_flow=120.0,
        cumulative_cash_flow=120.0,
        start_year=2025,
        initial_value=1000.0,
        gap_cash_flow=120.0,
    )

    assert result["shares"] == 1100.0
    assert result["shares_change"] == 100.0
    assert result["nav"] == 1.2
    assert result["daily_appreciation"] == 0.0
    assert result["month_appreciation"] == 100.0
    assert result["year_appreciation"] == 200.0
    assert yearly_data["2025"]["nav_change"] == pytest.approx(0.3)
    assert yearly_data["2025"]["appreciation"] == 250.0


def test_nav_calculator_build_and_validate_nav_record():
    valuation = PortfolioValuation(
        account="a",
        total_value_cny=1100.0,
        cash_value_cny=100.0,
        stock_value_cny=1000.0,
    )
    nav = NavCalculator.build_nav_record(
        today=date(2025, 3, 14),
        account="a",
        valuation=valuation,
        stock_value=1000.0,
        cash_value=100.0,
        total_value=1100.0,
        stock_ratio=1000 / 1100,
        cash_ratio=100 / 1100,
        daily_cash_flow=100.0,
        monthly_cash_flow=100.0,
        yearly_cash_flow=100.0,
        yearly_data={"2025": {"nav_change": 0.0, "appreciation": 0.0, "cash_flow": 100.0}},
        cumulative_cash_flow=100.0,
        start_year=2025,
        shares=1100.0,
        shares_change=100.0,
        nav=1.0,
        month_nav_change=0.0,
        year_nav_change=0.0,
        cumulative_nav_change=0.0,
        daily_appreciation=0.0,
        month_appreciation=0.0,
        year_appreciation=0.0,
        cumulative_appreciation=0.0,
        initial_value=1000.0,
        first_year_data=None,
        cagr=0.0,
    )

    NavCalculator.validate_nav_record(
        nav_record=nav,
        last_nav=NAVHistory(date=date(2025, 3, 13), account="a", total_value=1000.0, nav=1.0, shares=1000.0),
        prev_month_end_nav=NAVHistory(date=date(2025, 2, 28), account="a", total_value=1000.0, nav=1.0),
        prev_year_end_nav=NAVHistory(date=date(2024, 12, 31), account="a", total_value=1000.0, nav=1.0),
        daily_cash_flow=100.0,
        monthly_cash_flow=100.0,
        yearly_cash_flow=100.0,
        gap_cash_flow=100.0,
        initial_value=1000.0,
        cumulative_cash_flow=100.0,
    )


def test_nav_calculator_validate_rejects_inconsistent_total():
    nav = NAVHistory(
        date=date(2025, 3, 14),
        account="a",
        total_value=1200.0,
        cash_value=100.0,
        stock_value=1000.0,
        stock_weight=0.9,
        cash_weight=0.1,
        shares=1200.0,
        nav=1.0,
        cash_flow=0.0,
        share_change=0.0,
    )

    with pytest.raises(ValueError, match="total_value 不等于 stock_value \\+ cash_value"):
        NavCalculator.validate_nav_record(nav_record=nav)
