from datetime import date
from unittest.mock import Mock

from src.app.nav_summary_printer import NavSummaryPrinter
from src.models import NAVHistory
from src.portfolio import PortfolioManager


def _summary_kwargs():
    prev_month = NAVHistory(date=date(2026, 2, 28), account="a", total_value=1000.0, nav=1.0)
    prev_year = NAVHistory(date=date(2025, 12, 31), account="a", total_value=900.0, nav=0.9)
    year_end = NAVHistory(date=date(2026, 12, 31), account="a", total_value=1100.0, nav=1.1)
    yearly_data = {
        "2026": {
            "prev_end": prev_year,
            "end": year_end,
            "nav_change": 0.222222,
        }
    }
    return {
        "today": date(2026, 3, 19),
        "stock_value": 1000.0,
        "cash_value": 200.0,
        "total_value": 1200.0,
        "stock_ratio": 1000.0 / 1200.0,
        "cash_ratio": 200.0 / 1200.0,
        "current_year": "2026",
        "start_year": 2026,
        "yesterday_nav": None,
        "prev_year_end_nav": prev_year,
        "prev_month_end_nav": prev_month,
        "yearly_data": yearly_data,
        "shares": 1000.0,
        "shares_change": 10.0,
        "nav": 1.2,
        "month_nav_change": 0.2,
        "year_nav_change": 0.333333,
        "cumulative_nav_change": 0.333333,
        "daily_appreciation": 5.0,
        "month_appreciation": 100.0,
        "year_appreciation": 200.0,
        "cumulative_appreciation": 250.0,
        "initial_value": 900.0,
        "first_year_data": {"prev_end": prev_year},
        "cumulative_cash_flow": 50.0,
        "daily_cash_flow": 10.0,
        "monthly_cash_flow": 10.0,
        "cagr": 0.123456,
    }


def test_nav_summary_printer_outputs_expected_lines(capsys):
    printer = NavSummaryPrinter()

    printer.print_summary(**_summary_kwargs())

    output = capsys.readouterr().out
    assert "净值记录已保存 (2026-03-19)" in output
    assert "股票市值: ¥1,000.00" in output
    assert "现金结余: ¥200.00" in output
    assert "当月净值涨幅: 20.00%" in output
    assert "当年(2026)净值涨幅: 33.33%" in output
    assert "2026年净值涨幅: 22.22%" in output
    assert "成立以来年化收益(CAGR): 12.35%" in output
    assert "累计资产升值: ¥250.00" in output


def test_portfolio_print_nav_summary_delegates_to_printer():
    manager = PortfolioManager(storage=Mock(), price_fetcher=Mock())
    manager.nav_summary_printer = Mock()

    manager._print_nav_summary(**_summary_kwargs())

    manager.nav_summary_printer.print_summary.assert_called_once()
    assert manager.nav_summary_printer.print_summary.call_args.kwargs["today"] == date(2026, 3, 19)
