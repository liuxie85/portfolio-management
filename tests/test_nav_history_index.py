from datetime import date

from src.domain.nav_history_index import NavHistoryIndex
from src.models import NAVHistory


def _nav(day, total=100.0, nav=1.0):
    return NAVHistory(date=day, account="a", total_value=total, nav=nav)


def test_nav_history_index_finds_latest_year_end_and_prev_month_end():
    navs = [
        _nav(date(2025, 3, 13), 103.0, 1.03),
        _nav(date(2024, 12, 31), 100.0, 1.0),
        _nav(date(2025, 1, 2), 101.0, 1.01),
        _nav(date(2025, 2, 28), 102.0, 1.02),
    ]

    index = NavHistoryIndex.build(navs)

    assert NavHistoryIndex.find_latest_before(navs, date(2025, 3, 14), nav_index=index).date == date(2025, 3, 13)
    assert NavHistoryIndex.find_year_end(navs, "2024", nav_index=index).date == date(2024, 12, 31)
    assert NavHistoryIndex.find_year_end(navs, "2023", nav_index=index) is None
    assert NavHistoryIndex.find_prev_month_end(navs, 2025, 3, nav_index=index).date == date(2025, 2, 28)


def test_nav_history_index_fallback_queries_without_index():
    navs = [
        _nav(date(2025, 1, 2), 101.0, 1.01),
        _nav(date(2025, 2, 28), 102.0, 1.02),
        _nav(date(2025, 3, 13), 103.0, 1.03),
    ]

    assert NavHistoryIndex.find_latest_before(navs, date(2025, 3, 1)).date == date(2025, 2, 28)
    assert NavHistoryIndex.find_year_end(navs, "2025").date == date(2025, 3, 13)
    assert NavHistoryIndex.find_prev_month_end(navs, 2025, 3).date == date(2025, 2, 28)
    assert NavHistoryIndex.find_latest_before(navs, date(2025, 1, 1)) is None
