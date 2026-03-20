from datetime import date
from unittest.mock import Mock

from skill_api import PortfolioSkill
from src.models import NAVHistory


def _nav(d, mtd, ytd):
    return NAVHistory(
        record_id=f"nav-{d.isoformat()}",
        date=d,
        account='lx',
        total_value=100.0,
        cash_value=0.0,
        stock_value=100.0,
        fund_value=0.0,
        cn_stock_value=100.0,
        us_stock_value=0.0,
        hk_stock_value=0.0,
        stock_weight=1.0,
        cash_weight=0.0,
        shares=100.0,
        nav=1.0,
        cash_flow=0.0,
        share_change=0.0,
        mtd_nav_change=mtd,
        ytd_nav_change=ytd,
        mtd_pnl=0.0,
        ytd_pnl=0.0,
        details={},
    )


def test_audit_ignores_initial_record_without_month_base():
    skill = PortfolioSkill(account='lx')
    nav = _nav(date(2024, 1, 2), mtd=0.0, ytd=0.0)
    skill.storage.get_nav_history = Mock(return_value=[nav])
    skill.portfolio._find_prev_month_end_nav = Mock(return_value=None)
    skill.portfolio._find_year_end_nav = Mock(return_value=nav)
    skill.portfolio._get_monthly_cash_flow = Mock(return_value=0.0)
    skill.portfolio._get_yearly_cash_flow = Mock(return_value=0.0)
    skill.portfolio._calc_mtd_nav_change = Mock(return_value=0.123456)
    skill.portfolio._calc_ytd_nav_change = Mock(return_value=0.0)
    skill.portfolio._calc_mtd_pnl = Mock(return_value=0.0)
    skill.portfolio._calc_ytd_pnl = Mock(return_value=0.0)

    result = skill.audit_nav_history_metrics(write_report=False)

    assert result['summary']['mtd_nav_change_mismatch'] == 0
    assert result['summary']['base_missing_month'] == 1
    assert result['rows'][0]['audit_exemptions']['initial_without_month_base'] is True


def test_audit_ignores_swapped_false_positive_when_january_mtd_equals_ytd():
    skill = PortfolioSkill(account='lx')
    nav = _nav(date(2025, 1, 15), mtd=0.02, ytd=0.02)
    baseline = _nav(date(2024, 12, 31), mtd=0.0, ytd=0.0)
    skill.storage.get_nav_history = Mock(return_value=[baseline, nav])
    skill.portfolio._find_prev_month_end_nav = Mock(return_value=baseline)
    skill.portfolio._find_year_end_nav = Mock(return_value=baseline)
    skill.portfolio._get_monthly_cash_flow = Mock(return_value=0.0)
    skill.portfolio._get_yearly_cash_flow = Mock(return_value=0.0)
    skill.portfolio._calc_mtd_nav_change = Mock(return_value=0.02)
    skill.portfolio._calc_ytd_nav_change = Mock(return_value=0.02)
    skill.portfolio._calc_mtd_pnl = Mock(return_value=0.0)
    skill.portfolio._calc_ytd_pnl = Mock(return_value=0.0)

    result = skill.audit_nav_history_metrics(write_report=False)

    assert result['summary']['swapped_nav_change_like'] == 0
    row = [r for r in result['rows'] if r['date'] == '2025-01-15'][0]
    assert row['audit_exemptions']['january_mtd_equals_ytd'] is True
