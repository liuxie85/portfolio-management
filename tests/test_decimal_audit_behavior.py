from datetime import date
from unittest.mock import Mock

from skill_api import PortfolioSkill
from src.models import NAVHistory


def _nav(d, *, total=100.0, cash=0.0, stock=100.0, shares=100.0, nav=1.0, mtd=None, ytd=None, pnl=None, mtd_pnl=None, ytd_pnl=None):
    return NAVHistory(
        record_id=f"nav-{d.isoformat()}",
        date=d,
        account='lx',
        total_value=total,
        cash_value=cash,
        stock_value=stock,
        fund_value=0.0,
        cn_stock_value=stock,
        us_stock_value=0.0,
        hk_stock_value=0.0,
        stock_weight=round(stock / total, 6) if total else 0.0,
        cash_weight=round(cash / total, 6) if total else 0.0,
        shares=shares,
        nav=nav,
        cash_flow=0.0,
        share_change=0.0,
        mtd_nav_change=mtd,
        ytd_nav_change=ytd,
        pnl=pnl,
        mtd_pnl=mtd_pnl,
        ytd_pnl=ytd_pnl,
        details={},
    )


def test_metrics_audit_does_not_flag_nav_values_equal_after_quantize():
    skill = PortfolioSkill(account='lx')
    baseline = _nav(date(2025, 2, 28), nav=1.0, mtd=0.0, ytd=0.0)
    nav = _nav(date(2025, 3, 1), nav=1.123457, mtd=0.123457, ytd=None, mtd_pnl=12.34, ytd_pnl=None)
    skill.storage.get_nav_history = Mock(return_value=[baseline, nav])
    skill.portfolio._find_prev_month_end_nav = Mock(return_value=baseline)
    skill.portfolio._find_year_end_nav = Mock(return_value=None)
    skill.portfolio._get_monthly_cash_flow = Mock(return_value=0.0)
    skill.portfolio._get_yearly_cash_flow = Mock(return_value=None)
    skill.portfolio._calc_mtd_nav_change = Mock(side_effect=[0.0, 0.1234569999])
    skill.portfolio._calc_ytd_nav_change = Mock(side_effect=[None, None])
    skill.portfolio._calc_mtd_pnl = Mock(side_effect=[None, 12.3400001])
    skill.portfolio._calc_ytd_pnl = Mock(side_effect=[None, None])

    result = skill.audit_nav_history_metrics(write_report=False)

    row = [r for r in result['rows'] if r['date'] == '2025-03-01'][0]
    assert row['recomputed_mtd_nav_change'] == 0.123457
    assert row['recomputed_mtd_pnl'] == 12.34
    assert result['summary']['mtd_nav_change_mismatch'] == 0
    assert result['summary']['mtd_pnl_mismatch'] == 0


def test_reconcile_does_not_flag_money_values_equal_after_quantize():
    skill = PortfolioSkill(account='lx')
    prev = _nav(date(2025, 2, 28), total=1000.0, cash=100.0, stock=900.0, shares=1000.0, nav=1.0)
    curr = _nav(
        date(2025, 3, 1),
        total=1010.0,
        cash=110.0,
        stock=900.0,
        shares=1000.0,
        nav=1.01,
        mtd=0.01,
        ytd=0.01,
        pnl=10.0,
        mtd_pnl=10.0,
        ytd_pnl=10.0,
    )
    skill.storage.get_nav_history = Mock(return_value=[prev, curr])
    skill.storage.get_cash_flows = Mock(return_value=[])
    skill.portfolio._find_prev_month_end_nav = Mock(return_value=prev)
    skill.portfolio._find_year_end_nav = Mock(return_value=prev)
    skill.portfolio._calc_mtd_nav_change = Mock(return_value=0.0100000001)
    skill.portfolio._calc_ytd_nav_change = Mock(return_value=0.0100000001)
    skill.portfolio._calc_mtd_pnl = Mock(return_value=10.0000001)
    skill.portfolio._calc_ytd_pnl = Mock(return_value=10.0000001)

    result = skill.audit_nav_history_reconcile(write_report=False)

    row = [r for r in result['rows'] if r['date'] == '2025-03-01'][0]
    assert row['status'] == 'ok'
    assert row['anomalies'] == []
