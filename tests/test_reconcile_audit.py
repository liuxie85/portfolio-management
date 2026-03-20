from datetime import date
from unittest.mock import Mock

from skill_api import PortfolioSkill
from src.models import NAVHistory, CashFlow


def _nav(d, total, cash, stock, shares, nav, *, mtd=None, ytd=None, pnl=None, mtd_pnl=None, ytd_pnl=None):
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


def test_reconcile_marks_anomaly_for_inconsistent_total():
    skill = PortfolioSkill(account='lx')
    bad = _nav(date(2025, 3, 14), total=1200.0, cash=100.0, stock=1000.0, shares=1200.0, nav=1.0)
    skill.storage.get_nav_history = Mock(return_value=[bad])
    skill.storage.get_cash_flows = Mock(return_value=[])

    result = skill.audit_nav_history_reconcile(write_report=False)

    assert result['summary']['anomaly'] == 1
    assert result['rows'][0]['status'] == 'anomaly'
    assert any('total_value != stock_value + cash_value' in x for x in result['rows'][0]['anomalies'])
    assert result['rows'][0]['basis']['prev_nav_date'] is None
    assert 'anomaly_examples' in result['summary']


def test_reconcile_marks_exempt_for_initial_record_without_bases():
    skill = PortfolioSkill(account='lx')
    first = _nav(date(2025, 1, 2), total=1000.0, cash=100.0, stock=900.0, shares=1000.0, nav=1.0)
    skill.storage.get_nav_history = Mock(return_value=[first])
    skill.storage.get_cash_flows = Mock(return_value=[])
    skill.portfolio._find_prev_month_end_nav = Mock(return_value=None)
    skill.portfolio._find_year_end_nav = Mock(return_value=None)

    result = skill.audit_nav_history_reconcile(write_report=False)

    assert result['summary']['exempt'] == 1
    assert result['rows'][0]['status'] == 'exempt'
    assert 'missing_month_base' in result['rows'][0]['exemptions']
    assert 'missing_year_base' in result['rows'][0]['exemptions']


def test_reconcile_marks_ok_for_consistent_consecutive_record():
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
        ytd=None,
        pnl=10.0,
        mtd_pnl=10.0,
        ytd_pnl=None,
    )
    skill.storage.get_nav_history = Mock(return_value=[prev, curr])
    skill.storage.get_cash_flows = Mock(return_value=[])
    skill.portfolio._find_prev_month_end_nav = Mock(return_value=prev)
    skill.portfolio._find_year_end_nav = Mock(return_value=None)

    result = skill.audit_nav_history_reconcile(write_report=False)

    row = [r for r in result['rows'] if r['date'] == '2025-03-01'][0]
    assert row['status'] == 'exempt'
    assert row['anomalies'] == []
    assert 'missing_year_base' in row['exemptions']
    assert row['basis']['prev_nav_date'] == '2025-02-28'
    assert row['basis']['prev_month_end_date'] == '2025-02-28'
    assert row['cash_flow_basis']['daily_cash_flow'] == 0
    assert row['recomputed']['expected_daily_pnl'] == 10.0
