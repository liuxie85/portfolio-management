from datetime import date
from unittest.mock import Mock

from skill_api import PortfolioSkill
from src.models import NAVHistory, PortfolioValuation


def test_full_report_and_record_nav_share_same_snapshot():
    skill = PortfolioSkill(account='lx')
    valuation = PortfolioValuation(
        account='lx',
        total_value_cny=1000.0,
        cash_value_cny=200.0,
        stock_value_cny=700.0,
        fund_value_cny=100.0,
        shares=100.0,
        nav=10.0,
        holdings=[],
        warnings=[],
    )
    skill.portfolio.calculate_valuation = Mock(return_value=valuation)
    skill.storage.get_nav_history = Mock(return_value=[
        NAVHistory(
            date=date(2026, 3, 18),
            account='lx',
            total_value=990.0,
            cash_value=190.0,
            stock_value=700.0,
            fund_value=100.0,
            cn_stock_value=700.0,
            us_stock_value=0.0,
            hk_stock_value=0.0,
            stock_weight=0.8,
            cash_weight=0.2,
            shares=100.0,
            nav=9.9,
            cash_flow=0.0,
            share_change=0.0,
            mtd_nav_change=0.0,
            ytd_nav_change=0.0,
            mtd_pnl=0.0,
            ytd_pnl=0.0,
            details={},
        )
    ])
    skill.portfolio._find_latest_nav_before = Mock(return_value=skill.storage.get_nav_history.return_value[0])
    skill.portfolio._find_year_end_nav = Mock(return_value=skill.storage.get_nav_history.return_value[0])
    skill.portfolio._find_prev_month_end_nav = Mock(return_value=skill.storage.get_nav_history.return_value[0])
    skill.portfolio._get_daily_cash_flow = Mock(return_value=0.0)
    skill.portfolio._get_monthly_cash_flow = Mock(return_value=0.0)
    skill.portfolio._get_yearly_cash_flow = Mock(return_value=0.0)
    skill.portfolio._get_period_cash_flow = Mock(return_value=0.0)
    saved_nav = NAVHistory(
        date=date.today(),
        account='lx',
        total_value=1000.0,
        cash_value=200.0,
        stock_value=800.0,
        fund_value=100.0,
        cn_stock_value=700.0,
        us_stock_value=0.0,
        hk_stock_value=0.0,
        stock_weight=0.8,
        cash_weight=0.2,
        shares=100.0,
        nav=10.0,
        cash_flow=0.0,
        share_change=0.0,
        mtd_nav_change=0.01,
        ytd_nav_change=0.02,
        mtd_pnl=10.0,
        ytd_pnl=20.0,
        details={},
    )
    skill.portfolio.record_nav = Mock(return_value=saved_nav)

    snapshot = skill._build_snapshot()
    full = skill.full_report(snapshot=snapshot)
    rec = skill.record_nav(snapshot=snapshot)

    assert rec['snapshot_time'] == snapshot['snapshot_time']
    assert full['overview']['total_value'] == 1000.0
    assert round(full['nav']['total_value'], 2) == 1000.0
    assert rec['total_value'] == 1000.0
    assert rec['nav'] == 10.0


def test_repair_nav_history_metrics_dry_run_does_not_apply():
    skill = PortfolioSkill(account='lx')
    skill.audit_nav_history_accuracy = Mock(return_value={
        'success': True,
        'summary': {'repair_candidates': 1, 'exempt_rows': 0, 'ok_rows': 0},
        'metrics': {
            'rows': [{
                'record_id': 'rec1',
                'date': '2026-03-19',
                'recomputed_mtd_nav_change': 0.1,
                'recomputed_ytd_nav_change': 0.2,
                'recomputed_mtd_pnl': 10.0,
                'recomputed_ytd_pnl': 20.0,
                'base_missing': {'month': False, 'year': False},
            }]
        },
        'repair_candidates': [{
            'record_id': 'rec1',
            'date': '2026-03-19',
            'status': 'anomaly',
            'anomalies': ['mismatch'],
            'exemptions': [],
        }],
        'exempt_rows': [],
        'ok_rows': [],
    })
    skill.storage.client = Mock()

    res = skill.repair_nav_history_metrics(dry_run=True, write_report=False)

    assert res['success'] is True
    assert res['dry_run'] is True
    assert res['count'] == 1
    skill.storage.client.update_record.assert_not_called()
