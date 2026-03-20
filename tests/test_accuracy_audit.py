from unittest.mock import Mock

from skill_api import PortfolioSkill


def test_accuracy_audit_unifies_metrics_and_reconcile():
    skill = PortfolioSkill(account='lx')
    skill.audit_nav_history_metrics = Mock(return_value={
        'success': True,
        'summary': {'mtd_nav_change_mismatch': 1},
        'rows': [
            {
                'record_id': 'rec-ok',
                'date': '2026-03-18',
                'base_missing': {'month': False, 'year': False},
                'audit_exemptions': {},
            },
            {
                'record_id': 'rec-bad',
                'date': '2026-03-19',
                'base_missing': {'month': False, 'year': False},
                'audit_exemptions': {},
            },
            {
                'record_id': 'rec-ex',
                'date': '2026-01-02',
                'base_missing': {'month': True, 'year': False},
                'audit_exemptions': {'initial_without_month_base': True},
            },
        ]
    })
    skill.audit_nav_history_reconcile = Mock(return_value={
        'success': True,
        'summary': {'anomaly': 1, 'ok': 1, 'exempt': 1},
        'rows': [
            {'date': '2026-03-18', 'status': 'ok', 'anomalies': [], 'exemptions': []},
            {'date': '2026-03-19', 'status': 'anomaly', 'anomalies': ['bad'], 'exemptions': []},
            {'date': '2026-01-02', 'status': 'exempt', 'anomalies': [], 'exemptions': ['missing_month_base']},
        ]
    })

    result = skill.audit_nav_history_accuracy(write_report=False)

    assert result['success'] is True
    assert result['summary']['repair_candidates'] == 1
    assert result['summary']['exempt_rows'] == 1
    assert result['summary']['ok_rows'] == 1
    assert result['repair_candidates'][0]['record_id'] == 'rec-bad'
    assert result['exempt_rows'][0]['record_id'] == 'rec-ex'
    assert result['ok_rows'][0]['record_id'] == 'rec-ok'
