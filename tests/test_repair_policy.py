from unittest.mock import Mock

from skill_api import PortfolioSkill


def test_repair_only_updates_anomaly_rows():
    skill = PortfolioSkill(account='lx')
    skill.audit_nav_history_accuracy = Mock(return_value={
        'success': True,
        'summary': {'repair_candidates': 1, 'exempt_rows': 0, 'ok_rows': 1},
        'metrics': {
            'rows': [
                {
                    'record_id': 'rec-ok',
                    'date': '2026-03-18',
                    'recomputed_mtd_nav_change': 0.1,
                    'recomputed_ytd_nav_change': 0.2,
                    'recomputed_mtd_pnl': 10.0,
                    'recomputed_ytd_pnl': 20.0,
                    'base_missing': {'month': False, 'year': False},
                },
                {
                    'record_id': 'rec-bad',
                    'date': '2026-03-19',
                    'recomputed_mtd_nav_change': 0.3,
                    'recomputed_ytd_nav_change': 0.4,
                    'recomputed_mtd_pnl': 30.0,
                    'recomputed_ytd_pnl': 40.0,
                    'base_missing': {'month': False, 'year': False},
                },
            ]
        },
        'repair_candidates': [
            {'record_id': 'rec-bad', 'date': '2026-03-19', 'status': 'anomaly', 'anomalies': ['mtd mismatch'], 'exemptions': []},
        ],
        'exempt_rows': [],
        'ok_rows': [
            {'record_id': 'rec-ok', 'date': '2026-03-18', 'status': 'ok', 'exemptions': []},
        ],
    })
    skill.storage.update_nav_fields = Mock()

    result = skill.repair_nav_history_metrics(dry_run=True, write_report=False)

    assert result['repair_policy'] == 'anomaly_only_via_accuracy_audit'
    assert result['count'] == 1
    assert result['updates'][0]['record_id'] == 'rec-bad'
    assert result['skipped_count'] == 1
    assert result['skipped'][0]['record_id'] == 'rec-ok'
    skill.storage.update_nav_fields.assert_not_called()


def test_repair_clears_fields_when_base_missing_on_anomaly():
    skill = PortfolioSkill(account='lx')
    skill.audit_nav_history_metrics = Mock(return_value={
        'success': True,
        'summary': {},
        'rows': [{
            'record_id': 'rec1',
            'date': '2026-01-02',
            'recomputed_mtd_nav_change': None,
            'recomputed_ytd_nav_change': 0.05,
            'recomputed_mtd_pnl': None,
            'recomputed_ytd_pnl': 5.0,
            'base_missing': {'month': True, 'year': False},
        }]
    })
    skill.audit_nav_history_reconcile = Mock(return_value={
        'success': True,
        'rows': [{
            'date': '2026-01-02',
            'status': 'anomaly',
            'exemptions': [],
            'anomalies': ['month base issue'],
        }]
    })
    skill.storage.update_nav_fields = Mock()

    result = skill.repair_nav_history_metrics(dry_run=False, write_report=False)

    assert result['count'] == 1
    fields = result['updates'][0]['fields']
    assert fields['mtd_nav_change'] is None
    assert fields['mtd_pnl'] is None
    assert fields['ytd_nav_change'] == 0.05
    assert fields['ytd_pnl'] == 5.0
    skill.storage.update_nav_fields.assert_called_once_with('rec1', fields, dry_run=False)
