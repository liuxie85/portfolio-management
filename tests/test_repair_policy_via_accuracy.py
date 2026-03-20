from unittest.mock import Mock

from skill_api import PortfolioSkill


def test_repair_uses_accuracy_audit_pipeline():
    skill = PortfolioSkill(account='lx')
    skill.audit_nav_history_accuracy = Mock(return_value={
        'success': True,
        'summary': {'repair_candidates': 1, 'exempt_rows': 1, 'ok_rows': 1},
        'metrics': {
            'rows': [
                {
                    'record_id': 'rec-bad',
                    'date': '2026-03-19',
                    'base_missing': {'month': False, 'year': True},
                    'recomputed_mtd_nav_change': 0.3,
                    'recomputed_ytd_nav_change': None,
                    'recomputed_mtd_pnl': 30.0,
                    'recomputed_ytd_pnl': None,
                }
            ]
        },
        'repair_candidates': [
            {
                'record_id': 'rec-bad',
                'date': '2026-03-19',
                'status': 'anomaly',
                'anomalies': ['bad'],
                'exemptions': [],
            }
        ],
        'exempt_rows': [
            {
                'record_id': 'rec-ex',
                'date': '2026-01-02',
                'status': 'exempt',
                'exemptions': ['missing_month_base'],
            }
        ],
        'ok_rows': [
            {
                'record_id': 'rec-ok',
                'date': '2026-03-18',
                'status': 'ok',
                'exemptions': [],
            }
        ],
    })
    skill.storage.update_nav_fields = Mock()

    result = skill.repair_nav_history_metrics(dry_run=False, write_report=False)

    assert result['repair_policy'] == 'anomaly_only_via_accuracy_audit'
    assert result['count'] == 1
    update = result['updates'][0]
    assert update['record_id'] == 'rec-bad'
    assert update['fields']['mtd_nav_change'] == 0.3
    assert update['fields']['mtd_pnl'] == 30.0
    assert update['fields']['ytd_nav_change'] is None
    assert update['fields']['ytd_pnl'] is None
    assert result['accuracy_report']['repair_candidates'] == 1
    assert result['skipped_count'] == 2
    skill.storage.update_nav_fields.assert_called_once_with('rec-bad', update['fields'], dry_run=False)
