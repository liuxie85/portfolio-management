import unittest
from unittest.mock import Mock

from skill_api import PortfolioSkill


class TestGenerateReportDailyNavMetrics(unittest.TestCase):
    def test_generate_report_daily_includes_nav_metrics_backward_compatible(self):
        skill = PortfolioSkill.__new__(PortfolioSkill)
        snapshot = {
            'snapshot_time': '2026-03-29T08:00:00',
            'overview': {},
        }

        full_payload = {
            'success': True,
            'overview': {
                'total_value': 1000.0,
                'cash_ratio': 0.2,
                'stock_ratio': 0.8,
                'fund_ratio': 0.0,
            },
            'nav': {
                'date': '2026-03-29',
                'nav': 1.234567,
                'total_value': 1000.0,
                'cash_flow': 10.0,
                'pnl': 5.0,
                'mtd_nav_change': 0.0123,
                'ytd_nav_change': 0.0456,
                'mtd_pnl': 12.3,
                'ytd_pnl': 45.6,
                'details': {},
            },
            'returns': {
                'since_inception': {
                    'success': True,
                    'cagr_pct': 8.88,
                }
            },
            'top_holdings': [],
            'warnings': [],
        }

        skill.full_report = Mock(return_value=full_payload)

        result = skill.generate_report(report_type='daily', snapshot=snapshot, navs=[])

        self.assertTrue(result['success'])
        # backward compatibility: old fields still present
        self.assertEqual(result['nav'], 1.234567)
        self.assertEqual(result['total_value'], 1000.0)
        self.assertEqual(result['cash_flow'], 10.0)
        # newly exposed daily NAV metrics
        self.assertEqual(result['pnl'], 5.0)
        self.assertEqual(result['mtd_nav_change'], 0.0123)
        self.assertEqual(result['ytd_nav_change'], 0.0456)
        self.assertEqual(result['mtd_pnl'], 12.3)
        self.assertEqual(result['ytd_pnl'], 45.6)


if __name__ == '__main__':
    unittest.main()
