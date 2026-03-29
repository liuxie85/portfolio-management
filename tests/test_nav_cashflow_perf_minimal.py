"""Minimal (no pytest) tests for nav/cash-flow performance cache behavior."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.feishu_storage import FeishuStorage
from src.local_cache import LocalNavIndexCache, LocalCashFlowAggCache
from src.models import CashFlow, NAVHistory, PortfolioValuation
from src.portfolio import PortfolioManager


class StubNavCashClient:
    def __init__(self, nav_records: Optional[List[Dict[str, Any]]] = None, cash_records: Optional[List[Dict[str, Any]]] = None):
        self._nav_records = list(nav_records or [])
        self._cash_records = list(cash_records or [])
        self.list_records_calls: List[Dict[str, Any]] = []
        self.create_record_calls: List[Dict[str, Any]] = []

    def list_records(self, table_name: str, filter_str: str = None, field_names: List[str] = None, page_size: int = 500):
        self.list_records_calls.append({
            'table_name': table_name,
            'filter_str': filter_str,
            'field_names': list(field_names or []),
        })
        if table_name == 'nav_history':
            return [{'record_id': r['record_id'], 'fields': dict(r['fields'])} for r in self._nav_records]
        if table_name == 'cash_flow':
            return [{'record_id': r['record_id'], 'fields': dict(r['fields'])} for r in self._cash_records]
        return []

    def create_record(self, table_name: str, fields: Dict[str, Any]):
        self.create_record_calls.append({'table_name': table_name, 'fields': dict(fields)})
        if table_name == 'cash_flow':
            rid = f"cf_new_{len(self.create_record_calls)}"
            self._cash_records.append({'record_id': rid, 'fields': dict(fields)})
            return {'record_id': rid, 'fields': dict(fields)}
        return {'record_id': f'rec_{len(self.create_record_calls)}', 'fields': dict(fields)}


class StubStorageForRecordNav:
    def __init__(self):
        self.get_nav_history_calls = 0
        self._nav_index = {
            '_nav_objects': [
                NAVHistory(
                    record_id='rec_ye',
                    date=date(2025, 12, 31),
                    account='lx',
                    total_value=800.0,
                    shares=800.0,
                    nav=1.0,
                    cash_flow=0.0,
                    pnl=0.0,
                    mtd_nav_change=0.0,
                    ytd_nav_change=0.0,
                    mtd_pnl=0.0,
                    ytd_pnl=0.0,
                ),
                NAVHistory(
                    record_id='rec_me',
                    date=date(2026, 2, 28),
                    account='lx',
                    total_value=900.0,
                    shares=900.0,
                    nav=1.0,
                    cash_flow=0.0,
                    pnl=0.0,
                    mtd_nav_change=0.0,
                    ytd_nav_change=0.0,
                    mtd_pnl=0.0,
                    ytd_pnl=0.0,
                ),
                NAVHistory(
                    record_id='rec_last',
                    date=date(2026, 3, 14),
                    account='lx',
                    total_value=1000.0,
                    shares=1000.0,
                    nav=1.0,
                    cash_flow=0.0,
                    pnl=0.0,
                    mtd_nav_change=0.0,
                    ytd_nav_change=0.0,
                    mtd_pnl=0.0,
                    ytd_pnl=0.0,
                ),
            ],
        }
        self._cash_agg = {
            'daily': {},
            'monthly': {},
            'yearly': {},
            'cumulative': 0.0,
            'flows': [],
            'flow_count': 0,
        }

    def preload_nav_index(self, account: str, force_refresh: bool = False):
        return {'account': account, 'loaded': len(self._nav_index['_nav_objects']), 'source': 'memory'}

    def get_nav_index(self, account: str):
        return self._nav_index

    def preload_cash_flow_aggs(self, account: str, force_refresh: bool = False):
        return {'account': account, 'loaded': 0, 'source': 'memory'}

    def get_cash_flow_aggs(self, account: str):
        return self._cash_agg

    def get_nav_history(self, account: str, days: int = 365):
        self.get_nav_history_calls += 1
        return []


def test_nav_base_cache_month_boundary_and_invalidation_flag():
    client = StubNavCashClient(
        nav_records=[
            {
                'record_id': 'n1',
                'fields': {
                    'date': '2026-01-31', 'account': 'lx', 'total_value': 1000, 'shares': 1000, 'nav': 1.0,
                    'cash_flow': 0, 'pnl': 0, 'mtd_nav_change': 0, 'ytd_nav_change': 0, 'mtd_pnl': 0, 'ytd_pnl': 0,
                    'updated_at': '2026-01-31 20:00:00',
                },
            },
            {
                'record_id': 'n2',
                'fields': {
                    'date': '2026-02-15', 'account': 'lx', 'total_value': 1100, 'shares': 1000, 'nav': 1.1,
                    'cash_flow': 0, 'pnl': 100, 'mtd_nav_change': 0.1, 'ytd_nav_change': 0.1, 'mtd_pnl': 100, 'ytd_pnl': 100,
                    'updated_at': '2026-02-15 20:00:00',
                },
            },
        ]
    )
    tmp_nav = Path('/tmp/test_nav_index_cache_perf.json')
    if tmp_nav.exists():
        tmp_nav.unlink()

    storage = FeishuStorage(
        client=client,
        local_nav_index_cache=LocalNavIndexCache(cache_file=tmp_nav),
        local_cash_flow_agg_cache=LocalCashFlowAggCache(cache_file=Path('/tmp/test_cash_unused.json')),
    )

    first = storage.preload_nav_index('lx', force_refresh=True)
    assert first['loaded'] == 2
    idx1 = storage.get_nav_index('lx')
    assert idx1['month_end_base']['2026-01']['date'] == '2026-01-31'
    assert idx1['month_end_base']['2026-02']['date'] == '2026-02-15'

    # Backfill modifies February month-end candidate; should trigger invalidation detection.
    client._nav_records[1]['record_id'] = 'n2b'
    client._nav_records[1]['fields']['date'] = '2026-02-20'
    client._nav_records[1]['fields']['updated_at'] = '2026-02-20 20:00:00'

    second = storage.preload_nav_index('lx', force_refresh=True)
    assert second['invalidated'] is True
    idx2 = storage.get_nav_index('lx')
    assert idx2['month_end_base']['2026-01']['date'] == '2026-01-31'  # month boundary unchanged
    assert idx2['month_end_base']['2026-02']['date'] == '2026-02-20'


def test_cash_flow_agg_cache_updates_on_new_record():
    client = StubNavCashClient(
        cash_records=[
            {
                'record_id': 'cf1',
                'fields': {
                    'flow_date': '2026-01-05',
                    'account': 'lx',
                    'amount': 100,
                    'currency': 'CNY',
                    'cny_amount': 100,
                    'flow_type': 'DEPOSIT',
                    'updated_at': '2026-01-05 10:00:00',
                },
            }
        ]
    )
    tmp_cash = Path('/tmp/test_cash_flow_agg_perf.json')
    if tmp_cash.exists():
        tmp_cash.unlink()

    storage = FeishuStorage(
        client=client,
        local_nav_index_cache=LocalNavIndexCache(cache_file=Path('/tmp/test_nav_unused.json')),
        local_cash_flow_agg_cache=LocalCashFlowAggCache(cache_file=tmp_cash),
    )

    pre = storage.preload_cash_flow_aggs('lx', force_refresh=True)
    assert pre['loaded'] == 1
    agg1 = storage.get_cash_flow_aggs('lx')
    assert agg1['monthly']['2026-01'] == 100.0
    assert agg1['yearly']['2026'] == 100.0

    storage.add_cash_flow(
        CashFlow(
            flow_date=date(2026, 1, 6),
            account='lx',
            amount=50.0,
            cny_amount=50.0,
            currency='CNY',
            flow_type='DEPOSIT',
            source='manual',
        )
    )
    agg2 = storage.get_cash_flow_aggs('lx')
    assert agg2['daily']['2026-01-06'] == 50.0
    assert agg2['monthly']['2026-01'] == 150.0
    assert agg2['yearly']['2026'] == 150.0


def test_record_nav_avoids_get_nav_history_full_scan_when_preloaded():
    storage = StubStorageForRecordNav()
    pm = PortfolioManager(storage=storage, price_fetcher=None)

    valuation = PortfolioValuation(
        account='lx',
        total_value_cny=1000.0,
        cash_value_cny=200.0,
        stock_value_cny=800.0,
        fund_value_cny=0.0,
        cn_asset_value=0.0,
        us_asset_value=0.0,
        hk_asset_value=0.0,
        holdings=[],
        warnings=[],
    )

    nav = pm.record_nav('lx', valuation=valuation, nav_date=date(2026, 3, 15), persist=False)
    assert nav.account == 'lx'
    assert storage.get_nav_history_calls == 0
