"""Minimal (no pytest) tests for holdings bulk upsert behavior."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from src.feishu_storage import FeishuStorage
from src.models import AssetType, Holding


class StubLocalHoldingsIndexCache:
    def __init__(self):
        self.items: Dict[str, Dict[str, Any]] = {}
        self.flush_calls = 0

    def load_all(self) -> Dict[str, Dict[str, Any]]:
        return {k: dict(v) for k, v in self.items.items()}

    def upsert(self, cache_key: str, payload: Dict[str, Any], _flush: bool = False):
        self.items[cache_key] = dict(payload)
        if _flush:
            self.flush()

    def delete(self, cache_key: str, _flush: bool = False):
        self.items.pop(cache_key, None)
        if _flush:
            self.flush()

    def flush(self):
        self.flush_calls += 1


class StubBulkClient:
    def __init__(self, initial_records: Optional[List[Dict[str, Any]]] = None):
        self._records = list(initial_records or [])
        self.list_records_calls: List[Dict[str, Any]] = []
        self.batch_update_records_calls: List[List[Dict[str, Any]]] = []
        self.batch_create_records_calls: List[List[Dict[str, Any]]] = []

    def list_records(self, table_name: str, filter_str: str = None, field_names: List[str] = None, page_size: int = 500):
        assert table_name == 'holdings'
        self.list_records_calls.append(
            {
                'table_name': table_name,
                'filter_str': filter_str,
                'field_names': list(field_names or []),
                'page_size': page_size,
            }
        )

        account = None
        if filter_str and 'CurrentValue.[account] = "' in filter_str:
            account = filter_str.split('CurrentValue.[account] = "', 1)[1].split('"', 1)[0]

        out = []
        for r in self._records:
            if account and (r.get('fields') or {}).get('account') != account:
                continue
            out.append({'record_id': r['record_id'], 'fields': dict(r.get('fields') or {})})
        return out

    def batch_update_records(self, table_name: str, records: List[Dict]):
        assert table_name == 'holdings'
        self.batch_update_records_calls.append([{'record_id': x['record_id'], 'fields': dict(x['fields'])} for x in records])
        by_id = {r['record_id']: r for r in self._records}
        for rec in records:
            rid = rec['record_id']
            if rid in by_id:
                by_id[rid].setdefault('fields', {}).update(rec.get('fields') or {})
        return [{'record_id': r['record_id'], 'fields': dict(r.get('fields') or {})} for r in records]

    def batch_create_records(self, table_name: str, records: List[Dict[str, Any]]):
        assert table_name == 'holdings'
        self.batch_create_records_calls.append([{'fields': dict((x.get('fields') or {}))} for x in records])
        result = []
        for i, rec in enumerate(records, start=1):
            new_id = f"rec_new_{len(self._records) + i}"
            fields = dict(rec.get('fields') or {})
            self._records.append({'record_id': new_id, 'fields': fields})
            result.append({'record_id': new_id, 'fields': fields})
        return result


def test_bulk_upsert_additive_preloads_once_per_account_and_batches_updates():
    client = StubBulkClient(
        initial_records=[
            {
                'record_id': 'rec_lx_aapl',
                'fields': {
                    'asset_id': 'AAPL',
                    'asset_name': 'Apple',
                    'asset_type': 'us_stock',
                    'account': 'lx',
                    'market': 'futu',
                    'quantity': 10,
                    'currency': 'USD',
                },
            },
            {
                'record_id': 'rec_sy_msft',
                'fields': {
                    'asset_id': 'MSFT',
                    'asset_name': 'Microsoft',
                    'asset_type': 'us_stock',
                    'account': 'sy',
                    'market': 'futu',
                    'quantity': 7,
                    'currency': 'USD',
                },
            },
        ]
    )
    local_idx = StubLocalHoldingsIndexCache()
    storage = FeishuStorage(client=client, local_holdings_index_cache=local_idx)

    payload = [
        Holding(asset_id='AAPL', asset_name='Apple', asset_type=AssetType.US_STOCK, account='lx', market='futu', quantity=2, currency='USD'),
        Holding(asset_id='AAPL', asset_name='Apple', asset_type=AssetType.US_STOCK, account='lx', market='futu', quantity=3, currency='USD'),
        Holding(asset_id='MSFT', asset_name='Microsoft', asset_type=AssetType.US_STOCK, account='sy', market='futu', quantity=4, currency='USD'),
    ]

    result = storage.upsert_holdings_bulk(payload, mode='additive')

    # Preload happens at most once per account involved (lx + sy)
    assert len(client.list_records_calls) == 2
    assert all('CurrentValue.[account] = "' in (c.get('filter_str') or '') for c in client.list_records_calls)

    # One batch_update call with 3 updates
    assert len(client.batch_update_records_calls) == 1
    assert len(client.batch_update_records_calls[0]) == 3
    assert result['updated'] == 3
    assert result['created'] == 0

    # cache updated with accumulated quantity snapshots
    h1 = storage.get_holding('AAPL', 'lx', 'futu')
    h2 = storage.get_holding('MSFT', 'sy', 'futu')
    assert h1 is not None and h1.quantity == 15
    assert h2 is not None and h2.quantity == 11

    # persistent cache mirror updated
    key_aapl = storage._get_holding_cache_key('AAPL', 'lx', 'futu')
    key_msft = storage._get_holding_cache_key('MSFT', 'sy', 'futu')
    assert local_idx.items[key_aapl]['quantity'] == 15
    assert local_idx.items[key_msft]['quantity'] == 11
    assert local_idx.flush_calls >= 1


def test_bulk_upsert_replace_mixed_update_create_updates_caches():
    client = StubBulkClient(
        initial_records=[
            {
                'record_id': 'rec_lx_700',
                'fields': {
                    'asset_id': '00700',
                    'asset_name': '腾讯控股',
                    'asset_type': 'hk_stock',
                    'account': 'lx',
                    'market': 'futu',
                    'quantity': 100,
                    'currency': 'HKD',
                },
            }
        ]
    )
    local_idx = StubLocalHoldingsIndexCache()
    storage = FeishuStorage(client=client, local_holdings_index_cache=local_idx)

    storage.preload_holdings_index(account='lx')

    payload = [
        Holding(asset_id='00700', asset_name='腾讯控股', asset_type=AssetType.HK_STOCK, account='lx', market='futu', quantity=80, currency='HKD'),
        Holding(asset_id='09988', asset_name='阿里巴巴', asset_type=AssetType.HK_STOCK, account='lx', market='futu', quantity=50, currency='HKD'),
    ]

    result = storage.upsert_holdings_bulk(payload, mode='replace')

    # replace mode: no additional preloads after explicit preload
    assert len(client.list_records_calls) == 1

    assert len(client.batch_update_records_calls) == 1
    assert len(client.batch_update_records_calls[0]) == 1
    assert len(client.batch_create_records_calls) == 1
    assert len(client.batch_create_records_calls[0]) == 1
    assert result['updated'] == 1
    assert result['created'] == 1

    # in-memory cache
    h_tencent = storage.get_holding('00700', 'lx', 'futu')
    h_baba = storage.get_holding('09988', 'lx', 'futu')
    assert h_tencent is not None and h_tencent.quantity == 80
    assert h_baba is not None and h_baba.record_id is not None and h_baba.quantity == 50

    # persistent cache mirror
    key_tencent = storage._get_holding_cache_key('00700', 'lx', 'futu')
    key_baba = storage._get_holding_cache_key('09988', 'lx', 'futu')
    assert local_idx.items[key_tencent]['quantity'] == 80
    assert local_idx.items[key_baba]['quantity'] == 50
