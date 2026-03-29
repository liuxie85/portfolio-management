"""Minimal (no pytest) tests for holdings preload/index cache behavior."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from src.feishu_storage import FeishuStorage
from src.models import Holding, AssetType


class StubHoldingsClient:
    def __init__(self, initial_records: Optional[List[Dict[str, Any]]] = None):
        self._records = list(initial_records or [])
        self.list_records_calls: List[Dict[str, Any]] = []
        self.update_record_calls: List[Dict[str, Any]] = []
        self.create_record_calls: List[Dict[str, Any]] = []

    def list_records(self, table_name: str, filter_str: str = None, field_names: List[str] = None, page_size: int = 500):
        self.list_records_calls.append(
            {
                "table_name": table_name,
                "filter_str": filter_str,
                "field_names": list(field_names or []),
                "page_size": page_size,
            }
        )
        assert table_name == "holdings"

        # Very small filter parser for tests: CurrentValue.[account] = "xxx"
        account = None
        if filter_str and 'CurrentValue.[account] = "' in filter_str:
            account = filter_str.split('CurrentValue.[account] = "', 1)[1].split('"', 1)[0]

        out = []
        for r in self._records:
            if account and (r.get("fields") or {}).get("account") != account:
                continue
            out.append({"record_id": r["record_id"], "fields": dict(r.get("fields") or {})})
        return out

    def update_record(self, table_name: str, record_id: str, fields: Dict[str, Any]):
        assert table_name == "holdings"
        self.update_record_calls.append({"record_id": record_id, "fields": dict(fields)})
        # mutate backing record to emulate server state
        for r in self._records:
            if r["record_id"] == record_id:
                r.setdefault("fields", {}).update(fields)
                break
        return {"record_id": record_id, "fields": dict(fields)}

    def create_record(self, table_name: str, fields: Dict[str, Any]):
        assert table_name == "holdings"
        self.create_record_calls.append({"fields": dict(fields)})
        new_id = f"rec_new_{len(self.create_record_calls)}"
        self._records.append({"record_id": new_id, "fields": dict(fields)})
        return {"record_id": new_id, "fields": dict(fields)}



def test_preload_builds_index_and_projection_and_avoids_refetch():
    client = StubHoldingsClient(
        initial_records=[
            {
                "record_id": "rec_1",
                "fields": {
                    "asset_id": "AAPL",
                    "asset_name": "Apple",
                    "asset_type": "us_stock",
                    "account": "lx",
                    "market": "futu",
                    "quantity": 10,
                    "currency": "USD",
                    "avg_cost": 150,
                },
            }
        ]
    )
    storage = FeishuStorage(client=client)

    result = storage.preload_holdings_index(account="lx")
    assert result["loaded"] == 1
    assert len(client.list_records_calls) == 1
    call = client.list_records_calls[0]
    assert call["field_names"] == storage.HOLDING_PROJECTION_FIELDS
    assert 'CurrentValue.[account] = "lx"' in (call["filter_str"] or "")

    # hit cache, no extra list_records
    h = storage.get_holding("AAPL", "lx", "futu")
    assert h is not None
    assert h.record_id == "rec_1"
    assert len(client.list_records_calls) == 1

    # missing under preloaded account should return None directly
    missing = storage.get_holding("MSFT", "lx", "futu")
    assert missing is None
    assert len(client.list_records_calls) == 1


def test_upsert_uses_preloaded_cache_for_batch_updates():
    client = StubHoldingsClient(
        initial_records=[
            {
                "record_id": "rec_1",
                "fields": {
                    "asset_id": "000001",
                    "asset_name": "平安银行",
                    "asset_type": "a_stock",
                    "account": "lx",
                    "market": "",
                    "quantity": 100,
                    "currency": "CNY",
                },
            }
        ]
    )
    storage = FeishuStorage(client=client)
    storage.preload_holdings_index(account="lx")

    h1 = Holding(
        asset_id="000001",
        asset_name="平安银行",
        asset_type=AssetType.A_STOCK,
        account="lx",
        market="",
        quantity=20,
        currency="CNY",
    )
    h2 = Holding(
        asset_id="000001",
        asset_name="平安银行",
        asset_type=AssetType.A_STOCK,
        account="lx",
        market="",
        quantity=30,
        currency="CNY",
    )

    storage.upsert_holding(h1)
    storage.upsert_holding(h2)

    # only preload triggered one list; each upsert should update by cache (no re-list)
    assert len(client.list_records_calls) == 1
    assert len(client.update_record_calls) == 2
    assert client.update_record_calls[0]["fields"]["quantity"] == 120
    assert client.update_record_calls[1]["fields"]["quantity"] == 150


def test_upsert_create_after_preload_missing_key_without_refetch():
    client = StubHoldingsClient(initial_records=[])
    storage = FeishuStorage(client=client)
    storage.preload_holdings_index(account="lx")

    h = Holding(
        asset_id="00700",
        asset_name="腾讯控股",
        asset_type=AssetType.HK_STOCK,
        account="lx",
        market="futu",
        quantity=50,
        currency="HKD",
    )
    created = storage.upsert_holding(h)

    assert created.record_id == "rec_new_1"
    assert len(client.list_records_calls) == 1  # preload only
    assert len(client.create_record_calls) == 1
