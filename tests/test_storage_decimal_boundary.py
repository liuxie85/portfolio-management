from unittest.mock import Mock, patch

from src.sqlite_storage import SQLiteStorage
from src.feishu_storage import FeishuStorage
from src.models import Holding, AssetType


class DummyConn:
    def __init__(self):
        self.calls = []
    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        return self
    def fetchone(self):
        return None
    def commit(self):
        return None
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return False


def test_sqlite_update_nav_fields_quantizes_values():
    storage = SQLiteStorage.__new__(SQLiteStorage)
    conn = DummyConn()
    storage._connect = Mock(return_value=conn)

    result = SQLiteStorage.update_nav_fields(storage, 'rec1', {
        'mtd_nav_change': 0.1234567,
        'mtd_pnl': 1.005,
        'cash_flow': 2.005,
    }, dry_run=True)

    assert result['fields']['mtd_nav_change'] == 0.123457
    assert result['fields']['mtd_pnl'] == 1.01
    assert result['fields']['cash_flow'] == 2.01


@patch('src.feishu_storage.FeishuClient')
def test_feishu_update_nav_fields_quantizes_values(mock_client_cls):
    client = Mock()
    storage = FeishuStorage(client=client)

    result = storage.update_nav_fields('rec1', {
        'ytd_nav_change': 0.1234567,
        'ytd_pnl': 1.005,
        'share_change': 2.005,
    }, dry_run=True)

    fields = result['fields']
    assert fields['ytd_nav_change'] == 0.123457
    assert fields['ytd_pnl'] == 1.01
    assert fields['share_change'] == 2.01


@patch('src.sqlite_storage.SQLiteStorage._init_db', return_value=None)
def test_sqlite_update_holding_quantity_quantizes_cash_like_only(_mock_init_db):
    storage = SQLiteStorage(db_path=':memory:')
    conn = DummyConn()
    storage._connect = Mock(return_value=conn)
    storage.get_holding = Mock(return_value=Holding(
        record_id='h1', asset_id='CNY-CASH', asset_name='人民币现金', asset_type=AssetType.CASH,
        account='测试账户', quantity=1.005, currency='CNY'
    ))

    storage.update_holding_quantity('CNY-CASH', '测试账户', 0.005)

    params = conn.calls[0][1]
    assert params[0] == 1.01


@patch('src.feishu_storage.FeishuClient')
def test_feishu_update_holding_quantity_quantizes_cash_like_only(mock_client_cls):
    client = Mock()
    storage = FeishuStorage(client=client)
    storage.get_holding = Mock(return_value=Holding(
        record_id='h1', asset_id='CNY-CASH', asset_name='人民币现金', asset_type=AssetType.CASH,
        account='测试账户', quantity=1.005, currency='CNY'
    ))

    storage.update_holding_quantity('CNY-CASH', '测试账户', 0.005)

    args = client.update_record.call_args[0]
    update_fields = args[2]
    assert update_fields['quantity'] == 1.01
