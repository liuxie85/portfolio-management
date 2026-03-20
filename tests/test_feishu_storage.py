"""测试飞书存储层"""
import pytest
from datetime import date, datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import json

from src.feishu_storage import FeishuStorage
from src.models import (
    Holding, Transaction, CashFlow, NAVHistory, PriceCache,
    AssetType, TransactionType, AssetClass, Industry
)


class TestFeishuStorageInitialization:
    """测试飞书存储层初始化"""

    def test_init_with_client(self):
        """测试使用客户端初始化"""
        mock_client = Mock()
        storage = FeishuStorage(client=mock_client)
        assert storage.client == mock_client

    @patch('src.feishu_storage.FeishuClient')
    def test_init_auto_create_client(self, mock_client_class):
        """测试自动创建客户端"""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        storage = FeishuStorage()
        assert storage.client == mock_client


class TestFeishuStorageFieldConversion:
    """测试飞书存储层字段转换"""

    def setup_method(self):
        """每个测试方法前执行"""
        self.storage = FeishuStorage(client=Mock())

    def test_to_feishu_fields_holdings(self):
        """测试持仓表字段转换"""
        data = {
            'asset_id': '000001',
            'asset_name': '平安银行',
            'quantity': 1000.5,
            'avg_cost': 10.5,
            'tag': ['银行', '金融']
        }
        result = self.storage._to_feishu_fields(data, 'holdings')

        assert result['asset_id'] == '000001'
        assert result['quantity'] == 1000.5  # 数字类型
        assert result['avg_cost'] == 10.5    # 数字类型
        assert result['tag'] == json.dumps(['银行', '金融'], ensure_ascii=False)

    def test_to_feishu_fields_transactions(self):
        """测试交易表字段转换（文本类型）"""
        data = {
            'asset_id': '000001',
            'quantity': 100,
            'price': 10.5,
            'amount': 1050.0,
            'fee': 5.0
        }
        result = self.storage._to_feishu_fields(data, 'transactions')

        assert result['quantity'] == '100'  # 文本类型
        assert result['price'] == '10.5'
        assert result['amount'] == '1050.0'
        assert result['fee'] == '5.0'

    def test_to_feishu_fields_dates(self):
        """测试日期字段转换为 Unix 时间戳（毫秒）"""
        data = {
            'tx_date': date(2025, 3, 14),
            'created_at': datetime(2025, 3, 14, 10, 30, 0)
        }
        result = self.storage._to_feishu_fields(data, 'transactions')

        # 日期字段应转换为 Unix 时间戳（毫秒）
        assert isinstance(result['tx_date'], int)
        # 验证时间戳对应正确日期
        from datetime import timezone
        restored_date = datetime.fromtimestamp(result['tx_date'] / 1000).date()
        assert restored_date == date(2025, 3, 14)
        # datetime 也应转换为时间戳
        assert isinstance(result['created_at'], int)
        restored_dt = datetime.fromtimestamp(result['created_at'] / 1000)
        assert restored_dt.date() == date(2025, 3, 14)

    def test_to_feishu_fields_enums(self):
        """测试枚举字段转换"""
        data = {
            'asset_type': AssetType.A_STOCK,
            'tx_type': TransactionType.BUY,
            'asset_class': AssetClass.CN_ASSET,
            'industry': Industry.FINANCE
        }
        result = self.storage._to_feishu_fields(data, 'transactions')

        assert result['asset_type'] == 'a_stock'
        assert result['tx_type'] == 'BUY'
        assert result['asset_class'] == '中国资产'
        assert result['industry'] == '金融'

    def test_to_feishu_fields_asset_id(self):
        """测试asset_id特殊处理"""
        data = {'asset_id': 123456}  # 数字类型
        result = self.storage._to_feishu_fields(data, 'holdings')

        assert result['asset_id'] == '123456'  # 转为字符串

    def test_from_feishu_fields_holdings(self):
        """测试持仓表字段反向转换"""
        fields = {
            'asset_id': '000001',
            'asset_name': '平安银行',
            'quantity': '1000.5',
            'avg_cost': '10.5',
            'tag': '["银行", "金融"]'
        }
        result = self.storage._from_feishu_fields(fields, 'holdings')

        assert result['asset_id'] == '000001'
        assert result['quantity'] == 1000.5
        assert result['avg_cost'] == 10.5
        assert result['tag'] == ['银行', '金融']

    def test_from_feishu_fields_transactions(self):
        """测试交易表字段反向转换"""
        fields = {
            'asset_id': '000001',
            'quantity': '100',
            'price': '10.5',
            'amount': '1050.0'
        }
        result = self.storage._from_feishu_fields(fields, 'transactions')

        assert result['quantity'] == 100.0
        assert result['price'] == 10.5
        assert result['amount'] == 1050.0

    def test_from_feishu_fields_nav_history_details(self):
        """测试净值表details字段"""
        details = {'daily_pnl': 1000.0, 'nav_change': 0.05}
        fields = {
            'total_value': '1000000',
            'details': json.dumps(details, ensure_ascii=False)
        }
        result = self.storage._from_feishu_fields(fields, 'nav_history')

        assert result['total_value'] == 1000000.0
        assert result['details'] == details

    def test_from_feishu_fields_none_values(self):
        """测试空值处理"""
        fields = {
            'asset_id': '000001',
            'avg_cost': None,
            'quantity': ''
        }
        result = self.storage._from_feishu_fields(fields, 'holdings')

        assert result['asset_id'] == '000001'
        assert result['avg_cost'] is None
        assert result['quantity'] == ''

    def test_zero_values_are_not_dropped_in_conversion(self):
        tx_fields = self.storage._from_feishu_fields({'amount': '0', 'fee': '0', 'tax': '0'}, 'transactions')
        cf_fields = self.storage._from_feishu_fields({'amount': '0', 'cny_amount': '0', 'exchange_rate': '0'}, 'cash_flow')
        price = self.storage._dict_to_price_cache({'asset_id': 'AAPL', 'price': 10.0, 'currency': 'USD', 'cny_price': 0.0, 'change': 0.0, 'change_pct': 0.0, 'exchange_rate': 0.0})

        assert tx_fields['amount'] == 0.0
        assert tx_fields['fee'] == 0.0
        assert tx_fields['tax'] == 0.0
        assert cf_fields['amount'] == 0.0
        assert cf_fields['cny_amount'] == 0.0
        assert cf_fields['exchange_rate'] == 0.0
        assert price.cny_price == 0.0
        assert price.change == 0.0
        assert price.change_pct == 0.0
        assert price.exchange_rate == 0.0


class TestFeishuStorageEscapeFilter:
    """测试飞书存储层filter转义"""

    def setup_method(self):
        self.storage = FeishuStorage(client=Mock())

    def test_escape_filter_simple(self):
        """测试简单字符串转义"""
        result = self.storage._escape_filter_value('test_value')
        assert result == 'test_value'

    def test_escape_filter_with_quotes(self):
        """测试带引号的转义"""
        result = self.storage._escape_filter_value('value with "quotes"')
        assert result == 'value with \\"quotes\\"'

    def test_escape_filter_with_backslash(self):
        """测试带反斜杠的转义"""
        result = self.storage._escape_filter_value('value with \\ backslash')
        # 反斜杠转义为 \\
        assert '\\\\' in result

    def test_escape_filter_non_string(self):
        """测试非字符串转义"""
        result = self.storage._escape_filter_value(123)
        assert result == '123'


class TestFeishuStorageHoldingOperations:
    """测试飞书存储层持仓操作"""

    def setup_method(self):
        self.mock_client = Mock()
        self.storage = FeishuStorage(client=self.mock_client)

    def test_get_holding_with_market(self):
        """测试获取指定市场的持仓"""
        self.mock_client.list_records.return_value = [{
            'record_id': 'rec_123',
            'fields': {
                'asset_id': '00700',
                'asset_name': '腾讯控股',
                'asset_type': 'hk_stock',
                'account': '港股账户',
                'market': '富途',
                'quantity': '100',
                'currency': 'HKD'
            }
        }]

        result = self.storage.get_holding('00700', '港股账户', market='富途')

        assert result is not None
        assert result.asset_id == '00700'
        assert result.market == '富途'
        assert result.quantity == 100.0

    def test_get_holding_without_market(self):
        """测试获取持仓（不指定市场）"""
        self.mock_client.list_records.return_value = [
            {
                'record_id': 'rec_1',
                'fields': {
                    'asset_id': '000001',
                    'market': '华泰',
                    'quantity': '100',
                    'currency': 'CNY'
                }
            },
            {
                'record_id': 'rec_2',
                'fields': {
                    'asset_id': '000001',
                    'market': '',
                    'quantity': '200',
                    'currency': 'CNY'
                }
            }
        ]

        result = self.storage.get_holding('000001', '测试账户')

        # 应该返回market为空的记录
        assert result is not None
        assert result.record_id == 'rec_2'
        assert result.market == ""

    def test_get_holding_not_found(self):
        """测试持仓不存在"""
        self.mock_client.list_records.return_value = []

        result = self.storage.get_holding('999999', '测试账户')

        assert result is None

    def test_get_holdings(self):
        """测试获取持仓列表"""
        self.mock_client.list_records.return_value = [
            {
                'record_id': 'rec_1',
                'fields': {
                    'asset_id': '000001',
                    'asset_name': '平安银行',
                    'asset_type': 'a_stock',
                    'account': '测试账户',
                    'quantity': '1000',
                    'currency': 'CNY'
                }
            },
            {
                'record_id': 'rec_2',
                'fields': {
                    'asset_id': '00700',
                    'asset_name': '腾讯控股',
                    'asset_type': 'hk_stock',
                    'account': '测试账户',
                    'quantity': '0',  # 应该被过滤掉
                    'currency': 'HKD'
                }
            }
        ]

        holdings = self.storage.get_holdings(account='测试账户')

        assert len(holdings) == 1  # 数量为0的被过滤
        assert holdings[0].asset_id == '000001'

    def test_get_holdings_include_empty(self):
        """测试获取持仓列表包含空仓"""
        self.mock_client.list_records.return_value = [
            {
                'record_id': 'rec_1',
                'fields': {
                    'asset_id': '000001',
                    'quantity': '100',
                    'currency': 'CNY'
                }
            },
            {
                'record_id': 'rec_2',
                'fields': {
                    'asset_id': '000002',
                    'quantity': '0',
                    'currency': 'CNY'
                }
            }
        ]

        holdings = self.storage.get_holdings(include_empty=True)

        assert len(holdings) == 2  # 包含数量为0的

    def test_upsert_holding_create(self):
        """测试创建新持仓"""
        self.mock_client.list_records.return_value = []  # 不存在
        self.mock_client.create_record.return_value = {
            'record_id': 'new_rec_123',
            'fields': {}
        }

        holding = Holding(
            asset_id='000001',
            asset_name='平安银行',
            asset_type=AssetType.A_STOCK,
            account='测试账户',
            quantity=1000,
            currency='CNY'
        )

        result = self.storage.upsert_holding(holding)

        assert result.record_id == 'new_rec_123'
        self.mock_client.create_record.assert_called_once()

    def test_upsert_holding_update(self):
        """测试更新现有持仓"""
        self.mock_client.list_records.return_value = [{
            'record_id': 'existing_rec',
            'fields': {
                'asset_id': '000001',
                'asset_name': '平安',
                'quantity': '500',
                'currency': 'CNY'
            }
        }]
        self.mock_client.update_record.return_value = {
            'record_id': 'existing_rec',
            'fields': {}
        }

        holding = Holding(
            asset_id='000001',
            asset_name='平安银行股份有限公司',
            asset_type=AssetType.A_STOCK,
            account='测试账户',
            quantity=1000,
            currency='CNY'
        )

        result = self.storage.upsert_holding(holding)

        assert result.record_id == 'existing_rec'
        self.mock_client.update_record.assert_called_once()

    def test_update_holding_quantity(self):
        """测试更新持仓数量"""
        self.mock_client.list_records.return_value = [{
            'record_id': 'rec_123',
            'fields': {'quantity': '1000', 'currency': 'CNY'}
        }]

        self.storage.update_holding_quantity('000001', '测试账户', 500)

        self.mock_client.update_record.assert_called_once()
        call_args = self.mock_client.update_record.call_args
        assert call_args[0][2]['quantity'] == 1500  # 1000 + 500

    def test_delete_holding_if_zero(self):
        """测试持仓为0时删除"""
        self.mock_client.list_records.return_value = [{
            'record_id': 'rec_123',
            'fields': {'quantity': '0', 'currency': 'CNY'}
        }]

        self.storage.delete_holding_if_zero('000001', '测试账户')

        self.mock_client.delete_record.assert_called_once_with('holdings', 'rec_123')

    def test_delete_holding_if_not_zero(self):
        """测试持仓不为0时不删除"""
        self.mock_client.list_records.return_value = [{
            'record_id': 'rec_123',
            'fields': {'quantity': '100', 'currency': 'CNY'}
        }]

        self.storage.delete_holding_if_zero('000001', '测试账户')

        self.mock_client.delete_record.assert_not_called()

    def test_delete_holding_if_tiny_residual(self):
        """测试极小残值持仓会被视为零并删除"""
        self.mock_client.list_records.return_value = [{
            'record_id': 'rec_123',
            'fields': {'quantity': '0.0000000001', 'currency': 'CNY'}
        }]

        self.storage.delete_holding_if_zero('000001', '测试账户')

        self.mock_client.delete_record.assert_called_once_with('holdings', 'rec_123')

    def test_delete_holding_by_record_id(self):
        """测试通过记录ID删除持仓"""
        self.mock_client.delete_record.return_value = True

        result = self.storage.delete_holding_by_record_id('rec_123')

        assert result == True
        self.mock_client.delete_record.assert_called_once_with('holdings', 'rec_123')


class TestFeishuStorageTransactionOperations:
    """测试飞书存储层交易操作"""

    def setup_method(self):
        self.mock_client = Mock()
        self.storage = FeishuStorage(client=self.mock_client)

    def test_add_transaction(self):
        """测试添加交易记录"""
        self.mock_client.list_records.return_value = []
        self.mock_client.create_record.return_value = {
            'record_id': 'tx_rec_123',
            'fields': {}
        }

        tx = Transaction(
            tx_date=date(2025, 3, 14),
            tx_type=TransactionType.BUY,
            asset_id='000001',
            asset_name='平安银行',
            account='测试账户',
            quantity=1000,
            price=10.5,
            currency='CNY'
        )

        result = self.storage.add_transaction(tx)

        assert result.record_id == 'tx_rec_123'

    def test_add_transaction_with_request_id(self):
        """测试带request_id的交易（幂等性）"""
        # 模拟已存在相同request_id的记录
        self.mock_client.list_records.return_value = [{
            'record_id': 'existing_tx',
            'fields': {'request_id': 'req_123', 'asset_id': '000001'}
        }]

        tx = Transaction(
            tx_date=date(2025, 3, 14),
            tx_type=TransactionType.BUY,
            asset_id='000001',
            account='测试账户',
            quantity=1000,
            price=10.5,
            currency='CNY',
            request_id='req_123'
        )

        result = self.storage.add_transaction(tx)

        assert result.record_id == 'existing_tx'
        self.mock_client.create_record.assert_not_called()

    def test_add_transaction_raises_when_idempotency_fields_missing(self):
        tx = Transaction(
            tx_date=date(2025, 3, 14),
            tx_type=TransactionType.BUY,
            asset_id='000001',
            account='测试账户',
            quantity=1000,
            price=10.5,
            currency='CNY',
            request_id='req_123'
        )
        self.mock_client.list_records.side_effect = Exception('FieldNameNotFound')

        with pytest.raises(ValueError, match='缺少 request_id 字段'):
            self.storage.add_transaction(tx)

    def test_get_transaction(self):
        """测试获取单条交易记录"""
        self.mock_client.get_record.return_value = {
            'record_id': 'tx_rec',
            'fields': {
                'asset_id': '000001',
                'tx_date': '2025-03-14',
                'tx_type': 'BUY',
                'quantity': '1000',
                'price': '10.5',
                'currency': 'CNY'
            }
        }

        result = self.storage.get_transaction('tx_rec')

        assert result is not None
        assert result.asset_id == '000001'
        assert result.tx_type == TransactionType.BUY

    def test_get_transaction_not_found(self):
        """测试交易记录不存在"""
        self.mock_client.get_record.return_value = None

        result = self.storage.get_transaction('non_existent')

        assert result is None

    def test_get_transactions(self):
        """测试获取交易记录列表"""
        self.mock_client.list_records.return_value = [
            {
                'record_id': 'tx_1',
                'fields': {
                    'tx_date': '2025-03-14',
                    'asset_id': '000001',
                    'tx_type': 'BUY',
                    'quantity': '1000',
                    'price': '10.5',
                    'currency': 'CNY'
                }
            },
            {
                'record_id': 'tx_2',
                'fields': {
                    'tx_date': '2025-03-13',
                    'asset_id': '000002',
                    'tx_type': 'SELL',
                    'quantity': '-500',
                    'price': '11.0',
                    'currency': 'CNY'
                }
            }
        ]

        transactions = self.storage.get_transactions(account='测试账户')

        assert len(transactions) == 2
        # 按日期倒序排列
        assert transactions[0].tx_date == date(2025, 3, 14)
        assert transactions[1].tx_date == date(2025, 3, 13)

    def test_get_transactions_with_filter(self):
        """测试带筛选条件的交易查询"""
        self.mock_client.list_records.return_value = []

        self.storage.get_transactions(
            account='测试账户',
            start_date=date(2025, 3, 1),
            end_date=date(2025, 3, 14),
            tx_type='BUY'
        )

        call_args = self.mock_client.list_records.call_args
        assert 'filter_str' in call_args.kwargs
        filter_str = call_args.kwargs['filter_str']
        assert '测试账户' in filter_str
        assert 'BUY' in filter_str

    def test_delete_transaction_by_record_id(self):
        """测试通过记录ID删除交易"""
        self.mock_client.delete_record.return_value = True

        result = self.storage.delete_transaction_by_record_id('tx_rec')

        assert result == True
        self.mock_client.delete_record.assert_called_once_with('transactions', 'tx_rec')


class TestFeishuStorageCashFlowOperations:
    """测试飞书存储层出入金操作"""

    def setup_method(self):
        self.mock_client = Mock()
        self.storage = FeishuStorage(client=self.mock_client)

    def test_add_cash_flow(self):
        """测试添加出入金记录"""
        self.mock_client.list_records.return_value = []
        self.mock_client.create_record.return_value = {
            'record_id': 'cf_rec_123',
            'fields': {}
        }

        cf = CashFlow(
            flow_date=date(2025, 3, 14),
            account='测试账户',
            amount=100000,
            currency='CNY',
            cny_amount=100000,
            flow_type='DEPOSIT'
        )

        result = self.storage.add_cash_flow(cf)

        assert result.record_id == 'cf_rec_123'

    def test_add_cash_flow_raises_when_dedup_key_field_missing(self):
        self.mock_client.list_records.side_effect = Exception('FieldNameNotFound')

        cf = CashFlow(
            flow_date=date(2025, 3, 14),
            account='测试账户',
            amount=100000,
            currency='CNY',
            cny_amount=100000,
            flow_type='DEPOSIT'
        )

        with pytest.raises(ValueError, match='缺少 dedup_key 字段'):
            self.storage.add_cash_flow(cf)

    def test_get_cash_flow(self):
        """测试获取单条出入金记录"""
        self.mock_client.get_record.return_value = {
            'record_id': 'cf_rec',
            'fields': {
                'flow_date': '2025-03-14',
                'amount': '100000',
                'currency': 'CNY'
            }
        }

        result = self.storage.get_cash_flow('cf_rec')

        assert result is not None
        assert result.amount == 100000.0

    def test_get_cash_flows(self):
        """测试获取出入金记录列表"""
        self.mock_client.list_records.return_value = [
            {
                'record_id': 'cf_1',
                'fields': {
                    'flow_date': '2025-03-14',
                    'amount': '100000',
                    'cny_amount': '100000',
                    'currency': 'CNY'
                }
            },
            {
                'record_id': 'cf_2',
                'fields': {
                    'flow_date': '2025-03-13',
                    'amount': '-50000',
                    'cny_amount': '-50000',
                    'currency': 'CNY'
                }
            }
        ]

        flows = self.storage.get_cash_flows(
            account='测试账户',
            start_date=date(2025, 3, 1),
            end_date=date(2025, 3, 14)
        )

        assert len(flows) == 2

    def test_get_total_cash_flow_cny(self):
        """测试获取累计出入金总额"""
        self.mock_client.list_records.return_value = [
            {'fields': {'amount': '100000', 'cny_amount': '100000'}},
            {'fields': {'amount': '-30000', 'cny_amount': '-30000'}},
            {'fields': {'amount': '50000', 'cny_amount': '50000'}}
        ]

        total = self.storage.get_total_cash_flow_cny('测试账户')

        assert total == 120000.0  # 100000 - 30000 + 50000

    def test_delete_cash_flow_by_record_id(self):
        """测试通过记录ID删除出入金"""
        self.mock_client.delete_record.return_value = True

        result = self.storage.delete_cash_flow_by_record_id('cf_rec')

        assert result == True


class TestFeishuStorageNAVOperations:
    """测试飞书存储层净值操作"""

    def setup_method(self):
        self.mock_client = Mock()
        self.storage = FeishuStorage(client=self.mock_client)

    def test_save_nav_create(self):
        """测试保存新净值记录"""
        self.mock_client.list_records.return_value = []  # 不存在
        self.mock_client.create_record.return_value = {
            'record_id': 'nav_rec_123',
            'fields': {}
        }

        nav = NAVHistory(
            date=date(2025, 3, 14),
            account='测试账户',
            total_value=1000000.0,
            cash_value=100000.0,
            stock_value=900000.0,
            shares=1000000.0,
            nav=1.0
        )

        self.storage.save_nav(nav)

        assert nav.record_id == 'nav_rec_123'
        self.mock_client.create_record.assert_called_once()

    def test_save_nav_update(self):
        """测试更新现有净值记录"""
        self.mock_client.list_records.return_value = [{
            'record_id': 'existing_nav',
            'fields': {'date': '2025-03-14', 'nav': '0.95'}
        }]
        self.mock_client.update_record.return_value = {
            'record_id': 'existing_nav',
            'fields': {}
        }

        nav = NAVHistory(
            date=date(2025, 3, 14),
            account='测试账户',
            total_value=1000000.0,
            nav=1.0
        )

        self.storage.save_nav(nav)

        assert nav.record_id == 'existing_nav'
        self.mock_client.update_record.assert_called_once()

    def test_get_nav_history(self):
        """测试获取净值历史"""
        from datetime import timedelta
        today = date.today()
        yesterday = today - timedelta(days=1)
        day_before = today - timedelta(days=2)

        self.mock_client.list_records.return_value = [
            {
                'record_id': 'nav_1',
                'fields': {
                    'date': today.isoformat(),
                    'total_value': '1000000',
                    'nav': '1.0'
                }
            },
            {
                'record_id': 'nav_2',
                'fields': {
                    'date': yesterday.isoformat(),
                    'total_value': '990000',
                    'nav': '0.99'
                }
            }
        ]

        navs = self.storage.get_nav_history('测试账户', days=30)

        assert len(navs) == 2
        # 按日期正序排列
        assert navs[0].date == yesterday
        assert navs[1].date == today

    def test_get_latest_nav(self):
        """测试获取最新净值"""
        self.mock_client.list_records.return_value = [
            {
                'record_id': 'nav_1',
                'fields': {'date': '2025-03-13', 'nav': '0.99'}
            },
            {
                'record_id': 'nav_2',
                'fields': {'date': '2025-03-14', 'nav': '1.0'}
            },
            {
                'record_id': 'nav_3',
                'fields': {'date': '2025-03-12', 'nav': '0.98'}
            }
        ]

        result = self.storage.get_latest_nav('测试账户')

        assert result is not None
        assert result.date == date(2025, 3, 14)
        assert result.nav == 1.0

    def test_get_nav_on_date(self):
        """测试获取指定日期的净值"""
        self.mock_client.list_records.return_value = [{
            'record_id': 'nav_1',
            'fields': {'date': '2025-03-14', 'nav': '1.0'}
        }]

        result = self.storage.get_nav_on_date('测试账户', date(2025, 3, 14))

        assert result is not None
        assert result.date == date(2025, 3, 14)

    def test_get_latest_nav_before(self):
        """测试获取指定日期前的最新净值"""
        self.mock_client.list_records.return_value = [
            {
                'record_id': 'nav_1',
                'fields': {'date': '2025-03-12', 'nav': '0.98'}
            },
            {
                'record_id': 'nav_2',
                'fields': {'date': '2025-03-13', 'nav': '0.99'}
            }
        ]

        result = self.storage.get_latest_nav_before('测试账户', date(2025, 3, 14))

        assert result is not None
        assert result.date == date(2025, 3, 13)

    def test_get_total_shares(self):
        """测试获取总份额"""
        self.mock_client.list_records.return_value = [{
            'record_id': 'nav_1',
            'fields': {'date': '2025-03-14', 'shares': '1000000', 'nav': '1.0'}
        }]

        shares = self.storage.get_total_shares('测试账户')

        assert shares == 1000000.0

    def test_delete_nav_by_record_id(self):
        """测试通过记录ID删除净值"""
        self.mock_client.delete_record.return_value = True

        result = self.storage.delete_nav_by_record_id('nav_rec')

        assert result == True


class TestFeishuStoragePriceOperations:
    """测试价格缓存操作（已迁移到本地文件缓存）"""

    def setup_method(self):
        self.mock_client = Mock()
        self.storage = FeishuStorage(client=self.mock_client)
        self.mock_local_cache = Mock()
        self.storage._local_price_cache = self.mock_local_cache

    def test_get_price_valid(self):
        """测试获取有效价格缓存"""
        self.mock_local_cache.get.return_value = PriceCache(
            asset_id='000001',
            asset_name='平安银行',
            asset_type=AssetType.A_STOCK,
            price=10.5,
            currency='CNY',
            cny_price=10.5
        )

        result = self.storage.get_price('000001')

        assert result is not None
        assert result.price == 10.5
        self.mock_local_cache.get.assert_called_once_with('000001')

    def test_get_price_expired(self):
        """测试获取过期价格缓存返回None"""
        self.mock_local_cache.get.return_value = None

        result = self.storage.get_price('000001')

        assert result is None

    def test_get_price_not_found(self):
        """测试价格缓存不存在"""
        self.mock_local_cache.get.return_value = None

        result = self.storage.get_price('999999')

        assert result is None

    def test_save_price_create(self):
        """测试保存价格缓存"""
        price = PriceCache(
            asset_id='000001',
            asset_name='平安银行',
            asset_type=AssetType.A_STOCK,
            price=10.5,
            currency='CNY',
            cny_price=10.5
        )

        self.storage.save_price(price)

        self.mock_local_cache.save.assert_called_once_with(price)

    def test_save_price_update(self):
        """测试更新价格缓存"""
        price = PriceCache(
            asset_id='000001',
            asset_type=AssetType.A_STOCK,
            price=10.5,
            currency='CNY',
            cny_price=10.5
        )

        self.storage.save_price(price)

        self.mock_local_cache.save.assert_called_once_with(price)

    def test_get_all_prices(self):
        """测试获取所有有效价格缓存"""
        self.mock_local_cache.get_all.return_value = [
            PriceCache(asset_id='000001', price=10.5, currency='CNY', cny_price=10.5),
            PriceCache(asset_id='00700', price=400.0, currency='HKD', cny_price=400.0),
        ]

        prices = self.storage.get_all_prices()

        assert len(prices) == 2
        self.mock_local_cache.get_all.assert_called_once()

    def test_get_all_prices_filter_expired(self):
        """测试本地缓存自动过滤过期价格"""
        self.mock_local_cache.get_all.return_value = [
            PriceCache(asset_id='000001', price=10.5, currency='CNY', cny_price=10.5),
        ]

        prices = self.storage.get_all_prices()

        assert len(prices) == 1
        assert prices[0].asset_id == '000001'
