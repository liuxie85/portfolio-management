"""测试组合管理器"""
import pytest
from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import Mock, patch, MagicMock

from src.portfolio import PortfolioManager
from src.models import (
    Holding, Transaction, CashFlow, NAVHistory, PortfolioValuation,
    AssetType, TransactionType, AssetClass, Industry
)
from src.asset_utils import detect_asset_type


class TestPortfolioManagerInitialization:
    """测试组合管理器初始化"""

    def test_init_with_storage(self):
        """测试使用存储层初始化"""
        mock_storage = Mock()
        manager = PortfolioManager(storage=mock_storage)
        assert manager.storage == mock_storage

    def test_init_with_price_fetcher(self):
        """测试使用价格获取器初始化"""
        mock_storage = Mock()
        mock_fetcher = Mock()
        manager = PortfolioManager(storage=mock_storage, price_fetcher=mock_fetcher)
        assert manager.price_fetcher == mock_fetcher

    @patch('src.portfolio.PriceFetcher')
    def test_init_auto_create_fetcher(self, mock_fetcher_class):
        """测试自动创建价格获取器"""
        mock_fetcher = Mock()
        mock_fetcher_class.return_value = mock_fetcher
        mock_storage = Mock()

        manager = PortfolioManager(storage=mock_storage)

        assert manager.price_fetcher == mock_fetcher
        mock_fetcher_class.assert_called_once_with(storage=mock_storage)


class TestPortfolioManagerBuy:
    """测试买入操作"""

    def setup_method(self):
        self.mock_storage = Mock()
        self.mock_fetcher = Mock()
        self.manager = PortfolioManager(
            storage=self.mock_storage,
            price_fetcher=self.mock_fetcher
        )

    @patch.object(PortfolioManager, '_get_asset_name')
    def test_buy_success(self, mock_get_name):
        """测试买入成功"""
        mock_get_name.return_value = '平安银行'
        self.mock_storage.get_holding.return_value = None
        self.mock_storage.add_transaction.return_value = Transaction(
            tx_date=date(2025, 3, 14),
            tx_type=TransactionType.BUY,
            asset_id='000001',
            asset_name='平安银行',
            account='测试账户',
            quantity=1000,
            price=10.5,
            currency='CNY'
        )
        self.mock_storage.upsert_holding.return_value = Holding(
            asset_id='000001',
            asset_name='平安银行',
            asset_type=AssetType.A_STOCK,
            account='测试账户',
            quantity=1000,
            currency='CNY'
        )

        result = self.manager.buy(
            tx_date=date(2025, 3, 14),
            asset_id='000001',
            asset_name='平安',
            asset_type=AssetType.A_STOCK,
            account='测试账户',
            quantity=1000,
            price=10.5,
            currency='CNY',
            auto_deduct_cash=False  # 不扣减现金
        )

        assert result is not None
        assert result.asset_id == '000001'
        assert result.quantity == 1000
        self.mock_storage.add_transaction.assert_called_once()
        self.mock_storage.upsert_holding.assert_called_once()

    @patch.object(PortfolioManager, '_has_sufficient_cash')
    @patch.object(PortfolioManager, '_deduct_cash')
    @patch.object(PortfolioManager, '_get_asset_name')
    def test_buy_with_cash_deduction(self, mock_get_name, mock_deduct, mock_has_cash):
        """测试买入自动扣减现金"""
        mock_get_name.return_value = '平安银行'
        mock_has_cash.return_value = True  # 现金充足
        mock_deduct.return_value = True

        self.mock_storage.add_transaction.return_value = Mock()
        self.mock_storage.upsert_holding.return_value = Mock()

        result = self.manager.buy(
            tx_date=date(2025, 3, 14),
            asset_id='000001',
            asset_name='平安银行',
            asset_type=AssetType.A_STOCK,
            account='测试账户',
            quantity=1000,
            price=10.5,
            currency='CNY',
            fee=5.0,
            auto_deduct_cash=True
        )

        mock_has_cash.assert_called_once_with('测试账户', 10505.0)  # 1000 * 10.5 + 5
        mock_deduct.assert_called_once()
        # 扣减金额 = 1000 * 10.5 + 5 = 10505

    @patch.object(PortfolioManager, '_has_sufficient_cash')
    @patch.object(PortfolioManager, '_deduct_cash')
    @patch.object(PortfolioManager, '_get_asset_name')
    def test_buy_insufficient_cash(self, mock_get_name, mock_deduct, mock_has_cash):
        """测试买入现金不足"""
        mock_get_name.return_value = '平安银行'
        mock_has_cash.return_value = False  # 现金不足，应在扣减前检查

        with pytest.raises(ValueError) as exc_info:
            self.manager.buy(
                tx_date=date(2025, 3, 14),
                asset_id='000001',
                asset_name='平安银行',
                asset_type=AssetType.A_STOCK,
                account='测试账户',
                quantity=1000,
                price=10.5,
                currency='CNY',
                auto_deduct_cash=True
            )

        assert '现金不足' in str(exc_info.value)

    @patch.object(PortfolioManager, '_get_asset_name')
    def test_buy_non_cny_no_cash_deduction(self, mock_get_name):
        """测试非人民币买入不扣减现金"""
        mock_get_name.return_value = 'Apple Inc'
        self.mock_storage.add_transaction.return_value = Mock()
        self.mock_storage.upsert_holding.return_value = Mock()

        result = self.manager.buy(
            tx_date=date(2025, 3, 14),
            asset_id='AAPL',
            asset_name='Apple',
            asset_type=AssetType.US_STOCK,
            account='美股账户',
            quantity=100,
            price=175.0,
            currency='USD',
            auto_deduct_cash=True  # 即使设为True，非CNY也不扣减
        )

        # 不应该调用扣减现金

    @patch.object(PortfolioManager, '_get_asset_name')
    def test_buy_with_asset_class_and_industry(self, mock_get_name):
        """测试买入带资产类别和行业"""
        mock_get_name.return_value = '平安银行'
        self.mock_storage.add_transaction.return_value = Mock()
        self.mock_storage.upsert_holding.return_value = Mock()

        result = self.manager.buy(
            tx_date=date(2025, 3, 14),
            asset_id='000001',
            asset_name='平安银行',
            asset_type=AssetType.A_STOCK,
            account='测试账户',
            quantity=1000,
            price=10.5,
            currency='CNY',
            asset_class=AssetClass.CN_ASSET,
            industry=Industry.FINANCE,
            auto_deduct_cash=False
        )

        call_args = self.mock_storage.upsert_holding.call_args
        assert call_args[0][0].asset_class == AssetClass.CN_ASSET
        assert call_args[0][0].industry == Industry.FINANCE


class TestPortfolioManagerSell:
    """测试卖出操作"""

    def setup_method(self):
        self.mock_storage = Mock()
        self.mock_fetcher = Mock()
        self.manager = PortfolioManager(
            storage=self.mock_storage,
            price_fetcher=self.mock_fetcher
        )

    @patch.object(PortfolioManager, '_add_cash')
    def test_sell_success(self, mock_add_cash):
        """测试卖出成功"""
        self.mock_storage.get_holding.return_value = Holding(
            asset_id='000001',
            asset_name='平安银行',
            asset_type=AssetType.A_STOCK,
            account='测试账户',
            quantity=1000,
            currency='CNY'
        )
        self.mock_storage.add_transaction.return_value = Transaction(
            tx_date=date(2025, 3, 14),
            tx_type=TransactionType.SELL,
            asset_id='000001',
            asset_name='平安银行',
            account='测试账户',
            quantity=-500,
            price=11.0,
            currency='CNY'
        )

        result = self.manager.sell(
            tx_date=date(2025, 3, 14),
            asset_id='000001',
            account='测试账户',
            quantity=500,
            price=11.0,
            currency='CNY',
            auto_add_cash=False
        )

        assert result is not None
        assert result.quantity == -500
        self.mock_storage.update_holding_quantity.assert_called_once_with('000001', '测试账户', -500, None)

    @patch.object(PortfolioManager, '_add_cash')
    def test_sell_with_cash_addition(self, mock_add_cash):
        """测试卖出自动增加现金"""
        self.mock_storage.get_holding.return_value = Holding(
            asset_id='000001',
            asset_name='平安银行',
            asset_type=AssetType.A_STOCK,
            account='测试账户',
            quantity=1000,
            currency='CNY'
        )
        self.mock_storage.add_transaction.return_value = Mock()

        result = self.manager.sell(
            tx_date=date(2025, 3, 14),
            asset_id='000001',
            account='测试账户',
            quantity=500,
            price=11.0,
            currency='CNY',
            fee=5.0,
            auto_add_cash=True
        )

        mock_add_cash.assert_called_once()
        # 增加金额 = 500 * 11 - 5 = 5495

    def test_sell_no_holding(self):
        """测试卖出没有持仓的资产"""
        self.mock_storage.get_holding.return_value = None
        self.mock_fetcher.fetch.return_value = {'name': '未知资产'}
        self.mock_storage.add_transaction.return_value = Mock()

        result = self.manager.sell(
            tx_date=date(2025, 3, 14),
            asset_id='UNKNOWN',
            account='测试账户',
            quantity=100,
            price=10.0,
            currency='CNY'
        )

        assert result is not None

    def test_sell_delete_zero_holding(self):
        """测试卖出后持仓为0时删除"""
        self.mock_storage.get_holding.return_value = Holding(
            asset_id='000001',
            asset_name='平安银行',
            asset_type=AssetType.A_STOCK,
            account='测试账户',
            quantity=500,
            currency='CNY'
        )
        self.mock_storage.add_transaction.return_value = Mock()

        result = self.manager.sell(
            tx_date=date(2025, 3, 14),
            asset_id='000001',
            account='测试账户',
            quantity=500,  # 全部卖出
            price=11.0,
            currency='CNY'
        )

        self.mock_storage.delete_holding_if_zero.assert_called_once_with('000001', '测试账户', None)


class TestPortfolioManagerDepositWithdraw:
    """测试出入金操作"""

    def setup_method(self):
        self.mock_storage = Mock()
        self.manager = PortfolioManager(storage=self.mock_storage)

    def test_deposit(self):
        """测试入金"""
        self.mock_storage.add_cash_flow.return_value = CashFlow(
            flow_date=date(2025, 3, 14),
            account='测试账户',
            amount=100000,
            currency='CNY',
            flow_type='DEPOSIT'
        )
        self.mock_storage.get_holding.return_value = None
        self.mock_storage.upsert_holding.return_value = Mock()

        result = self.manager.deposit(
            flow_date=date(2025, 3, 14),
            account='测试账户',
            amount=100000,
            currency='CNY'
        )

        assert result is not None
        assert result.amount == 100000
        assert result.flow_type == 'DEPOSIT'
        self.mock_storage.add_cash_flow.assert_called_once()

    def test_deposit_with_exchange_rate(self):
        """测试入金带汇率"""
        self.mock_storage.add_cash_flow.return_value = Mock()
        self.mock_storage.get_holding.return_value = None
        self.mock_storage.upsert_holding.return_value = Mock()

        result = self.manager.deposit(
            flow_date=date(2025, 3, 14),
            account='测试账户',
            amount=10000,
            currency='USD',
            cny_amount=72000,
            exchange_rate=7.2
        )

        call_args = self.mock_storage.add_cash_flow.call_args
        assert call_args[0][0].cny_amount == 72000
        assert call_args[0][0].exchange_rate == 7.2

    def test_withdraw(self):
        """测试出金"""
        self.mock_storage.add_cash_flow.return_value = CashFlow(
            flow_date=date(2025, 3, 14),
            account='测试账户',
            amount=-50000,
            currency='CNY',
            flow_type='WITHDRAW'
        )
        self.mock_storage.get_holding.return_value = Holding(
            asset_id='CNY-CASH',
            asset_name='人民币现金',
            asset_type=AssetType.CASH,
            account='测试账户',
            quantity=100000,
            currency='CNY'
        )

        result = self.manager.withdraw(
            flow_date=date(2025, 3, 14),
            account='测试账户',
            amount=50000,
            currency='CNY'
        )

        assert result is not None
        assert result.amount == -50000
        assert result.flow_type == 'WITHDRAW'

    def test_update_cash_holding_cny(self):
        """测试更新人民币现金持仓"""
        self.mock_storage.get_holding.return_value = Holding(
            asset_id='CNY-CASH',
            asset_name='人民币现金',
            asset_type=AssetType.CASH,
            account='测试账户',
            quantity=50000,
            currency='CNY'
        )

        self.manager._update_cash_holding('测试账户', 10000, 'CNY', 10000)

        self.mock_storage.update_holding_quantity.assert_called_once()

    def test_update_cash_holding_usd(self):
        """测试更新美元现金持仓"""
        self.mock_storage.get_holding.return_value = None
        self.mock_storage.upsert_holding.return_value = Mock()

        self.manager._update_cash_holding('测试账户', 1000, 'USD', 7200)

        call_args = self.mock_storage.upsert_holding.call_args
        assert call_args[0][0].asset_id == 'USD-CASH'
        assert call_args[0][0].currency == 'USD'


class TestPortfolioManagerCashOperations:
    """测试现金操作"""

    def setup_method(self):
        self.mock_storage = Mock()
        self.manager = PortfolioManager(storage=self.mock_storage)

    def test_deduct_cash_success(self):
        """测试扣减现金成功"""
        self.mock_storage.get_holding.side_effect = [
            Holding(asset_id='CNY-CASH', asset_name='人民币现金', asset_type=AssetType.CASH, account='测试账户', quantity=10000, currency='CNY'),
            None  # 没有MMF
        ]

        result = self.manager._deduct_cash('测试账户', 5000)

        assert result == True
        self.mock_storage.update_holding_quantity.assert_called_once_with('CNY-CASH', '测试账户', -5000)

    def test_deduct_cash_with_mmf(self):
        """测试先扣现金再扣货币基金"""
        self.mock_storage.get_holding.side_effect = [
            Holding(asset_id='CNY-CASH', asset_name='人民币现金', asset_type=AssetType.CASH, account='测试账户', quantity=3000, currency='CNY'),
            Holding(asset_id='CNY-MMF', asset_name='货币基金', asset_type=AssetType.MMF, account='测试账户', quantity=10000, currency='CNY')
        ]

        result = self.manager._deduct_cash('测试账户', 5000)

        assert result == True
        # 现金扣3000，货币基金扣2000
        assert self.mock_storage.update_holding_quantity.call_count == 2

    def test_deduct_cash_insufficient(self):
        """测试现金不足"""
        self.mock_storage.get_holding.side_effect = [
            Holding(asset_id='CNY-CASH', asset_name='人民币现金', asset_type=AssetType.CASH, account='测试账户', quantity=1000, currency='CNY'),
            Holding(asset_id='CNY-MMF', asset_name='货币基金', asset_type=AssetType.MMF, account='测试账户', quantity=1000, currency='CNY')
        ]

        result = self.manager._deduct_cash('测试账户', 5000)

        assert result == False

    def test_deduct_cash_zero_amount(self):
        """测试扣减金额为0"""
        result = self.manager._deduct_cash('测试账户', 0)
        assert result == True
        self.mock_storage.get_holding.assert_not_called()

    def test_add_cash_existing(self):
        """测试增加现有现金持仓"""
        self.mock_storage.get_holding.return_value = Holding(
            asset_id='CNY-CASH',
            asset_name='人民币现金',
            asset_type=AssetType.CASH,
            account='测试账户',
            quantity=10000,
            currency='CNY'
        )

        result = self.manager._add_cash('测试账户', 5000)

        assert result == True
        self.mock_storage.update_holding_quantity.assert_called_once_with('CNY-CASH', '测试账户', 5000)

    def test_add_cash_create_new(self):
        """测试创建新现金持仓"""
        self.mock_storage.get_holding.return_value = None
        self.mock_storage.upsert_holding.return_value = Mock()

        result = self.manager._add_cash('测试账户', 5000)

        assert result == True
        call_args = self.mock_storage.upsert_holding.call_args
        assert call_args[0][0].asset_id == 'CNY-CASH'
        assert call_args[0][0].quantity == 5000

    def test_add_cash_zero_amount(self):
        """测试增加现金为0"""
        result = self.manager._add_cash('测试账户', 0)
        assert result == True
        self.mock_storage.get_holding.assert_not_called()


class TestPortfolioManagerValuation:
    """测试组合估值"""

    def setup_method(self):
        self.mock_storage = Mock()
        self.mock_fetcher = Mock()
        self.manager = PortfolioManager(
            storage=self.mock_storage,
            price_fetcher=self.mock_fetcher
        )

    def test_calculate_valuation_empty(self):
        """测试空仓估值"""
        self.mock_storage.get_holdings.return_value = []

        result = self.manager.calculate_valuation('测试账户')

        assert result.total_value_cny == 0
        assert result.holdings == []

    def test_calculate_valuation_with_prices(self):
        """测试有价格的估值计算"""
        self.mock_storage.get_holdings.return_value = [
            Holding(
                asset_id='000001',
                asset_name='平安银行',
                asset_type=AssetType.A_STOCK,
                account='测试账户',
                quantity=1000,
                currency='CNY',
                asset_class=AssetClass.CN_ASSET
            ),
            Holding(
                asset_id='CNY-CASH',
                asset_name='人民币现金',
                asset_type=AssetType.CASH,
                account='测试账户',
                quantity=50000,
                currency='CNY',
                asset_class=AssetClass.CASH
            )
        ]
        self.mock_fetcher.fetch_batch.return_value = {
            '000001': {'price': 10.5, 'cny_price': 10.5, 'currency': 'CNY'}
        }
        self.mock_storage.get_total_shares.return_value = 1000000

        result = self.manager.calculate_valuation('测试账户')

        assert result.total_value_cny == 60500.0  # 1000*10.5 + 50000
        assert result.stock_value_cny == 10500.0
        assert result.cash_value_cny == 50000.0
        assert result.cn_asset_value == 10500.0

    def test_calculate_valuation_no_fetcher(self):
        """测试无价格获取器时使用缓存"""
        self.mock_storage.get_holdings.return_value = [
            Holding(
                asset_id='000001',
                asset_name='平安银行',
                asset_type=AssetType.A_STOCK,
                account='测试账户',
                quantity=1000,
                currency='CNY'
            )
        ]
        self.mock_storage.get_price.return_value = Mock(
            cny_price=10.5,
            price=10.5,
            currency='CNY'
        )
        self.mock_storage.get_total_shares.return_value = 1000.0

        manager = PortfolioManager(storage=self.mock_storage, price_fetcher=None)
        result = manager.calculate_valuation('测试账户')

        assert result.total_value_cny == 10500.0

    def test_calculate_valuation_holding_weights(self):
        """测试持仓占比计算"""
        self.mock_storage.get_holdings.return_value = [
            Holding(
                asset_id='000001',
                asset_name='平安银行',
                asset_type=AssetType.A_STOCK,
                account='测试账户',
                quantity=1000,
                currency='CNY',
                asset_class=AssetClass.CN_ASSET,
                market_value_cny=10500.0
            ),
            Holding(
                asset_id='00700',
                asset_name='腾讯控股',
                asset_type=AssetType.HK_STOCK,
                account='测试账户',
                quantity=100,
                currency='HKD',
                asset_class=AssetClass.HK_ASSET,
                market_value_cny=40000.0
            )
        ]
        self.mock_fetcher.fetch_batch.return_value = {
            '000001': {'price': 10.5, 'cny_price': 10.5},
            '00700': {'price': 400, 'cny_price': 400}
        }
        self.mock_storage.get_total_shares.return_value = 1000.0

        result = self.manager.calculate_valuation('测试账户')

        # 检查持仓权重
        for holding in result.holdings:
            if holding.asset_id == '000001':
                assert holding.weight == round(10500.0 / 50500.0, 6)
            elif holding.asset_id == '00700':
                assert holding.weight == round(40000.0 / 50500.0, 6)


class TestPortfolioManagerAssetDistribution:
    """测试资产分布"""

    def setup_method(self):
        self.mock_storage = Mock()
        self.mock_fetcher = Mock()
        self.manager = PortfolioManager(
            storage=self.mock_storage,
            price_fetcher=self.mock_fetcher
        )

    def test_get_asset_distribution(self):
        """测试获取资产分布"""
        self.mock_storage.get_holdings.return_value = [
            Holding(
                asset_id='000001',
                asset_name='平安银行',
                asset_type=AssetType.A_STOCK,
                account='测试账户',
                quantity=1000,
                currency='CNY',
                asset_class=AssetClass.CN_ASSET
            ),
            Holding(
                asset_id='AAPL',
                asset_name='Apple Inc',
                asset_type=AssetType.US_STOCK,
                account='测试账户',
                quantity=100,
                currency='USD',
                asset_class=AssetClass.US_ASSET
            ),
            Holding(
                asset_id='CNY-CASH',
                asset_name='人民币现金',
                asset_type=AssetType.CASH,
                account='测试账户',
                quantity=50000,
                currency='CNY',
                asset_class=AssetClass.CASH
            )
        ]
        self.mock_fetcher.fetch_batch.return_value = {
            '000001': {'price': 10.5, 'cny_price': 10.5},
            'AAPL': {'price': 175, 'cny_price': 1260},
            'CNY-CASH': {'price': 1, 'cny_price': 1}
        }
        self.mock_storage.get_total_shares.return_value = 1000.0

        result = self.manager.get_asset_distribution('测试账户')

        assert '现金' in result
        assert '股票' in result
        # 总市值 = 10500(A股) + 126000(美股) + 50000(现金) = 186500
        total = 186500.0
        assert result['现金'] == 50000.0 / total
        assert result['股票'] == 136500.0 / total  # 10500 + 126000
        # 现金不属于中国资产/美国资产，只有股票按市场分类
        assert result['中国资产'] == 10500.0 / total  # 仅A股
        assert result['美国资产'] == 126000.0 / total  # 仅美股

    def test_get_industry_distribution(self):
        """测试获取行业分布"""
        self.mock_storage.get_holdings.return_value = [
            Holding(
                asset_id='000001',
                asset_name='平安银行',
                asset_type=AssetType.A_STOCK,
                account='测试账户',
                quantity=1000,
                currency='CNY',
                industry=Industry.FINANCE
            ),
            Holding(
                asset_id='00700',
                asset_name='腾讯控股',
                asset_type=AssetType.HK_STOCK,
                account='测试账户',
                quantity=100,
                currency='HKD',
                industry=Industry.INTERNET
            )
        ]
        self.mock_fetcher.fetch_batch = Mock(return_value={
            '000001': {'price': 10.5, 'cny_price': 10.5, 'currency': 'CNY'},
            '00700': {'price': 440, 'cny_price': 400, 'currency': 'HKD'}
        })

        result = self.manager.get_industry_distribution('测试账户')

        assert '金融' in result
        assert '互联网' in result


class TestPortfolioManagerNAVRecord:
    """测试净值记录"""

    def setup_method(self):
        self.mock_storage = Mock()
        self.mock_fetcher = Mock()
        self.manager = PortfolioManager(
            storage=self.mock_storage,
            price_fetcher=self.mock_fetcher
        )

    def test_record_nav_first_time(self):
        """测试首次记录净值"""
        valuation = PortfolioValuation(
            account='测试账户',
            total_value_cny=1000000.0,
            cash_value_cny=100000.0,
            stock_value_cny=900000.0
        )
        self.mock_storage.get_cash_flows.return_value = []
        self.mock_storage.save_nav.return_value = None
        self.mock_storage.get_nav_history.return_value = []  # 无历史记录
        self.mock_storage.get_latest_nav_before.return_value = None  # 无之前记录

        result = self.manager.record_nav('测试账户', valuation, nav_date=date(2025, 3, 14))

        assert result is not None
        assert result.total_value == 1000000.0
        assert result.shares == 1000000.0  # 首次，份额=净值
        assert result.nav == 1.0
        assert result.mtd_nav_change is None
        assert result.ytd_nav_change is None
        assert result.mtd_pnl is None
        assert result.ytd_pnl is None

    def test_record_nav_with_existing(self):
        """测试已有净值记录"""
        valuation = PortfolioValuation(
            account='测试账户',
            total_value_cny=1100000.0,
            cash_value_cny=100000.0,
            stock_value_cny=1000000.0
        )
        # 模拟已有净值记录
        existing_nav = NAVHistory(
            date=date(2025, 3, 13),
            account='测试账户',
            total_value=1000000.0,
            nav=1.0,
            shares=1000000.0
        )
        self.mock_storage.get_latest_nav_before.return_value = existing_nav
        self.mock_storage.get_cash_flows.return_value = []
        self.mock_storage.save_nav.return_value = None
        self.mock_storage.get_nav_history.return_value = [existing_nav]

        result = self.manager.record_nav('测试账户', valuation, nav_date=date(2025, 3, 14))

        assert result is not None
        # 无资金变动，份额不变
        assert result.shares == 1000000.0
        assert result.nav == 1.1  # 110万/100万份

    def test_record_nav_with_cash_flow(self):
        """测试有出入金的净值记录"""
        valuation = PortfolioValuation(
            account='测试账户',
            total_value_cny=1050000.0,
            cash_value_cny=150000.0,
            stock_value_cny=900000.0
        )
        existing_nav = NAVHistory(
            date=date(2025, 3, 13),
            account='测试账户',
            total_value=1000000.0,
            nav=1.0,
            shares=1000000.0
        )
        deposit = CashFlow(flow_date=date(2025, 3, 14), account='测试账户', amount=50000, currency='CNY', cny_amount=50000, flow_type='DEPOSIT')
        self.mock_storage.get_cash_flows.return_value = [deposit]
        self.mock_storage.save_nav.return_value = None
        self.mock_storage.get_nav_history.return_value = [existing_nav]

        result = self.manager.record_nav('测试账户', valuation, nav_date=date(2025, 3, 14))

        # 份额变动 = 50000 / 1.0 = 50000
        assert result.shares == 1050000.0
        # 净值 = 1050000 / 1050000 = 1.0
        assert result.nav == 1.0
        self.mock_storage.get_cash_flows.assert_called_once()

    def test_get_last_day_nav(self):
        """测试获取昨日净值"""
        yesterday_nav = NAVHistory(
            date=date(2025, 3, 13),
            account='测试账户',
            total_value=1000000.0,
            nav=1.0
        )
        self.mock_storage.get_nav_on_date.return_value = yesterday_nav

        result = self.manager._get_last_day_nav('测试账户', date(2025, 3, 14))

        assert result is not None
        assert result.date == date(2025, 3, 13)

    def test_get_daily_cash_flow(self):
        """测试获取当日资金变动"""
        self.mock_storage.get_cash_flows.return_value = [
            CashFlow(flow_date=date(2025, 3, 14), account='测试账户', amount=50000, currency='CNY', cny_amount=50000, flow_type='DEPOSIT'),
            CashFlow(flow_date=date(2025, 3, 14), account='测试账户', amount=-10000, currency='CNY', cny_amount=-10000, flow_type='WITHDRAW')
        ]

        result = self.manager._get_daily_cash_flow('测试账户', date(2025, 3, 14))

        assert result == 40000.0  # 50000 - 10000

    def test_summarize_cash_flows(self):
        """测试 record_nav 资金变动一次取数后内存汇总"""
        flows = [
            CashFlow(flow_date=date(2024, 12, 31), account='测试账户', amount=10000, currency='CNY', cny_amount=10000, flow_type='DEPOSIT'),
            CashFlow(flow_date=date(2025, 3, 1), account='测试账户', amount=20000, currency='CNY', cny_amount=20000, flow_type='DEPOSIT'),
            CashFlow(flow_date=date(2025, 3, 14), account='测试账户', amount=5000, currency='CNY', cny_amount=5000, flow_type='DEPOSIT'),
        ]
        self.mock_storage.get_cash_flows.return_value = flows
        last_nav = NAVHistory(date=date(2025, 3, 13), account='测试账户', total_value=1000000.0, nav=1.0, shares=1000000.0)

        result = self.manager._summarize_cash_flows(
            account='测试账户',
            today=date(2025, 3, 14),
            start_year=2024,
            last_nav=last_nav,
        )

        assert result['daily'] == 5000
        assert result['monthly'] == 25000
        assert result['yearly']['2024'] == 10000
        assert result['yearly']['2025'] == 25000
        assert result['cumulative'] == 35000
        assert result['gap'] == 5000
        self.mock_storage.get_cash_flows.assert_called_once_with('测试账户', date(2024, 1, 1), date(2025, 3, 14))

    def test_get_yearly_cash_flow(self):
        """测试获取当年资金变动"""
        self.mock_storage.get_cash_flows.return_value = [
            CashFlow(flow_date=date(2025, 1, 15), account='测试账户', amount=100000, currency='CNY', cny_amount=100000, flow_type='DEPOSIT'),
            CashFlow(flow_date=date(2025, 2, 15), account='测试账户', amount=50000, currency='CNY', cny_amount=50000, flow_type='DEPOSIT')
        ]

        result = self.manager._get_yearly_cash_flow('测试账户', '2025')

        assert result == 150000.0

    def test_get_initial_value(self):
        """测试获取初始值"""
        # 模拟最早的净值记录（净值接近1）
        self.mock_storage.get_nav_history.return_value = [
            NAVHistory(date=date(2024, 1, 2), account='测试账户', total_value=2317869.76, nav=1.0)
        ]

        result = self.manager._get_initial_value('测试账户')

        assert result == 2317869.76

    def test_get_initial_value_no_history(self):
        """测试无历史记录时使用默认值"""
        self.mock_storage.get_nav_history.return_value = []

        result = self.manager._get_initial_value('测试账户')

        assert result == 2317869.76  # 默认值

    def test_calc_period_return(self):
        """测试通用区间收益率计算"""
        assert self.manager._calc_period_return(1.1, 1.0) == 0.1
        assert self.manager._calc_period_return(1.1, None) == 0.0
        assert self.manager._calc_period_return(1.1, 0) == 0.0

    def test_calc_period_metrics_return_none_without_base(self):
        """测试缺基准时月/年收益与升值返回 None，而不是 0"""
        assert self.manager._calc_mtd_nav_change(1.1, None) is None
        assert self.manager._calc_ytd_nav_change(1.1, None) is None
        assert self.manager._calc_mtd_pnl(1000.0, None, 0.0) is None
        assert self.manager._calc_ytd_pnl(1000.0, None, 0.0) is None

    def test_decimal_quantize_helpers(self):
        """测试 Decimal 量化规则稳定"""
        assert self.manager._quantize_money('1.005') == Decimal('1.01')
        assert self.manager._quantize_nav('1.1234567') == Decimal('1.123457')
        assert self.manager._quantize_weight('0.3333336') == Decimal('0.333334')

    def test_nav_lookup_index(self):
        """测试 NAV 预索引查询结果正确"""
        navs = [
            NAVHistory(date=date(2024, 12, 31), account='测试账户', total_value=100.0, nav=1.0),
            NAVHistory(date=date(2025, 1, 2), account='测试账户', total_value=101.0, nav=1.01),
            NAVHistory(date=date(2025, 2, 28), account='测试账户', total_value=102.0, nav=1.02),
            NAVHistory(date=date(2025, 3, 13), account='测试账户', total_value=103.0, nav=1.03),
        ]
        index = self.manager._build_nav_lookup(navs)

        assert self.manager._find_latest_nav_before(navs, date(2025, 3, 14), nav_index=index).date == date(2025, 3, 13)
        assert self.manager._find_year_end_nav(navs, '2024', nav_index=index).date == date(2024, 12, 31)
        assert self.manager._find_year_end_nav(navs, '2023', nav_index=index) is None
        assert self.manager._find_prev_month_end_nav(navs, 2025, 3, nav_index=index).date == date(2025, 2, 28)

    def test_validate_nav_record_passes_for_consistent_data(self):
        """测试一致的 NAV 记录可通过自校验"""
        prev_month_end = NAVHistory(date=date(2025, 2, 28), account='测试账户', total_value=1000.0, nav=1.0)
        prev_year_end = NAVHistory(date=date(2024, 12, 31), account='测试账户', total_value=1000.0, nav=1.0)
        last_nav = NAVHistory(date=date(2025, 3, 13), account='测试账户', total_value=1000.0, nav=1.0, shares=1000.0)
        nav_record = NAVHistory(
            date=date(2025, 3, 14),
            account='测试账户',
            total_value=1100.0,
            cash_value=100.0,
            stock_value=1000.0,
            stock_weight=0.909091,
            cash_weight=0.090909,
            shares=1100.0,
            nav=1.0,
            cash_flow=100.0,
            share_change=100.0,
            mtd_nav_change=0.0,
            ytd_nav_change=0.0,
            mtd_pnl=0.0,
            ytd_pnl=0.0,
            details={'cumulative_appreciation': 0.0},
        )

        self.manager._validate_nav_record(
            nav_record=nav_record,
            last_nav=last_nav,
            prev_month_end_nav=prev_month_end,
            prev_year_end_nav=prev_year_end,
            daily_cash_flow=100.0,
            monthly_cash_flow=100.0,
            yearly_cash_flow=100.0,
            gap_cash_flow=100.0,
            initial_value=1000.0,
            cumulative_cash_flow=100.0,
        )

    def test_validate_nav_record_raises_for_inconsistent_total(self):
        """测试不一致的 NAV 记录会被自校验拦下"""
        nav_record = NAVHistory(
            date=date(2025, 3, 14),
            account='测试账户',
            total_value=1200.0,
            cash_value=100.0,
            stock_value=1000.0,
            stock_weight=0.9,
            cash_weight=0.1,
            shares=1200.0,
            nav=1.0,
            cash_flow=200.0,
            share_change=200.0,
            mtd_nav_change=0.0,
            ytd_nav_change=0.0,
            mtd_pnl=0.0,
            ytd_pnl=0.0,
            details={'cumulative_appreciation': 0.0},
        )

        with pytest.raises(ValueError, match='total_value 不等于 stock_value \+ cash_value'):
            self.manager._validate_nav_record(nav_record=nav_record)


class TestPortfolioManagerShares:
    """测试份额管理"""

    def setup_method(self):
        self.mock_storage = Mock()
        self.manager = PortfolioManager(storage=self.mock_storage)

    def test_get_shares(self):
        """测试获取份额"""
        self.mock_storage.get_total_shares.return_value = 1000000.0

        result = self.manager.get_shares('测试账户')

        assert result == 1000000.0

    def test_calculate_shares_change(self):
        """测试计算份额变动"""
        self.mock_storage.get_latest_nav.return_value = NAVHistory(
            date=date(2025, 3, 14),
            account='测试账户',
            total_value=1100000.0,
            nav=1.1
        )

        result = self.manager.calculate_shares_change('测试账户', 11000)

        assert result == 10000.0  # 11000 / 1.1

    def test_calculate_shares_change_no_nav(self):
        """测试无净值时份额变动计算"""
        self.mock_storage.get_latest_nav.return_value = None

        result = self.manager.calculate_shares_change('测试账户', 10000)

        assert result == 10000.0  # 使用默认值1.0

    def test_calculate_shares_change_zero_nav(self):
        """测试净值为0时的份额变动计算"""
        self.mock_storage.get_latest_nav.return_value = NAVHistory(
            date=date(2025, 3, 14),
            account='测试账户',
            total_value=0.0,
            nav=0
        )

        result = self.manager.calculate_shares_change('测试账户', 10000)

        assert result == 10000.0  # 使用默认值1.0


class TestPortfolioManagerAssetTypeDetection:
    """测试资产类型检测（已迁移到 asset_utils 模块）"""

    def test_detect_asset_type_cash(self):
        """测试现金类型检测"""
        asset_type, currency, _ = detect_asset_type('CNY-CASH')
        assert asset_type == AssetType.CASH
        assert currency == 'CNY'

    def test_detect_asset_type_a_stock(self):
        """测试A股类型检测"""
        assert detect_asset_type('000001')[0] == AssetType.A_STOCK
        assert detect_asset_type('600000')[0] == AssetType.A_STOCK
        assert detect_asset_type('300750')[0] == AssetType.A_STOCK
        # 科创板
        assert detect_asset_type('688981')[0] == AssetType.A_STOCK
        assert detect_asset_type('689009')[0] == AssetType.A_STOCK
        # 创业板注册制
        assert detect_asset_type('301039')[0] == AssetType.A_STOCK

    def test_detect_asset_type_hk_stock(self):
        """测试港股类型检测"""
        assert detect_asset_type('00700')[0] == AssetType.HK_STOCK
        assert detect_asset_type('09988')[0] == AssetType.HK_STOCK
        assert detect_asset_type('00001')[0] == AssetType.HK_STOCK

    def test_detect_asset_type_fund(self):
        """测试基金类型检测"""
        assert detect_asset_type('110022')[0] == AssetType.FUND

    def test_detect_asset_type_us_stock(self):
        """测试美股类型检测"""
        assert detect_asset_type('AAPL')[0] == AssetType.US_STOCK
        assert detect_asset_type('TSLA')[0] == AssetType.US_STOCK
