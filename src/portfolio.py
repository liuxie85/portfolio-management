"""
组合计算逻辑
"""
from datetime import date, datetime

from .time_utils import bj_today
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Optional

from .models import (
    Holding, Transaction, CashFlow, NAVHistory,
    PortfolioValuation, AssetType, TransactionType, AssetClass,
    CASH_ASSET_ID, MMF_ASSET_ID
)
from .price_fetcher import PriceFetcher
from .reporting_utils import normalize_holding_type
from . import config


def _snapshot_digest(snapshots) -> str:
    """Compute a stable digest of holdings snapshot content.

    We intentionally ignore fields that may change due to pricing noise if desired.
    Current policy: include quantity + market_value_cny + currency + market + asset_id.
    """
    import json
    import hashlib

    items = []
    for s in snapshots:
        items.append({
            'account': s.account,
            'as_of': s.as_of,
            'asset_id': s.asset_id,
            'market': s.market,
            'currency': s.currency,
            'quantity': s.quantity,
            'market_value_cny': s.market_value_cny,
        })
    items.sort(key=lambda x: (x['account'], x['as_of'], x['market'], x['asset_id']))
    raw = json.dumps(items, ensure_ascii=False, separators=(',', ':'), sort_keys=True).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()



class PortfolioManager:
    """组合管理器"""

    MONEY_QUANT = Decimal('0.01')
    NAV_QUANT = Decimal('0.000001')
    WEIGHT_QUANT = Decimal('0.000001')

    def __init__(self, storage: Any, price_fetcher: Optional[PriceFetcher] = None):
        self.storage = storage
        self.price_fetcher = price_fetcher or PriceFetcher(storage=storage)

    @staticmethod
    def _to_decimal(value: Any) -> Decimal:
        if value is None:
            return Decimal('0')
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    @classmethod
    def _quantize_money(cls, value: Any) -> Decimal:
        return cls._to_decimal(value).quantize(cls.MONEY_QUANT, rounding=ROUND_HALF_UP)

    @classmethod
    def _quantize_nav(cls, value: Any) -> Decimal:
        return cls._to_decimal(value).quantize(cls.NAV_QUANT, rounding=ROUND_HALF_UP)

    @classmethod
    def _quantize_weight(cls, value: Any) -> Decimal:
        return cls._to_decimal(value).quantize(cls.WEIGHT_QUANT, rounding=ROUND_HALF_UP)

    @classmethod
    def _normalize_transaction_payload(cls, *, quantity: Any, price: Any, fee: Any = 0.0) -> Dict[str, float]:
        quantity_dec = cls._to_decimal(quantity)
        price_dec = cls._quantize_money(price)
        fee_dec = cls._quantize_money(fee)
        amount_dec = cls._quantize_money(quantity_dec * price_dec)
        return {
            'quantity': float(quantity_dec),
            'price': float(price_dec),
            'fee': float(fee_dec),
            'amount': float(amount_dec),
        }

    @classmethod
    def _normalize_cash_flow_payload(cls, *, amount: Any, currency: str = 'CNY', cny_amount: Any = None, exchange_rate: Any = None) -> Dict[str, Optional[float]]:
        amount_dec = cls._quantize_money(amount)

        normalized_currency = (currency or 'CNY').upper()
        if normalized_currency != 'CNY' and cny_amount is None and exchange_rate is None:
            raise ValueError(f"外币现金流必须显式提供 cny_amount 或 exchange_rate: currency={normalized_currency}")

        rate_dec = cls._to_decimal(exchange_rate) if exchange_rate is not None else Decimal('1')
        if cny_amount is not None:
            cny_amount_dec = cls._quantize_money(cny_amount)
        else:
            cny_amount_dec = cls._quantize_money(amount_dec * rate_dec)

        return {
            'amount': float(amount_dec),
            'cny_amount': float(cny_amount_dec),
            'exchange_rate': float(rate_dec),
        }

    @classmethod
    def _normalize_holding_payload(cls, *, quantity: Any, avg_cost: Any = None, cash_like: bool = False) -> Dict[str, Optional[float]]:
        quantity_dec = cls._quantize_money(quantity) if cash_like else cls._to_decimal(quantity)
        avg_cost_dec = cls._quantize_money(avg_cost) if avg_cost is not None else None
        return {
            'quantity': float(quantity_dec),
            'avg_cost': float(avg_cost_dec) if avg_cost_dec is not None else None,
        }

    # ========== 交易处理 ==========

    def _get_asset_name(self, asset_id: str, asset_type: AssetType, user_provided_name: str = None, timeout: float = 5.0) -> str:
        """根据代码获取资产完整名称

        Args:
            asset_id: 资产代码
            asset_type: 资产类型
            user_provided_name: 用户提供的名称（作为备选）
            timeout: 超时时间（秒），默认5秒

        Returns:
            资产完整名称
        """
        import threading

        # 用守护线程 + join(timeout) 做超时控制，避免嵌套 ThreadPoolExecutor 死锁
        _result = {'data': None, 'error': None}

        def _do_fetch():
            try:
                _result['data'] = self.price_fetcher.fetch(asset_id)
            except Exception as e:
                _result['error'] = e

        t = threading.Thread(target=_do_fetch, daemon=True)
        t.start()
        t.join(timeout=timeout)

        if _result['data'] and _result['data'].get('name'):
            return _result['data']['name']
        if t.is_alive():
            print(f"[警告] 获取资产名称超时 {asset_id}，使用备选名称")
        elif _result['error']:
            print(f"[警告] 获取资产名称失败 {asset_id}: {_result['error']}")


        # 如果获取失败，使用用户提供的名称
        if user_provided_name:
            return user_provided_name

        # 最后返回代码作为名称
        return asset_id

    def buy(self, tx_date: date, asset_id: str, asset_name: str, asset_type: AssetType,
            account: str, quantity: float, price: float, currency: str,
            market: Optional[str] = None, fee: float = 0, remark: str = "",
            asset_class: Optional[AssetClass] = None, industry: Optional[str] = None,
            auto_deduct_cash: bool = True, request_id: str = None) -> Transaction:
        """
        买入资产
        默认自动扣减现金：先扣现金(CNY-CASH)，不足部分扣货币基金(CNY-MMF)
        采用先校验、后执行的策略确保原子性
        """
        # 自动查询完整名称（基于代码）
        full_asset_name = self._get_asset_name(asset_id, asset_type, asset_name)
        if full_asset_name != asset_name:
            print(f"[名称自动补全] {asset_name} -> {full_asset_name}")

        # 计算总成本（含手续费）
        tx_payload = self._normalize_transaction_payload(quantity=quantity, price=price, fee=fee)
        total_cost = float(self._quantize_money(self._to_decimal(tx_payload['amount']) + self._to_decimal(tx_payload['fee'])))

        # 1. 先校验现金是否充足（如启用），但不实际扣减
        if auto_deduct_cash and currency == 'CNY':
            if not self._has_sufficient_cash(account, total_cost):
                raise ValueError(f"账户 {account} 现金不足，需要 ¥{total_cost:,.2f}")

        # 2. 先记录交易（数据库操作），这是核心记录
        tx = Transaction(
            tx_date=tx_date,
            tx_type=TransactionType.BUY,
            asset_id=asset_id,
            asset_name=full_asset_name,
            asset_type=asset_type,
            account=account,
            market=market,
            quantity=tx_payload['quantity'],
            price=tx_payload['price'],
            amount=tx_payload['amount'],
            currency=currency,
            fee=tx_payload['fee'],
            remark=remark,
            request_id=request_id
        )

        try:
            tx = self.storage.add_transaction(tx)
        except Exception as e:
            print(f"[买入失败] 记录交易失败: {e}")
            raise

        # 3. 更新持仓（核心数据）
        holding_payload = self._normalize_holding_payload(quantity=quantity)
        holding = Holding(
            asset_id=asset_id,
            asset_name=full_asset_name,
            asset_type=asset_type,
            account=account,
            market=market,
            quantity=holding_payload['quantity'],
            currency=currency,
            asset_class=asset_class,
            industry=industry
        )

        try:
            self.storage.upsert_holding(holding)
        except Exception as e:
            # 持仓更新失败，但交易已记录。打印警告但不回滚，
            # 因为交易记录是核心，持仓可以通过对账修复
            print(f"[警告] 持仓更新失败，但交易已记录: {e}")

        # 4. 最后扣减现金（非核心，失败可补偿）
        if auto_deduct_cash and currency == 'CNY':
            try:
                cash_deducted = self._deduct_cash(account, total_cost)
                if not cash_deducted:
                    # 现金扣减失败，记录警告。这是可补偿的操作，
                    # 用户可以通过手动调整现金来修复
                    print(f"[警告] 买入交易已记录，但现金扣减失败。请手动调整账户 {account} 的现金余额 ¥{total_cost:,.2f}")
            except Exception as e:
                print(f"[警告] 现金扣减异常: {e}")

        return tx

    def sell(self, tx_date: date, asset_id: str, account: str, quantity: float,
             price: float, currency: str, market: Optional[str] = None,
             fee: float = 0, remark: str = "",
             auto_add_cash: bool = True, request_id: str = None) -> Transaction:
        """
        卖出资产 (不更新成本，仅减少持仓)
        默认自动增加现金到 CNY-CASH
        """
        # 1. 获取资产名称和类型
        holding = self.storage.get_holding(asset_id, account, market)
        if holding:
            asset_name = holding.asset_name
            asset_type = holding.asset_type
        else:
            # 没有持仓时，尝试查询名称
            asset_type = None  # 未知类型
            asset_name = self._get_asset_name(asset_id, asset_type, asset_id)
            print(f"[警告] 未找到持仓记录，尝试查询名称: {asset_id} -> {asset_name}")

        # 2. 记录交易 (数量为负)
        tx_payload = self._normalize_transaction_payload(quantity=-quantity, price=price, fee=fee)
        tx = Transaction(
            tx_date=tx_date,
            tx_type=TransactionType.SELL,
            asset_id=asset_id,
            asset_name=asset_name,
            asset_type=asset_type,
            account=account,
            market=market,
            quantity=tx_payload['quantity'],  # 负数表示卖出
            price=tx_payload['price'],
            amount=tx_payload['amount'],
            currency=currency,
            fee=tx_payload['fee'],
            remark=remark,
            request_id=request_id
        )
        tx = self.storage.add_transaction(tx)

        # 3. 更新持仓 (减少数量)
        sell_holding_payload = self._normalize_holding_payload(quantity=-quantity)
        self.storage.update_holding_quantity(asset_id, account, sell_holding_payload['quantity'], market)

        # 4. 如果持仓为0，删除记录
        self.storage.delete_holding_if_zero(asset_id, account, market)

        # 5. 增加现金（如启用）
        if auto_add_cash and currency == 'CNY':
            gross_proceeds = self._quantize_money(self._to_decimal(abs(quantity)) * self._to_decimal(price))
            total_proceeds = float(self._quantize_money(self._to_decimal(gross_proceeds) - self._to_decimal(tx_payload['fee'])))
            self._add_cash(account, total_proceeds)

        return tx

    def deposit(self, flow_date: date, account: str, amount: float, currency: str,
                cny_amount: Optional[float] = None, exchange_rate: Optional[float] = None,
                source: str = "", remark: str = "") -> CashFlow:
        """入金 - 增加份额"""
        # 1. 记录出入金
        cf_payload = self._normalize_cash_flow_payload(amount=amount, currency=currency, cny_amount=cny_amount, exchange_rate=exchange_rate)
        cf = CashFlow(
            flow_date=flow_date,
            account=account,
            amount=cf_payload['amount'],
            currency=currency,
            cny_amount=cf_payload['cny_amount'],
            exchange_rate=cf_payload['exchange_rate'],
            flow_type="DEPOSIT",
            source=source,
            remark=remark
        )
        cf = self.storage.add_cash_flow(cf)

        # 2. 更新现金持仓
        self._update_cash_holding(account, cf_payload['amount'], currency, cf_payload['cny_amount'])

        return cf

    def withdraw(self, flow_date: date, account: str, amount: float, currency: str,
                 cny_amount: Optional[float] = None, exchange_rate: Optional[float] = None,
                 remark: str = "") -> CashFlow:
        """出金 - 减少份额"""
        # 1. 记录出入金 (金额为负)
        cf_payload = self._normalize_cash_flow_payload(amount=amount, currency=currency, cny_amount=cny_amount, exchange_rate=exchange_rate)
        cf = CashFlow(
            flow_date=flow_date,
            account=account,
            amount=-cf_payload['amount'],
            currency=currency,
            cny_amount=-cf_payload['cny_amount'],
            exchange_rate=cf_payload['exchange_rate'],
            flow_type="WITHDRAW",
            remark=remark
        )
        cf = self.storage.add_cash_flow(cf)

        # 2. 更新现金持仓
        self._update_cash_holding(account, -cf_payload['amount'], currency, -cf_payload['cny_amount'])

        return cf

    def _update_cash_holding(self, account: str, amount: float, currency: str, cny_amount: float):
        """更新现金持仓（旧版方法，保持兼容）"""
        # 根据币种确定资产ID
        from .models import Currency, USD_CASH_ASSET_ID, HKD_CASH_ASSET_ID
        if currency == Currency.CNY:
            asset_id = CASH_ASSET_ID
        elif currency == Currency.USD:
            asset_id = USD_CASH_ASSET_ID
        elif currency == Currency.HKD:
            asset_id = HKD_CASH_ASSET_ID
        else:
            asset_id = f'{currency}-CASH'

        cash_holding = self.storage.get_holding(asset_id, account)

        cash_payload = self._normalize_holding_payload(quantity=amount, cash_like=True)
        if cash_holding:
            # 更新现有现金持仓
            self.storage.update_holding_quantity(asset_id, account, cash_payload['quantity'])
        else:
            # 新建现金持仓
            holding = Holding(
                asset_id=asset_id,
                asset_name=f'{currency}现金',
                asset_type=AssetType.CASH,
                account=account,
                quantity=cash_payload['quantity'],
                currency=currency,
                asset_class=AssetClass.CASH,
                industry="现金"
            )
            self.storage.upsert_holding(holding)

    def _get_cash_like_holdings(self, account: str):
        """一次性获取人民币现金与货币基金持仓，供现金校验/扣减复用。"""
        cash_holding = self.storage.get_holding(CASH_ASSET_ID, account)
        mmf_holding = self.storage.get_holding(MMF_ASSET_ID, account)
        return cash_holding, mmf_holding

    def _deduct_cash(self, account: str, amount: float) -> bool:
        """
        扣减现金
        逻辑：先扣 CASH_ASSET_ID，不足部分扣 MMF_ASSET_ID
        返回：是否成功
        """
        if amount <= 0:
            return True

        remaining = self._to_decimal(amount)
        cash_holding, mmf_holding = self._get_cash_like_holdings(account)

        # 1. 先扣现金 (CASH_ASSET_ID)
        if cash_holding and cash_holding.quantity > 0:
            cash_qty = self._to_decimal(cash_holding.quantity)
            deduct_from_cash = min(cash_qty, remaining)
            self.storage.update_holding_quantity(CASH_ASSET_ID, account, float(-self._quantize_money(deduct_from_cash)))
            remaining -= deduct_from_cash
            print(f"  从 {CASH_ASSET_ID} 扣除: ¥{float(self._quantize_money(deduct_from_cash)):,.2f}")

        # 2. 如果还不够，扣货币基金 (MMF_ASSET_ID)
        if remaining > 0 and mmf_holding and mmf_holding.quantity > 0:
            mmf_qty = self._to_decimal(mmf_holding.quantity)
            deduct_from_mmf = min(mmf_qty, remaining)
            self.storage.update_holding_quantity(MMF_ASSET_ID, account, float(-self._quantize_money(deduct_from_mmf)))
            remaining -= deduct_from_mmf
            print(f"  从 {MMF_ASSET_ID} 扣除: ¥{float(self._quantize_money(deduct_from_mmf)):,.2f}")

        # 3. 检查是否扣完
        if remaining > 0:
            print(f"  ✗ 现金不足，还需: ¥{float(self._quantize_money(remaining)):,.2f}")
            return False

        return True

    def _has_sufficient_cash(self, account: str, amount: float) -> bool:
        """
        检查现金是否充足（仅检查，不扣减）
        逻辑：先检查 CASH_ASSET_ID，再检查 MMF_ASSET_ID
        返回：是否充足
        """
        if amount <= 0:
            return True

        cash_holding, mmf_holding = self._get_cash_like_holdings(account)

        total_cash = Decimal('0')
        if cash_holding and cash_holding.quantity > 0:
            total_cash += self._to_decimal(cash_holding.quantity)
        if mmf_holding and mmf_holding.quantity > 0:
            total_cash += self._to_decimal(mmf_holding.quantity)

        return total_cash >= self._to_decimal(amount)

    def _add_cash(self, account: str, amount: float) -> bool:
        """
        增加现金到 CNY-CASH
        返回：是否成功
        """
        if amount <= 0:
            return True

        amount_dec = self._quantize_money(amount)
        cash_holding = self.storage.get_holding(CASH_ASSET_ID, account)

        if cash_holding:
            # 增加现有现金持仓
            self.storage.update_holding_quantity(CASH_ASSET_ID, account, float(amount_dec))
        else:
            # 新建现金持仓
            holding = Holding(
                asset_id=CASH_ASSET_ID,
                asset_name='人民币现金',
                asset_type=AssetType.CASH,
                account=account,
                quantity=float(amount_dec),
                currency='CNY',
                asset_class=AssetClass.CASH,
                industry="现金"
            )
            self.storage.upsert_holding(holding)

        print(f"  增加到 {CASH_ASSET_ID}: ¥{float(amount_dec):,.2f}")
        return True

    # ========== 估值计算 ==========

    def calculate_valuation(self, account: str, fetch_prices: bool = True, price_timeout_seconds: int = 25,
                            allow_stale_price_fallback: bool = True,
                            price_market_closed_ttl_multiplier: float = 1.0) -> PortfolioValuation:
        """计算账户估值

        Args:
            account: 账户
            fetch_prices: 是否拉取价格
            price_timeout_seconds: 本次价格批量获取总超时（秒）
            allow_stale_price_fallback: 超时/异常时是否允许回退到“仅缓存”（可能过期），避免日报/记账卡死
        """
        # 1. 获取持仓
        holdings = self.storage.get_holdings(account=account)

        if not holdings:
            return PortfolioValuation(account=account, total_value_cny=0)

        # 2. 获取价格（统一通过 price_fetcher，自动处理缓存）
        prices = {}
        price_errors = []
        normalization_warnings = []
        if self.price_fetcher and fetch_prices:
            # 构建名称映射
            name_map = {h.asset_id: h.asset_name for h in holdings}

            # If markets are closed, we can tolerate longer cache TTL to avoid slow realtime fetch.
            try:
                from .market_time import MarketTimeUtil
                from .models import AssetType

                mkt_map = {}
                for h in holdings:
                    at = h.asset_type
                    atv = at.value if at else None
                    if atv in (AssetType.A_STOCK.value, AssetType.CN_FUND.value):
                        mkt_map[h.asset_id] = 'cn'
                    elif atv in (AssetType.HK_STOCK.value, AssetType.HK_FUND.value):
                        mkt_map[h.asset_id] = 'hk'
                    elif atv in (AssetType.US_STOCK.value, AssetType.US_FUND.value):
                        mkt_map[h.asset_id] = 'us'

                now_open_cn = MarketTimeUtil.is_cn_market_open()
                now_open_hk = MarketTimeUtil.is_hk_market_open()
                now_open_us = MarketTimeUtil.is_us_market_open()

                any_open = (now_open_cn or now_open_hk or now_open_us)

                # Snapshot perf: when markets are closed, allow using expired local cache within a window
                # Snapshot perf: when markets are closed, allow using expired local cache within a window
                accept_stale_when_closed_flag = (not any_open)
                max_stale_after_expiry_sec_flag = 24 * 3600
                market_closed_ttl_multiplier = (price_market_closed_ttl_multiplier if not any_open else 1.0)
            except Exception:
                accept_stale_when_closed_flag = False
                max_stale_after_expiry_sec_flag = 0
                market_closed_ttl_multiplier = 1.0

            # 用守护线程实现总超时，避免某些数据源卡死导致日报/record_nav 卡住
            import threading
            _fetch_result = {'prices': None, 'error': None}

            def _do_fetch():
                try:
                    _fetch_result['prices'] = self.price_fetcher.fetch_batch(
                        [h.asset_id for h in holdings],
                        name_map=name_map,
                        asset_type_map={h.asset_id: h.asset_type for h in holdings},
                        market_closed_ttl_multiplier=market_closed_ttl_multiplier,
                        accept_stale_when_closed=bool(accept_stale_when_closed_flag),
                        use_concurrent=True,
                        skip_us=False
                    )
                except Exception as e:
                    _fetch_result['error'] = e

            t = threading.Thread(target=_do_fetch, daemon=True)
            t.start()
            t.join(timeout=price_timeout_seconds)

            if _fetch_result['prices'] is not None:
                prices = _fetch_result['prices']
            else:
                if t.is_alive():
                    price_errors.append(f"价格获取超时（{price_timeout_seconds}秒），回退到缓存")
                elif _fetch_result['error']:
                    price_errors.append(f"价格获取异常，回退到缓存: {_fetch_result['error']}")

                if allow_stale_price_fallback:
                    # fallback: 仅用缓存，不启动并发避免线程泄漏
                    prices = self.price_fetcher.fetch_batch(
                        [h.asset_id for h in holdings],
                        name_map=name_map,
                        asset_type_map={h.asset_id: h.asset_type for h in holdings},
                        market_closed_ttl_multiplier=market_closed_ttl_multiplier,
                        accept_stale_when_closed=bool(accept_stale_when_closed_flag),
                        use_concurrent=False,
                        skip_us=True,
                        use_cache_only=True
                    )
                else:
                    prices = {}
        else:
            # 无 fetcher 时，从缓存获取（可能过期）
            for h in holdings:
                price = self.storage.get_price(h.asset_id)
                if price:
                    prices[h.asset_id] = price

        # 3. 计算各持仓市值（内部用 Decimal，输出保持兼容 float）
        total_value_cny = Decimal('0')
        cash_value_cny = Decimal('0')
        stock_value_cny = Decimal('0')
        fund_value_cny = Decimal('0')
        cn_asset_value = Decimal('0')
        us_asset_value = Decimal('0')
        hk_asset_value = Decimal('0')

        # 记录本次价格命中情况
        price_meta = {
            'from_cache': 0,
            'from_realtime': 0,
            'stale_fallback': 0,
            'missing': 0,
        }

        for holding in holdings:
            price = prices.get(holding.asset_id, {})
            normalized_type = normalize_holding_type(holding)

            if price and isinstance(price, dict):
                if price.get('is_from_cache'):
                    price_meta['from_cache'] += 1
                else:
                    price_meta['from_realtime'] += 1
                if price.get('source') == 'cache_fallback' or price.get('is_stale'):
                    price_meta['stale_fallback'] += 1
            else:
                price_meta['missing'] += 1

            # 记录分类兜底 warning
            raw_type = holding.asset_type.value if holding.asset_type else None
            if normalized_type == 'cash' and raw_type not in ('cash', 'mmf') and str(holding.asset_id).upper().endswith('-CASH'):
                warn = f"分类兜底: {holding.asset_id}: 原始 asset_type={raw_type or 'None'}，按代码后缀归一为 cash"
                if warn not in normalization_warnings:
                    normalization_warnings.append(warn)

            quantity_dec = self._to_decimal(holding.quantity)

            if price and 'price' in price:
                # fetch_batch 返回的是字典
                price_dec = self._to_decimal(price['price'])
                cny_price_dec = self._to_decimal(price.get('cny_price', price['price']))
                holding.current_price = float(price_dec)
                holding.cny_price = float(cny_price_dec)
                market_value_dec = self._quantize_money(quantity_dec * cny_price_dec)
                holding.market_value_cny = float(market_value_dec)
            else:
                # 无价格时使用持仓数量作为市值估算（现金等）
                # 根据币种判断汇率，外币默认为 None 避免错误计算
                if holding.currency == 'CNY':
                    holding.cny_price = 1.0
                    market_value_dec = self._quantize_money(quantity_dec)
                    holding.market_value_cny = float(market_value_dec)
                else:
                    holding.cny_price = None
                    market_value_dec = Decimal('0')
                    holding.market_value_cny = None

                if normalized_type == 'cash' and holding.currency != 'CNY' and holding.market_value_cny is None:
                    price_errors.append(f"{holding.asset_name}({holding.asset_id}): 无法获取汇率")
                elif normalized_type != 'cash' and holding.quantity != 0:
                    price_errors.append(f"{holding.asset_name}({holding.asset_id}): 价格缺失，无法可靠估值")

            market_value = market_value_dec
            total_value_cny += market_value

            # 按统一资产分类口径分类
            if normalized_type == 'cash':
                cash_value_cny += market_value
            elif normalized_type == 'fund':
                fund_value_cny += market_value
            else:
                stock_value_cny += market_value

            # 按市场分类
            if holding.asset_class == AssetClass.CN_ASSET:
                cn_asset_value += market_value
            elif holding.asset_class == AssetClass.US_ASSET:
                us_asset_value += market_value
            elif holding.asset_class == AssetClass.HK_ASSET:
                hk_asset_value += market_value

        # 4. 计算持仓占比
        for holding in holdings:
            if total_value_cny > 0 and holding.market_value_cny is not None:
                weight_dec = self._to_decimal(holding.market_value_cny) / total_value_cny
                holding.weight = float(self._quantize_weight(weight_dec))

        # 5. 获取总份额和计算净值
        total_shares = self.storage.get_total_shares(account)
        total_shares_dec = self._to_decimal(total_shares)
        nav = float(self._quantize_nav(total_value_cny / total_shares_dec)) if total_shares_dec > 0 else None

        warnings = []
        warnings.extend(normalization_warnings)
        warnings.extend(price_errors)

        # best-effort: attach Tencent batch meta if price_fetcher is present
        tencent_meta = None
        if self.price_fetcher is not None:
            tencent_meta = getattr(self.price_fetcher, '_last_tencent_batch_meta', None)

        extra = ''
        if isinstance(tencent_meta, dict) and tencent_meta.get('requests') is not None:
            extra = f"; tencent_batch=reqs={tencent_meta.get('requests')}, elapsed_ms={tencent_meta.get('elapsed_ms')}, returned={tencent_meta.get('returned_codes')}/{tencent_meta.get('requested_codes')}"

        warnings.append(
            f"[价格汇总] realtime={price_meta['from_realtime']}, cache={price_meta['from_cache']}, "
            f"stale_fallback={price_meta['stale_fallback']}, missing={price_meta['missing']}" + extra
        )

        return PortfolioValuation(
            account=account,
            total_value_cny=float(self._quantize_money(total_value_cny)),
            cash_value_cny=float(self._quantize_money(cash_value_cny)),
            stock_value_cny=float(self._quantize_money(stock_value_cny)),
            fund_value_cny=float(self._quantize_money(fund_value_cny)),
            cn_asset_value=float(self._quantize_money(cn_asset_value)),
            us_asset_value=float(self._quantize_money(us_asset_value)),
            hk_asset_value=float(self._quantize_money(hk_asset_value)),
            shares=total_shares,
            nav=nav,
            holdings=holdings,
            warnings=warnings,
        )

    # ========== 净值记录 ==========

    def record_nav(self, account: str, valuation: Optional[PortfolioValuation] = None,
                   nav_date: Optional[date] = None, persist: bool = True,
                   overwrite_existing: bool = True, dry_run: bool = False,
                   use_bulk_persist: bool = False) -> NAVHistory:
        """
        记录每日净值（按Excel账户净值sheet逻辑）
        计算字段：股票市值、现金结余、账户净值、占比、份额变动、涨幅、资产升值

        按日计算：
        - 当日资金变动 = 当日出入金总和
        - 当日资产升值 = 今日账户净值 - 昨日账户净值 - 当日资金变动
        """
        if valuation is None:
            valuation = self.calculate_valuation(account)

        today = nav_date or bj_today()
        current_year = today.strftime('%Y')
        start_year = config.get_start_year()

        # ===== 1. 基础市值计算 =====
        stock_value = valuation.stock_value_cny + valuation.fund_value_cny
        cash_value = valuation.cash_value_cny
        total_value = stock_value + cash_value

        # ===== 2. 占比计算 =====
        stock_ratio = stock_value / total_value if total_value > 0 else 0
        cash_ratio = cash_value / total_value if total_value > 0 else 0

        # ===== 3. 获取历史数据（预加载 NAV 索引，避免重复全表扫描）=====
        self.storage.preload_nav_index(account)
        nav_idx_payload = self.storage.get_nav_index(account)
        all_navs = list(nav_idx_payload.get('_nav_objects') or [])
        nav_index = self._build_nav_lookup(all_navs)

        # 从索引中提取各细分查询结果
        yesterday_nav = self._find_latest_nav_before(all_navs, today, nav_index=nav_index)
        prev_year_end_nav = self._find_year_end_nav(all_navs, str(today.year - 1), nav_index=nav_index)
        prev_month_end_nav = self._find_prev_month_end_nav(all_navs, today.year, today.month, nav_index=nav_index)
        last_nav = yesterday_nav  # 直接引用，避免重复计算

        # 各年份数据（动态：从 start_year 到当前年份）
        yearly_data = {}
        for yr in range(start_year, today.year + 1):
            yr_str = str(yr)
            yearly_data[yr_str] = {
                'prev_end': self._find_year_end_nav(all_navs, str(yr - 1), nav_index=nav_index),
                'end': self._find_year_end_nav(all_navs, yr_str, nav_index=nav_index),
            }

        # ===== 4. 资金变动计算（一次取数，内存汇总） =====
        cash_flow_summary = self._summarize_cash_flows(
            account=account,
            today=today,
            start_year=start_year,
            last_nav=last_nav,
        )
        daily_cash_flow = cash_flow_summary['daily']
        monthly_cash_flow = cash_flow_summary['monthly']
        yearly_cash_flow = cash_flow_summary['yearly'].get(current_year, 0.0)
        for yr_str, yd in yearly_data.items():
            yd['cash_flow'] = cash_flow_summary['yearly'].get(yr_str, 0.0)
        cumulative_cash_flow = cash_flow_summary['cumulative']
        gap_cash_flow = cash_flow_summary['gap']

        # ===== 5-7. 份额、涨幅、升值计算 =====
        calc = self._calc_nav_metrics(
            account=account, today=today, total_value=total_value,
            yesterday_nav=yesterday_nav, prev_year_end_nav=prev_year_end_nav,
            prev_month_end_nav=prev_month_end_nav,
            last_nav=last_nav, yearly_data=yearly_data,
            daily_cash_flow=daily_cash_flow, monthly_cash_flow=monthly_cash_flow,
            yearly_cash_flow=yearly_cash_flow,
            cumulative_cash_flow=cumulative_cash_flow, start_year=start_year,
            gap_cash_flow=gap_cash_flow,
            all_navs=all_navs,
        )

        # ===== 8. 写入 holdings_snapshot（用于审计/可复算） =====
        # Snapshot is written *before* nav_history so that each NAV point is reproducible.
        # Only write when persist=True.
        if persist:
            try:
                from .snapshot_models import HoldingSnapshot

                as_of = today.strftime('%Y-%m-%d')  # business date in Asia/Shanghai
                snapshots = []
                for h in valuation.holdings:
                    market = (h.market or '')
                    dedup_key = f"{account}:{as_of}:{market}:{h.asset_id}"
                    snapshots.append(
                        HoldingSnapshot(
                            as_of=as_of,
                            account=account,
                            asset_id=h.asset_id,
                            market=market,
                            quantity=h.quantity,
                            currency=h.currency,
                            price=h.current_price,
                            cny_price=h.cny_price,
                            market_value_cny=h.market_value_cny,
                            dedup_key=dedup_key,
                            asset_name=h.asset_name,
                            avg_cost=h.avg_cost,
                            source='record_nav',
                        )
                    )

                # Skip snapshot write if unchanged compared to existing Feishu snapshot for the same day.
                # This ensures we write at most once per day unless content truly changes.
                dry_preview = self.storage.batch_upsert_holding_snapshots(snapshots, dry_run=True)
                should_write_snapshot = bool(dry_preview.get('to_create') or dry_preview.get('to_update'))

                if should_write_snapshot:
                    self.storage.batch_upsert_holding_snapshots(snapshots, dry_run=dry_run)
                else:
                    # No change: skip Feishu write to reduce latency/quota usage.
                    pass

                # Local snapshot (secondary): best-effort write for debugging / fallback (does not affect correctness).
                try:
                    from pathlib import Path
                    import json
                    out_dir = Path(__file__).resolve().parents[1] / '.data' / 'holdings_snapshot' / account
                    out_dir.mkdir(parents=True, exist_ok=True)
                    out_file = out_dir / f'{as_of}.json'
                    digest = _snapshot_digest(snapshots)

                    payload = {
                        'as_of': as_of,
                        'account': account,
                        'count': len(snapshots),
                        'digest': digest,
                        'snapshots': [s.model_dump() for s in snapshots],
                    }
                    out_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
                except Exception:
                    pass
            except Exception as e:
                # Snapshot is part of accuracy/auditability contract; do not silently ignore.
                raise RuntimeError(f"Failed to write holdings_snapshot for {today} ({account}): {e}") from e

        # ===== 9. 构建并保存净值记录 =====
        nav_record = self._build_nav_record(
            today=today, account=account, valuation=valuation,
            stock_value=stock_value, cash_value=cash_value, total_value=total_value,
            stock_ratio=stock_ratio, cash_ratio=cash_ratio,
            daily_cash_flow=daily_cash_flow, monthly_cash_flow=monthly_cash_flow,
            yearly_cash_flow=yearly_cash_flow,
            yearly_data=yearly_data, cumulative_cash_flow=cumulative_cash_flow,
            start_year=start_year, **calc,
        )
        # Runtime self-check can be expensive (extra quantize/compare). Keep it on by default,
        # but allow scheduled jobs to skip it for speed.
        if not bool(config.get('nav.disable_runtime_validation', False)):
            self._validate_nav_record(
                nav_record=nav_record,
                last_nav=last_nav,
                prev_month_end_nav=prev_month_end_nav,
                prev_year_end_nav=prev_year_end_nav,
                daily_cash_flow=daily_cash_flow,
                monthly_cash_flow=monthly_cash_flow,
                yearly_cash_flow=yearly_cash_flow,
                gap_cash_flow=gap_cash_flow,
                initial_value=calc.get('initial_value'),
                cumulative_cash_flow=cumulative_cash_flow,
            )
        if persist:
            if use_bulk_persist and (not dry_run) and overwrite_existing:
                self.storage.upsert_nav_bulk([nav_record], mode='replace', allow_partial=False)
            else:
                self.storage.save_nav(nav_record, overwrite_existing=overwrite_existing, dry_run=dry_run)

        # ===== 9. 打印摘要 =====
        if persist and not dry_run:
            self._print_nav_summary(
                today=today, stock_value=stock_value, cash_value=cash_value,
                total_value=total_value, stock_ratio=stock_ratio, cash_ratio=cash_ratio,
                current_year=current_year, start_year=start_year,
                yesterday_nav=yesterday_nav, prev_year_end_nav=prev_year_end_nav,
                prev_month_end_nav=prev_month_end_nav,
                yearly_data=yearly_data,
                daily_cash_flow=daily_cash_flow, cumulative_cash_flow=cumulative_cash_flow,
                **calc,
            )

        return nav_record

    @classmethod
    def _calc_period_return(cls, current_value: float, base_value: Optional[float]) -> float:
        """计算通用区间收益率；内部用 Decimal，返回 float 兼容旧接口。"""
        if base_value is None:
            return 0.0
        current_dec = cls._to_decimal(current_value)
        base_dec = cls._to_decimal(base_value)
        if base_dec <= 0:
            return 0.0
        return float((current_dec - base_dec) / base_dec)

    @classmethod
    def _calc_mtd_nav_change(cls, nav: float, prev_month_end_nav) -> Optional[float]:
        """计算月初至今净值涨幅（基准：上月末净值）；缺基准返回 None。"""
        base_nav = prev_month_end_nav.nav if prev_month_end_nav else None
        if base_nav is None or base_nav <= 0:
            return None
        return cls._calc_period_return(nav, base_nav)

    @classmethod
    def _calc_ytd_nav_change(cls, nav: float, prev_year_end_nav) -> Optional[float]:
        """计算年初至今净值涨幅（基准：上一年末净值）；缺基准返回 None。"""
        base_nav = prev_year_end_nav.nav if prev_year_end_nav else None
        if base_nav is None or base_nav <= 0:
            return None
        return cls._calc_period_return(nav, base_nav)

    @classmethod
    def _calc_mtd_pnl(cls, total_value: float, prev_month_end_nav, monthly_cash_flow: float) -> Optional[float]:
        """计算月初至今资产升值额（基准：上月末总资产）；缺基准返回 None。"""
        if prev_month_end_nav:
            total_dec = cls._to_decimal(total_value)
            base_dec = cls._to_decimal(prev_month_end_nav.total_value)
            cash_flow_dec = cls._to_decimal(monthly_cash_flow)
            return float(total_dec - base_dec - cash_flow_dec)
        return None

    @classmethod
    def _calc_ytd_pnl(cls, total_value: float, prev_year_end_nav, yearly_cash_flow: float) -> Optional[float]:
        """计算年初至今资产升值额（基准：上一年末总资产）；缺基准返回 None。"""
        if prev_year_end_nav:
            total_dec = cls._to_decimal(total_value)
            base_dec = cls._to_decimal(prev_year_end_nav.total_value)
            cash_flow_dec = cls._to_decimal(yearly_cash_flow)
            return float(total_dec - base_dec - cash_flow_dec)
        return None

    def _calc_nav_metrics(
        self, *, account, today, total_value, yesterday_nav, prev_year_end_nav,
        prev_month_end_nav, last_nav, yearly_data, daily_cash_flow,
        monthly_cash_flow, yearly_cash_flow,
        cumulative_cash_flow, start_year, gap_cash_flow=None,
        all_navs=None,
    ) -> dict:
        """计算份额、净值涨幅、资产升值等指标，返回中间结果 dict"""
        # -- 份额计算 --
        # 使用 gap_cash_flow（上次记录 NAV 后到今天的全部出入金）代替 daily_cash_flow
        # 解决非每日记录时中间入金未计入份额的问题
        cf_for_shares = gap_cash_flow if gap_cash_flow is not None else daily_cash_flow
        cf_for_shares_dec = self._to_decimal(cf_for_shares)
        total_value_dec = self._to_decimal(total_value)
        last_nav_nav_dec = self._to_decimal(last_nav.nav) if (last_nav and last_nav.nav is not None) else None
        last_nav_shares_dec = self._to_decimal(last_nav.shares) if (last_nav and last_nav.shares is not None) else None

        if last_nav and last_nav_nav_dec is not None and last_nav_nav_dec > 0:
            shares_change_dec = cf_for_shares_dec / last_nav_nav_dec
            shares_dec = (last_nav_shares_dec or Decimal('0')) + shares_change_dec
        else:
            shares_change_dec = cf_for_shares_dec
            shares_dec = total_value_dec

        # Quantize nav to NAV_QUANT early, so derived fields (mtd/ytd) are consistent with stored nav.
        # shares_dec can legitimately be 0 in "清仓/关闭" scenario. In that case we define nav=1.0.
        nav_dec = (total_value_dec / shares_dec) if shares_dec > 0 else Decimal('1.0')
        nav_dec = self._quantize_nav(nav_dec)

        shares_change = float(shares_change_dec)
        shares = float(shares_dec)
        nav = float(nav_dec)

        # -- 月初至今涨幅（基准：上月末净值） --
        month_nav_change = self._calc_mtd_nav_change(nav, prev_month_end_nav)

        # -- 年初至今涨幅（基准：上一年末净值） --
        year_nav_change = self._calc_ytd_nav_change(nav, prev_year_end_nav)

        # -- 各年份净值涨幅（基准：各年上一年末净值） --
        for yd in yearly_data.values():
            base, e = yd['prev_end'], yd['end']
            if e and base and base.nav is not None and base.nav > 0:
                yd['nav_change'] = self._calc_period_return(e.nav, base.nav)
            else:
                yd['nav_change'] = None

        # -- 累计净值涨幅 --
        cumulative_nav_change = 0.0
        first_year_data = yearly_data.get(str(start_year))
        if first_year_data and first_year_data['prev_end']:
            cumulative_nav_change = self._calc_period_return(nav, first_year_data['prev_end'].nav)

        # -- 日资产升值（仅当上一条记录恰好是前一天时才计算；否则置空） --
        if yesterday_nav and yesterday_nav.date and (today - yesterday_nav.date).days == 1:
            daily_appreciation = float(total_value_dec - self._to_decimal(yesterday_nav.total_value) - cf_for_shares_dec)
        else:
            daily_appreciation = None

        # -- 月资产升值（基准：上月末总值） --
        month_appreciation = self._calc_mtd_pnl(total_value, prev_month_end_nav, monthly_cash_flow)

        # -- 年资产升值（基准：上一年末总值） --
        year_appreciation = self._calc_ytd_pnl(total_value, prev_year_end_nav, yearly_cash_flow)

        # -- 各年份资产升值 --
        initial_value = self._get_initial_value(account, all_navs=all_navs)
        sorted_years = sorted(yearly_data.keys())
        for i, yr_str in enumerate(sorted_years):
            yd = yearly_data[yr_str]
            if i == 0:
                if yd['end'] and initial_value is not None:
                    yd['appreciation'] = yd['end'].total_value - initial_value - yd['cash_flow']
                else:
                    yd['appreciation'] = None
            else:
                prev_yd = yearly_data[sorted_years[i - 1]]
                if yd['end'] and prev_yd['end']:
                    yd['appreciation'] = yd['end'].total_value - prev_yd['end'].total_value - yd['cash_flow']
                else:
                    yd['appreciation'] = None

        # -- 累计资产升值 --
        cumulative_appreciation = (total_value - initial_value - cumulative_cash_flow) if initial_value else 0.0

        # -- CAGR (复合年增长率) --
        cagr = 0.0
        if first_year_data and first_year_data['prev_end'] and first_year_data['prev_end'].nav > 0 and nav > 0:
            days_since_start = (today - first_year_data['prev_end'].date).days
            years_since_start = days_since_start / 365.25
            if years_since_start > 0:
                cagr = (nav / first_year_data['prev_end'].nav) ** (1 / years_since_start) - 1

        return dict(
            shares=shares, shares_change=shares_change, nav=nav,
            month_nav_change=month_nav_change, year_nav_change=year_nav_change,
            cumulative_nav_change=cumulative_nav_change,
            daily_appreciation=daily_appreciation,
            month_appreciation=month_appreciation, year_appreciation=year_appreciation,
            cumulative_appreciation=cumulative_appreciation,
            initial_value=initial_value,
            first_year_data=first_year_data,
            cagr=cagr,
        )

    @classmethod
    def _approx_equal(cls, a: Optional[float], b: Optional[float], tolerance: float = 1e-6) -> bool:
        """近似相等判断；内部转 Decimal 后比较，减少 float 噪音。"""
        if a is None or b is None:
            return a is b
        return abs(cls._to_decimal(a) - cls._to_decimal(b)) <= cls._to_decimal(tolerance)

    @classmethod
    def _approx_equal_quantized(cls, a: Optional[float], b: Optional[float], quantizer, *, tolerance: float = 0.0) -> bool:
        """Compare two numbers after applying the same quantizer.

        This avoids false negatives where one side is quantized (e.g., stored field) and
        the other is raw computed (e.g., expected_*), which can differ by one quant unit.
        """
        if a is None or b is None:
            return a is b
        qa = quantizer(a)
        qb = quantizer(b)
        if tolerance and tolerance > 0:
            return cls._approx_equal(float(qa), float(qb), tolerance=tolerance)
        return qa == qb

    @classmethod
    def _money_equal(cls, a: Optional[float], b: Optional[float]) -> bool:
        if a is None or b is None:
            return a is b
        return cls._quantize_money(a) == cls._quantize_money(b)

    @classmethod
    def _nav_equal(cls, a: Optional[float], b: Optional[float]) -> bool:
        if a is None or b is None:
            return a is b
        return cls._quantize_nav(a) == cls._quantize_nav(b)

    def _validate_nav_record(
        self, *, nav_record: NAVHistory, last_nav=None,
        prev_month_end_nav=None, prev_year_end_nav=None,
        daily_cash_flow: float = 0.0, monthly_cash_flow: float = 0.0,
        yearly_cash_flow: float = 0.0, gap_cash_flow: Optional[float] = None,
        initial_value: Optional[float] = None, cumulative_cash_flow: float = 0.0,
    ):
        """对即将写入的 NAV 记录做运行时自校验，防止不自洽数据静默落库。"""
        errors = []

        # 1. 总值分解必须一致（从今天起强制拆分字段必填）
        # cash_value/stock_value are mandatory for any new NAV record.
        if nav_record.cash_value is None or nav_record.stock_value is None:
            errors.append("cash_value/stock_value 缺失（必填）")
        else:
            expected_total = float(self._quantize_money(
                self._to_decimal(nav_record.stock_value) + self._to_decimal(nav_record.cash_value)
            ))
            # Allow tiny rounding drift (fen-level). Use 0.06 to avoid false negatives across quantization chains.
            if not self._approx_equal(nav_record.total_value, expected_total, tolerance=0.06):
                errors.append(f"total_value 不等于 stock_value + cash_value: {nav_record.total_value} != {expected_total}")

        # 2. 仓位权重之和应接近 1
        if nav_record.total_value and nav_record.total_value > 0 and nav_record.stock_weight is not None and nav_record.cash_weight is not None:
            weights_sum = nav_record.stock_weight + nav_record.cash_weight
            if not self._approx_equal(weights_sum, 1.0, tolerance=1e-4):
                errors.append(f"stock_weight + cash_weight 不接近 1: {weights_sum}")

        # 3. 净值应等于 total_value / shares
        if nav_record.shares and nav_record.shares > 0 and nav_record.nav is not None:
            expected_nav = float(self._quantize_nav(self._to_decimal(nav_record.total_value) / self._to_decimal(nav_record.shares)))
            if not self._approx_equal(nav_record.nav, expected_nav, tolerance=1e-6):
                errors.append(f"nav 不等于 total_value / shares: {nav_record.nav} != {expected_nav}")

        # 4. 无资金流时，份额不应变化
        effective_cash_flow = gap_cash_flow if gap_cash_flow is not None else daily_cash_flow
        if last_nav and last_nav.shares is not None and (effective_cash_flow == 0 or self._approx_equal(effective_cash_flow, 0.0, tolerance=0.01)):
            expected_shares = float(self._quantize_money(last_nav.shares))
            if not self._approx_equal(nav_record.shares, expected_shares, tolerance=0.01):
                errors.append(f"无资金流时 shares 不应变化: {nav_record.shares} != {expected_shares}")
            if not self._money_equal(nav_record.share_change, 0.0):
                errors.append(f"无资金流时 share_change 不应变化: {nav_record.share_change}")

        # 5. 月/年净值涨幅与基准一致
        expected_mtd = self._calc_mtd_nav_change(nav_record.nav, prev_month_end_nav) if nav_record.nav is not None else None
        # compare after quantization to avoid one-quant false negatives
        if not self._approx_equal_quantized(nav_record.mtd_nav_change, expected_mtd, self._quantize_nav):
            errors.append(f"mtd_nav_change 不一致: {nav_record.mtd_nav_change} != {expected_mtd}")

        expected_ytd = self._calc_ytd_nav_change(nav_record.nav, prev_year_end_nav) if nav_record.nav is not None else None
        if not self._approx_equal_quantized(nav_record.ytd_nav_change, expected_ytd, self._quantize_nav):
            errors.append(f"ytd_nav_change 不一致: {nav_record.ytd_nav_change} != {expected_ytd}")

        # 6. 月/年资产升值与基准一致
        expected_mtd_pnl = self._calc_mtd_pnl(nav_record.total_value, prev_month_end_nav, monthly_cash_flow)
        if expected_mtd_pnl is not None:
            expected_mtd_pnl = float(self._quantize_money(expected_mtd_pnl))
        if not self._money_equal(nav_record.mtd_pnl, expected_mtd_pnl):
            errors.append(f"mtd_pnl 不一致: {nav_record.mtd_pnl} != {expected_mtd_pnl}")

        expected_ytd_pnl = self._calc_ytd_pnl(nav_record.total_value, prev_year_end_nav, yearly_cash_flow)
        if expected_ytd_pnl is not None:
            expected_ytd_pnl = float(self._quantize_money(expected_ytd_pnl))
        if not self._money_equal(nav_record.ytd_pnl, expected_ytd_pnl):
            errors.append(f"ytd_pnl 不一致: {nav_record.ytd_pnl} != {expected_ytd_pnl}")

        # 7. 累计资产升值应与 details 一致
        if initial_value is not None and nav_record.details is not None:
            expected_cum_pnl = float(self._quantize_money(
                self._to_decimal(nav_record.total_value) - self._to_decimal(initial_value) - self._to_decimal(cumulative_cash_flow)
            ))
            stored_cum_pnl = nav_record.details.get('cumulative_appreciation')
            if stored_cum_pnl is not None and not self._money_equal(stored_cum_pnl, expected_cum_pnl):
                errors.append(f"details.cumulative_appreciation 不一致: {stored_cum_pnl} != {expected_cum_pnl}")

        if errors:
            raise ValueError("NAV 记录自校验失败: " + " | ".join(errors))

    def _build_nav_record(
        self, *, today, account, valuation, stock_value, cash_value, total_value,
        stock_ratio, cash_ratio, daily_cash_flow, monthly_cash_flow,
        yearly_cash_flow,
        yearly_data, cumulative_cash_flow, start_year,
        shares, shares_change, nav,
        month_nav_change, year_nav_change,
        cumulative_nav_change, daily_appreciation,
        month_appreciation, year_appreciation,
        cumulative_appreciation, initial_value, first_year_data,
        cagr=0.0,
    ) -> NAVHistory:
        """构建 NAVHistory 对象（含 details 字典）"""
        # details 保留各年份明细和累计数据
        details = {
            'monthly_cash_flow': float(self._quantize_money(monthly_cash_flow)),
            'year_cash_flow': float(self._quantize_money(yearly_cash_flow)),
            'cumulative_nav_change': float(self._quantize_nav(cumulative_nav_change)),
            'cumulative_appreciation': float(self._quantize_money(cumulative_appreciation)),
            'initial_value': float(self._quantize_money(initial_value)) if initial_value is not None else None,
            'cumulative_cash_flow': float(self._quantize_money(cumulative_cash_flow)),
            'cagr': float(self._quantize_nav(cagr)),
            'cagr_pct': float(self._quantize_money(cagr * 100)),
        }
        for yr_str, yd in yearly_data.items():
            nav_change = yd.get('nav_change')
            appreciation = yd.get('appreciation')
            details[f'nav_change_{yr_str}'] = float(self._quantize_nav(nav_change)) if nav_change is not None else None
            details[f'appreciation_{yr_str}'] = float(self._quantize_money(appreciation)) if appreciation is not None else None
            details[f'cash_flow_{yr_str}'] = float(self._quantize_money(yd.get('cash_flow', 0)))

        return NAVHistory(
            date=today,
            account=account,
            total_value=float(self._quantize_money(total_value)),
            cash_value=float(self._quantize_money(cash_value)),
            stock_value=float(self._quantize_money(stock_value)),
            fund_value=float(self._quantize_money(valuation.fund_value_cny)),
            cn_stock_value=float(self._quantize_money(valuation.cn_asset_value)),
            us_stock_value=float(self._quantize_money(valuation.us_asset_value)),
            hk_stock_value=float(self._quantize_money(valuation.hk_asset_value)),
            stock_weight=float(self._quantize_weight(stock_ratio)),
            cash_weight=float(self._quantize_weight(cash_ratio)),
            shares=float(self._quantize_money(shares)),
            nav=float(self._quantize_nav(nav)),
            cash_flow=float(self._quantize_money(daily_cash_flow)),
            share_change=float(self._quantize_money(shares_change)),
            mtd_nav_change=float(self._quantize_nav(month_nav_change)) if month_nav_change is not None else None,
            ytd_nav_change=float(self._quantize_nav(year_nav_change)) if year_nav_change is not None else None,
            pnl=float(self._quantize_money(daily_appreciation)) if daily_appreciation is not None else None,
            mtd_pnl=float(self._quantize_money(month_appreciation)) if month_appreciation is not None else None,
            ytd_pnl=float(self._quantize_money(year_appreciation)) if year_appreciation is not None else None,
            details=details,
        )

    def _print_nav_summary(
        self, *, today, stock_value, cash_value, total_value,
        stock_ratio, cash_ratio, current_year, start_year,
        yesterday_nav, prev_year_end_nav, prev_month_end_nav,
        yearly_data,
        shares, shares_change, nav,
        month_nav_change, year_nav_change,
        cumulative_nav_change, daily_appreciation,
        month_appreciation, year_appreciation,
        cumulative_appreciation, initial_value, first_year_data,
        cumulative_cash_flow=0, daily_cash_flow=0, monthly_cash_flow=0, cagr=0.0, **_extra,
    ):
        """打印净值摘要（类似Excel格式）"""
        print(f"\n净值记录已保存 ({today}):")
        print(f"  股票市值: ¥{stock_value:,.2f} ({stock_ratio*100:.2f}%)")
        print(f"  现金结余: ¥{cash_value:,.2f} ({cash_ratio*100:.2f}%)")
        print(f"  账户净值: ¥{total_value:,.2f}")
        print(f"  总份额: {shares:,.2f}")
        print(f"  单位净值: {nav:.4f}")
        print(f"  当日资金变动: ¥{daily_cash_flow:,.2f}")
        print(f"  份额变动: {shares_change:,.2f}")
        if prev_month_end_nav:
            print(f"  当月净值涨幅: {month_nav_change*100:.2f}%")
        if prev_year_end_nav:
            print(f"  当年({current_year})净值涨幅: {year_nav_change*100:.2f}%")
        for yr_str, yd in sorted(yearly_data.items()):
            if yd['prev_end'] and yd['end']:
                print(f"  {yr_str}年净值涨幅: {yd['nav_change']*100:.2f}%")
        if first_year_data and first_year_data['prev_end']:
            print(f"  累计净值涨幅({start_year}起): {cumulative_nav_change*100:.2f}%")
            if cagr != 0.0:
                print(f"  成立以来年化收益(CAGR): {cagr*100:.2f}%")
        if initial_value:
            print(f"  累计资产升值: ¥{cumulative_appreciation:,.2f} ({total_value:,.0f} - {initial_value:,.0f} - {cumulative_cash_flow:,.0f})")

    def _get_last_day_nav(self, account: str, current_date: date) -> Optional[NAVHistory]:
        """获取昨日净值记录（严格要求指定日期的前一天）"""
        from datetime import timedelta
        yesterday = current_date - timedelta(days=1)
        return self.storage.get_nav_on_date(account, yesterday)

    @classmethod
    def _sum_cash_flows(cls, flows) -> float:
        """汇总 cash_flow 列表的人民币金额；内部用 Decimal，输出 float 兼容。"""
        total = Decimal('0')
        for f in flows:
            if f.cny_amount:
                total += cls._to_decimal(f.cny_amount)
        return float(total)

    def _summarize_cash_flows(self, account: str, today: date, start_year: int, last_nav=None) -> dict:
        """使用预加载聚合缓存计算资金变动口径。"""
        self.storage.preload_cash_flow_aggs(account)
        agg = self.storage.get_cash_flow_aggs(account)

        daily_map = agg.get('daily') or {}
        monthly_map = agg.get('monthly') or {}
        yearly_map = agg.get('yearly') or {}

        daily = self._to_decimal(daily_map.get(today.strftime('%Y-%m-%d'), 0.0))
        monthly = self._to_decimal(monthly_map.get(today.strftime('%Y-%m'), 0.0))

        yearly = {}
        for yr in range(start_year, today.year + 1):
            yr_str = str(yr)
            yearly[yr_str] = float(self._to_decimal(yearly_map.get(yr_str, 0.0)))

        cumulative = Decimal('0')
        for day_str, amount in daily_map.items():
            try:
                d = datetime.strptime(day_str[:10], '%Y-%m-%d').date()
            except Exception:
                continue
            if d >= date(start_year, 1, 1) and d <= today:
                cumulative += self._to_decimal(amount)

        gap = Decimal('0')
        gap_start = last_nav.date if last_nav else None
        for day_str, amount in daily_map.items():
            try:
                d = datetime.strptime(day_str[:10], '%Y-%m-%d').date()
            except Exception:
                continue
            if d > today:
                continue
            if gap_start is None:
                if d == today:
                    gap += self._to_decimal(amount)
            elif d > gap_start:
                gap += self._to_decimal(amount)

        return {
            'daily': float(daily),
            'monthly': float(monthly),
            'yearly': yearly,
            'cumulative': float(cumulative),
            'gap': float(gap),
        }

    def _get_daily_cash_flow(self, account: str, flow_date: date) -> float:
        """获取当日资金变动（优先聚合缓存）。"""
        self.storage.preload_cash_flow_aggs(account)
        agg = self.storage.get_cash_flow_aggs(account)
        return float(self._to_decimal((agg.get('daily') or {}).get(flow_date.strftime('%Y-%m-%d'), 0.0)))

    def _get_yearly_cash_flow(self, account: str, year: str) -> float:
        """获取当年累计资金变动（优先聚合缓存）。"""
        self.storage.preload_cash_flow_aggs(account)
        agg = self.storage.get_cash_flow_aggs(account)
        return float(self._to_decimal((agg.get('yearly') or {}).get(str(year), 0.0)))

    def _get_monthly_cash_flow(self, account: str, year: int, month: int) -> float:
        """获取当月累计资金变动（优先聚合缓存）。"""
        self.storage.preload_cash_flow_aggs(account)
        agg = self.storage.get_cash_flow_aggs(account)
        return float(self._to_decimal((agg.get('monthly') or {}).get(f"{year:04d}-{month:02d}", 0.0)))

    def _get_period_cash_flow(self, account: str, start_date: date, end_date: date) -> float:
        """获取指定期间的累计资金变动（基于日聚合缓存）。"""
        self.storage.preload_cash_flow_aggs(account)
        agg = self.storage.get_cash_flow_aggs(account)
        daily = agg.get('daily') or {}
        total = Decimal('0')
        for ds, amount in daily.items():
            try:
                d = datetime.strptime(ds[:10], '%Y-%m-%d').date()
            except Exception:
                continue
            if d < start_date or d > end_date:
                continue
            total += self._to_decimal(amount)
        return float(total)

    def _get_initial_value(self, account: str, all_navs: list = None) -> Optional[float]:
        """获取初始账户净值（净值=1时的初始值）
        从数据库最早的净值记录推算，或使用 config 中的默认值"""
        navs = all_navs if all_navs is not None else self.storage.get_nav_history(account, days=365*2)
        if not navs:
            return config.get_initial_value() or None

        # 按日期排序，取最早的
        earliest_nav = min(navs, key=lambda x: x.date)

        # 如果最早记录的净值接近1，使用其total_value
        if earliest_nav and earliest_nav.nav and abs(earliest_nav.nav - 1.0) < 0.01:
            return earliest_nav.total_value

        # 否则使用配置中的初始值
        return config.get_initial_value() or None

    # ========== 内存查询辅助（避免重复 API 调用）==========

    @staticmethod
    def _build_nav_lookup(navs: list) -> dict:
        """为 NAV 历史构建按年/月和日期的预索引，避免重复全表扫描。"""
        year_end_map = {}
        year_first_map = {}
        month_end_map = {}
        sorted_navs = sorted(navs, key=lambda n: n.date)

        for nav in sorted_navs:
            yr = nav.date.year
            ym = (nav.date.year, nav.date.month)
            year_end_map[yr] = nav
            year_first_map.setdefault(yr, nav)
            month_end_map[ym] = nav

        dates = [n.date for n in sorted_navs]
        return {
            'sorted_navs': sorted_navs,
            'dates': dates,
            'year_end_map': year_end_map,
            'year_first_map': year_first_map,
            'month_end_map': month_end_map,
        }

    @staticmethod
    def _find_latest_nav_before(navs: list, before_date: date, nav_index: dict = None):
        """从内存 NAV 列表中找指定日期之前的最新记录"""
        if nav_index:
            import bisect
            idx = bisect.bisect_left(nav_index['dates'], before_date) - 1
            if idx >= 0:
                return nav_index['sorted_navs'][idx]
            return None

        candidates = [n for n in navs if n.date < before_date]
        return max(candidates, key=lambda n: n.date) if candidates else None

    @staticmethod
    def _find_year_end_nav(navs: list, year: str, nav_index: dict = None):
        """从内存 NAV 列表中找指定年份的年末记录。

        仅接受该自然年内真实存在的最后一条记录作为 year-end 基准；
        不再默认拿下一年第一条记录冒充上一年末，避免把数据缺口伪装成有效锚点。
        """
        yr = int(year)
        if nav_index:
            return nav_index['year_end_map'].get(yr)

        year_navs = [n for n in navs if n.date.year == yr]
        if year_navs:
            return max(year_navs, key=lambda n: n.date)
        return None

    @staticmethod
    def _find_prev_month_end_nav(navs: list, year: int, month: int, nav_index: dict = None):
        """从内存 NAV 列表中找上月末记录"""
        if month == 1:
            prev_year, prev_month = year - 1, 12
        else:
            prev_year, prev_month = year, month - 1

        if nav_index:
            return nav_index['month_end_map'].get((prev_year, prev_month))

        prev_month_navs = [n for n in navs if n.date.year == prev_year and n.date.month == prev_month]
        return max(prev_month_navs, key=lambda n: n.date) if prev_month_navs else None

    def _get_cumulative_cash_flow_from_year(self, account: str, from_year: str, to_date: date) -> float:
        """获取从某年开始到指定日期的累计资金变动（基于聚合缓存）。"""
        return self._get_period_cash_flow(account, date(int(from_year), 1, 1), to_date)

    # ========== 份额管理 ==========

    def get_shares(self, account: str) -> float:
        """获取账户总份额"""
        return self.storage.get_total_shares(account)

    def calculate_shares_change(self, account: str, cny_amount: float, nav: Optional[float] = None) -> float:
        """
        计算入金/出金对应的份额变动
        份额变动 = 人民币金额 / 当前净值
        """
        if nav is None:
            # 获取最新净值
            latest_nav = self.storage.get_latest_nav(account)
            nav = latest_nav.nav if latest_nav else 1.0

        if nav <= 0:
            nav = 1.0

        return cny_amount / nav

    # ========== 统计报表 ==========

    def get_asset_distribution(self, account: str) -> Dict[str, float]:
        """获取资产分布"""
        valuation = self.calculate_valuation(account)

        if valuation.total_value_cny == 0:
            return {}

        return {
            "现金": valuation.cash_value_cny / valuation.total_value_cny,
            "股票": valuation.stock_value_cny / valuation.total_value_cny,
            "基金": valuation.fund_value_cny / valuation.total_value_cny,
            "中国资产": valuation.cn_asset_value / valuation.total_value_cny,
            "美国资产": valuation.us_asset_value / valuation.total_value_cny,
            "港股资产": valuation.hk_asset_value / valuation.total_value_cny,
        }

    def get_industry_distribution(self, account: str) -> Dict[str, float]:
        """获取行业分布"""
        holdings = self.storage.get_holdings(account=account)

        # 使用 fetch_batch 批量获取价格（与 calculate_valuation 统一）
        prices = {}
        if self.price_fetcher and holdings:
            name_map = {h.asset_id: h.asset_name for h in holdings}
            prices = self.price_fetcher.fetch_batch(
                [h.asset_id for h in holdings],
                name_map=name_map,
                use_concurrent=True,
                skip_us=False
            )

        industry_values = {}
        total_value = 0.0

        for holding in holdings:
            price_data = prices.get(holding.asset_id, {})

            if price_data and 'cny_price' in price_data:
                cny_price = price_data['cny_price']
            else:
                # 无价格时根据币种判断
                cny_price = 1.0 if holding.currency == 'CNY' else None

            market_value = holding.quantity * cny_price if cny_price else 0

            industry = holding.industry.value if holding.industry else "其他"
            industry_values[industry] = industry_values.get(industry, 0) + market_value
            total_value += market_value

        if total_value == 0:
            return {}

        return {k: v / total_value for k, v in industry_values.items()}
