"""
组合计算逻辑
"""
from datetime import date, datetime
from typing import Any, Dict, Optional

from .models import (
    Holding, Transaction, CashFlow, NAVHistory,
    PortfolioValuation, AssetType, TransactionType, AssetClass,
    CASH_ASSET_ID, MMF_ASSET_ID
)
from .price_fetcher import PriceFetcher
from . import config


class PortfolioManager:
    """组合管理器"""

    def __init__(self, storage: Any, price_fetcher: Optional[PriceFetcher] = None):
        self.storage = storage
        self.price_fetcher = price_fetcher or PriceFetcher(storage=storage)

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
        total_cost = quantity * price + fee

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
            quantity=quantity,
            price=price,
            currency=currency,
            fee=fee,
            remark=remark,
            request_id=request_id
        )

        try:
            tx = self.storage.add_transaction(tx)
        except Exception as e:
            print(f"[买入失败] 记录交易失败: {e}")
            raise

        # 3. 更新持仓（核心数据）
        holding = Holding(
            asset_id=asset_id,
            asset_name=full_asset_name,
            asset_type=asset_type,
            account=account,
            market=market,
            quantity=quantity,
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
        tx = Transaction(
            tx_date=tx_date,
            tx_type=TransactionType.SELL,
            asset_id=asset_id,
            asset_name=asset_name,
            asset_type=asset_type,
            account=account,
            market=market,
            quantity=-quantity,  # 负数表示卖出
            price=price,
            currency=currency,
            fee=fee,
            remark=remark,
            request_id=request_id
        )
        tx = self.storage.add_transaction(tx)

        # 3. 更新持仓 (减少数量)
        self.storage.update_holding_quantity(asset_id, account, -quantity, market)

        # 4. 如果持仓为0，删除记录
        self.storage.delete_holding_if_zero(asset_id, account, market)

        # 5. 增加现金（如启用）
        if auto_add_cash and currency == 'CNY':
            total_proceeds = quantity * price - fee
            self._add_cash(account, total_proceeds)

        return tx

    def deposit(self, flow_date: date, account: str, amount: float, currency: str,
                cny_amount: Optional[float] = None, exchange_rate: Optional[float] = None,
                source: str = "", remark: str = "") -> CashFlow:
        """入金 - 增加份额"""
        # 1. 记录出入金
        cf = CashFlow(
            flow_date=flow_date,
            account=account,
            amount=amount,
            currency=currency,
            cny_amount=cny_amount or amount,
            exchange_rate=exchange_rate or 1.0,
            flow_type="DEPOSIT",
            source=source,
            remark=remark
        )
        cf = self.storage.add_cash_flow(cf)

        # 2. 更新现金持仓
        self._update_cash_holding(account, amount, currency, cny_amount or amount)

        return cf

    def withdraw(self, flow_date: date, account: str, amount: float, currency: str,
                 cny_amount: Optional[float] = None, exchange_rate: Optional[float] = None,
                 remark: str = "") -> CashFlow:
        """出金 - 减少份额"""
        # 1. 记录出入金 (金额为负)
        cf = CashFlow(
            flow_date=flow_date,
            account=account,
            amount=-amount,
            currency=currency,
            cny_amount=-(cny_amount or amount),
            exchange_rate=exchange_rate or 1.0,
            flow_type="WITHDRAW",
            remark=remark
        )
        cf = self.storage.add_cash_flow(cf)

        # 2. 更新现金持仓
        self._update_cash_holding(account, -amount, currency, -(cny_amount or amount))

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

        if cash_holding:
            # 更新现有现金持仓
            self.storage.update_holding_quantity(asset_id, account, amount)
        else:
            # 新建现金持仓
            holding = Holding(
                asset_id=asset_id,
                asset_name=f'{currency}现金',
                asset_type=AssetType.CASH,
                account=account,
                quantity=amount,
                currency=currency,
                asset_class=AssetClass.CASH,
                industry="现金"
            )
            self.storage.upsert_holding(holding)

    def _deduct_cash(self, account: str, amount: float) -> bool:
        """
        扣减现金
        逻辑：先扣 CASH_ASSET_ID，不足部分扣 MMF_ASSET_ID
        返回：是否成功
        """
        if amount <= 0:
            return True

        remaining = amount

        # 一次性获取两个持仓，减少 API 调用
        cash_holding = self.storage.get_holding(CASH_ASSET_ID, account)
        mmf_holding = self.storage.get_holding(MMF_ASSET_ID, account)

        # 1. 先扣现金 (CASH_ASSET_ID)
        if cash_holding and cash_holding.quantity > 0:
            deduct_from_cash = min(cash_holding.quantity, remaining)
            self.storage.update_holding_quantity(CASH_ASSET_ID, account, -deduct_from_cash)
            remaining -= deduct_from_cash
            print(f"  从 {CASH_ASSET_ID} 扣除: ¥{deduct_from_cash:,.2f}")

        # 2. 如果还不够，扣货币基金 (MMF_ASSET_ID)
        if remaining > 0 and mmf_holding and mmf_holding.quantity > 0:
            deduct_from_mmf = min(mmf_holding.quantity, remaining)
            self.storage.update_holding_quantity(MMF_ASSET_ID, account, -deduct_from_mmf)
            remaining -= deduct_from_mmf
            print(f"  从 {MMF_ASSET_ID} 扣除: ¥{deduct_from_mmf:,.2f}")

        # 3. 检查是否扣完
        if remaining > 0:
            print(f"  ✗ 现金不足，还需: ¥{remaining:,.2f}")
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

        # 一次性获取两个持仓，减少 API 调用
        cash_holding = self.storage.get_holding(CASH_ASSET_ID, account)
        mmf_holding = self.storage.get_holding(MMF_ASSET_ID, account)

        total_cash = 0.0
        if cash_holding and cash_holding.quantity > 0:
            total_cash += cash_holding.quantity
        if mmf_holding and mmf_holding.quantity > 0:
            total_cash += mmf_holding.quantity

        return total_cash >= amount

    def _add_cash(self, account: str, amount: float) -> bool:
        """
        增加现金到 CNY-CASH
        返回：是否成功
        """
        if amount <= 0:
            return True

        cash_holding = self.storage.get_holding(CASH_ASSET_ID, account)

        if cash_holding:
            # 增加现有现金持仓
            self.storage.update_holding_quantity(CASH_ASSET_ID, account, amount)
        else:
            # 新建现金持仓
            holding = Holding(
                asset_id=CASH_ASSET_ID,
                asset_name='人民币现金',
                asset_type=AssetType.CASH,
                account=account,
                quantity=amount,
                currency='CNY',
                asset_class=AssetClass.CASH,
                industry="现金"
            )
            self.storage.upsert_holding(holding)

        print(f"  增加到 {CASH_ASSET_ID}: ¥{amount:,.2f}")
        return True

    # ========== 估值计算 ==========

    def calculate_valuation(self, account: str, fetch_prices: bool = True) -> PortfolioValuation:
        """计算账户估值"""
        # 1. 获取持仓
        holdings = self.storage.get_holdings(account=account)

        if not holdings:
            return PortfolioValuation(account=account, total_value_cny=0)

        # 2. 获取价格（统一通过 price_fetcher，自动处理缓存）
        prices = {}
        if self.price_fetcher:
            # 构建名称映射
            name_map = {h.asset_id: h.asset_name for h in holdings}
            # fetch_batch 会自动检查缓存、获取新价格、保存缓存
            prices = self.price_fetcher.fetch_batch(
                [h.asset_id for h in holdings],
                name_map=name_map,
                use_concurrent=True,
                skip_us=False
            )
        else:
            # 无 fetcher 时，从缓存获取（可能过期）
            for h in holdings:
                price = self.storage.get_price(h.asset_id)
                if price:
                    prices[h.asset_id] = price

        # 3. 计算各持仓市值
        total_value_cny = 0.0
        cash_value_cny = 0.0
        stock_value_cny = 0.0
        fund_value_cny = 0.0
        cn_asset_value = 0.0
        us_asset_value = 0.0
        hk_asset_value = 0.0

        for holding in holdings:
            price = prices.get(holding.asset_id, {})

            if price and 'price' in price:
                # fetch_batch 返回的是字典
                holding.current_price = price['price']
                holding.cny_price = price.get('cny_price', price['price'])
                holding.market_value_cny = holding.quantity * holding.cny_price
            else:
                # 无价格时使用持仓数量作为市值估算（现金等）
                # 根据币种判断汇率，外币默认为 None 避免错误计算
                holding.cny_price = 1.0 if holding.currency == 'CNY' else None
                holding.market_value_cny = holding.quantity * holding.cny_price if holding.cny_price else None

            market_value = holding.market_value_cny or 0
            total_value_cny += market_value

            # 按资产类型分类
            if holding.asset_type == AssetType.CASH:
                cash_value_cny += market_value
            elif holding.asset_type == AssetType.FUND:
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
            if total_value_cny > 0 and holding.market_value_cny:
                holding.weight = holding.market_value_cny / total_value_cny

        # 5. 获取总份额和计算净值
        total_shares = self.storage.get_total_shares(account)
        nav = total_value_cny / total_shares if total_shares > 0 else None

        return PortfolioValuation(
            account=account,
            total_value_cny=total_value_cny,
            cash_value_cny=cash_value_cny,
            stock_value_cny=stock_value_cny,
            fund_value_cny=fund_value_cny,
            cn_asset_value=cn_asset_value,
            us_asset_value=us_asset_value,
            hk_asset_value=hk_asset_value,
            shares=total_shares,
            nav=nav,
            holdings=holdings
        )

    # ========== 净值记录 ==========

    def record_nav(self, account: str, valuation: Optional[PortfolioValuation] = None,
                   nav_date: Optional[date] = None) -> NAVHistory:
        """
        记录每日净值（按Excel账户净值sheet逻辑）
        计算字段：股票市值、现金结余、账户净值、占比、份额变动、涨幅、资产升值

        按日计算：
        - 当日资金变动 = 当日出入金总和
        - 当日资产升值 = 今日账户净值 - 昨日账户净值 - 当日资金变动
        """
        if valuation is None:
            valuation = self.calculate_valuation(account)

        today = nav_date or date.today()
        current_year = today.strftime('%Y')
        start_year = config.get_start_year()

        # ===== 1. 基础市值计算 =====
        stock_value = valuation.stock_value_cny + valuation.fund_value_cny
        cash_value = valuation.cash_value_cny
        total_value = stock_value + cash_value

        # ===== 2. 占比计算 =====
        stock_ratio = stock_value / total_value if total_value > 0 else 0
        cash_ratio = cash_value / total_value if total_value > 0 else 0

        # ===== 3. 获取历史数据（单次 API 调用获取全部 NAV）=====
        all_navs = self.storage.get_nav_history(account, days=9999)

        # 从全量数据中提取各细分查询结果，避免重复 API 调用
        yesterday_nav = self._find_latest_nav_before(all_navs, today)
        prev_year_end_nav = self._find_year_end_nav(all_navs, str(today.year - 1))
        prev_month_end_nav = self._find_prev_month_end_nav(all_navs, today.year, today.month)
        last_nav = yesterday_nav  # 直接引用，避免重复计算

        # 各年份数据（动态：从 start_year 到当前年份）
        yearly_data = {}
        for yr in range(start_year, today.year + 1):
            yr_str = str(yr)
            yearly_data[yr_str] = {
                'prev_end': self._find_year_end_nav(all_navs, str(yr - 1)),
                'end': self._find_year_end_nav(all_navs, yr_str),
            }

        # ===== 4. 资金变动计算 =====
        daily_cash_flow = self._get_daily_cash_flow(account, today)
        monthly_cash_flow = self._get_monthly_cash_flow(account, today.year, today.month)
        yearly_cash_flow = self._get_yearly_cash_flow(account, current_year)
        for yr_str, yd in yearly_data.items():
            yd['cash_flow'] = self._get_yearly_cash_flow(account, yr_str)
        cumulative_cash_flow = self._get_cumulative_cash_flow_from_year(account, str(start_year), today)

        # 计算上次记录净值后到今天的全部出入金（解决非每日记录时中间入金丢失问题）
        if last_nav:
            from datetime import timedelta
            gap_start = last_nav.date + timedelta(days=1)
            gap_cash_flow = self._get_period_cash_flow(account, gap_start, today)
        else:
            gap_cash_flow = daily_cash_flow

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

        # ===== 8. 构建并保存净值记录 =====
        nav_record = self._build_nav_record(
            today=today, account=account, valuation=valuation,
            stock_value=stock_value, cash_value=cash_value, total_value=total_value,
            stock_ratio=stock_ratio, cash_ratio=cash_ratio,
            daily_cash_flow=daily_cash_flow, monthly_cash_flow=monthly_cash_flow,
            yearly_cash_flow=yearly_cash_flow,
            yearly_data=yearly_data, cumulative_cash_flow=cumulative_cash_flow,
            start_year=start_year, **calc,
        )
        self.storage.save_nav(nav_record)

        # ===== 9. 打印摘要 =====
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
        if last_nav and last_nav.nav and last_nav.nav > 0:
            shares_change = cf_for_shares / last_nav.nav
            shares = (last_nav.shares or 0) + shares_change
        else:
            shares_change = cf_for_shares
            shares = total_value

        nav = total_value / shares if shares > 0 else 1.0

        # -- 月初至今涨幅（基准：上月末净值） --
        if prev_month_end_nav and prev_month_end_nav.nav and prev_month_end_nav.nav > 0:
            month_nav_change = (nav - prev_month_end_nav.nav) / prev_month_end_nav.nav
        else:
            month_nav_change = 0.0

        # -- 年初至今涨幅（基准：上一年末净值） --
        if prev_year_end_nav and prev_year_end_nav.nav and prev_year_end_nav.nav > 0:
            year_nav_change = (nav - prev_year_end_nav.nav) / prev_year_end_nav.nav
        else:
            year_nav_change = 0.0

        # -- 各年份净值涨幅（基准：各年上一年末净值） --
        for yd in yearly_data.values():
            base, e = yd['prev_end'], yd['end']
            yd['nav_change'] = ((e.nav - base.nav) / base.nav) if (base and e and base.nav and base.nav > 0) else 0.0

        # -- 累计净值涨幅 --
        cumulative_nav_change = 0.0
        first_year_data = yearly_data.get(str(start_year))
        if first_year_data and first_year_data['prev_end'] and first_year_data['prev_end'].nav > 0:
            cumulative_nav_change = (nav - first_year_data['prev_end'].nav) / first_year_data['prev_end'].nav

        # -- 日资产升值（使用 gap 期间全部资金变动，确保间隔期入金不被算作收益） --
        if yesterday_nav:
            daily_appreciation = total_value - yesterday_nav.total_value - cf_for_shares
        else:
            daily_appreciation = 0.0

        # -- 月资产升值（基准：上月末总值） --
        month_appreciation = (total_value - prev_month_end_nav.total_value - monthly_cash_flow) if prev_month_end_nav else 0.0

        # -- 年资产升值（基准：上一年末总值） --
        year_appreciation = (total_value - prev_year_end_nav.total_value - yearly_cash_flow) if prev_year_end_nav else 0.0

        # -- 各年份资产升值 --
        initial_value = self._get_initial_value(account, all_navs=all_navs)
        sorted_years = sorted(yearly_data.keys())
        for i, yr_str in enumerate(sorted_years):
            yd = yearly_data[yr_str]
            if i == 0:
                if yd['end'] and initial_value:
                    yd['appreciation'] = yd['end'].total_value - initial_value - yd['cash_flow']
                else:
                    yd['appreciation'] = 0.0
            else:
                prev_yd = yearly_data[sorted_years[i - 1]]
                if yd['end'] and prev_yd['end']:
                    yd['appreciation'] = yd['end'].total_value - prev_yd['end'].total_value - yd['cash_flow']
                else:
                    yd['appreciation'] = 0.0

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
            'monthly_cash_flow': monthly_cash_flow,
            'year_cash_flow': yearly_cash_flow,
            'cumulative_nav_change': round(cumulative_nav_change, 6),
            'cumulative_appreciation': round(cumulative_appreciation, 2),
            'initial_value': initial_value,
            'cumulative_cash_flow': cumulative_cash_flow,
            'cagr': round(cagr, 6),
            'cagr_pct': round(cagr * 100, 2),
        }
        for yr_str, yd in yearly_data.items():
            details[f'nav_change_{yr_str}'] = round(yd.get('nav_change', 0), 6)
            details[f'appreciation_{yr_str}'] = round(yd.get('appreciation', 0), 2)
            details[f'cash_flow_{yr_str}'] = yd.get('cash_flow', 0)

        return NAVHistory(
            date=today,
            account=account,
            total_value=round(total_value, 2),
            cash_value=round(cash_value, 2),
            stock_value=round(stock_value, 2),
            fund_value=round(valuation.fund_value_cny, 2),
            cn_stock_value=round(valuation.cn_asset_value, 2),
            us_stock_value=round(valuation.us_asset_value, 2),
            hk_stock_value=round(valuation.hk_asset_value, 2),
            stock_weight=round(stock_ratio, 6),
            cash_weight=round(cash_ratio, 6),
            shares=round(shares, 2),
            nav=round(nav, 6),
            cash_flow=round(daily_cash_flow, 2),
            share_change=round(shares_change, 2),
            mtd_nav_change=round(month_nav_change, 6),
            ytd_nav_change=round(year_nav_change, 6),
            pnl=round(daily_appreciation, 2),
            mtd_pnl=round(month_appreciation, 2),
            ytd_pnl=round(year_appreciation, 2),
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
        """获取昨日净值记录（指定日期的前一天）"""
        from datetime import timedelta
        yesterday = current_date - timedelta(days=1)

        # 使用通用方法获取
        return self.storage.get_latest_nav_before(account, yesterday)

    def _get_initial_nav(self, account: str) -> Optional[NAVHistory]:
        """获取初始净值记录（最早的记录，净值=1时的记录）"""
        # 获取近2年的记录，找最早的
        navs = self.storage.get_nav_history(account, days=730)
        if not navs:
            return None

        # 按日期排序，取最早的
        earliest = min(navs, key=lambda x: x.date)
        return earliest

    def _get_year_end_nav(self, account: str, year: str) -> Optional[NAVHistory]:
        """获取年末净值记录（当年最后一天，或下年初第一天）"""
        year_start = date(int(year), 1, 1)
        year_end = date(int(year), 12, 31)
        next_year_start = date(int(year) + 1, 1, 1)

        # 获取该年及次年的记录
        navs = self.storage.get_nav_history(account, days=730)

        # 筛选出该年份的记录，找最后一天
        year_navs = [n for n in navs if n.date.year == int(year)]
        if year_navs:
            year_navs.sort(key=lambda x: x.date, reverse=True)
            return year_navs[0]

        # 如果没有当年记录，找次年的第一条
        next_year_navs = [n for n in navs if n.date.year == int(year) + 1]
        if next_year_navs:
            next_year_navs.sort(key=lambda x: x.date)
            return next_year_navs[0]

        return None

    def _get_daily_cash_flow(self, account: str, flow_date: date) -> float:
        """获取当日资金变动（从cash_flow表）"""
        # 使用通用的 get_cash_flows 方法
        flows = self.storage.get_cash_flows(account, flow_date, flow_date)
        return sum(f.cny_amount for f in flows if f.cny_amount)

    def _get_yearly_cash_flow(self, account: str, year: str) -> float:
        """获取当年累计资金变动"""
        year_start = date(int(year), 1, 1)
        year_end = date(int(year), 12, 31)

        flows = self.storage.get_cash_flows(account, year_start, year_end)
        return sum(f.cny_amount for f in flows if f.cny_amount)

    def _get_monthly_cash_flow(self, account: str, year: int, month: int) -> float:
        """获取当月累计资金变动"""
        month_start = date(year, month, 1)
        if month == 12:
            month_end = date(year, 12, 31)
        else:
            month_end = date(year, month + 1, 1)
            from datetime import timedelta
            month_end = month_end - timedelta(days=1)
        flows = self.storage.get_cash_flows(account, month_start, month_end)
        return sum(f.cny_amount for f in flows if f.cny_amount)

    def _get_period_cash_flow(self, account: str, start_date: date, end_date: date) -> float:
        """获取指定期间的累计资金变动"""
        flows = self.storage.get_cash_flows(account, start_date, end_date)
        return sum(f.cny_amount for f in flows if f.cny_amount)

    def _get_prev_month_end_nav(self, account: str, year: int, month: int) -> Optional[NAVHistory]:
        """获取上月末净值记录"""
        if month == 1:
            prev_year, prev_month = year - 1, 12
        else:
            prev_year, prev_month = year, month - 1

        navs = self.storage.get_nav_history(account, days=100)
        prev_month_navs = [n for n in navs if n.date.year == prev_year and n.date.month == prev_month]
        if prev_month_navs:
            return max(prev_month_navs, key=lambda x: x.date)
        return None

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
    def _find_latest_nav_before(navs: list, before_date: date):
        """从内存 NAV 列表中找指定日期之前的最新记录"""
        candidates = [n for n in navs if n.date < before_date]
        return max(candidates, key=lambda n: n.date) if candidates else None

    @staticmethod
    def _find_year_end_nav(navs: list, year: str):
        """从内存 NAV 列表中找指定年份的年末记录"""
        yr = int(year)
        year_navs = [n for n in navs if n.date.year == yr]
        if year_navs:
            return max(year_navs, key=lambda n: n.date)
        # 没有当年记录，找次年第一条
        next_year_navs = [n for n in navs if n.date.year == yr + 1]
        if next_year_navs:
            return min(next_year_navs, key=lambda n: n.date)
        return None

    @staticmethod
    def _find_prev_month_end_nav(navs: list, year: int, month: int):
        """从内存 NAV 列表中找上月末记录"""
        if month == 1:
            prev_year, prev_month = year - 1, 12
        else:
            prev_year, prev_month = year, month - 1
        prev_month_navs = [n for n in navs if n.date.year == prev_year and n.date.month == prev_month]
        return max(prev_month_navs, key=lambda n: n.date) if prev_month_navs else None

    def _get_cumulative_cash_flow_from_year(self, account: str, from_year: str, to_date: date) -> float:
        """获取从某年开始到指定日期的累计资金变动"""
        year_start = date(int(from_year), 1, 1)

        flows = self.storage.get_cash_flows(account, year_start, to_date)
        return sum(f.cny_amount for f in flows if f.cny_amount)

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
