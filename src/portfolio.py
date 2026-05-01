"""
组合计算逻辑
"""
from datetime import date

from decimal import Decimal
from typing import Any, Dict, Optional, Union

from .models import (
    Holding, Transaction, CashFlow, NAVHistory,
    PortfolioValuation, AssetType, AssetClass,
    CASH_ASSET_ID, MMF_ASSET_ID
)
from .price_fetcher import PriceFetcher
from .app import (
    AssetNameService,
    CashFlowSummaryService,
    CashService,
    CompensationService,
    NavBaselineService,
    NavRecordService,
    NavSummaryPrinter,
    ReportingService,
    ShareService,
    SnapshotService,
    TradeService,
    ValuationService,
)
from .domain import NavCalculator, NavHistoryIndex, PayloadNormalizer
from . import config


_DEFAULT_PRICE_FETCHER = object()


class PortfolioManager:
    """组合管理器"""

    MONEY_QUANT = Decimal('0.01')
    NAV_QUANT = Decimal('0.000001')
    WEIGHT_QUANT = Decimal('0.000001')

    def __init__(self, storage: Any, price_fetcher: Optional[PriceFetcher] = _DEFAULT_PRICE_FETCHER):
        self.storage = storage
        self.price_fetcher = PriceFetcher(storage=storage) if price_fetcher is _DEFAULT_PRICE_FETCHER else price_fetcher
        self.asset_name_service = AssetNameService(manager=self)
        self.compensation = CompensationService(storage=storage)
        self.cash_service = CashService(storage=storage)
        self.cash_flow_summary_service = CashFlowSummaryService(storage=storage)
        self.nav_baseline_service = NavBaselineService(storage=storage)
        self.trade_service = TradeService(manager=self, storage=storage)
        self.valuation_service = ValuationService(manager=self, storage=storage, price_fetcher=self.price_fetcher)
        self.snapshot_service = SnapshotService(storage=storage)
        self.nav_record_service = NavRecordService(manager=self, storage=storage)
        self.nav_summary_printer = NavSummaryPrinter()
        self.reporting_service = ReportingService(manager=self, storage=storage)
        self.share_service = ShareService(storage=storage)
        self.nav_calculator = NavCalculator()

    def _record_compensation(self, *, operation_type: str, account: str, payload: Dict[str, Any], error: Union[Exception, str], related_record_id: Optional[str] = None):
        """Best-effort repair task recording for partial multi-table writes."""
        try:
            self.compensation.record(
                operation_type=operation_type,
                account=account,
                payload=payload,
                error=error,
                related_record_id=related_record_id,
            )
        except Exception as comp_error:
            print(f"[警告] 补偿任务记录失败: {comp_error}")

    @staticmethod
    def _to_decimal(value: Any) -> Decimal:
        return PayloadNormalizer.to_decimal(value)

    @classmethod
    def _quantize_money(cls, value: Any) -> Decimal:
        return PayloadNormalizer.quantize_money(value)

    @classmethod
    def _quantize_nav(cls, value: Any) -> Decimal:
        return PayloadNormalizer.quantize_nav(value)

    @classmethod
    def _quantize_weight(cls, value: Any) -> Decimal:
        return PayloadNormalizer.quantize_weight(value)

    @classmethod
    def _normalize_transaction_payload(cls, *, quantity: Any, price: Any, fee: Any = 0.0) -> Dict[str, float]:
        return PayloadNormalizer.normalize_transaction_payload(quantity=quantity, price=price, fee=fee)

    @classmethod
    def _normalize_cash_flow_payload(cls, *, amount: Any, currency: str = 'CNY', cny_amount: Any = None, exchange_rate: Any = None) -> Dict[str, Optional[float]]:
        return PayloadNormalizer.normalize_cash_flow_payload(
            amount=amount,
            currency=currency,
            cny_amount=cny_amount,
            exchange_rate=exchange_rate,
        )

    @classmethod
    def _normalize_holding_payload(cls, *, quantity: Any, avg_cost: Any = None, cash_like: bool = False) -> Dict[str, Optional[float]]:
        return PayloadNormalizer.normalize_holding_payload(quantity=quantity, avg_cost=avg_cost, cash_like=cash_like)

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
        return self.asset_name_service.get_asset_name(
            asset_id=asset_id,
            asset_type=asset_type,
            user_provided_name=user_provided_name,
            timeout=timeout,
        )

    def buy(self, tx_date: date, asset_id: str, asset_name: str, asset_type: AssetType,
            account: str, quantity: float, price: float, currency: str,
            broker: Optional[str] = None, fee: float = 0, remark: str = "",
            asset_class: Optional[AssetClass] = None, industry: Optional[str] = None,
            auto_deduct_cash: bool = True, request_id: str = None) -> Transaction:
        """
        买入资产
        默认自动扣减现金：先扣现金(CNY-CASH)，不足部分扣货币基金(CNY-MMF)
        采用先校验、后执行的策略确保原子性
        """
        return self.trade_service.buy(
            tx_date=tx_date,
            asset_id=asset_id,
            asset_name=asset_name,
            asset_type=asset_type,
            account=account,
            quantity=quantity,
            price=price,
            currency=currency,
            broker=broker,
            fee=fee,
            remark=remark,
            asset_class=asset_class,
            industry=industry,
            auto_deduct_cash=auto_deduct_cash,
            request_id=request_id,
        )

    def sell(self, tx_date: date, asset_id: str, account: str, quantity: float,
             price: float, currency: str, broker: Optional[str] = None,
             fee: float = 0, remark: str = "",
             auto_add_cash: bool = True, request_id: str = None) -> Transaction:
        """
        卖出资产 (不更新成本，仅减少持仓)
        默认自动增加现金到 CNY-CASH
        """
        return self.trade_service.sell(
            tx_date=tx_date,
            asset_id=asset_id,
            account=account,
            quantity=quantity,
            price=price,
            currency=currency,
            broker=broker,
            fee=fee,
            remark=remark,
            auto_add_cash=auto_add_cash,
            request_id=request_id,
        )

    def deposit(self, flow_date: date, account: str, amount: float, currency: str,
                cny_amount: Optional[float] = None, exchange_rate: Optional[float] = None,
                source: str = "", remark: str = "") -> CashFlow:
        """入金 - 增加份额"""
        return self.trade_service.deposit(
            flow_date=flow_date,
            account=account,
            amount=amount,
            currency=currency,
            cny_amount=cny_amount,
            exchange_rate=exchange_rate,
            source=source,
            remark=remark,
        )

    def withdraw(self, flow_date: date, account: str, amount: float, currency: str,
                 cny_amount: Optional[float] = None, exchange_rate: Optional[float] = None,
                 remark: str = "") -> CashFlow:
        """出金 - 减少份额"""
        return self.trade_service.withdraw(
            flow_date=flow_date,
            account=account,
            amount=amount,
            currency=currency,
            cny_amount=cny_amount,
            exchange_rate=exchange_rate,
            remark=remark,
        )

    def _update_cash_holding(self, account: str, amount: float, currency: str, cny_amount: float):
        """更新现金持仓（旧版方法，保持兼容）"""
        return self.cash_service.update_cash_holding(account, amount, currency, cny_amount)

    def _get_cash_like_holdings(self, account: str):
        """一次性获取人民币现金与货币基金持仓，供现金校验/扣减复用。"""
        return self.cash_service.get_cash_like_holdings(account)

    def _deduct_cash(self, account: str, amount: float) -> bool:
        """
        扣减现金
        逻辑：先扣 CASH_ASSET_ID，不足部分扣 MMF_ASSET_ID
        返回：是否成功
        """
        return self.cash_service.deduct_cash(account, amount)

    def _has_sufficient_cash(self, account: str, amount: float) -> bool:
        """
        检查现金是否充足（仅检查，不扣减）
        逻辑：先检查 CASH_ASSET_ID，再检查 MMF_ASSET_ID
        返回：是否充足
        """
        return self.cash_service.has_sufficient_cash(account, amount)

    def _add_cash(self, account: str, amount: float) -> bool:
        """
        增加现金到 CNY-CASH
        返回：是否成功
        """
        return self.cash_service.add_cash(account, amount)

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
        self.valuation_service.price_fetcher = self.price_fetcher
        return self.valuation_service.calculate_valuation(
            account=account,
            fetch_prices=fetch_prices,
            price_timeout_seconds=price_timeout_seconds,
            allow_stale_price_fallback=allow_stale_price_fallback,
            price_market_closed_ttl_multiplier=price_market_closed_ttl_multiplier,
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
        return self.nav_record_service.record_nav(
            account=account,
            valuation=valuation,
            nav_date=nav_date,
            persist=persist,
            overwrite_existing=overwrite_existing,
            dry_run=dry_run,
            use_bulk_persist=use_bulk_persist,
        )

    @classmethod
    def _calc_period_return(cls, current_value: float, base_value: Optional[float]) -> float:
        """计算通用区间收益率；内部用 Decimal，返回 float 兼容旧接口。"""
        return NavCalculator.calc_period_return(current_value, base_value)

    @classmethod
    def _calc_mtd_nav_change(cls, nav: float, prev_month_end_nav) -> Optional[float]:
        """计算月初至今净值涨幅（基准：上月末净值）；缺基准返回 None。"""
        return NavCalculator.calc_mtd_nav_change(nav, prev_month_end_nav)

    @classmethod
    def _calc_ytd_nav_change(cls, nav: float, prev_year_end_nav) -> Optional[float]:
        """计算年初至今净值涨幅（基准：上一年末净值）；缺基准返回 None。"""
        return NavCalculator.calc_ytd_nav_change(nav, prev_year_end_nav)

    @classmethod
    def _calc_mtd_pnl(cls, total_value: float, prev_month_end_nav, monthly_cash_flow: float) -> Optional[float]:
        """计算月初至今资产升值额（基准：上月末总资产）；缺基准返回 None。"""
        return NavCalculator.calc_mtd_pnl(total_value, prev_month_end_nav, monthly_cash_flow)

    @classmethod
    def _calc_ytd_pnl(cls, total_value: float, prev_year_end_nav, yearly_cash_flow: float) -> Optional[float]:
        """计算年初至今资产升值额（基准：上一年末总资产）；缺基准返回 None。"""
        return NavCalculator.calc_ytd_pnl(total_value, prev_year_end_nav, yearly_cash_flow)

    def _calc_nav_metrics(
        self, *, account, today, total_value, yesterday_nav, prev_year_end_nav,
        prev_month_end_nav, last_nav, yearly_data, daily_cash_flow,
        monthly_cash_flow, yearly_cash_flow,
        cumulative_cash_flow, start_year, gap_cash_flow=None,
        all_navs=None,
    ) -> dict:
        """计算份额、净值涨幅、资产升值等指标，返回中间结果 dict"""
        initial_value = self._get_initial_value(account, all_navs=all_navs)
        return self.nav_calculator.calc_nav_metrics(
            today=today,
            total_value=total_value,
            yesterday_nav=yesterday_nav,
            prev_year_end_nav=prev_year_end_nav,
            prev_month_end_nav=prev_month_end_nav,
            last_nav=last_nav,
            yearly_data=yearly_data,
            daily_cash_flow=daily_cash_flow,
            monthly_cash_flow=monthly_cash_flow,
            yearly_cash_flow=yearly_cash_flow,
            cumulative_cash_flow=cumulative_cash_flow,
            start_year=start_year,
            initial_value=initial_value,
            gap_cash_flow=gap_cash_flow,
        )

    @classmethod
    def _approx_equal(cls, a: Optional[float], b: Optional[float], tolerance: float = 1e-6) -> bool:
        """近似相等判断；内部转 Decimal 后比较，减少 float 噪音。"""
        return NavCalculator.approx_equal(a, b, tolerance=tolerance)

    @classmethod
    def _approx_equal_quantized(cls, a: Optional[float], b: Optional[float], quantizer, *, tolerance: float = 0.0) -> bool:
        """Compare two numbers after applying the same quantizer.

        This avoids false negatives where one side is quantized (e.g., stored field) and
        the other is raw computed (e.g., expected_*), which can differ by one quant unit.
        """
        return NavCalculator.approx_equal_quantized(a, b, quantizer, tolerance=tolerance)

    @classmethod
    def _money_equal(cls, a: Optional[float], b: Optional[float]) -> bool:
        return NavCalculator.money_equal(a, b)

    @classmethod
    def _nav_equal(cls, a: Optional[float], b: Optional[float]) -> bool:
        return NavCalculator.nav_equal(a, b)

    def _validate_nav_record(
        self, *, nav_record: NAVHistory, last_nav=None,
        prev_month_end_nav=None, prev_year_end_nav=None,
        daily_cash_flow: float = 0.0, monthly_cash_flow: float = 0.0,
        yearly_cash_flow: float = 0.0, gap_cash_flow: Optional[float] = None,
        initial_value: Optional[float] = None, cumulative_cash_flow: float = 0.0,
    ):
        """对即将写入的 NAV 记录做运行时自校验，防止不自洽数据静默落库。"""
        self.nav_calculator.validate_nav_record(
            nav_record=nav_record,
            last_nav=last_nav,
            prev_month_end_nav=prev_month_end_nav,
            prev_year_end_nav=prev_year_end_nav,
            daily_cash_flow=daily_cash_flow,
            monthly_cash_flow=monthly_cash_flow,
            yearly_cash_flow=yearly_cash_flow,
            gap_cash_flow=gap_cash_flow,
            initial_value=initial_value,
            cumulative_cash_flow=cumulative_cash_flow,
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
        return self.nav_calculator.build_nav_record(
            today=today,
            account=account,
            valuation=valuation,
            stock_value=stock_value,
            cash_value=cash_value,
            total_value=total_value,
            stock_ratio=stock_ratio,
            cash_ratio=cash_ratio,
            daily_cash_flow=daily_cash_flow,
            monthly_cash_flow=monthly_cash_flow,
            yearly_cash_flow=yearly_cash_flow,
            yearly_data=yearly_data,
            cumulative_cash_flow=cumulative_cash_flow,
            start_year=start_year,
            shares=shares,
            shares_change=shares_change,
            nav=nav,
            month_nav_change=month_nav_change,
            year_nav_change=year_nav_change,
            cumulative_nav_change=cumulative_nav_change,
            daily_appreciation=daily_appreciation,
            month_appreciation=month_appreciation,
            year_appreciation=year_appreciation,
            cumulative_appreciation=cumulative_appreciation,
            initial_value=initial_value,
            first_year_data=first_year_data,
            cagr=cagr,
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
        return self.nav_summary_printer.print_summary(
            today=today,
            stock_value=stock_value,
            cash_value=cash_value,
            total_value=total_value,
            stock_ratio=stock_ratio,
            cash_ratio=cash_ratio,
            current_year=current_year,
            start_year=start_year,
            yesterday_nav=yesterday_nav,
            prev_year_end_nav=prev_year_end_nav,
            prev_month_end_nav=prev_month_end_nav,
            yearly_data=yearly_data,
            shares=shares,
            shares_change=shares_change,
            nav=nav,
            month_nav_change=month_nav_change,
            year_nav_change=year_nav_change,
            cumulative_nav_change=cumulative_nav_change,
            daily_appreciation=daily_appreciation,
            month_appreciation=month_appreciation,
            year_appreciation=year_appreciation,
            cumulative_appreciation=cumulative_appreciation,
            initial_value=initial_value,
            first_year_data=first_year_data,
            cumulative_cash_flow=cumulative_cash_flow,
            daily_cash_flow=daily_cash_flow,
            monthly_cash_flow=monthly_cash_flow,
            cagr=cagr,
            **_extra,
        )

    def _get_last_day_nav(self, account: str, current_date: date) -> Optional[NAVHistory]:
        """获取昨日净值记录（严格要求指定日期的前一天）"""
        return self.nav_baseline_service.get_last_day_nav(account, current_date)

    @classmethod
    def _sum_cash_flows(cls, flows) -> float:
        """汇总 cash_flow 列表的人民币金额；内部用 Decimal，输出 float 兼容。"""
        return CashFlowSummaryService.sum_cash_flows(flows)

    def _summarize_cash_flows(self, account: str, today: date, start_year: int, last_nav=None) -> dict:
        """使用预加载聚合缓存计算资金变动口径。"""
        return self.cash_flow_summary_service.summarize(account, today, start_year, last_nav=last_nav)

    def _get_daily_cash_flow(self, account: str, flow_date: date) -> float:
        """获取当日资金变动（优先聚合缓存）。"""
        return self.cash_flow_summary_service.daily(account, flow_date)

    def _get_yearly_cash_flow(self, account: str, year: str) -> float:
        """获取当年累计资金变动（优先聚合缓存）。"""
        return self.cash_flow_summary_service.yearly(account, year)

    def _get_monthly_cash_flow(self, account: str, year: int, month: int) -> float:
        """获取当月累计资金变动（优先聚合缓存）。"""
        return self.cash_flow_summary_service.monthly(account, year, month)

    def _get_period_cash_flow(self, account: str, start_date: date, end_date: date) -> float:
        """获取指定期间的累计资金变动（基于日聚合缓存）。"""
        return self.cash_flow_summary_service.period(account, start_date, end_date)

    def _get_initial_value(self, account: str, all_navs: list = None) -> Optional[float]:
        """获取初始账户净值（净值=1时的初始值）
        从数据库最早的净值记录推算，或使用 config 中的默认值"""
        return self.nav_baseline_service.get_initial_value(account, all_navs=all_navs)

    # ========== 内存查询辅助（避免重复 API 调用）==========

    @staticmethod
    def _build_nav_lookup(navs: list) -> dict:
        """为 NAV 历史构建按年/月和日期的预索引，避免重复全表扫描。"""
        return NavHistoryIndex.build(navs)

    @staticmethod
    def _find_latest_nav_before(navs: list, before_date: date, nav_index: dict = None):
        """从内存 NAV 列表中找指定日期之前的最新记录"""
        return NavHistoryIndex.find_latest_before(navs, before_date, nav_index=nav_index)

    @staticmethod
    def _find_year_end_nav(navs: list, year: str, nav_index: dict = None):
        """从内存 NAV 列表中找指定年份的年末记录。

        仅接受该自然年内真实存在的最后一条记录作为 year-end 基准；
        不再默认拿下一年第一条记录冒充上一年末，避免把数据缺口伪装成有效锚点。
        """
        return NavHistoryIndex.find_year_end(navs, year, nav_index=nav_index)

    @staticmethod
    def _find_prev_month_end_nav(navs: list, year: int, month: int, nav_index: dict = None):
        """从内存 NAV 列表中找上月末记录"""
        return NavHistoryIndex.find_prev_month_end(navs, year, month, nav_index=nav_index)

    def _get_cumulative_cash_flow_from_year(self, account: str, from_year: str, to_date: date) -> float:
        """获取从某年开始到指定日期的累计资金变动（基于聚合缓存）。"""
        return self._get_period_cash_flow(account, date(int(from_year), 1, 1), to_date)

    # ========== 份额管理 ==========

    def get_shares(self, account: str) -> float:
        """获取账户总份额"""
        return self.share_service.get_shares(account)

    def calculate_shares_change(self, account: str, cny_amount: float, nav: Optional[float] = None) -> float:
        """
        计算入金/出金对应的份额变动
        份额变动 = 人民币金额 / 当前净值
        """
        return self.share_service.calculate_shares_change(account, cny_amount, nav=nav)

    # ========== 统计报表 ==========

    def get_asset_distribution(self, account: str) -> Dict[str, float]:
        """获取资产分布"""
        return self.reporting_service.get_asset_distribution(account)

    def get_industry_distribution(self, account: str) -> Dict[str, float]:
        """获取行业分布"""
        return self.reporting_service.get_industry_distribution(account)
