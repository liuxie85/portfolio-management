#!/usr/bin/env python3
"""
Portfolio Management Skill API
投资组合管理 Skill 统一入口

基于飞书多维表作为数据存储，支持多端同步
"""
import sys
import json
from pathlib import Path
from datetime import date, datetime, timedelta

from src.time_utils import bj_today, bj_now_naive
from typing import Dict, Any, Optional, Iterable, List

# 确保能 import 到 src 模块
SKILL_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(SKILL_DIR))

from src.feishu_storage import FeishuStorage, FeishuClient
from src.portfolio import PortfolioManager
from src.price_fetcher import PriceFetcher
from src.storage import create_storage
from src.reporting_utils import normalize_asset_type, normalization_warning, is_cash_like
from src.models import AssetType, AssetClass, Industry, Holding, NAVHistory
from src.asset_utils import (
    validate_code as validate_asset_code,
    detect_asset_type,
    parse_date,
)
from src.broker_message_parser import parse_futu_fill_message
from src.app import FutuBalanceSyncService, PortfolioReadService
from src.app.audit_service import AuditService
from src.write_guard import validate_and_normalize_trade_input, validate_and_normalize_nav_input
from src import config


# ========== 配置 ==========

DEFAULT_ACCOUNT = config.get_account()


def _iter_account_values(value: Any) -> Iterable[str]:
    """Yield normalized account strings from raw storage/client field shapes."""
    if value is None:
        return
    if isinstance(value, str):
        account = value.strip()
        if account:
            yield account
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            yield from _iter_account_values(item)
        return
    if isinstance(value, dict):
        for key in ("text", "name", "value"):
            if key in value:
                yield from _iter_account_values(value.get(key))
        return

    account = str(value).strip()
    if account:
        yield account


def _normalize_accounts(accounts: Any) -> Optional[List[str]]:
    """Normalize account input from Python callers, CLI comma strings, or MCP JSON."""
    if accounts is None:
        return None
    if isinstance(accounts, str):
        raw_items = accounts.split(",")
    elif isinstance(accounts, (list, tuple, set)):
        raw_items = list(accounts)
    else:
        raw_items = [accounts]

    normalized: List[str] = []
    seen = set()
    for item in raw_items:
        for account in _iter_account_values(item):
            if account not in seen:
                seen.add(account)
                normalized.append(account)
    return normalized


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _round_money(value: float) -> float:
    return round(float(value or 0.0), 2)


def _snapshot_failure(nav_record: NAVHistory) -> Optional[Dict[str, Any]]:
    details = getattr(nav_record, "details", None) or {}
    snapshot_error = details.get("snapshot_error")
    if not snapshot_error:
        return None
    return {
        "snapshot_status": details.get("snapshot_status") or "failed",
        "snapshot_persisted": bool(details.get("snapshot_persisted")),
        "snapshot_error": snapshot_error,
    }


# ========== 核心 API 类 ==========

class PortfolioSkill:
    """投资组合管理 Skill 核心类

    额外能力：支持从券商成交提醒消息（如富途成交提醒）解析并写入 transactions 表。
    """

    def build_snapshot(self) -> Dict[str, Any]:
        """构建统一估值快照，供 full_report / record_nav 复用，避免时点差。"""
        return self._read_service().build_snapshot()

    # backward compatibility
    def _build_snapshot(self) -> Dict[str, Any]:
        return self.build_snapshot()

    def audit_nav_history_metrics(self, account: Optional[str] = None, days: int = 900, write_report: bool = True) -> Dict[str, Any]:
        """审计 nav_history 四个核心派生字段，与当前代码公式逐条比对。"""
        return self._audit_service.audit_nav_history_metrics(account=account, days=days, write_report=write_report)

    def audit_nav_history_reconcile(self, account: Optional[str] = None, days: int = 900, write_report: bool = True) -> Dict[str, Any]:
        """按日期顺序对 nav_history 做历史对账，输出 ok / exempt / anomaly。"""
        return self._audit_service.audit_nav_history_reconcile(account=account, days=days, write_report=write_report)

    def audit_nav_history_accuracy(self, account: Optional[str] = None, days: int = 900, write_report: bool = True) -> Dict[str, Any]:
        """统一准确性审计入口：汇总 metrics / reconcile / repair candidates。"""
        return self._audit_service.audit_nav_history_accuracy(account=account, days=days, write_report=write_report)

    def repair_nav_history_metrics(self, account: Optional[str] = None, days: int = 900, dry_run: bool = True, write_report: bool = True) -> Dict[str, Any]:
        """按统一准确性审计结果修复 nav_history 派生字段；仅修复真正 anomaly，默认 dry_run。"""
        return self._audit_service.repair_nav_history_metrics(account=account, days=days, dry_run=dry_run, write_report=write_report)

    def __init__(
        self,
        account: str = DEFAULT_ACCOUNT,
        feishu_client: FeishuClient = None,
        storage: Optional[FeishuStorage] = None,
        portfolio: Optional[PortfolioManager] = None,
        price_fetcher: Optional[PriceFetcher] = None,
    ):
        """
        初始化 Skill

        Args:
            account: 账户标识，默认 "lx"
            feishu_client: 飞书客户端实例（可选，用于自定义配置）
            storage: 存储实例（可选，用于测试或离线注入）
            portfolio: PortfolioManager 实例（可选，用于测试或离线注入）
            price_fetcher: 价格获取器实例（可选）
        """
        self.account = account
        self.storage = storage or (FeishuStorage(feishu_client) if feishu_client else create_storage(healthcheck=False))
        self.portfolio = portfolio or PortfolioManager(self.storage)
        self.price_fetcher = price_fetcher or PriceFetcher(storage=self.storage)
        self._audit_service = AuditService(
            storage=self.storage,
            portfolio=self.portfolio,
            account=account,
            report_dir=SKILL_DIR / 'audit',
            api=self,
        )

    # ---------- 交易记录 ----------

    def buy(self, code: str, name: str, quantity: float, price: float,
            date_str: str = None, broker: str = "平安证券", fee: float = 0,
            auto_deduct_cash: bool = False, request_id: str = None,
            skip_validation: bool = False) -> Dict[str, Any]:
        """
        记录买入交易

        Args:
            code: 资产代码（如 600519、AAPL）
            name: 资产名称
            quantity: 买入数量
            price: 买入价格
            date_str: 交易日期 (YYYY-MM-DD)，默认今天
            broker: 券商/平台，默认 "平安证券"
            fee: 手续费
            auto_deduct_cash: 是否自动扣减现金，默认 False
            request_id: 请求唯一标识（用于幂等性控制）
            skip_validation: 是否跳过代码有效性校验（默认校验）

        Returns:
            {"success": bool, "transaction": dict, "message": str}
        """
        try:
            tx_date = parse_date(date_str)

            # 代码格式校验（不自动补齐，格式错误直接报错）
            validated_code = validate_asset_code(code)

            asset_type, currency, asset_class = detect_asset_type(validated_code)

            # 代码有效性校验（通过价格接口验证）
            if not skip_validation:
                price_data = self.price_fetcher.fetch(validated_code)
                if not price_data or 'error' in price_data or not price_data.get('price'):
                    return {
                        "success": False,
                        "error": f"代码 {validated_code} 无效或无法获取价格",
                        "message": f"代码 {validated_code} 无效或无法获取价格，请检查代码是否正确。如需强制记录，请设置 skip_validation=True"
                    }

            tx = self.portfolio.buy(
                tx_date=tx_date,
                asset_id=validated_code,
                asset_name=name,
                asset_type=asset_type,
                account=self.account,
                quantity=quantity,
                price=price,
                currency=currency,
                broker=broker,
                fee=fee,
                asset_class=asset_class,
                industry=Industry.OTHER,
                auto_deduct_cash=auto_deduct_cash,
                request_id=request_id
            )

            # 使用实际保存的完整名称（可能已从接口自动获取）
            saved_name = tx.asset_name or name
            return {
                "success": True,
                "transaction": {
                    "record_id": tx.record_id,
                    "date": tx.tx_date.isoformat(),
                    "type": tx.tx_type.value,
                    "code": tx.asset_id,
                    "name": saved_name,
                    "quantity": tx.quantity,
                    "price": tx.price,
                    "amount": tx.quantity * tx.price,
                    "fee": tx.fee,
                    "total_cost": tx.quantity * tx.price + tx.fee
                },
                "message": f"买入记录已保存: {saved_name} {quantity}股 @ ¥{price}"
            }
        except Exception as e:
            return {"success": False, "error": str(e), "message": f"记录失败: {e}"}

    def sell(self, code: str, quantity: float, price: float,
             date_str: str = None, broker: str = None, fee: float = 0,
             auto_add_cash: bool = False, request_id: str = None) -> Dict[str, Any]:
        """
        记录卖出交易

        Args:
            code: 资产代码
            quantity: 卖出数量
            price: 卖出价格
            date_str: 交易日期 (YYYY-MM-DD)
            broker: 券商/平台
            fee: 手续费
            auto_add_cash: 是否自动增加现金
            request_id: 请求唯一标识（用于幂等性控制）

        Returns:
            {"success": bool, "transaction": dict, "message": str}
        """
        try:
            tx_date = parse_date(date_str)

            # 代码格式校验（不自动补齐，格式错误直接报错）
            validated_code = validate_asset_code(code)

            # 获取持仓信息
            holding = self.storage.get_holding(validated_code, self.account, broker)
            if not holding:
                return {
                    "success": False,
                    "error": f"未找到持仓: {validated_code}",
                    "message": f"未找到持仓: {validated_code}"
                }

            tx = self.portfolio.sell(
                tx_date=tx_date,
                asset_id=validated_code,
                account=self.account,
                quantity=quantity,
                price=price,
                currency=holding.currency,
                broker=broker or holding.broker,
                fee=fee,
                auto_add_cash=auto_add_cash,
                request_id=request_id
            )

            return {
                "success": True,
                "transaction": {
                    "record_id": tx.record_id,
                    "date": tx.tx_date.isoformat(),
                    "code": tx.asset_id,
                    "name": tx.asset_name,
                    "quantity": quantity,
                    "price": price,
                    "proceeds": quantity * price - fee,
                    "fee": fee
                },
                "message": f"卖出记录已保存: {tx.asset_name} {quantity}股 @ ¥{price}"
            }
        except Exception as e:
            return {"success": False, "error": str(e), "message": f"记录失败: {e}"}

    def deposit(self, amount: float, date_str: str = None,
                remark: str = "入金", currency: str = "CNY") -> Dict[str, Any]:
        """记录入金"""
        try:
            flow_date = parse_date(date_str)
            cf = self.portfolio.deposit(
                flow_date=flow_date,
                account=self.account,
                amount=amount,
                currency=currency,
                remark=remark
            )
            return {
                "success": True,
                "cashflow": {
                    "record_id": cf.record_id,
                    "date": cf.flow_date.isoformat(),
                    "amount": cf.amount,
                    "currency": cf.currency,
                    "remark": remark
                },
                "message": f"入金记录已保存: ¥{amount:,.2f}"
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def withdraw(self, amount: float, date_str: str = None,
                 remark: str = "出金", currency: str = "CNY") -> Dict[str, Any]:
        """记录出金"""
        try:
            flow_date = parse_date(date_str)
            cf = self.portfolio.withdraw(
                flow_date=flow_date,
                account=self.account,
                amount=amount,
                currency=currency,
                remark=remark
            )
            return {
                "success": True,
                "cashflow": {
                    "record_id": cf.record_id,
                    "date": cf.flow_date.isoformat(),
                    "amount": -amount,
                    "currency": cf.currency,
                    "remark": remark
                },
                "message": f"出金记录已保存: ¥{amount:,.2f}"
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ---------- 持仓查询 ----------

    def record_transaction_from_message(self, message: str,
                                        broker: str = "富途",
                                        fee: float = 0,
                                        auto_cash: bool = False,
                                        request_id: str = None,
                                        dry_run: bool = True,
                                        skip_validation: bool = False) -> Dict[str, Any]:
        """解析券商成交提醒并写入交易表（transactions）。

        当前支持富途成交提醒：
        - 成功买入20股$富途控股 (FUTU.US)$，成交价格：147 ... 2026/03/12 21:59:45 (香港)

        dry_run=True 时只返回解析结构，不写入。
        """
        parsed = parse_futu_fill_message(message, default_market=broker)
        if not parsed.ok:
            return {"success": False, "error": parsed.error, "parsed": parsed.__dict__}

        # map to skill buy/sell
        # derive code in our system: strip suffix like .US/.HK if needed
        code = parsed.asset_id or ""
        # portfolio-management asset_id for US is typically ticker like FUTU (not FUTU.US)
        code_norm = code.replace('.US', '').replace('.HK', '') if code else None

        # date
        date_str = parsed.tx_date

        # name
        name = parsed.asset_name or code_norm or code

        # Build a deterministic request_id unless user provided
        rid = request_id or parsed.request_id

        if dry_run:
            return {
                "success": True,
                "dry_run": True,
                "parsed": parsed.__dict__,
                "action": {
                    "tx_type": parsed.tx_type,
                    "code": code_norm,
                    "name": name,
                    "quantity": parsed.quantity,
                    "price": parsed.price,
                    "date_str": date_str,
                    "broker": broker,
                    "fee": fee,
                    "request_id": rid,
                    "auto_cash": auto_cash,
                }
            }

        if parsed.tx_type == 'BUY':
            return self.buy(
                code=code_norm,
                name=name,
                quantity=float(parsed.quantity),
                price=float(parsed.price),
                date_str=date_str,
                broker=broker,
                fee=fee,
                auto_deduct_cash=auto_cash,
                request_id=rid,
                skip_validation=skip_validation,
            )
        else:
            return self.sell(
                code=code_norm,
                quantity=float(parsed.quantity),
                price=float(parsed.price),
                date_str=date_str,
                broker=broker,
                fee=fee,
                auto_add_cash=auto_cash,
                request_id=rid,
            )


    def get_holdings(self, include_cash: bool = True, group_by_market: bool = False,
                     include_price: bool = False, timeout: int = 10) -> Dict[str, Any]:
        """获取持仓列表

        Args:
            include_cash: 是否包含现金资产
            group_by_market: 是否按券商分组
            include_price: 是否包含实时价格
            timeout: 价格获取超时时间（秒）
        """
        try:
            return self._read_service().get_holdings(
                include_cash=include_cash,
                group_by_market=group_by_market,
                include_price=include_price,
            )
        except Exception as e:
            return {"success": False, "error": str(e)}

    def list_accounts(self, include_default: bool = True) -> Dict[str, Any]:
        """发现当前数据集中出现过的账户。

        账户来源保持只读：优先从 holdings 的存储 API 获取，再轻量读取交易、
        现金流和净值表中的 account 字段。任一来源失败时返回 warning，不阻断
        其他来源和默认账户。
        """
        accounts = set()
        sources: Dict[str, List[str]] = {}
        warnings = []

        def remember(source: str, account_values: Iterable[str]) -> None:
            source_accounts = set()
            for account in account_values:
                if not account:
                    continue
                accounts.add(account)
                source_accounts.add(account)
            sources[source] = sorted(source_accounts)

        try:
            get_holdings_fn = getattr(self.storage, "get_holdings")
            try:
                holdings = get_holdings_fn(account=None, include_empty=True)
            except TypeError:
                holdings = get_holdings_fn(account=None)
            remember("holdings", (getattr(h, "account", None) for h in holdings or []))
        except Exception as e:
            warnings.append({"source": "holdings", "error": str(e)})

        client = getattr(self.storage, "client", None)
        list_records = getattr(client, "list_records", None)
        if callable(list_records):
            for table in ("transactions", "cash_flow", "nav_history"):
                try:
                    records = list_records(table, field_names=["account"])
                    values = []
                    for record in records or []:
                        raw_fields = record.get("fields") or {}
                        fields = raw_fields
                        convert_fields = getattr(self.storage, "_from_feishu_fields", None)
                        if callable(convert_fields):
                            try:
                                fields = convert_fields(raw_fields, table)
                            except Exception:
                                fields = raw_fields
                        values.extend(_iter_account_values(fields.get("account")))
                    remember(table, values)
                except Exception as e:
                    warnings.append({"source": table, "error": str(e)})

        if include_default and self.account:
            accounts.add(self.account)

        result = {
            "success": True,
            "default_account": self.account,
            "accounts": sorted(accounts),
            "count": len(accounts),
            "sources": sources,
        }
        if warnings:
            result["warnings"] = warnings
        return result

    def _read_service(self) -> PortfolioReadService:
        return PortfolioReadService(
            account=self.account,
            storage=self.storage,
            portfolio=self.portfolio,
            reporting_service=self.portfolio.reporting_service,
        )

    # Backward-compatible helper for tests and old callers.
    @staticmethod
    def _format_holdings_result(
        *,
        result: Dict[str, Any],
        holdings: list,
        group_by_market: bool,
        include_price: bool,
    ) -> Dict[str, Any]:
        return PortfolioReadService._format_holdings_result(
            result=result,
            holdings=holdings,
            group_by_market=group_by_market,
            include_price=include_price,
        )


    def get_position(self, holdings_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """获取仓位分析

        Args:
            holdings_data: 已获取的持仓数据，如果提供则直接使用，避免重复查询
        """
        try:
            return self._read_service().get_position(holdings_data=holdings_data)
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_distribution(self, holdings_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """获取资产分布

        Args:
            holdings_data: 已获取的持仓数据，如果提供则直接使用，避免重复查询
        """
        try:
            return self._read_service().get_distribution(holdings_data=holdings_data)
        except Exception as e:
            return {"success": False, "error": str(e)}

    @staticmethod
    def _snapshot_from_holdings_data(holdings_data: Dict[str, Any]) -> Dict[str, Any]:
        return PortfolioReadService._snapshot_from_holdings_data(holdings_data)

    # ---------- 净值和收益 ----------

    def get_nav(self, days: int = 30) -> Dict[str, Any]:
        """获取账户净值

        Args:
            days: 最近 N 天（默认 30）。
        """
        try:
            # 一次 API 调用获取最近 N 天，从中取 latest
            navs = self.storage.get_nav_history(self.account, days=days)
            if not navs:
                return {"success": False, "message": "无净值记录"}

            latest = navs[-1]  # navs 已按日期升序排列

            # 构建 latest 响应，核心指标已为顶层字段
            latest_data = {
                "date": latest.date.isoformat(),
                "nav": latest.nav,
                "shares": latest.shares,
                "total_value": latest.total_value,
                "stock_value": latest.stock_value,
                "cash_value": latest.cash_value,
                "stock_weight": latest.stock_weight,
                "cash_weight": latest.cash_weight,
                "cash_flow": latest.cash_flow,
                "share_change": latest.share_change,
                "mtd_nav_change": latest.mtd_nav_change,
                "ytd_nav_change": latest.ytd_nav_change,
                "mtd_pnl": latest.mtd_pnl,
                "ytd_pnl": latest.ytd_pnl,
            }

            # 添加 details 中的扩展数据（各年份明细、累计等）
            if latest.details:
                latest_data["details"] = latest.details
                # 动态展开各年份数据 (nav_change_YYYY, appreciation_YYYY, ...)
                for k, v in latest.details.items():
                    if k.startswith(("nav_change_", "appreciation_", "cash_flow_")) and k not in latest_data:
                        latest_data[k] = v
                # 展开累计数据
                for key in ("cumulative_appreciation", "cumulative_nav_change",
                            "year_cash_flow", "initial_value"):
                    if key in latest.details:
                        latest_data[key] = latest.details[key]

            # 构建 history，包含关键指标
            history = []
            for n in navs:
                item = {
                    "date": n.date.isoformat(),
                    "nav": n.nav,
                    "share_change": n.share_change,
                }
                history.append(item)

            return {
                "success": True,
                "latest": latest_data,
                "history": history
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_return(self, period_type: str, period: str = None) -> Dict[str, Any]:
        """
        获取收益率

        Args:
            period_type: "month", "year", "since_inception"
            period: 月份(2025-03) 或 年份(2025)
        """
        try:
            if period_type == "month":
                return self._calc_month_return(period)
            elif period_type == "year":
                return self._calc_year_return(period)
            elif period_type in ("since_inception", "since2024"):
                return self._calc_since_inception_return()
            else:
                return {"success": False, "error": f"不支持的周期类型: {period_type}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _calc_month_return(self, month: str, _navs: list = None) -> Dict:
        """计算月收益率（环比：较上月末的变化）"""
        navs = _navs if _navs is not None else self.storage.get_nav_history(self.account, days=365)
        month_navs = [n for n in navs if n.date.strftime('%Y-%m') == month]

        if len(month_navs) < 1:
            return {"success": False, "message": f"{month} 数据不足"}

        # 当月最后一天净值
        end_nav = max(month_navs, key=lambda x: x.date)

        # 获取上月最后一天净值（作为基准）
        year, mon = int(month[:4]), int(month[5:7])
        if mon == 1:
            prev_month = f"{year-1}-12"
        else:
            prev_month = f"{year}-{mon-1:02d}"

        prev_month_navs = [n for n in navs if n.date.strftime('%Y-%m') == prev_month]
        if prev_month_navs:
            # 上月有数据，使用上月最后一天作为基准
            start_nav = max(prev_month_navs, key=lambda x: x.date)
            start_nav_label = "上月末"
        else:
            # 上月无数据，使用当月第一天作为基准（首次记录）
            start_nav = min(month_navs, key=lambda x: x.date)
            start_nav_label = "月初"

        ret = (end_nav.nav - start_nav.nav) / start_nav.nav * 100 if start_nav.nav > 0 else 0

        return {
            "success": True,
            "period": month,
            "return_pct": ret,
            "start_nav": start_nav.nav,
            "end_nav": end_nav.nav,
            "start_date": start_nav.date.isoformat(),
            "end_date": end_nav.date.isoformat(),
            "base": start_nav_label
        }

    def _calc_year_return(self, year: str, _navs: list = None) -> Dict:
        """计算年收益率（环比：较上年末的变化）"""
        navs = _navs if _navs is not None else self.storage.get_nav_history(self.account, days=730)
        year_navs = [n for n in navs if n.date.strftime('%Y') == year]

        if len(year_navs) < 1:
            return {"success": False, "message": f"{year} 数据不足"}

        # 当年最后一天净值
        end_nav = max(year_navs, key=lambda x: x.date)

        # 获取上年最后一天净值（作为基准）
        prev_year = str(int(year) - 1)
        prev_year_navs = [n for n in navs if n.date.strftime('%Y') == prev_year]

        if prev_year_navs:
            # 上年有数据，使用上年最后一天作为基准
            start_nav = max(prev_year_navs, key=lambda x: x.date)
            start_nav_label = "上年末"
        else:
            # 上年无数据，使用当年第一天作为基准（首次记录）
            start_nav = min(year_navs, key=lambda x: x.date)
            start_nav_label = "年初"

        ret = (end_nav.nav - start_nav.nav) / start_nav.nav * 100 if start_nav.nav > 0 else 0

        return {
            "success": True,
            "period": year,
            "return_pct": ret,
            "start_nav": start_nav.nav,
            "end_nav": end_nav.nav,
            "start_date": start_nav.date.isoformat(),
            "end_date": end_nav.date.isoformat(),
            "base": start_nav_label
        }

    def _calc_since_inception_return(self, _navs: list = None) -> Dict:
        """计算自 start_year 以来收益（以上年末净值为基准，标准化为1）"""
        start_year = config.get_start_year()
        BASE_DATE = date(start_year - 1, 12, 31)

        if _navs is not None:
            # 使用预获取的净值数据
            base_candidates = [n for n in _navs if n.date <= BASE_DATE]
            base_nav = max(base_candidates, key=lambda n: n.date) if base_candidates else None
            latest = _navs[-1] if _navs else None
        else:
            base_nav = self.storage.get_nav_on_date(self.account, BASE_DATE)
            if not base_nav:
                base_nav = self.storage.get_latest_nav_before(self.account, BASE_DATE)
            latest = self.storage.get_latest_nav(self.account)

        if not base_nav or not latest:
            return {"success": False, "message": "数据不足"}

        actual_start_nav = base_nav.nav
        actual_latest_nav = latest.nav
        if not actual_start_nav or actual_start_nav <= 0:
            return {"success": False, "message": "基准净值无效"}
        normalized_nav = actual_latest_nav / actual_start_nav

        total_ret = (normalized_nav - 1.0) * 100
        days = (latest.date - BASE_DATE).days
        years = days / 365.25
        cagr = ((normalized_nav) ** (1/years) - 1) * 100 if years > 0 else 0

        return {
            "success": True,
            "period": f"{start_year}至今",
            "return_pct": total_ret,
            "total_return_pct": total_ret,
            "cagr": cagr,
            "cagr_pct": cagr,
            "days": days,
            "start_nav": 1.0,
            "start_date": BASE_DATE.isoformat(),
            "latest_nav": round(normalized_nav, 4),
            "actual_start_nav": actual_start_nav,
            "actual_latest_nav": actual_latest_nav,
            "base": f"{start_year - 1}年末"
        }

    # ---------- 现金管理 ----------

    def get_cash(self) -> Dict[str, Any]:
        """获取现金资产明细"""
        try:
            holdings = self.storage.get_holdings(account=self.account)
            cash_holdings = [h for h in holdings if h.asset_type in [AssetType.CASH, AssetType.MMF]]

            items = []
            by_currency = {}
            for h in cash_holdings:
                currency = h.currency or 'CNY'
                items.append({
                    "code": h.asset_id,
                    "name": h.asset_name,
                    "amount": h.quantity,
                    "currency": currency,
                    "type": h.asset_type.value
                })
                by_currency[currency] = by_currency.get(currency, 0) + h.quantity

            return {
                "success": True,
                "by_currency": by_currency,
                "items": items,
                "count": len(items)
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def add_cash(self, amount: float, asset: str = "CNY-CASH") -> Dict[str, Any]:
        """增加现金"""
        try:
            holding = self.storage.get_holding(asset, self.account)
            if holding:
                new_qty = holding.quantity + amount
                self.storage.update_holding_quantity(asset, self.account, amount, getattr(holding, 'broker', None))
                return {
                    "success": True,
                    "asset": asset,
                    "amount": amount,
                    "balance": new_qty,
                    "message": f"{asset} 增加 ¥{amount:,.2f}，当前余额: ¥{new_qty:,.2f}"
                }
            else:
                return {"success": False, "error": f"未找到 {asset}，需要先创建"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def sub_cash(self, amount: float, asset: str = "CNY-CASH") -> Dict[str, Any]:
        """减少现金"""
        try:
            holding = self.storage.get_holding(asset, self.account)
            if not holding:
                return {"success": False, "error": f"未找到 {asset}"}

            if holding.quantity < amount:
                return {
                    "success": False,
                    "error": f"余额不足，当前: ¥{holding.quantity:,.2f}"
                }

            new_qty = holding.quantity - amount
            self.storage.update_holding_quantity(asset, self.account, -amount, getattr(holding, 'broker', None))
            return {
                "success": True,
                "asset": asset,
                "amount": amount,
                "balance": new_qty,
                "message": f"{asset} 减少 ¥{amount:,.2f}，当前余额: ¥{new_qty:,.2f}"
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def sync_futu_cash_mmf(
        self,
        broker: str = "富途",
        dry_run: bool = True,
        cash_balance: float = None,
        mmf_balance: float = None,
    ) -> Dict[str, Any]:
        """通过富途 OpenAPI 同步现金/货基余额到 holdings。

        默认预览不写入；测试或人工校准可传入 cash_balance/mmf_balance 跳过 API。
        """
        try:
            service = FutuBalanceSyncService(self.storage)
            return service.sync_cash_and_mmf(
                account=self.account,
                broker=broker,
                dry_run=dry_run,
                cash_balance=cash_balance,
                mmf_balance=mmf_balance,
            )
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ---------- 完整报告 ----------

    def generate_report(self, report_type: str = "daily",
                        record_nav: bool = False, price_timeout: int = 30,
                        snapshot: Optional[Dict[str, Any]] = None,
                        navs: Optional[list] = None,
                        overwrite_existing: bool = True,
                        dry_run: bool = False) -> Dict[str, Any]:
        """生成日报/月报/年报

        Args:
            report_type: "daily" | "monthly" | "yearly"
            record_nav: 是否自动记录今日净值
            price_timeout: 价格获取超时时间（秒）
        """
        snapshot = snapshot or self.build_snapshot()
        full = self.full_report(price_timeout=price_timeout, snapshot=snapshot, navs=navs)
        if not full.get("success"):
            return full

        # 记录净值在报告生成之后，避免影响报告数据
        nav_recorded = None
        if record_nav:
            nav_recorded = self.record_nav(
                price_timeout=price_timeout,
                snapshot=snapshot,
                overwrite_existing=overwrite_existing,
                dry_run=dry_run,
            )

        nav = full.get("nav") or {}
        nav_details = nav.get("details") or {}
        returns = full.get("returns") or {}
        since_inception = returns.get("since_inception") or {}
        cagr_value = nav_details.get("cagr")
        cagr_pct_value = nav_details.get("cagr_pct")
        if cagr_value is None and since_inception.get("success"):
            # 兼容 nav_history 表没有 details 字段的情况
            cagr_pct_value = since_inception.get("cagr_pct")
            cagr_value = (cagr_pct_value / 100) if cagr_pct_value is not None else since_inception.get("cagr")

        report_warnings = list(full.get("warnings") or [])

        if report_type == "daily":
            return {
                "success": True,
                "snapshot_time": snapshot.get("snapshot_time"),
                "report_type": "日报",
                "date": nav.get("date"),
                "overview": full["overview"],
                "nav": nav.get("nav"),
                "total_value": nav.get("total_value"),
                "cash_flow": nav.get("cash_flow"),
                # 与 full_report()['nav'] 对齐，补充净值收益指标（向后兼容：仅新增字段）
                "pnl": nav.get("pnl"),
                "mtd_nav_change": nav.get("mtd_nav_change"),
                "ytd_nav_change": nav.get("ytd_nav_change"),
                "mtd_pnl": nav.get("mtd_pnl"),
                "ytd_pnl": nav.get("ytd_pnl"),
                "top_holdings": full.get("top_holdings"),
                "cagr": cagr_value,
                "cagr_pct": cagr_pct_value,
                "warnings": report_warnings,
            }

        elif report_type == "monthly":
            return {
                "success": True,
                "snapshot_time": snapshot.get("snapshot_time"),
                "report_type": "月报",
                "date": nav.get("date"),
                "overview": full["overview"],
                "nav": nav.get("nav"),
                "total_value": nav.get("total_value"),
                "monthly_return": returns.get("monthly"),
                "mtd_nav_change": nav.get("mtd_nav_change"),
                "mtd_pnl": nav.get("mtd_pnl"),
                "top_holdings": full.get("top_holdings"),
                "distribution": full.get("distribution"),
                "cagr": cagr_value,
                "cagr_pct": cagr_pct_value,
            }

        elif report_type == "yearly":
            # 收集各年份涨幅和升值（从 nav.details 读取）
            yearly_breakdown = {}
            for k, v in nav_details.items():
                if k.startswith(("nav_change_", "appreciation_", "cash_flow_")):
                    yearly_breakdown[k] = v

            return {
                "success": True,
                "snapshot_time": snapshot.get("snapshot_time"),
                "report_type": "年报",
                "date": nav.get("date"),
                "overview": full["overview"],
                "nav": nav.get("nav"),
                "total_value": nav.get("total_value"),
                "yearly_return": returns.get("yearly"),
                "ytd_nav_change": nav.get("ytd_nav_change"),
                "ytd_pnl": nav.get("ytd_pnl"),
                "since_inception": returns.get("since_inception"),
                "risk": {
                    "volatility": returns.get("historical_volatility"),
                    "max_drawdown": returns.get("max_drawdown"),
                },
                "yearly_breakdown": yearly_breakdown,
                "cumulative_nav_change": nav_details.get("cumulative_nav_change"),
                "cumulative_appreciation": nav_details.get("cumulative_appreciation"),
                "top_holdings": full.get("top_holdings"),
                "distribution": full.get("distribution"),
            }

        else:
            return {"success": False, "error": f"不支持的报告类型: {report_type}，可选: daily/monthly/yearly"}

    def _merge_daily_top_holdings(self, holdings: list, total_value: float, top_n: int = 10) -> list:
        """日报 Top 持仓合并口径：
        1) 同代码（跨券商/市场）合并为一行
        2) 现金/货基（asset_type= cash/mmf 或代码后缀 -CASH/-MMF）合并为一行
        3) 权重按 total_value 重新计算
        """
        if not holdings:
            return []

        merged_by_code: Dict[str, Dict[str, Any]] = {}
        cash_bucket: Dict[str, Any] = {
            "code": "CASH+MMF",
            "name": "现金及货基",
            "quantity": 0.0,
            "type": "cash",
            "normalized_type": "cash",
            "broker": "多券商汇总",
            "currency": "MIXED",
            "price": None,
            "cny_price": None,
            "market_value": 0.0,
            "weight": 0.0,
            "_parts": set(),
        }

        for h in holdings:
            code = str(h.get("code") or "").strip()
            if not code:
                continue

            normalized_type = h.get("normalized_type")
            raw_type = h.get("type")
            is_cash = bool(normalized_type == "cash" or is_cash_like(raw_type, code))
            mv = float(h.get("market_value") or 0.0)
            qty = float(h.get("quantity") or 0.0)

            if is_cash:
                cash_bucket["quantity"] += qty
                cash_bucket["market_value"] += mv
                cash_bucket["_parts"].add(code)
                continue

            key = code.upper()
            if key not in merged_by_code:
                merged_by_code[key] = {
                    "code": code,
                    "name": h.get("name"),
                    "quantity": qty,
                    "type": raw_type,
                    "normalized_type": normalized_type,
                    "broker": "多券商汇总",
                    "currency": h.get("currency") or "MIXED",
                    "price": None,
                    "cny_price": None,
                    "market_value": mv,
                    "weight": 0.0,
                    "_parts": {code},
                }
            else:
                row = merged_by_code[key]
                row["quantity"] += qty
                row["market_value"] += mv
                row["_parts"].add(code)
                # 若币种不一致，标记 MIXED
                if row.get("currency") != (h.get("currency") or "MIXED"):
                    row["currency"] = "MIXED"

        merged_rows = list(merged_by_code.values())
        if cash_bucket["_parts"]:
            cash_bucket["code"] = "CASH+MMF"
            cash_bucket["name"] = "现金及货基(合并)"
            merged_rows.append(cash_bucket)

        for row in merged_rows:
            row.pop("_parts", None)
            mv = float(row.get("market_value") or 0.0)
            row["weight"] = (mv / total_value) if total_value > 0 else 0.0

        merged_rows.sort(key=lambda x: float(x.get("market_value") or 0.0), reverse=True)
        return merged_rows[:top_n]

    def full_report(self, price_timeout: int = 30, snapshot: Optional[Dict[str, Any]] = None, navs: Optional[list] = None) -> Dict[str, Any]:
        """生成完整报告（只读，不记录净值）

        利用实时持仓价格合成"今日"虚拟净值，确保收益统计始终可用，
        即使当天尚未调用 record_nav()。

        Args:
            price_timeout: 价格获取超时时间（秒），默认30秒
        """
        try:
            snapshot = snapshot or self.build_snapshot()
            valuation = snapshot["valuation"]
            holdings_data = snapshot["holdings_data"]
            position_data = snapshot["position_data"]

            # 一次性获取全部净值历史（1 次 API 调用）
            all_navs = navs if navs is not None else self.storage.get_nav_history(self.account, days=9999)

            # --- 合成实时虚拟净值 ---
            # 用统一估值结果 + 最近一次记录的份额，推算当前净值与四个派生指标
            today = bj_today()
            live_total = valuation.total_value_cny
            live_cash = valuation.cash_value_cny
            live_stock = valuation.stock_value_cny + valuation.fund_value_cny

            working_navs = [n for n in all_navs if n.date < today]
            synthetic_nav = None
            if all_navs and live_total > 0:
                last_nav = all_navs[-1]
                if last_nav.shares and last_nav.shares > 0:
                    current_year = str(today.year)
                    yesterday_nav = self.portfolio._find_latest_nav_before(all_navs, today)
                    prev_year_end_nav = self.portfolio._find_year_end_nav(all_navs, str(today.year - 1))
                    prev_month_end_nav = self.portfolio._find_prev_month_end_nav(all_navs, today.year, today.month)
                    daily_cash_flow = self.portfolio._get_daily_cash_flow(self.account, today)
                    monthly_cash_flow = self.portfolio._get_monthly_cash_flow(self.account, today.year, today.month)
                    yearly_cash_flow = self.portfolio._get_yearly_cash_flow(self.account, current_year)
                    if last_nav and last_nav.date < today:
                        from datetime import timedelta
                        gap_start = last_nav.date + timedelta(days=1)
                        gap_cash_flow = self.portfolio._get_period_cash_flow(self.account, gap_start, today)
                        base_shares = last_nav.shares or 0
                        base_nav = last_nav.nav
                    else:
                        gap_cash_flow = daily_cash_flow
                        base_shares = last_nav.shares or 0
                        base_nav = last_nav.nav

                    synthetic_share_change = (gap_cash_flow / base_nav) if base_nav else 0.0
                    synthetic_shares = base_shares + synthetic_share_change
                    synthetic_nav_value = live_total / synthetic_shares if synthetic_shares > 0 else 1.0
                    synthetic_mtd_nav_change = self.portfolio._calc_mtd_nav_change(synthetic_nav_value, prev_month_end_nav)
                    synthetic_ytd_nav_change = self.portfolio._calc_ytd_nav_change(synthetic_nav_value, prev_year_end_nav)
                    synthetic_mtd_pnl = self.portfolio._calc_mtd_pnl(live_total, prev_month_end_nav, monthly_cash_flow)
                    synthetic_ytd_pnl = self.portfolio._calc_ytd_pnl(live_total, prev_year_end_nav, yearly_cash_flow)
                    synthetic_daily_pnl = None
                    if yesterday_nav and yesterday_nav.date and (today - yesterday_nav.date).days == 1:
                        synthetic_daily_pnl = live_total - yesterday_nav.total_value - gap_cash_flow

                    synthetic_nav = NAVHistory(
                        date=today,
                        account=self.account,
                        total_value=round(live_total, 2),
                        cash_value=round(live_cash, 2),
                        stock_value=round(live_stock, 2),
                        fund_value=round(valuation.fund_value_cny, 2),
                        cn_stock_value=round(valuation.cn_asset_value, 2),
                        us_stock_value=round(valuation.us_asset_value, 2),
                        hk_stock_value=round(valuation.hk_asset_value, 2),
                        shares=round(synthetic_shares, 2),
                        nav=round(synthetic_nav_value, 6),
                        stock_weight=round(live_stock / live_total, 6) if live_total > 0 else 0,
                        cash_weight=round(live_cash / live_total, 6) if live_total > 0 else 0,
                        cash_flow=round(daily_cash_flow, 2),
                        share_change=round(synthetic_share_change, 2),
                        mtd_nav_change=round(synthetic_mtd_nav_change, 6) if synthetic_mtd_nav_change is not None else None,
                        ytd_nav_change=round(synthetic_ytd_nav_change, 6) if synthetic_ytd_nav_change is not None else None,
                        pnl=round(synthetic_daily_pnl, 2) if synthetic_daily_pnl is not None else None,
                        mtd_pnl=round(synthetic_mtd_pnl, 2) if synthetic_mtd_pnl is not None else None,
                        ytd_pnl=round(synthetic_ytd_pnl, 2) if synthetic_ytd_pnl is not None else None,
                        details={"is_synthetic": True},
                    )
                    working_navs.append(synthetic_nav)

            # 从 working_navs（含虚拟今日）构建 nav_latest
            nav_latest = None
            if working_navs:
                latest = working_navs[-1]
                nav_latest = {
                    "date": latest.date.isoformat(),
                    "nav": latest.nav,
                    "shares": latest.shares,
                    "total_value": latest.total_value,
                    "stock_value": latest.stock_value,
                    "cash_value": latest.cash_value,
                    "stock_weight": latest.stock_weight,
                    "cash_weight": latest.cash_weight,
                }
                # 这些字段仅在已记录的 NAV 中有值（虚拟 NAV 为 None）
                if latest.mtd_nav_change is not None:
                    nav_latest["mtd_nav_change"] = latest.mtd_nav_change
                    nav_latest["ytd_nav_change"] = latest.ytd_nav_change
                    nav_latest["pnl"] = latest.pnl
                    nav_latest["mtd_pnl"] = latest.mtd_pnl
                    nav_latest["ytd_pnl"] = latest.ytd_pnl
                    nav_latest["cash_flow"] = latest.cash_flow
                    nav_latest["share_change"] = latest.share_change
                if latest.details:
                    nav_latest["details"] = latest.details

            # 计算风险指标（复用 all_navs，不含虚拟净值）
            hist_volatility, hist_max_dd = self._calc_risk_metrics(all_navs)

            # 获取资产分布（复用持仓数据）
            distribution_data = self.get_distribution(holdings_data=holdings_data)
            distribution_result = distribution_data.get("by_type", []) if distribution_data.get("success") else []

            # 计算收益率（使用 working_navs，含虚拟今日净值）
            current_year = str(today.year)
            current_month = today.strftime('%Y-%m')

            monthly_return = self._calc_month_return(current_month, _navs=working_navs)
            yearly_return = self._calc_year_return(current_year, _navs=working_navs)
            since_inception = self._calc_since_inception_return(_navs=working_navs)

            # 获取 top10 持仓列表（日报口径：同代码跨券商合并 + 现金/货基合并）
            top_holdings_list = self._merge_daily_top_holdings(
                holdings=holdings_data.get("holdings", []),
                total_value=holdings_data.get("total_value", 0) or 0,
                top_n=10,
            )

            return {
                "success": True,
                "generated_at": bj_now_naive().isoformat(),
                "overview": {
                    "total_value": holdings_data.get("total_value", 0),
                    "cash_ratio": position_data.get("cash_ratio", 0),
                    "stock_ratio": position_data.get("stock_ratio", 0),
                    "fund_ratio": position_data.get("fund_ratio", 0)
                },
                "nav": nav_latest,
                "returns": {
                    "monthly": monthly_return,
                    "yearly": yearly_return,
                    "since_inception": since_inception,
                    "historical_volatility": hist_volatility,
                    "max_drawdown": hist_max_dd
                },
                "top_holdings": top_holdings_list,
                "distribution": distribution_result
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def close_nav(self, date_str: str = None,
                  total_value: float = None,
                  cash_value: float = None,
                  stock_value: float = 0.0,
                  overwrite_existing: bool = True,
                  dry_run: bool = True,
                  confirm: bool = False) -> Dict[str, Any]:
        """显式记录“清仓/关闭”状态的净值点（shares=0）。

        为什么要单独做一个入口：
        - shares=0 是合法业务语义，但必须显式触发，不能靠缺失字段/默认 0 混入。
        - 该入口不会去拉价格/估值；你提供 total_value（以及可选 cash/stock 拆分），我们按 CLOSED 规则写入。

        约定：
        - shares 固定写 0
        - nav 固定写 1.0
        - details 写入 {"status":"CLOSED"}
        - 允许 total_value > 0（残余现金等），但建议同时提供 cash_value/stock_value 以保持拆分自洽。

        安全约束：默认 dry_run=True；真正写入必须 confirm=True 且 dry_run=False。
        """
        try:
            nav_date = parse_date(date_str)

            if (not dry_run) and (not confirm):
                return {
                    "success": False,
                    "error": "Refuse to write nav_history without confirm=True (safety guard).",
                    "date": nav_date.isoformat(),
                    "dry_run": dry_run,
                    "confirm": confirm,
                }

            # normalize CLOSED semantics
            v = validate_and_normalize_nav_input(nav=None, shares=0, status='CLOSED')
            if not v['ok']:
                return {"success": False, "error": "invalid CLOSED nav input", "details": v}

            # determine totals
            if total_value is None:
                if cash_value is not None and stock_value is not None:
                    total_value = float(cash_value) + float(stock_value)
                else:
                    return {
                        "success": False,
                        "error": "total_value is required (or provide both cash_value and stock_value)",
                    }

            if cash_value is None and stock_value is not None:
                cash_value = float(total_value) - float(stock_value)
            if stock_value is None and cash_value is not None:
                stock_value = float(total_value) - float(cash_value)

            # If still missing, fall back to a safe split: all cash.
            if cash_value is None and stock_value is None:
                cash_value = float(total_value)
                stock_value = 0.0

            nav_record = NAVHistory(
                date=nav_date,
                account=self.account,
                total_value=round(float(total_value), 2),
                cash_value=round(float(cash_value), 2) if cash_value is not None else None,
                stock_value=round(float(stock_value), 2) if stock_value is not None else None,
                shares=0.0,
                nav=1.0,
                details={"status": "CLOSED"},
            )

            storage_preview = self.storage.write_nav_record(nav_record, overwrite_existing=overwrite_existing, dry_run=True)
            if dry_run:
                return {
                    "success": True,
                    "dry_run": True,
                    "date": nav_date.isoformat(),
                    "nav": nav_record.nav,
                    "shares": nav_record.shares,
                    "total_value": nav_record.total_value,
                    "fields": storage_preview.get("fields"),
                    "existing": storage_preview.get("existing"),
                }

            # real write
            self.storage.write_nav_record(nav_record, overwrite_existing=overwrite_existing, dry_run=False)
            return {
                "success": True,
                "dry_run": False,
                "date": nav_date.isoformat(),
                "nav": nav_record.nav,
                "shares": nav_record.shares,
                "total_value": nav_record.total_value,
                "message": f"已记录 {nav_date} 清仓净值点（CLOSED）：shares=0, nav=1.0",
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def record_nav(self, price_timeout: int = 30, snapshot: Optional[Dict[str, Any]] = None,
                   overwrite_existing: bool = True, dry_run: bool = True,
                   confirm: bool = False, use_bulk_persist: bool = False) -> Dict[str, Any]:
        """记录今日净值（独立方法，与报告生成解耦）

        ⚠️ 安全约束：默认 dry_run=True，避免被日报/调试调用误写入历史。
        只有在 confirm=True 且 dry_run=False 时才会真正写入。

        Args:
            price_timeout: 价格获取超时时间（秒）
            snapshot: 可复用的统一估值快照
            overwrite_existing: 是否允许覆盖同日已有净值记录
            dry_run: 仅演练，不实际写入（默认 True）
            confirm: 明确确认写入（默认 False）
        """
        try:
            snapshot = snapshot or self.build_snapshot()
            valuation = snapshot["valuation"]
            today = bj_today()

            if (not dry_run) and (not confirm):
                return {
                    "success": False,
                    "error": "Refuse to write nav_history without confirm=True (safety guard).",
                    "date": today.isoformat(),
                    "dry_run": dry_run,
                    "confirm": confirm,
                }

            nav_record = self.portfolio.record_nav(
                self.account,
                valuation=valuation,
                nav_date=today,
                persist=True,
                overwrite_existing=overwrite_existing,
                dry_run=dry_run,
                use_bulk_persist=use_bulk_persist,
            )
            storage_result = None
            result = {
                "success": True,
                "date": today.isoformat(),
                "nav": nav_record.nav,
                "total_value": nav_record.total_value,
                "shares": nav_record.shares,
                "message": (f"已演练 {today} 净值写入: {nav_record.nav:.4f}" if dry_run else f"已记录 {today} 净值: {nav_record.nav:.4f}")
            }
            result["snapshot_time"] = snapshot.get("snapshot_time")
            result["dry_run"] = dry_run
            if storage_result is not None:
                result["storage"] = storage_result
            if valuation.warnings:
                result["warnings"] = valuation.warnings
            failure = _snapshot_failure(nav_record)
            if failure:
                result.update(failure)
                result["success"] = False
                result["status"] = "failed" if dry_run else "partial"
                result["error"] = failure["snapshot_error"]
                result["message"] = (
                    f"净值已演练，但 holdings_snapshot 写入校验失败: {failure['snapshot_error']}"
                    if dry_run
                    else f"净值已写入，但 holdings_snapshot 写入失败: {failure['snapshot_error']}"
                )
            return result
        except Exception as e:
            return {"success": False, "error": str(e)}

    def init_nav_history(
        self,
        date_str: str = None,
        price_timeout: int = 30,
        dry_run: bool = True,
        confirm: bool = False,
        use_bulk_persist: bool = False,
    ) -> Dict[str, Any]:
        """为新账户初始化第一条 nav_history。

        该入口只服务“已有 holdings、尚无 nav_history”的账户：
        - 若账户已有任意 nav_history，直接拒绝，避免污染历史。
        - 第一条记录会自然得到 nav=1.0、shares=total_value。
        - 默认 dry_run=True；真实写入必须 dry_run=False 且 confirm=True。
        """
        try:
            nav_date = parse_date(date_str) if date_str else bj_today()

            if (not dry_run) and (not confirm):
                return {
                    "success": False,
                    "error": "Refuse to initialize nav_history without confirm=True (safety guard).",
                    "account": self.account,
                    "date": nav_date.isoformat(),
                    "dry_run": dry_run,
                    "confirm": confirm,
                }

            existing_navs = self.storage.get_nav_history(self.account, days=9999)
            if existing_navs:
                latest = max(existing_navs, key=lambda n: n.date)
                earliest = min(existing_navs, key=lambda n: n.date)
                return {
                    "success": False,
                    "error": "nav_history already exists; initialization is only for empty accounts.",
                    "account": self.account,
                    "existing_count": len(existing_navs),
                    "earliest_date": earliest.date.isoformat(),
                    "latest_date": latest.date.isoformat(),
                    "dry_run": dry_run,
                }

            snapshot = self.build_snapshot()
            valuation = snapshot["valuation"]
            if valuation.total_value_cny <= 0:
                return {
                    "success": False,
                    "error": "Cannot initialize nav_history with non-positive total_value.",
                    "account": self.account,
                    "date": nav_date.isoformat(),
                    "total_value": valuation.total_value_cny,
                    "warnings": valuation.warnings,
                }

            nav_record = self.portfolio.record_nav(
                self.account,
                valuation=valuation,
                nav_date=nav_date,
                persist=True,
                overwrite_existing=False,
                dry_run=dry_run,
                use_bulk_persist=use_bulk_persist,
            )

            result = {
                "success": True,
                "account": self.account,
                "date": nav_date.isoformat(),
                "dry_run": dry_run,
                "nav": nav_record.nav,
                "shares": nav_record.shares,
                "total_value": nav_record.total_value,
                "cash_value": nav_record.cash_value,
                "stock_value": nav_record.stock_value,
                "fund_value": nav_record.fund_value,
                "snapshot_time": snapshot.get("snapshot_time"),
                "message": (
                    f"已演练初始化 {self.account} 的 nav_history: {nav_record.nav:.4f}"
                    if dry_run
                    else f"已初始化 {self.account} 的 nav_history: {nav_record.nav:.4f}"
                ),
            }
            if valuation.warnings:
                result["warnings"] = valuation.warnings
            failure = _snapshot_failure(nav_record)
            if failure:
                result.update(failure)
                result["success"] = False
                result["status"] = "failed" if dry_run else "partial"
                result["error"] = failure["snapshot_error"]
                result["message"] = (
                    f"初始化已演练，但 holdings_snapshot 写入校验失败: {failure['snapshot_error']}"
                    if dry_run
                    else f"nav_history 已初始化，但 holdings_snapshot 写入失败: {failure['snapshot_error']}"
                )
            return result
        except Exception as e:
            return {"success": False, "error": str(e), "account": self.account}

    def _calc_risk_metrics(self, navs) -> tuple:
        """计算风险指标：波动率和最大回撤"""
        import statistics

        if len(navs) < 2:
            return 0, 0

        # 过滤无效 nav
        valid_navs = [n for n in navs if n.nav and n.nav > 0]
        if len(valid_navs) < 2:
            return 0, 0

        returns = []
        for i in range(1, len(valid_navs)):
            r = (valid_navs[i].nav - valid_navs[i-1].nav) / valid_navs[i-1].nav
            returns.append(r)

        volatility = statistics.stdev(returns) * (252 ** 0.5) * 100 if len(returns) > 1 else 0

        max_dd = 0
        peak = valid_navs[0].nav
        for nav in valid_navs[1:]:
            if nav.nav > peak:
                peak = nav.nav
            dd = (peak - nav.nav) / peak
            if dd > max_dd:
                max_dd = dd

        return volatility, max_dd * 100

    # ---------- 价格查询 ----------

    def get_price(self, code: str) -> Dict[str, Any]:
        """查询资产价格"""
        try:
            asset_type, currency, _ = detect_asset_type(code)
            result = self.price_fetcher.fetch(code)

            if result and 'price' in result:
                return {
                    "success": True,
                    "code": code.upper(),
                    "name": result.get('name', 'N/A'),
                    "price": result['price'],
                    "currency": result.get('currency', currency),
                    "cny_price": result.get('cny_price'),
                    "change_pct": result.get('change_pct'),
                    "source": result.get('source', 'N/A')
                }
            else:
                return {"success": False, "error": f"无法获取 {code} 的价格"}
        except Exception as e:
            return {"success": False, "error": str(e)}


# ========== 数据库初始化 ==========

def init_db(account: str = DEFAULT_ACCOUNT, initial_cash: float = 0) -> Dict[str, Any]:
    """
    初始化投资组合数据库（飞书多维表）

    Args:
        account: 账户标识，默认 "lx"
        initial_cash: 初始现金金额（可选），默认 0

    Returns:
        {"success": bool, "message": str}

    Example:
        # 初始化空数据库
        init_db()

        # 初始化并设置初始现金 10万元
        init_db(initial_cash=100000)
    """
    try:
        storage = FeishuStorage()

        # 检查飞书配置
        if not storage.client.app_token:
            raise ValueError("未配置 FEISHU_APP_TOKEN，无法连接飞书多维表")

        # 创建初始现金持仓（如果需要）
        if initial_cash > 0:
            cash_holding = storage.get_holding('CNY-CASH', account)
            if not cash_holding:
                holding = Holding(
                    asset_id='CNY-CASH',
                    asset_name='人民币现金',
                    asset_type=AssetType.CASH,
                    account=account,
                    quantity=initial_cash,
                    currency='CNY',
                    asset_class=AssetClass.CASH,
                    industry=Industry.CASH
                )
                storage.upsert_holding(holding)

        # 检查数据库状态
        holdings = storage.get_holdings(account=account)
        nav_history = storage.get_nav_history(account, days=1)

        return {
            "success": True,
            "account": account,
            "initial_cash": initial_cash,
            "current_holdings": len(holdings),
            "nav_records": len(nav_history),
            "message": f"已初始化飞书多维表\n" +
                      f"  - 持仓记录: {len(holdings)} 条\n" +
                      f"  - 净值记录: {len(nav_history)} 条\n" +
                      (f"  - 初始现金: ¥{initial_cash:,.2f}" if initial_cash > 0 else "")
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": f"初始化失败: {e}"
        }


# ========== 便捷函数（供 Skill 直接调用） ==========

_skill_instances: dict = {}
_skill_lock = __import__('threading').Lock()

def get_skill(account: str = None) -> PortfolioSkill:
    """获取指定账户的 Skill 实例（线程安全，按 account 缓存）。

    同一 account 共享实例；不同 account 的实例共享同一 FeishuClient 以复用连接和缓存。
    account=None 时使用 config 中的默认账户。
    """
    acct = account or DEFAULT_ACCOUNT
    if acct not in _skill_instances:
        with _skill_lock:
            if acct not in _skill_instances:
                # 首个实例正常创建；后续实例复用首个实例的 feishu_client
                if _skill_instances:
                    first = next(iter(_skill_instances.values()))
                    _skill_instances[acct] = PortfolioSkill(account=acct, feishu_client=first.storage.client)
                else:
                    _skill_instances[acct] = PortfolioSkill(account=acct)
    return _skill_instances[acct]


def _get_default_skill() -> PortfolioSkill:
    """获取默认 Skill 实例（向后兼容）"""
    return get_skill()


# 交易记录
def buy(code: str, name: str, quantity: float, price: float, account: str = None, **kwargs) -> Dict:
    """买入资产"""
    return get_skill(account).buy(code, name, quantity, price, **kwargs)

def sell(code: str, quantity: float, price: float, account: str = None, **kwargs) -> Dict:
    """卖出资产"""
    return get_skill(account).sell(code, quantity, price, **kwargs)


def record_transaction_from_message(message: str,
                                    broker: str = "富途",
                                    fee: float = 0,
                                    auto_cash: bool = False,
                                    request_id: str = None,
                                    dry_run: bool = True,
                                    skip_validation: bool = False,
                                    account: str = None) -> Dict:
    """从券商成交提醒消息中解析并记录交易。

    当前支持（富途成交提醒，示例）：
    - 成功买入20股$富途控股 (FUTU.US)$，成交价格：147 ... 2026/03/12 21:59:45 (香港)

    Args:
      message: 原始消息全文
      market: 交易渠道/券商（默认 富途）
      fee: 手续费（消息里通常没有，默认 0，可手填）
      auto_cash: 买入时自动扣现金 / 卖出时自动加现金
      request_id: 幂等键（不传则系统会自动生成）
      dry_run: True 时只返回解析结果，不写入交易表
      skip_validation: 是否跳过代码有效性校验
    """
    return get_skill(account).record_transaction_from_message(
        message=message,
        broker=broker,
        fee=fee,
        auto_cash=auto_cash,
        request_id=request_id,
        dry_run=dry_run,
        skip_validation=skip_validation,
    )

def deposit(amount: float, account: str = None, **kwargs) -> Dict:
    """入金"""
    return get_skill(account).deposit(amount, **kwargs)

def withdraw(amount: float, account: str = None, **kwargs) -> Dict:
    """出金"""
    return get_skill(account).withdraw(amount, **kwargs)

# 持仓查询
def get_holdings(account: str = None, **kwargs) -> Dict:
    """全部持仓"""
    return get_skill(account).get_holdings(**kwargs)

def get_position(account: str = None) -> Dict:
    """仓位分析"""
    return get_skill(account).get_position()

def get_distribution(account: str = None) -> Dict:
    """资产分布"""
    return get_skill(account).get_distribution()

def list_accounts(include_default: bool = True) -> Dict:
    """列出当前数据集中出现过的账户。"""
    return get_skill().list_accounts(include_default=include_default)

# 净值收益
def get_nav(days: int = 30, account: str = None) -> Dict:
    """账户净值

    Args:
        days: 获取最近 N 天历史（默认 30）。对日报发布通常只需要 2 天即可。
    """
    return get_skill(account).get_nav(days=days)

def get_return(period_type: str, period: str = None, account: str = None) -> Dict:
    """查询收益率"""
    return get_skill(account).get_return(period_type, period)

# 现金管理
def get_cash(account: str = None) -> Dict:
    """现金资产"""
    return get_skill(account).get_cash()

def add_cash(amount: float, account: str = None, **kwargs) -> Dict:
    """增加现金"""
    return get_skill(account).add_cash(amount, **kwargs)

def sub_cash(amount: float, account: str = None, **kwargs) -> Dict:
    """减少现金"""
    return get_skill(account).sub_cash(amount, **kwargs)

def sync_futu_cash_mmf(account: str = None, **kwargs) -> Dict:
    """通过富途 OpenAPI 同步现金/货基余额到 holdings"""
    return get_skill(account).sync_futu_cash_mmf(**kwargs)

# 报告
def generate_report(report_type: str = "daily", record_nav: bool = False, price_timeout: int = 30, navs=None, account: str = None) -> Dict:
    """生成日报/月报/年报"""
    return get_skill(account).generate_report(report_type=report_type, record_nav=record_nav, price_timeout=price_timeout, navs=navs)

def full_report(price_timeout: int = 30, account: str = None) -> Dict:
    """完整报告（只读，不记录净值）

    Args:
        price_timeout: 价格获取超时时间（秒），默认30秒
    """
    return get_skill(account).full_report(price_timeout=price_timeout)


def _report_value_breakdown(report: Dict[str, Any]) -> Dict[str, float]:
    overview = report.get("overview") or {}
    total_value = _as_float(overview.get("total_value"), 0.0)

    cash_ratio = _as_float(overview.get("cash_ratio"), 0.0)
    stock_ratio = _as_float(overview.get("stock_ratio"), 0.0)
    fund_ratio = _as_float(overview.get("fund_ratio"), 0.0)

    nav = report.get("nav") or {}
    if cash_ratio == 0 and stock_ratio == 0 and fund_ratio == 0 and total_value:
        cash_value = _as_float(nav.get("cash_value"), 0.0)
        stock_value = _as_float(nav.get("stock_value"), 0.0)
        fund_value = _as_float(nav.get("fund_value"), 0.0)
    else:
        cash_value = total_value * cash_ratio
        stock_value = total_value * stock_ratio
        fund_value = total_value * fund_ratio

    return {
        "total_value": _round_money(total_value),
        "cash_value": _round_money(cash_value),
        "stock_value": _round_money(stock_value),
        "fund_value": _round_money(fund_value),
        "non_cash_value": _round_money(stock_value + fund_value),
    }


def multi_account_overview(accounts: Any = None, price_timeout: int = 30,
                           include_details: bool = False) -> Dict:
    """生成多个账户的只读资产概览。

    Args:
        accounts: 账户列表，或逗号分隔字符串；为空时自动发现账户。
        price_timeout: 传给单账户 full_report 的价格超时时间。
        include_details: 是否在每个账户条目中附带完整 full_report。
    """
    try:
        target_accounts = _normalize_accounts(accounts)
        discovery = None
        if target_accounts is None:
            discovery = list_accounts(include_default=True)
            if not discovery.get("success"):
                return discovery
            target_accounts = discovery.get("accounts") or []

        items = []
        errors = []
        summary_values = {
            "total_value": 0.0,
            "cash_value": 0.0,
            "stock_value": 0.0,
            "fund_value": 0.0,
            "non_cash_value": 0.0,
        }

        for account in target_accounts:
            report = get_skill(account).full_report(price_timeout=price_timeout)
            if not report.get("success"):
                error = {
                    "account": account,
                    "error": report.get("error") or report.get("message") or "unknown error",
                }
                errors.append(error)
                items.append({"account": account, "success": False, **error})
                continue

            values = _report_value_breakdown(report)
            for key in summary_values:
                summary_values[key] += values[key]

            item = {
                "account": account,
                "success": True,
                **values,
                "overview": report.get("overview") or {},
                "nav": report.get("nav"),
                "returns": report.get("returns") or {},
            }
            if include_details:
                item["report"] = report
            items.append(item)

        successful_count = sum(1 for item in items if item.get("success"))
        failed_count = len(errors)
        total_value = summary_values["total_value"]
        summary = {key: _round_money(value) for key, value in summary_values.items()}
        summary.update({
            "cash_ratio": summary["cash_value"] / total_value if total_value > 0 else 0,
            "stock_ratio": summary["stock_value"] / total_value if total_value > 0 else 0,
            "fund_ratio": summary["fund_value"] / total_value if total_value > 0 else 0,
        })

        if not target_accounts:
            status = "empty"
            success = True
        elif successful_count == 0:
            status = "failed"
            success = False
        elif failed_count:
            status = "partial"
            success = True
        else:
            status = "ok"
            success = True

        result = {
            "success": success,
            "status": status,
            "generated_at": bj_now_naive().isoformat(),
            "default_account": DEFAULT_ACCOUNT,
            "accounts": target_accounts,
            "account_count": len(target_accounts),
            "successful_count": successful_count,
            "failed_count": failed_count,
            "summary": summary,
            "items": items,
        }
        if discovery is not None:
            result["discovery"] = discovery
        if errors:
            result["errors"] = errors
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}

def record_nav(price_timeout: int = 30, dry_run: bool = True, confirm: bool = False,
               overwrite_existing: bool = True, use_bulk_persist: bool = False, account: str = None) -> Dict:
    """记录今日净值

    ⚠️ 默认 dry_run=True，避免误写入。
    真正写入必须传：dry_run=False 且 confirm=True。
    """
    return get_skill(account).record_nav(
        price_timeout=price_timeout,
        dry_run=dry_run,
        confirm=confirm,
        overwrite_existing=overwrite_existing,
        use_bulk_persist=use_bulk_persist,
    )


def init_nav_history(date_str: str = None, price_timeout: int = 30, dry_run: bool = True,
                     confirm: bool = False, use_bulk_persist: bool = False,
                     account: str = None) -> Dict:
    """为新账户初始化第一条 nav_history。

    ⚠️ 默认 dry_run=True，且只允许空 nav_history 账户初始化。
    真正写入必须传：dry_run=False 且 confirm=True。
    """
    return get_skill(account).init_nav_history(
        date_str=date_str,
        price_timeout=price_timeout,
        dry_run=dry_run,
        confirm=confirm,
        use_bulk_persist=use_bulk_persist,
    )


def close_nav(date_str: str = None,
              total_value: float = None,
              cash_value: float = None,
              stock_value: float = 0.0,
              overwrite_existing: bool = True,
              dry_run: bool = True,
              confirm: bool = False,
              account: str = None) -> Dict:
    """显式记录“清仓/关闭”净值点（shares=0, nav=1.0）。

    允许 total_value > 0（残余现金等）。

    ⚠️ 默认 dry_run=True；真正写入必须 dry_run=False 且 confirm=True。
    """
    return get_skill(account).close_nav(
        date_str=date_str,
        total_value=total_value,
        cash_value=cash_value,
        stock_value=stock_value,
        overwrite_existing=overwrite_existing,
        dry_run=dry_run,
        confirm=confirm,
    )

# 价格
def get_price(code: str, account: str = None) -> Dict:
    """查询价格"""
    return get_skill(account).get_price(code)


# 数据清理
def clean_data(table: str = None, account: str = None, dry_run: bool = True,
               code: str = None, date_before: str = None,
               empty_only: bool = False, confirm: bool = False) -> Dict:
    """
    清理测试数据

    Args:
        table: 要清理的表 ('holdings', 'transactions', 'cash_flow', 'nav_history', 'all')
        account: 按账户过滤，默认当前账户
        dry_run: 是否只预览不删除（默认 True，设为 False 才实际删除）
        code: 按资产代码过滤（如 'TEST'）
        date_before: 删除指定日期之前的数据 (YYYY-MM-DD)
        empty_only: 只清理空记录（asset_id 为空 或 quantity/price 为 0）

    Returns:
        {"success": bool, "deleted": {...}, "preview": [...]}

    Example:
        # 预览要删除的数据
        clean_data(table='transactions', code='TEST')

        # 实际删除
        clean_data(table='transactions', code='TEST', dry_run=False, confirm=True)

        # 清理空记录
        clean_data(table='all', empty_only=True, dry_run=False, confirm=True)
    """
    try:
        skill = get_skill(account)
        target_account = account or skill.account
        storage = skill.storage

        # 将 date_before 转换为时间戳（毫秒）用于与飞书字段比较
        # 业务语义：北京时间 00:00
        date_before_ts = None
        if date_before:
            from datetime import datetime as dt, timezone, timedelta
            bj = timezone(timedelta(hours=8))
            d = dt.strptime(date_before, "%Y-%m-%d").replace(tzinfo=bj)
            date_before_ts = int(d.timestamp() * 1000)

        results = {
            'holdings': 0,
            'transactions': 0,
            'cash_flow': 0,
            'nav_history': 0
        }
        preview = []

        tables_to_clean = ['holdings', 'transactions', 'cash_flow', 'nav_history'] if table == 'all' else [table]

        if (not dry_run) and (not confirm):
            return {
                'success': False,
                'error': 'Refuse to delete data without confirm=True (safety guard).',
                'dry_run': dry_run,
                'confirm': confirm,
            }

        for tbl in tables_to_clean:
            if tbl == 'holdings':
                # 获取所有记录（包括 quantity=0 的）
                # Always filter by account in destructive actions
                records = storage.client.list_records('holdings', filter_str=f'CurrentValue.[account] = "{target_account}"')
                for record in records:
                    fields = record.get('fields', {})
                    r_id = record.get('record_id')
                    asset_id = fields.get('asset_id', '')
                    quantity = fields.get('quantity', 0)

                    should_delete = False
                    if empty_only:
                        # 空记录：asset_id 为空 或 quantity 为 0 或空
                        if not asset_id or asset_id.strip() == '' or not quantity or quantity == 0 or quantity == '0':
                            should_delete = True
                    else:
                        if code and asset_id == code.upper():
                            should_delete = True

                    if should_delete:
                        preview.append({
                            'table': tbl,
                            'record_id': r_id,
                            'asset_id': asset_id,
                            'quantity': quantity,
                            'reason': 'empty_record' if empty_only else 'code_match'
                        })
                        if not dry_run and r_id:
                            if storage.delete_holding_by_record_id(r_id):
                                results[tbl] += 1

            elif tbl == 'transactions':
                records = storage.client.list_records('transactions', filter_str=f'CurrentValue.[account] = "{target_account}"')
                for record in records:
                    fields = record.get('fields', {})
                    r_id = record.get('record_id')
                    asset_id = fields.get('asset_id', '')
                    tx_date = fields.get('tx_date', '')
                    quantity = fields.get('quantity', 0)
                    price = fields.get('price', 0)

                    should_delete = False
                    if empty_only:
                        # 空记录：asset_id 为空 或 quantity/price 为 0
                        if (not asset_id or asset_id.strip() == '' or
                            not quantity or quantity == 0 or quantity == '0' or
                            not price or price == 0 or price == '0'):
                            should_delete = True
                    else:
                        if code and asset_id == code.upper():
                            should_delete = True
                        if date_before_ts and tx_date:
                            ts = tx_date if isinstance(tx_date, (int, float)) else 0
                            if ts and ts <= date_before_ts:
                                should_delete = True

                    if should_delete:
                        preview.append({
                            'table': tbl,
                            'record_id': r_id,
                            'date': tx_date,
                            'asset_id': asset_id,
                            'reason': 'empty_record' if empty_only else 'matched'
                        })
                        if not dry_run and r_id:
                            if storage.delete_transaction_by_record_id(r_id):
                                results[tbl] += 1

            elif tbl == 'cash_flow':
                records = storage.client.list_records('cash_flow', filter_str=f'CurrentValue.[account] = "{target_account}"')
                for record in records:
                    fields = record.get('fields', {})
                    r_id = record.get('record_id')
                    flow_date = fields.get('flow_date', '')
                    amount = fields.get('amount', 0)

                    should_delete = False
                    if empty_only:
                        # 空记录：amount 为 0 或空
                        if not amount or amount == 0 or amount == '0' or amount == '':
                            should_delete = True
                    else:
                        if date_before_ts and flow_date:
                            ts = flow_date if isinstance(flow_date, (int, float)) else 0
                            if ts and ts <= date_before_ts:
                                should_delete = True

                    if should_delete:
                        preview.append({
                            'table': tbl,
                            'record_id': r_id,
                            'date': flow_date,
                            'amount': amount,
                            'reason': 'empty_record' if empty_only else 'matched'
                        })
                        if not dry_run and r_id:
                            if storage.delete_cash_flow_by_record_id(r_id):
                                results[tbl] += 1

            elif tbl == 'nav_history':
                records = storage.client.list_records('nav_history', filter_str=f'CurrentValue.[account] = "{target_account}"')
                for record in records:
                    fields = record.get('fields', {})
                    r_id = record.get('record_id')
                    nav_date = fields.get('date', '')
                    total_value = fields.get('total_value', 0)
                    nav = fields.get('nav', 0)

                    should_delete = False
                    if empty_only:
                        # 空记录：total_value 或 nav 为 0
                        if (not total_value or total_value == 0 or total_value == '0' or
                            not nav or nav == 0 or nav == '0'):
                            should_delete = True
                    else:
                        if date_before_ts and nav_date:
                            ts = nav_date if isinstance(nav_date, (int, float)) else 0
                            if ts and ts <= date_before_ts:
                                should_delete = True

                    if should_delete:
                        preview.append({
                            'table': tbl,
                            'record_id': r_id,
                            'date': nav_date,
                            'nav': nav,
                            'reason': 'empty_record' if empty_only else 'matched'
                        })
                        if not dry_run and r_id:
                            if storage.delete_nav_by_record_id(r_id):
                                results[tbl] += 1

        return {
            'success': True,
            'dry_run': dry_run,
            'empty_only': empty_only,
            'account': target_account,
            'filters': {'code': code, 'date_before': date_before},
            'deleted_count': results if not dry_run else {k: 0 for k in results},
            'preview': preview[:50],  # 最多显示50条
            'total_preview': len(preview),
            'message': f'{"【预览模式】" if dry_run else "【已删除】"} {"空记录" if empty_only else "匹配记录"}: {len(preview)} 条'
        }
    except Exception as e:
        return {'success': False, 'error': str(e)}
