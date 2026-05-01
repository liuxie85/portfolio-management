#!/usr/bin/env python3
"""
Portfolio Management MCP Server

基于 Model Context Protocol (MCP) 暴露投资组合管理能力，
供 OpenClaw、Claude Desktop 等 MCP 兼容客户端调用。

启动方式:
    python mcp_server.py            # stdio 模式（默认）
    python mcp_server.py --sse      # SSE 模式（HTTP）
"""
import sys
import json
from pathlib import Path

# 确保能 import 到 src 和 skill_api
SKILL_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(SKILL_DIR))

from mcp.server.fastmcp import FastMCP

from src import config
from src.service.client import PortfolioServiceClient, PortfolioServiceUnavailable

from skill_api import (
    get_skill,
    buy,
    sell,
    deposit,
    withdraw,
    record_transaction_from_message,
    get_holdings,
    get_position,
    get_distribution,
    get_nav,
    get_return,
    get_cash,
    get_price,
    list_accounts,
    multi_account_overview,
    record_nav,
    close_nav,
    generate_report,
    sync_futu_cash_mmf,
    clean_data,
)

mcp = FastMCP(
    "portfolio-management",
    instructions=(
        "投资组合管理工具集。支持交易记录（买入/卖出）、持仓查询、净值管理、"
        "现金管理、报告生成等功能。数据存储在飞书多维表中。\n"
        "写入类操作默认带有安全保护（dry_run=True），需显式确认后才会真正写入。"
    ),
)


# ========== 交易类 Tools ==========


def _account_or_default(account: str = None) -> str:
    return account or config.get_account()


def _service_or_fallback(service_call, fallback_call):
    try:
        client = PortfolioServiceClient(timeout=0.5)
        return service_call(client)
    except PortfolioServiceUnavailable:
        return fallback_call()


@mcp.tool()
def tool_buy(
    code: str,
    name: str,
    quantity: float,
    price: float,
    date_str: str = None,
    broker: str = "平安证券",
    fee: float = 0,
    auto_deduct_cash: bool = False,
    request_id: str = None,
    skip_validation: bool = False,
    account: str = None,
) -> str:
    """记录买入交易。

    Args:
        code: 资产代码（如 600519、AAPL、00700）
        name: 资产名称
        quantity: 买入数量
        price: 买入价格
        date_str: 交易日期 (YYYY-MM-DD)，默认今天
        broker: 券商/平台，默认 "平安证券"
        fee: 手续费，默认 0
        auto_deduct_cash: 是否自动扣减现金，默认 False
        request_id: 幂等键（防重复提交）
        skip_validation: 是否跳过代码有效性校验
        account: 账户标识，默认使用配置中的默认账户
    """
    result = buy(
        code, name, quantity, price,
        account=account,
        date_str=date_str, broker=broker, fee=fee,
        auto_deduct_cash=auto_deduct_cash,
        request_id=request_id,
        skip_validation=skip_validation,
    )
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
def tool_sell(
    code: str,
    quantity: float,
    price: float,
    date_str: str = None,
    broker: str = None,
    fee: float = 0,
    auto_add_cash: bool = False,
    request_id: str = None,
    account: str = None,
) -> str:
    """记录卖出交易。

    Args:
        code: 资产代码
        quantity: 卖出数量
        price: 卖出价格
        date_str: 交易日期 (YYYY-MM-DD)，默认今天
        broker: 券商/平台
        fee: 手续费，默认 0
        auto_add_cash: 是否自动增加现金
        request_id: 幂等键（防重复提交）
        account: 账户标识，默认使用配置中的默认账户
    """
    result = sell(
        code, quantity, price,
        account=account,
        date_str=date_str, broker=broker, fee=fee,
        auto_add_cash=auto_add_cash,
        request_id=request_id,
    )
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
def tool_deposit(
    amount: float,
    date_str: str = None,
    remark: str = "入金",
    currency: str = "CNY",
    account: str = None,
) -> str:
    """记录入金（资金转入投资账户）。

    Args:
        amount: 入金金额
        date_str: 日期 (YYYY-MM-DD)，默认今天
        remark: 备注
        currency: 币种，默认 CNY
        account: 账户标识，默认使用配置中的默认账户
    """
    result = deposit(amount, account=account, date_str=date_str, remark=remark, currency=currency)
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
def tool_withdraw(
    amount: float,
    date_str: str = None,
    remark: str = "出金",
    currency: str = "CNY",
    account: str = None,
) -> str:
    """记录出金（资金转出投资账户）。

    Args:
        amount: 出金金额
        date_str: 日期 (YYYY-MM-DD)，默认今天
        remark: 备注
        currency: 币种，默认 CNY
        account: 账户标识，默认使用配置中的默认账户
    """
    result = withdraw(amount, account=account, date_str=date_str, remark=remark, currency=currency)
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
def tool_record_transaction_from_message(
    message: str,
    broker: str = "富途",
    fee: float = 0,
    auto_cash: bool = False,
    request_id: str = None,
    dry_run: bool = True,
    skip_validation: bool = False,
    account: str = None,
) -> str:
    """从券商成交提醒消息中解析并记录交易。

    当前支持富途成交提醒格式。默认 dry_run=True，只返回解析结果不写入。

    Args:
        message: 原始消息全文
        broker: 交易渠道/券商，默认 "富途"
        fee: 手续费，默认 0
        auto_cash: 是否自动增减现金
        request_id: 幂等键
        dry_run: True 时只解析不写入（默认 True）
        skip_validation: 是否跳过代码有效性校验
        account: 账户标识，默认使用配置中的默认账户
    """
    result = record_transaction_from_message(
        message, broker=broker, fee=fee,
        auto_cash=auto_cash, request_id=request_id,
        dry_run=dry_run, skip_validation=skip_validation,
        account=account,
    )
    return json.dumps(result, ensure_ascii=False, default=str)


# ========== 查询类 Tools ==========


@mcp.tool()
def tool_list_accounts(include_default: bool = True) -> str:
    """列出当前数据集中出现过的账户。

    Args:
        include_default: 是否包含配置中的默认账户，即使该账户暂无数据。
    """
    result = _service_or_fallback(
        lambda client: client.list_accounts(include_default=include_default),
        lambda: list_accounts(include_default=include_default),
    )
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
def tool_multi_account_overview(
    accounts: str = None,
    price_timeout: int = 30,
    include_details: bool = False,
) -> str:
    """获取多账户只读资产概览。

    Args:
        accounts: 逗号分隔的账户列表；为空时自动发现账户。
        price_timeout: 单账户报告的价格获取超时（秒）
        include_details: 是否返回每个账户的完整 full_report 明细。
    """
    result = _service_or_fallback(
        lambda client: client.multi_account_overview(
            accounts=accounts,
            price_timeout=price_timeout,
            include_details=include_details,
        ),
        lambda: multi_account_overview(
            accounts=accounts,
            price_timeout=price_timeout,
            include_details=include_details,
        ),
    )
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
def tool_get_holdings(
    include_cash: bool = True,
    group_by_market: bool = False,
    include_price: bool = False,
    account: str = None,
) -> str:
    """获取当前持仓列表。

    Args:
        include_cash: 是否包含现金资产，默认 True
        group_by_market: 是否按券商分组
        include_price: 是否包含实时价格
        account: 账户标识，默认使用配置中的默认账户
    """
    result = _service_or_fallback(
        lambda client: client.get_holdings(
            account=_account_or_default(account),
            include_cash=include_cash,
            group_by_market=group_by_market,
            include_price=include_price,
        ),
        lambda: get_holdings(
            account=account,
            include_cash=include_cash,
            group_by_market=group_by_market,
            include_price=include_price,
        ),
    )
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
def tool_get_position(account: str = None) -> str:
    """获取仓位分析（股票/现金/基金占比等）。

    Args:
        account: 账户标识，默认使用配置中的默认账户
    """
    result = get_position(account=account)
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
def tool_get_distribution(account: str = None) -> str:
    """获取资产分布（按地域/行业）。

    Args:
        account: 账户标识，默认使用配置中的默认账户
    """
    result = get_distribution(account=account)
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
def tool_get_price(code: str, account: str = None) -> str:
    """查询资产实时价格或汇率。

    Args:
        code: 资产代码（如 600519、AAPL、USDCNY）
        account: 账户标识，默认使用配置中的默认账户
    """
    result = get_price(code, account=account)
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
def tool_get_cash(account: str = None) -> str:
    """获取现金资产明细（各币种余额）。

    Args:
        account: 账户标识，默认使用配置中的默认账户
    """
    result = _service_or_fallback(
        lambda client: client.get_cash(account=_account_or_default(account)),
        lambda: get_cash(account=account),
    )
    return json.dumps(result, ensure_ascii=False, default=str)


# ========== 净值与收益 Tools ==========


@mcp.tool()
def tool_get_nav(days: int = 30, account: str = None) -> str:
    """获取账户净值及历史。

    Args:
        days: 获取最近 N 天历史，默认 30
        account: 账户标识，默认使用配置中的默认账户
    """
    result = _service_or_fallback(
        lambda client: client.get_nav(account=_account_or_default(account), days=days),
        lambda: get_nav(days=days, account=account),
    )
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
def tool_get_return(period_type: str, period: str = None, account: str = None) -> str:
    """查询收益率。

    Args:
        period_type: 周期类型 - "month"（月度）、"year"（年度）、"since_inception"（成立以来）
        period: 具体周期 - 月份如 "2025-03"，年份如 "2025"；since_inception 时不需要
        account: 账户标识，默认使用配置中的默认账户
    """
    result = get_return(period_type, period, account=account)
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
def tool_record_nav(
    price_timeout: int = 30,
    dry_run: bool = True,
    confirm: bool = False,
    overwrite_existing: bool = True,
    account: str = None,
) -> str:
    """记录今日净值。

    ⚠️ 默认 dry_run=True，不会实际写入。真正写入需设置 dry_run=False 且 confirm=True。

    Args:
        price_timeout: 价格获取超时（秒）
        dry_run: 预览模式（默认 True）
        confirm: 确认写入（与 dry_run=False 配合使用）
        overwrite_existing: 是否覆盖已有记录
        account: 账户标识，默认使用配置中的默认账户
    """
    result = record_nav(
        price_timeout=price_timeout,
        dry_run=dry_run,
        confirm=confirm,
        overwrite_existing=overwrite_existing,
        account=account,
    )
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
def tool_close_nav(
    date_str: str = None,
    total_value: float = None,
    cash_value: float = None,
    stock_value: float = 0.0,
    overwrite_existing: bool = True,
    dry_run: bool = True,
    confirm: bool = False,
    account: str = None,
) -> str:
    """记录清仓/关闭净值点（shares=0, nav=1.0）。

    ⚠️ 默认 dry_run=True。真正写入需 dry_run=False 且 confirm=True。

    Args:
        date_str: 日期 (YYYY-MM-DD)，默认今天
        total_value: 总市值（允许残余现金 > 0）
        cash_value: 现金价值
        stock_value: 股票价值，默认 0
        overwrite_existing: 是否覆盖已有记录
        dry_run: 预览模式（默认 True）
        confirm: 确认写入
        account: 账户标识，默认使用配置中的默认账户
    """
    result = close_nav(
        date_str=date_str,
        total_value=total_value,
        cash_value=cash_value,
        stock_value=stock_value,
        overwrite_existing=overwrite_existing,
        dry_run=dry_run,
        confirm=confirm,
        account=account,
    )
    return json.dumps(result, ensure_ascii=False, default=str)


# ========== 报告类 Tools ==========


@mcp.tool()
def tool_generate_report(
    report_type: str = "daily",
    record_nav: bool = False,
    price_timeout: int = 30,
    account: str = None,
) -> str:
    """生成投资报告（日报/月报/年报）。

    Args:
        report_type: 报告类型 - "daily"（日报）、"monthly"（月报）、"yearly"（年报）
        record_nav: 是否同时记录净值，默认 False
        price_timeout: 价格获取超时（秒）
        account: 账户标识，默认使用配置中的默认账户
    """
    if record_nav:
        result = generate_report(
            report_type=report_type,
            record_nav=record_nav,
            price_timeout=price_timeout,
            account=account,
        )
    else:
        result = _service_or_fallback(
            lambda client: client.generate_report(
                account=_account_or_default(account),
                report_type=report_type,
                price_timeout=price_timeout,
            ),
            lambda: generate_report(
                report_type=report_type,
                record_nav=False,
                price_timeout=price_timeout,
                account=account,
            ),
        )
    return json.dumps(result, ensure_ascii=False, default=str)


# ========== 同步类 Tools ==========


@mcp.tool()
def tool_sync_futu_cash_mmf(
    dry_run: bool = True,
    cash_balance: float = None,
    mmf_balance: float = None,
    account: str = None,
) -> str:
    """通过富途 OpenAPI 同步现金/货基余额到 holdings。

    可手动传入余额跳过 API 调用。

    Args:
        dry_run: 预览模式，默认 True；设为 False 才写入 holdings
        cash_balance: 手动指定现金余额（跳过 API）
        mmf_balance: 手动指定货基余额（跳过 API）
        account: 账户标识，默认使用配置中的默认账户
    """
    kwargs = {"dry_run": dry_run}
    if cash_balance is not None:
        kwargs["cash_balance"] = cash_balance
    if mmf_balance is not None:
        kwargs["mmf_balance"] = mmf_balance
    result = sync_futu_cash_mmf(account=account, **kwargs)
    return json.dumps(result, ensure_ascii=False, default=str)


# ========== 数据管理 Tools ==========


@mcp.tool()
def tool_clean_data(
    table: str = None,
    account: str = None,
    dry_run: bool = True,
    code: str = None,
    date_before: str = None,
    empty_only: bool = False,
    confirm: bool = False,
) -> str:
    """清理测试数据。

    ⚠️ 默认 dry_run=True。实际删除需 dry_run=False 且 confirm=True。

    Args:
        table: 目标表 - 'holdings', 'transactions', 'cash_flow', 'nav_history', 'all'
        account: 按账户过滤
        dry_run: 预览模式（默认 True）
        code: 按资产代码过滤（如 'TEST'）
        date_before: 删除指定日期之前的数据 (YYYY-MM-DD)
        empty_only: 只清理空记录
        confirm: 确认删除
    """
    result = clean_data(
        table=table, account=account, dry_run=dry_run,
        code=code, date_before=date_before,
        empty_only=empty_only, confirm=confirm,
    )
    return json.dumps(result, ensure_ascii=False, default=str)


if __name__ == "__main__":
    transport = "stdio"
    if "--sse" in sys.argv:
        transport = "sse"
    mcp.run(transport=transport)
