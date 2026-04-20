---
name: portfolio-management
description: |
  投资组合管理 Skill。支持飞书持仓/交易/现金流/NAV 管理、行情估值、快照审计、日报输出。
---

# Portfolio Management Skill

## 使用边界

- 主入口：`skill_api.py`
- 日报数据/HTML 唯一入口：`scripts/publish_daily_report.py`
- 核心 facade：`src/portfolio.py`、`src/price_fetcher.py`
- 详细架构：`docs/architecture.md`
- Schema：`docs/schema.md`、`docs/migrations.md`

所有投资数据、价格、净值、收益、现金流结论必须来自脚本执行结果或持久化数据。缺字段、缺基准、口径不一致时，直接说明“当前无法可靠计算”，不要估算。
`scripts/generate_daily_report_html.py` 仅作 renderer-only，不允许自行拉取 snapshot/report。

Linux 生产部署默认约定：代码从远端仓库同步，真实 `config.json`、`.data/`、`reports/` 放仓外，仓内用软链接引用；不要提交真实配置、缓存或报告产物。若部署流程会重建工作区，必须在部署后重建这些软链接。

## 常用 API

```python
from skill_api import (
    get_price,
    get_holdings,
    buy,
    sell,
    deposit,
    withdraw,
    sync_futu_cash_mmf,
    record_nav,
    full_report,
)
```

- `get_price(code, account=None)`：查询价格或汇率。
- `get_holdings(include_price=True, group_by_market=True, account=None)`：查询持仓。
- `buy(code, name, quantity, price, account=None, **kwargs)`：记录买入。
- `sell(code, quantity, price, account=None, **kwargs)`：记录卖出。
- `deposit(amount, account=None, **kwargs)` / `withdraw(amount, account=None, **kwargs)`：记录出入金。
- `sync_futu_cash_mmf(dry_run=True, account=None)`：通过富途 OpenAPI 同步现金/货基余额到 holdings。
- `record_nav(account=None)`：记录 NAV，并写入 holdings snapshot。
- `full_report(account=None)`：生成完整报告。

写入类操作必须明确账户、日期、券商/平台、币种、手续费等关键字段；不确定时先 dry-run 或询问。

## 架构边界

```text
src/app/
  asset_name_service.py          # 资产名称查询
  cash_service.py                # 现金持仓副作用
  futu_balance_sync_service.py   # 富途现金/货基余额同步
  cash_flow_summary_service.py   # 现金流聚合读取
  compensation_service.py        # 跨表写入补偿任务
  nav_baseline_service.py        # NAV 基准读取
  nav_record_service.py          # record_nav 编排
  nav_summary_printer.py         # NAV 摘要输出
  reporting_service.py           # 资产/行业分布
  share_service.py               # 份额读取与变动计算
  snapshot_service.py            # holdings_snapshot 写入
  trade_service.py               # 买卖/出入金编排
  valuation_service.py           # 估值编排

src/domain/
  nav_calculator.py              # NAV 公式、校验、记录构建
  nav_history_index.py           # NAV 历史内存索引
  payload_normalizer.py          # Decimal 与 payload 规范化

src/pricing/
  classifier.py                  # 行情路由/类型分类
  service.py                     # PriceService：缓存、TTL、fallback
  provider.py                    # PriceProvider 协议
  providers/                     # A/H/US/基金/ETF/legacy provider
```

`src/app/__init__.py` 和 `src/domain/__init__.py` 是公共导出边界。新增公共服务必须加入 `__all__`，并补 `tests/test_module_exports.py`。

## 开发规则

- 不要把新业务逻辑继续塞进 `PortfolioManager` 或 `PriceFetcher`；它们只做兼容 facade。
- 新编排逻辑放 `src/app/`，纯计算放 `src/domain/`，行情源放 `src/pricing/providers/`。
- 保留旧方法名和 patch 点，优先 wrapper 委托，避免破坏已有脚本和测试。
- 跨表写入部分成功时必须记录补偿任务。
- Schema 变更必须更新 `docs/schema.md`，并登记 `src/migrations/feishu/registry.py`。
- 涉及数据口径、历史回填、收益/NAV/现金流逻辑修改时，完成后必须给数据检查报告。

## 验证命令

```bash
# 编译检查
python3 -X pycache_prefix=/tmp/pm_pycache -m compileall src

# 核心回归
python3 -m pytest \
  tests/test_module_exports.py \
  tests/test_nav_record_service.py \
  tests/test_snapshot_service.py \
  tests/test_trade_service.py \
  tests/test_valuation_service.py \
  tests/test_pricing_service.py \
  tests/test_compensation_service.py \
  tests/test_migrations.py

# Schema 迁移计划
python3 scripts/migrate_schema.py

# Schema live 检查 / NAV 历史修复
python3 scripts/migrate_schema.py check-live
python3 scripts/nav_history_repair.py backfill --account lx --from 2025-01-01 --to 2025-01-31 --dry-run
```

部分旧集成测试会实例化真实 `PortfolioSkill()` 并依赖飞书配置；本地无真实配置时，优先跑 touched-area 单测并说明环境限制。
