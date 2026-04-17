# portfolio-management

基于飞书多维表的投资组合净值系统：用基金净值法记账，管理持仓、交易、现金流、估值、NAV 和日报发布。

## 入口

- 人类使用：`README.md`、`docs/INDEX.md`
- Agent 使用：`SKILL.md`
- Python API：`skill_api.py`
- 架构说明：`docs/architecture.md`
- Schema 与迁移：`docs/schema.md`、`docs/migrations.md`

## Quickstart

```bash
python3 -m venv .venv
./.venv/bin/pip install -U pip
./.venv/bin/pip install -r requirements.txt
cp config.example.json config.json
```

配置 `config.json` 或环境变量：

- `FEISHU_APP_ID`
- `FEISHU_APP_SECRET`
- `FEISHU_APP_TOKEN`
- `FEISHU_TABLE_HOLDINGS`
- `FEISHU_TABLE_TRANSACTIONS`
- `FEISHU_TABLE_NAV_HISTORY`
- `FEISHU_TABLE_CASH_FLOW`
- `FEISHU_TABLE_HOLDINGS_SNAPSHOT`
- `FEISHU_TABLE_COMPENSATION_TASKS`
- `FEISHU_TABLE_SCHEMA_VERSION`

## 常用调用

```python
from skill_api import buy, sell, deposit, withdraw, get_holdings, full_report, record_nav

get_holdings(include_price=True, group_by_market=True)
record_nav()
full_report()
buy("600519", "贵州茅台", 100, 1800, market="平安证券")
sell("600519", 50, 1900, market="平安证券")
deposit(50000, remark="入金")
withdraw(10000, remark="出金")
```

## 当前结构

```text
src/
├── app/                  # 应用服务：交易、现金、估值、NAV、快照、报表、补偿
├── domain/               # 纯计算：NAV 公式、历史索引、payload 规范化
├── pricing/              # 行情插件化：PriceService + Provider
├── migrations/           # Schema 版本化迁移登记
├── portfolio.py          # 兼容 facade，保留旧方法入口
├── price_fetcher.py      # 兼容 facade，委托 pricing service
├── feishu_storage.py     # 飞书表读写与本地缓存索引
└── feishu_client.py      # 飞书 API 客户端
```

`src/app/__init__.py` 和 `src/domain/__init__.py` 是公共导出边界。新增服务优先放到对应包，并补充 `__all__`。

## 关键约束

- 写入前默认先 dry-run；不要提交真实 `config.json` 或密钥。
- 业务日期统一使用北京时间语义。
- NAV 写入前先写 `holdings_snapshot`，保证可审计和可复算。
- 交易/现金/持仓跨表失败要记录补偿任务，不静默吞掉。
- Schema 变更必须登记到 `src/migrations/feishu/registry.py`，并更新 `docs/schema.md`。
- `PortfolioManager` 和 `PriceFetcher` 是兼容 facade，新逻辑不要继续塞回巨型文件。

## 常用命令

```bash
# touched-area 回归
python3 -m pytest tests/test_module_exports.py tests/test_nav_record_service.py tests/test_snapshot_service.py tests/test_trade_service.py tests/test_valuation_service.py tests/test_pricing_service.py

# 编译检查，避免写 __pycache__ 到源码树
python3 -X pycache_prefix=/tmp/pm_pycache -m compileall src

# Schema 迁移计划，只打印不写飞书
python3 scripts/migrate_schema.py

# 标记迁移已应用到本地状态
python3 scripts/migrate_schema.py --apply
```

## 文档索引

- `docs/INDEX.md`：项目地图和诊断命令
- `docs/architecture.md`：架构图与优化 TODO
- `docs/architecture.mmd`：Mermaid 架构图
- `docs/schema.md`：飞书表结构
- `docs/migrations.md`：迁移说明

## License

MIT
