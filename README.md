# portfolio-management

基于飞书多维表的投资组合净值系统：用基金净值法记账，管理持仓、交易、现金流、估值、NAV 和日报发布。

## 入口

- 人类使用：`README.md`、`docs/INDEX.md`
- Agent 使用：`SKILL.md`
- Python API：`skill_api.py`
- MCP Server：`mcp_server.py`（供 OpenClaw、Claude Desktop 等 MCP 客户端调用）
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

## Linux 部署约定

生产主机建议只从远端仓库同步代码，真实配置和运行数据放在仓外，仓内用软链接引用：

```bash
mkdir -p /opt/portfolio-management/config
mkdir -p /var/lib/portfolio-management/.data
mkdir -p /var/lib/portfolio-management/reports

ln -s /opt/portfolio-management/config/config.json ./config.json
ln -s /var/lib/portfolio-management/.data ./.data
ln -s /var/lib/portfolio-management/reports ./reports
```

`config.json`、`.data/`、`reports/` 已被 `.gitignore` 忽略。若部署工具会清理未跟踪文件（如 `rsync --delete`、重建工作区），部署后需要重新创建这些软链接。

## 常用调用

```python
from skill_api import buy, sell, deposit, withdraw, get_holdings, full_report, record_nav, sync_futu_cash_mmf

get_holdings(include_price=True, group_by_market=True)
get_holdings(include_price=True, account="alice")
sync_futu_cash_mmf(dry_run=True)
sync_futu_cash_mmf(dry_run=True, account="alice")
record_nav()
record_nav(account="alice")
full_report()
full_report(account="alice")
buy("600519", "贵州茅台", 100, 1800, broker="平安证券")
buy("600519", "贵州茅台", 100, 1800, broker="平安证券", account="alice")
sell("600519", 50, 1900, broker="平安证券")
deposit(50000, remark="入金")
withdraw(10000, remark="出金")
```

日报数据与 HTML 统一从 `scripts/publish_daily_report.py` 生成；`scripts/generate_daily_report_html.py` 仅负责渲染已准备好的 bundle。

常用 CLI 也支持显式指定账户：

```bash
python scripts/pm.py cash --account alice
python scripts/pm.py holdings --account alice --json
python scripts/publish_daily_report.py --account alice
```

## MCP Server

将全部 Skill API 暴露为 MCP tools，供 OpenClaw、Claude Desktop、Cursor 等 MCP 兼容客户端使用。

```bash
# stdio 模式（默认，适合本地 MCP 客户端）
python mcp_server.py

# SSE 模式（HTTP，适合远程调用）
python mcp_server.py --sse
```

MCP 客户端配置示例：

```json
{
  "mcpServers": {
    "portfolio-management": {
      "command": "python",
      "args": ["mcp_server.py"],
      "cwd": "/path/to/portfolio-management"
    }
  }
}
```

共注册 17 个 tools，覆盖交易、持仓、净值、现金、报告、同步等全部功能。写入类操作默认带 `dry_run=True` 安全保护。

## 当前结构

```text
src/
├── app/                  # 应用服务：交易、现金、富途余额同步、估值、NAV、快照、报表、补偿
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

# Schema live 检查 / code-side expectations
python3 scripts/migrate_schema.py check-live
python3 scripts/migrate_schema.py expectations

# 标记迁移已应用到本地状态
python3 scripts/migrate_schema.py --apply

# NAV 历史修复统一入口
python3 scripts/nav_history_repair.py backfill --account lx --from 2025-01-01 --to 2025-01-31 --dry-run
```

## 文档索引

- `docs/INDEX.md`：项目地图和诊断命令
- `docs/architecture.md`：架构图与优化 TODO
- `docs/architecture.mmd`：Mermaid 架构图
- `docs/schema.md`：飞书表结构
- `docs/migrations.md`：迁移说明

## License

MIT
