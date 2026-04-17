# Runbook

## 常用命令速查（pm CLI）

为了把常用查询从“记函数名”变成“敲命令”，提供一个薄 CLI：`scripts/pm.py`。

准备：
```bash
cd /home/node/.openclaw/workspace/portfolio-management
. .venv/bin/activate
```

常用查询（只读，默认不写入任何表）：
```bash
# 查持仓（默认不拉实时价格；主要用于结构/数量核对）
python scripts/pm.py holdings

# 查现金（按 cash 类持仓汇总）
python scripts/pm.py cash
python scripts/pm.py cash --json

# 查净值（nav_history 最新一条 + 最近若干条 history）
python scripts/pm.py nav
python scripts/pm.py nav --json

# 预览日报数据（只读，不是正式日报入口；如需更快/更稳可调 timeout）
python scripts/pm.py report daily --preview
python scripts/pm.py report daily --preview --timeout 25 --json
```

说明：
- `--json` 适合做自动化/二次处理；默认输出对人更友好。
- CLI 目前仅暴露只读命令；涉及写入的动作（如 `record_nav/close_nav`）仍需走显式 confirm 语义。
- 正式日报数据/HTML/发布入口只有 `scripts/publish_daily_report.py`；`pm report` 仅作 preview。

## 清仓 / 关闭账户：写入 shares=0 的净值点（close_nav）

背景：
- `shares=0` 是合法业务语义（清仓/关闭），但必须**显式触发**，不能靠“缺失字段默认 0”混入。
- 为了让下游收益率/回撤等逻辑稳定，我们约定清仓点：`nav=1.0`，并在 `details` 写入 `{"status":"CLOSED"}`。

使用方式：
- 先演练（不会写入）：
  - `close_nav(date_str="YYYY-MM-DD", total_value=..., dry_run=True)`
- 真写入（必须显式确认）：
  - `close_nav(date_str="YYYY-MM-DD", total_value=..., dry_run=False, confirm=True)`

口径说明：
- 允许 `total_value > 0`（例如残余现金/零碎资产）。
- 建议同时提供 `cash_value` / `stock_value`，让拆分字段自洽（否则会默认把 `total_value` 全部计入 `cash_value`，`stock_value=0`）。

安全约束：
- 默认 `dry_run=True`；任何真正写入都必须 `confirm=True`。
- 清仓点不会触发价格拉取/估值计算；它是一个“人工定义的状态点”。

## 1) Daily report link returns 502

- Confirm an HTTP server is listening on `0.0.0.0:3000` inside the container.
- Confirm publish root points to the directory containing `investment-daily-YYYY-MM-DD/index.html`.

## 2) "Report didn't refresh prices"

- Run `python scripts/diagnose_pricing.py --account lx`.
- Check:
  - `summary.realtime/cache/stale_fallback/missing`
  - `tencent_batch` meta (requests/elapsed/coverage)
  - per-asset `state` and `source`

## 3) Missing price for US tickers

- Ensure `finnhub_api_key` is set.
- If Finnhub fails, Yahoo chart API may be rate-limited.

## 4) Date off by one day

- Business dates are Beijing time.
- Check code uses `src/time_utils.py` helpers.

## 5) Feishu field not found

- Compare actual Bitable fields with `docs/schema.md`.
- Use `python scripts/migrate_schema.py check-live`.
