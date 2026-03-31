# portfolio-management 1 页 Runbook（prod 运行/排障）

> 目标：让“今天日报/净值为什么不对/链接为什么 502”能在 5 分钟内定位。

## 0) Repo / 入口
- repo：`/home/node/.openclaw/workspace/portfolio-management`
- 定时任务入口：`scripts/publish_daily_report.py`
- CLI（只读查询为主）：`scripts/pm.py`

---

## 1) 业务主流程（按 IO 顺序）
1) `skill.build_snapshot()`：生成统一快照（持仓 + 价格/汇率 + 估值）
2) `record_nav()`：写入 `nav_history`（cron 模式会写；默认安全约束是 dry_run）
3) `generate_report(daily)`：生成日报结构化数据
4) `render_html()`：渲染 GitHub 风格 HTML
5) publish：把 HTML 写入对外静态目录，并返回链接

---

## 2) 真相来源（数据/中间表）
### Feishu Bitable（事实表/中间表）
字段名以 `docs/schema.md` 为准：
- `holdings`
- `transactions`
- `cash_flow`
- `nav_history`
- `price_cache`
- `holdings_snapshot`（审计/回放）

### 本地缓存/回放（中间文件）
- `.data/holdings_snapshot/<account>/<YYYY-MM-DD>.json`
- `.data/nav_index_cache.json`
- `.data/cash_flow_agg_cache.json`
- `.data/holdings_index.json`
- `audit/*`

---

## 3) 报告/发布 IO（最常出问题的部分）
### 生成/归档
- `reports/investment-daily-YYYY-MM-DD.html`

### 对外发布目录（静态站根）
- 当前 publish 根：`/home/node/.openclaw/workspace/prototypes`
- 日报路径：`prototypes/investment-daily-YYYY-MM-DD/index.html`

### 对外域名
- `https://openclaw-pub-<instance>.imlgz.com/<path>/` → 反代到容器内 `0.0.0.0:3000`

---

## 4) 最小验收（每次改动都跑）
### 4.1 编译检查
```bash
cd /home/node/.openclaw/workspace/portfolio-management
./.venv/bin/python -m py_compile \
  scripts/publish_daily_report.py \
  scripts/generate_daily_report_html.py \
  src/feishu_storage.py
```

### 4.2 生成日报（不写 nav_history）
```bash
./.venv/bin/python scripts/publish_daily_report.py \
  --dry-run --price-timeout 10 \
  --publish-root /home/node/.openclaw/workspace/prototypes
```

### 4.3 本地验证（不走外网）
```bash
curl -I http://127.0.0.1:3000/investment-daily-YYYY-MM-DD/ | head
```

---

## 5) 常见故障（直接定位）
### 5.1 日报链接 502
原因：容器内没有服务监听 3000。
检查：
```bash
lsof -iTCP:3000 -sTCP:LISTEN -nP | head
```
启动静态服务（最简）：
```bash
cd /home/node/.openclaw/workspace/portfolio-management
nohup python3 -m http.server 3000 --bind 0.0.0.0 --directory /home/node/.openclaw/workspace/prototypes \
  > /home/node/.openclaw/workspace/prototypes/.httpserver3000.log 2>&1 &
```

### 5.2 价格汇总 realtime/cache/stale/missing 怎么看
- realtime：本次从实时源拉到的数量
- cache：本次直接命中缓存数量
- stale_fallback：实时失败后用过期缓存兜底数量
- missing：最终缺价格数量（应为 0；否则拒绝写 nav_history 或在 warnings 强提示）

### 5.3 现金“数量 vs 金额”不一致
通常是把不同币种的现金数量相加导致；金额是折算到 CNY 后的市值。
建议：合并行不要展示 quantity（或拆币种分行）。

---

## 6) 写入保护（避免污染 nav_history）
- `record_nav()` 默认 `dry_run=True`，只有 `dry_run=False && confirm=True` 才真正写。
- `FeishuStorage.save_nav()` 会对 `nav_history` 做写入前校验（missing ≠ 0，关键字段缺失直接 fail-fast）。
