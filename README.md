# portfolio-management

基于飞书多维表（Bitable）的投资组合净值系统：**用基金净值法记账**，输出持仓/现金/收益统计，并生成日报。

- For Agents：看 `SKILL.md`（能力边界、API 入口、约束）
- For Humans：看本文（Quickstart / Cheatsheet / 发布与排障）

- 适用：长期投资组合的“账 + 报告”一体化
- 不适用：高频交易撮合；把不可靠行情当最终真相

> Python 统一入口：`skill_api.py`

---

## Hard Rules（别踩）

1. **配置里有密钥**：真实 `config.json` / `config.local.*` 不要提交。
2. **写入动作先 dry-run**（买卖/现金流/仓位变更）。
3. **外部发布依赖 3000 端口**：publish domain 转发到本容器 `:3000`，未监听会出现 502。

---

## 能做什么

- 交易记录：买入、卖出、入金、出金
- 持仓查询：仓位、资产分布、实时价格（多源）
- 净值系统：份额随资金流动调整，净值反映真实收益
- 收益统计：日/月/年、成立以来、年化（CAGR）
- 报告：日报/月报/年报（含波动率/最大回撤等）

---

## Quickstart

### 1) 安装依赖

```bash
cd /home/node/.openclaw/workspace/portfolio-management
python3 -m venv .venv
./.venv/bin/pip install -U pip
./.venv/bin/pip install -r requirements.txt
```

### 2) 配置

```bash
cp config.example.json config.json
```

需要填写：
- `feishu.app_id` / `feishu.app_secret`
- `feishu.tables.*`：多维表 URL（持仓、交易、净值、现金流）
- `finnhub_api_key`（可选）

---

## Cheatsheet（常用）

### A. Python API（示例）

```python
from skill_api import full_report, record_nav, buy, get_holdings

report = full_report()
record_nav()
buy("600519", "贵州茅台", 100, 1800)
get_holdings(include_price=True)
```

### B. SSH / Deploy Key 自检（重启/换机后第一件事）

```bash
cd /home/node/.openclaw/workspace/portfolio-management
scripts/ssh_selfcheck.sh
```

---

## HTML 日报发布（OpenClaw Publish Domain）

本项目支持生成一份 **GitHub 风格**的 HTML 日报，并发布到对外域名（形如 `https://openclaw-pub-<instance>.imlgz.com/`）。

### 生成并发布

```bash
cd /home/node/.openclaw/workspace/portfolio-management

# 生成 HTML（写到 ./public/index.html）
./.venv/bin/python scripts/generate_daily_report_html.py

# 发布到 OpenClaw 静态根目录（/home/node/.openclaw/workspace/published/...）
./.venv/bin/python scripts/publish_daily_report_html_to_openclaw_pub.py
```

### 访问

- `https://openclaw-pub-<instance>.imlgz.com/investment-daily-YYYY-MM-DD/`

> 说明：发布服务是容器内 `publish-server.js`（端口 3000）。
> 若出现 502：优先确认 3000 是否在监听；也可以手动跑一次：
>
> ```bash
> python3 /home/node/.openclaw/workspace/tools/ensure_publish_server.py
> ```

---

## 文件结构

```
portfolio-management/
├── SKILL.md
├── skill_api.py
├── config.example.json
├── requirements.txt
├── scripts/
├── src/
└── tests/
```

---

## License

MIT
