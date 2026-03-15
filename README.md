# portfolio-management

投资组合管理 Skill —— 基于飞书多维表的基金净值法记账系统。

## 功能

- **交易记录**：买入、卖出、入金、出金
- **持仓查询**：实时价格、仓位分析、资产分布
- **净值系统**：基金净值法，份额随资金流动调整，净值反映真实收益
- **收益统计**：日/月/年收益率、成立以来收益、年化收益（CAGR）
- **完整报告**：日报/月报/年报，含风险指标（波动率、最大回撤）

## 支持的资产类型

| 类型 | 代码格式 | 示例 |
|------|----------|------|
| A股 | 6位数字 | 600519 |
| 港股 | 4-5位数字 | 00700 |
| 美股 | 英文字母 | AAPL |
| 场内ETF | 6位数字 | 510300 |
| 场外基金 | 6位数字 | 110011 |
| 现金 | XXX-CASH | CNY-CASH |

## 数据源

- **A股/港股/ETF/基金**：akshare
- **美股**：Finnhub + Yahoo Finance + yfinance
- **存储**：飞书多维表（Bitable API）

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置

复制模板并填入真实值：

```bash
cp config.example.json config.json
```

需要填写：
- `feishu.app_id` / `feishu.app_secret` — 飞书应用凭证
- `feishu.tables.*` — 飞书多维表 URL（持仓、交易、净值、现金流）
- `finnhub_api_key` — Finnhub API Key（美股价格，可选）

也支持环境变量配置（优先级：环境变量 > config.json > 默认值），详见 [SKILL.md](SKILL.md)。

### 3. 使用

```python
from skill_api import full_report, record_nav, buy, get_holdings

# 查看完整报告
report = full_report()

# 记录今日净值
record_nav()

# 买入
buy("600519", "贵州茅台", 100, 1800)

# 查看持仓（含实时价格）
get_holdings(include_price=True)
```

## 文件结构

```
portfolio-management/
├── SKILL.md              # Skill 定义（供 AI Agent 读取）
├── skill_api.py          # 统一 API 入口
├── config.example.json   # 配置模板
├── requirements.txt      # Python 依赖
├── src/
│   ├── models.py         # 数据模型
│   ├── config.py         # 配置管理
│   ├── feishu_client.py  # 飞书 API 客户端
│   ├── feishu_storage.py # 飞书存储层
│   ├── portfolio.py      # 核心业务逻辑（净值计算）
│   ├── price_fetcher.py  # 多源价格获取
│   ├── asset_utils.py    # 资产代码工具
│   ├── market_time.py    # 交易时间判断
│   └── local_cache.py    # 本地缓存
└── tests/                # 单元测试
```

## 许可证

MIT License
