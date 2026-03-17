---
name: portfolio-management
description: |
  投资组合管理 Skill，支持持仓记录、交易操作、净值计算和报告输出。支持美股、港股、A股、基金。

  支持资产类型：A股(6位数字)、港股(4-5位数字)、美股(字母代码)、
  场内ETF(6位数字)、场外基金(6位数字)、现金(CNY-CASH等)。

  数据存储在飞书多维表，需先配置环境变量。
---

# 投资组合管理 Skill

## 设计原则

本 Skill 采用 **Agent 框架 + 结构化执行** 模式：

- **LLM 职责**：意图识别、参数映射、结果摘要
- **Agent 职责**：本地代码执行、数据查询、API 调用

请求不会直接交给 LLM 自由发挥，而是通过固定阶段完成：
1. **意图解析** → 识别用户要执行的操作类型
2. **参数映射** → 提取结构化参数
3. **本地执行** → 调用对应 API 函数
4. **结果格式化** → 生成可读输出

---

## 环境变量

```bash
FEISHU_APP_ID=cli_xxxxxxxxxx
FEISHU_APP_SECRET=xxxxxxxxxx

# 多维表配置
FEISHU_APP_TOKEN=bascnxxxxxxxxxxxxx
FEISHU_TABLE_HOLDINGS=tblxxxxxxxx
FEISHU_TABLE_TRANSACTIONS=tblxxxxxxxx
FEISHU_TABLE_NAV_HISTORY=tblxxxxxxxx
FEISHU_TABLE_CASH_FLOW=tblxxxxxxxx

# 可选
PORTFOLIO_ACCOUNT=lx
FINNHUB_API_KEY=xxxxxxxxxx  # 美股价格（可选）
```

---

## 核心操作

### 1. 查询价格

**意图**: 查询单个资产的实时/缓存价格

**API**: `get_price(code: str) -> Dict`

**参数**:
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| code | str | 是 | 资产代码（如 600519、AAPL、00700） |

**返回**:
```json
{
  "success": true,
  "code": "600519",
  "name": "贵州茅台",
  "price": 1413.64,
  "currency": "CNY",
  "cny_price": 1413.64,
  "change_pct": 1.25,
  "source": "腾讯财经"
}
```

**示例调用**:
```python
from skill_api import get_price
get_price("600519")   # A股
get_price("AAPL")     # 美股
get_price("00700")    # 港股
```

---

### 2. 查询汇率

**意图**: 查询外币兑人民币汇率

**API**: `get_price(code: str)` （汇率作为特殊资产处理）

**参数**:
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| code | str | 是 | 货币代码（如 USD-CASH、HKD-CASH） |

**返回**:
```json
{
  "success": true,
  "code": "USD-CASH",
  "name": "美元现金",
  "price": 1.0,
  "cny_price": 7.25,
  "source": "exchangerate-api"
}
```

**示例调用**:
```python
get_price("USD-CASH")  # 美元汇率
get_price("HKD-CASH")  # 港币汇率
```

---

### 3. 查询持仓明细

**意图**: 查看当前持仓列表，可选包含实时价格

**API**: `get_holdings(include_price=True, group_by_market=False) -> Dict`

**参数**:
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| include_price | bool | 否 | 是否获取实时价格，默认 False |
| group_by_market | bool | 否 | 是否按券商分组，默认 False |

**返回**:
```json
{
  "success": true,
  "count": 5,
  "total_value": 1500000.00,
  "cash_value": 225000.00,
  "cash_ratio": 0.15,
  "holdings": [
    {"code": "600519", "name": "贵州茅台", "quantity": 100, "price": 1413.64, "market_value": 141364, "weight": 0.094},
    {"code": "00700", "name": "腾讯控股", "quantity": 200, "price": 547.50, "market_value": 109500, "weight": 0.073}
  ]
}
```

**示例调用**:
```python
from skill_api import get_holdings

# 基础查询（无价格）
get_holdings()

# 包含实时价格
get_holdings(include_price=True)

# 按券商分组
get_holdings(include_price=True, group_by_market=True)
```

---

### 4. 记录买入操作

**意图**: 记录一笔买入交易

**API**: `buy(code, name, quantity, price, **kwargs) -> Dict`

**参数**:
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| code | str | 是 | 资产代码 |
| name | str | 是 | 资产名称 |
| quantity | float | 是 | 买入数量 |
| price | float | 是 | 买入价格 |
| date_str | str | 否 | 交易日期，默认今天 |
| market | str | 否 | 券商/平台，默认"平安证券" |
| fee | float | 否 | 手续费，默认 0 |
| request_id | str | 否 | 幂等键，防重复提交 |

**返回**:
```json
{
  "success": true,
  "transaction": {
    "date": "2025-03-17",
    "code": "600519",
    "name": "贵州茅台",
    "quantity": 100,
    "price": 1500.00,
    "amount": 150000.00,
    "fee": 5.00,
    "total_cost": 150005.00
  },
  "message": "买入记录已保存: 贵州茅台 100股 @ ¥1500"
}
```

**示例调用**:
```python
from skill_api import buy

buy(code="600519", name="贵州茅台", quantity=100, price=1500)

# 带日期和券商
buy(code="600519", name="贵州茅台", quantity=100, price=1500,
    date_str="2025-03-01", market="平安证券", fee=5)

# 带幂等键（防止重复提交）
buy(code="600519", name="贵州茅台", quantity=100, price=1500,
    request_id="order_20250317_001")
```

---

### 5. 记录卖出操作

**意图**: 记录一笔卖出交易

**API**: `sell(code, quantity, price, **kwargs) -> Dict`

**参数**:
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| code | str | 是 | 资产代码 |
| quantity | float | 是 | 卖出数量 |
| price | float | 是 | 卖出价格 |
| date_str | str | 否 | 交易日期，默认今天 |
| market | str | 否 | 券商/平台 |
| fee | float | 否 | 手续费，默认 0 |
| request_id | str | 否 | 幂等键 |

**返回**:
```json
{
  "success": true,
  "transaction": {
    "date": "2025-03-17",
    "code": "600519",
    "name": "贵州茅台",
    "quantity": 50,
    "price": 1600.00,
    "proceeds": 79995.00,
    "fee": 5.00
  },
  "message": "卖出记录已保存: 贵州茅台 50股 @ ¥1600"
}
```

**示例调用**:
```python
from skill_api import sell

sell(code="600519", quantity=50, price=1600)

# 带手续费
sell(code="600519", quantity=50, price=1600, fee=5)
```

---

### 6. 记录入金/出金操作

**意图**: 记录资金转入/转出

**API**:
- `deposit(amount, **kwargs)` - 入金
- `withdraw(amount, **kwargs)` - 出金

**参数**:
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| amount | float | 是 | 金额 |
| date_str | str | 否 | 日期，默认今天 |
| remark | str | 否 | 备注 |
| currency | str | 否 | 币种，默认 CNY |

**返回**:
```json
{
  "success": true,
  "cashflow": {
    "date": "2025-03-17",
    "amount": 50000.00,
    "currency": "CNY",
    "remark": "工资入金"
  },
  "message": "入金记录已保存: ¥50,000.00"
}
```

**示例调用**:
```python
from skill_api import deposit, withdraw

# 入金
deposit(amount=50000, remark="工资入金")

# 出金
withdraw(amount=30000, remark="消费")
```

---

### 7. 生成日报/月报/年报

**意图**: 生成投资组合报告

**API**: `generate_report(report_type, record_nav=False) -> Dict`

**参数**:
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| report_type | str | 是 | 报告类型: "daily"/"monthly"/"yearly" |
| record_nav | bool | 否 | 是否同时记录今日净值，默认 False |

**返回**: 结构化报告数据

**示例调用**:
```python
from skill_api import generate_report

# 日报
generate_report("daily")

# 月报
generate_report("monthly")

# 年报
generate_report("yearly")

# 日报并记录净值
generate_report("daily", record_nav=True)
```

---

### 8. 记录/更新净值

**意图**: 记录当日净值快照

**API**: `record_nav() -> Dict`

**返回**:
```json
{
  "success": true,
  "date": "2025-03-17",
  "nav": 1.2345,
  "total_value": 1500000.00,
  "shares": 1215000.00,
  "message": "已记录 2025-03-17 净值: 1.2345"
}
```

**示例调用**:
```python
from skill_api import record_nav

record_nav()
```

---

## 辅助操作

### 查询净值历史

```python
from skill_api import get_nav

get_nav()  # 返回最新净值 + 30天历史
```

### 查询收益率

```python
from skill_api import get_return

get_return("daily")              # 当日收益
get_return("month", "2025-03")   # 指定月份
get_return("year", "2025")       # 指定年度
get_return("since_inception")    # 自成立以来
```

### 查询现金

```python
from skill_api import get_cash, add_cash, sub_cash

get_cash()           # 查看现金明细
add_cash(10000)      # 增加现金
sub_cash(5000)       # 减少现金
```

### 完整报告（只读）

```python
from skill_api import full_report

full_report()        # 完整报告，不记录净值
```

### 数据清理

```python
from skill_api import clean_data

# 预览要删除的数据
clean_data(table='transactions', code='TEST')

# 实际删除
clean_data(table='transactions', code='TEST', dry_run=False)
```

---

## 数据源

| 资产类型 | 主数据源 | 备用源 |
|---------|---------|--------|
| A股/ETF | 腾讯财经 | AKShare |
| 港股 | 腾讯财经 | AKShare |
| 美股 | Finnhub API | Yahoo Finance |
| 场外基金 | AKShare | 东方财富 |
| 汇率 | exchangerate-api.com | 新浪财经 |

---

## 缓存策略

| 数据类型 | 缓存时长 |
|---------|---------|
| 交易时间价格 | 30 分钟 |
| 非交易时间价格 | 到下次开盘 |
| 基金净值 | 到 19:00 更新 |
| 汇率 | 24 小时 |

---

## 数据表结构

| 表名 | 说明 | 业务主键 |
|------|------|----------|
| holdings | 持仓表 | (asset_id, account, market) |
| transactions | 交易记录表 | dedup_key + request_id |
| cash_flow | 出入金记录表 | dedup_key |
| nav_history | 净值历史表 | (account, date) |

---

## 飞书 API 限制

- **日期字段**: 不支持比较操作符（>=, <=），客户端过滤
- **QPS 限制**: 20 QPS，内置限流 + 重试
- **批量操作**: 单次最多 500 条
