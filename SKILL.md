---
name: portfolio-management
description: |
  投资组合管理 Skill，支持持仓记录、交易操作、净值计算和报告输出。

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

### 数据口径与安全约束（强制）

- 所有数据查询、报表结果、收益计算、汇总结果，**必须依赖脚本实际执行结果**。
- 所有价格、汇率、持仓、市值、净值、收益、现金流等字段，**必须来自代码返回值或持久化数据源**，不得由 LLM 自行脑补。
- 如果脚本返回空值、缺字段、无历史基准、无计算公式、字段口径不一致，**必须直接明确告知用户该数据当前不可得/不可可靠计算**。
- **禁止任何形式的推测数据**，包括但不限于：
  - 用缺失字段反推昨收/昨值
  - 用涨跌幅倒推价格
  - 用部分资产数据外推组合收益
  - 用不同时点/不同市场口径混合计算单日盈亏
- 如果存在多种口径（例如净值口径、逐仓口径、盘中口径、收盘口径），必须明确说明差异；在口径未统一前，不输出确定性数值结论。
- 宁可返回“当前无法可靠计算”，也不要输出未经脚本严格支持的估算值。
- **凡是涉及数据变更、口径修正、历史回填、字段映射调整、收益/净值/现金流/估值逻辑修改的开发工作，完成后必须生成“数据检查报告”**，至少包含：
  - 修改范围（改了哪些文件/字段/公式）
  - 检查对象（检查了哪些日期/账户/记录）
  - 修改前 vs 修改后（关键字段对比）
  - 发现的异常/限制/仍不可靠之处
  - 本次结论（哪些结果现在可用，哪些仍不可可靠使用）
- 如果本次开发涉及历史数据修复，必须先保留修复前快照或备份文件，再执行回填；报告中要写明备份位置。
- 如果本次开发没有生成数据检查报告，则视为该数据开发工作未完成，不应直接给出“已修好”的结论。

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

**意图**: 查看当前持仓列表，按券商/平台分组展示

**API**: `get_holdings(include_price=True, group_by_market=True) -> Dict`

**参数**:
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| include_price | bool | 否 | 是否获取实时价格，默认 False |
| group_by_market | bool | 否 | 是否按券商分组，默认 False |

**输出格式约束**:

持仓明细必须按券商/平台分组展示，格式如下：

```
## 持仓明细 (总市值: ¥1,500,000.00, 现金比例: 15.0%)

### 平安证券 (¥900,000.00, 60.0%)
| 代码 | 名称 | 数量 | 现价 | 市值 | 权重 |
|------|------|------|------|------|------|
| 600519 | 贵州茅台 | 100 | ¥1,413.64 | ¥141,364 | 9.4% |
| 00700 | 腾讯控股 | 200 | ¥547.50 | ¥109,500 | 7.3% |

### 华泰证券 (¥375,000.00, 25.0%)
| 代码 | 名称 | 数量 | 现价 | 市值 | 权重 |
|------|------|------|------|------|------|
| 000858 | 五粮液 | 200 | ¥1,125.00 | ¥225,000 | 15.0% |

### 现金 (¥225,000.00, 15.0%)
| 代码 | 名称 | 金额 | 币种 |
|------|------|------|------|
| CNY-CASH | 人民币现金 | ¥200,000 | CNY |
| USD-CASH | 美元现金 | $3,500 | USD |
```

**返回数据结构**:
```json
{
  "success": true,
  "count": 5,
  "total_value": 1500000.00,
  "cash_value": 225000.00,
  "cash_ratio": 0.15,
  "by_market": {
    "平安证券": [
      {"code": "600519", "name": "贵州茅台", "quantity": 100, "price": 1413.64, "market_value": 141364, "weight": 0.094}
    ]
  },
  "market_values": {
    "平安证券": 900000.00,
    "华泰证券": 375000.00
  }
}
```

**示例调用**:
```python
from skill_api import get_holdings

# 按券商分组展示（推荐）
get_holdings(include_price=True, group_by_market=True)

# 基础查询（无价格）
get_holdings()

# 包含实时价格（不分组）
get_holdings(include_price=True)
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
| request_id | str | 否 | 幂等键：同一个 request_id 重复提交会被视为同一笔；不传时系统会自动生成，以允许同日同资产多笔交易 |

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
buy(code="600519", name="600519", quantity=100, price=1500,
    date_str="2025-03-01", market="平安证券", fee=5)

# 带幂等键（防止重复提交；同一个 request_id 只会记一次）
buy(code="600519", name="600519", quantity=100, price=1500,
    request_id="order_20250317_001")

# 不传 request_id：系统会自动生成一个（允许同日同资产多笔交易写入）
buy(code="600519", name="600519", quantity=100, price=1500)
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
| request_id | str | 否 | 幂等键：同一个 request_id 重复提交会被视为同一笔；不传时系统会自动生成，以允许同日同资产多笔交易 |

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

---

#### 日报格式

日报必须包含以下内容：

```
## 投资组合日报 (2025-03-17)

### 净值概览
| 指标 | 数值 |
|------|------|
| 单位净值 | 1.2345 |
| 总市值 | ¥1,500,000.00 |
| 股票市值 | ¥1,275,000.00 (85.0%) |
| 现金市值 | ¥225,000.00 (15.0%) |

### 收益统计
| 周期 | 涨幅 | 盈亏 |
|------|------|------|
| 当月 | +3.25% | +¥48,750 |
| 当年 | +8.50% | +¥127,500 |
| 自成立以来(CAGR) | +11.8% | - |

### Top10 持仓
| 代码 | 名称 | 市值 | 权重 |
|------|------|------|------|
| 600519 | 贵州茅台 | ¥141,364 | 9.4% |
| 00700 | 腾讯控股 | ¥109,500 | 7.3% |
| 000858 | 五粮液 | ¥225,000 | 15.0% |
| ... | ... | ... | ... |

### 资产类型分布
| 类型 | 市值 | 占比 |
|------|------|------|
| A股 | ¥900,000 | 60.0% |
| 港股 | ¥375,000 | 25.0% |
| 现金 | ¥225,000 | 15.0% |
```

**日报返回数据结构**:
```json
{
  "success": true,
  "report_type": "日报",
  "date": "2025-03-17",
  "overview": {
    "total_value": 1500000.00,
    "cash_ratio": 0.15,
    "stock_ratio": 0.85
  },
  "nav": 1.2345,
  "total_value": 1500000.00,
  "pnl": 12750.00,
  "cash_flow": 0,
  "top_holdings": [...],
  "distribution": [
    {"type": "a_stock", "value": 900000, "ratio": 0.60},
    {"type": "hk_stock", "value": 375000, "ratio": 0.25},
    {"type": "cash", "value": 225000, "ratio": 0.15}
  ],
  "cagr": 11.8
}
```

---

#### 月报格式

月报在日报基础上增加：

```
## 投资组合月报 (2025-03)

### 本月收益
| 指标 | 数值 |
|------|------|
| 月度净值涨幅 | +3.25% |
| 月度资产升值 | +¥48,750 |
| 本月现金流 | +¥50,000 |

### 持仓明细（按券商分组）
...（同持仓明细格式）
```

---

#### 年报格式

年报在月报基础上增加：

```
## 投资组合年报 (2025)

### 年度收益
| 指标 | 数值 |
|------|------|
| 年度净值涨幅 | +8.50% |
| 年度资产升值 | +¥127,500 |
| 年化收益率(CAGR) | +12.5% |
| 最大回撤 | -8.3% |

### 各年份收益
| 年份 | 净值涨幅 | 资产升值 | 现金流 |
|------|----------|----------|--------|
| 2025 | +8.50% | +¥127,500 | +¥100,000 |
| 2024 | +15.20% | +¥180,000 | +¥200,000 |

### 累计收益
| 指标 | 数值 |
|------|------|
| 累计净值涨幅 | +23.45% |
| 累计资产升值 | +¥307,500 |
| 自成立以来年化 | +11.8% |
```

---

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
| transactions | 交易记录表 | request_id（幂等键） + dedup_key（内容指纹） |
| cash_flow | 出入金记录表 | dedup_key |
| nav_history | 净值历史表 | (account, date) |
| holdings_snapshot | 持仓快照表（用于审计/可复算） | dedup_key（建议形如 account:YYYY-MM-DD:market:asset_id） |

---

## 飞书 API 限制

- **日期字段**: 不支持比较操作符（>=, <=），客户端过滤
- **QPS 限制**: 20 QPS，内置限流 + 重试
- **批量操作**: 单次最多 500 条

---

## 时区约定（重要）

本项目所有“业务日期”（交易日期 tx_date、出入金日期 flow_date、净值日期 date、快照 as_of）都按 **北京时间（Asia/Shanghai, UTC+8）** 理解。

- 外部输入：`date_str` 一律按 `YYYY-MM-DD` 解析为北京时间的业务日期。
- 生成默认日期：未传 `date_str` 时，以“北京时间 today”为准（避免服务器在 UTC 时区导致跨日）。
- 飞书日期字段：与飞书交互的 Unix 时间戳（毫秒）按 UTC+8 解析/生成，避免 00:00 边界被截成前一天。

