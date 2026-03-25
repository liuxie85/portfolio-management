"""时间工具（统一使用北京时间语义）

本项目的业务日期/时间按北京时间（Asia/Shanghai, UTC+8）理解。
注意：为了避免在项目里引入“天真时间(naive) + 本机时区(UTC)”造成跨日漂移，
这里统一提供北京时间的 now/today。

实现策略：
- 对于业务逻辑（today、默认日期、缓存过期比较），使用 *naive 的北京时间*（去掉 tzinfo），
  保持与当前代码里大量 datetime.now()/date.today() 的比较/序列化方式兼容。
- 对于需要与 Feishu 时间戳交互的地方，仍应使用显式 tz（见 FeishuStorage.FEISHU_DATE_TZ）。
"""

from __future__ import annotations

from datetime import datetime, date
from zoneinfo import ZoneInfo


TZ_SHANGHAI = ZoneInfo("Asia/Shanghai")


def bj_now() -> datetime:
    """北京时间 now（tz-aware）。"""
    return datetime.now(TZ_SHANGHAI)


def bj_now_naive() -> datetime:
    """北京时间 now（naive）。

    用于：字符串时间戳、缓存过期比较等（与现有逻辑保持兼容）。
    """
    return bj_now().replace(tzinfo=None)


def bj_today() -> date:
    """北京时间 today（业务日期）。"""
    return bj_now_naive().date()
