"""
SQLite 存储层

目标：保留现有业务/Skill API，不改上层调用方式；当飞书不可用时可作为主存储或优雅回退。
"""
import json
import sqlite3
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Optional, Dict, Any, List

from . import config
from .models import (
    Holding, Transaction, CashFlow, NAVHistory, PriceCache,
    AssetType, TransactionType, AssetClass, Industry,
    DATETIME_FORMAT,
)
from .local_cache import LocalPriceCache


class SQLiteStorage:
    """本地 SQLite 存储层"""

    MONEY_QUANT = Decimal('0.01')
    NAV_QUANT = Decimal('0.000001')
    WEIGHT_QUANT = Decimal('0.000001')

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = Path(db_path or config.get_sqlite_path())
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local_price_cache = LocalPriceCache()
        self._init_db()

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS holdings (
                    record_id TEXT PRIMARY KEY,
                    asset_id TEXT NOT NULL,
                    asset_name TEXT NOT NULL,
                    asset_type TEXT NOT NULL,
                    market TEXT DEFAULT '',
                    account TEXT NOT NULL,
                    quantity REAL NOT NULL DEFAULT 0,
                    avg_cost REAL,
                    currency TEXT NOT NULL,
                    asset_class TEXT,
                    industry TEXT,
                    tag TEXT,
                    created_at TEXT,
                    updated_at TEXT
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_holdings_biz
                ON holdings(asset_id, account, market);

                CREATE TABLE IF NOT EXISTS transactions (
                    record_id TEXT PRIMARY KEY,
                    request_id TEXT,
                    dedup_key TEXT,
                    tx_date TEXT NOT NULL,
                    tx_type TEXT NOT NULL,
                    asset_id TEXT NOT NULL,
                    asset_name TEXT,
                    asset_type TEXT,
                    market TEXT DEFAULT '',
                    account TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    price REAL NOT NULL,
                    amount REAL,
                    currency TEXT NOT NULL,
                    fee REAL DEFAULT 0,
                    tax REAL DEFAULT 0,
                    related_account TEXT,
                    remark TEXT,
                    source TEXT DEFAULT 'manual'
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_request_id ON transactions(request_id);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_dedup_key ON transactions(dedup_key);

                CREATE TABLE IF NOT EXISTS cash_flow (
                    record_id TEXT PRIMARY KEY,
                    dedup_key TEXT,
                    flow_date TEXT NOT NULL,
                    account TEXT NOT NULL,
                    amount REAL NOT NULL,
                    currency TEXT NOT NULL,
                    cny_amount REAL,
                    exchange_rate REAL,
                    flow_type TEXT NOT NULL,
                    source TEXT,
                    remark TEXT
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_cash_flow_dedup_key ON cash_flow(dedup_key);

                CREATE TABLE IF NOT EXISTS nav_history (
                    record_id TEXT PRIMARY KEY,
                    date TEXT NOT NULL,
                    account TEXT NOT NULL,
                    total_value REAL NOT NULL,
                    cash_value REAL DEFAULT 0,
                    stock_value REAL DEFAULT 0,
                    fund_value REAL DEFAULT 0,
                    cn_stock_value REAL DEFAULT 0,
                    us_stock_value REAL DEFAULT 0,
                    hk_stock_value REAL DEFAULT 0,
                    stock_weight REAL,
                    cash_weight REAL,
                    shares REAL,
                    nav REAL,
                    cash_flow REAL,
                    share_change REAL,
                    mtd_nav_change REAL,
                    ytd_nav_change REAL,
                    pnl REAL,
                    mtd_pnl REAL,
                    ytd_pnl REAL,
                    details TEXT
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_nav_history_biz ON nav_history(account, date);
                """
            )
            conn.commit()

    @staticmethod
    def _gen_id(prefix: str) -> str:
        return f"{prefix}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"

    @staticmethod
    def _to_decimal(v: Any) -> Decimal:
        if v is None:
            return Decimal('0')
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))

    @classmethod
    def _quantize_money(cls, v: Any) -> float:
        return float(cls._to_decimal(v).quantize(cls.MONEY_QUANT, rounding=ROUND_HALF_UP))

    @classmethod
    def _quantize_nav(cls, v: Any) -> float:
        return float(cls._to_decimal(v).quantize(cls.NAV_QUANT, rounding=ROUND_HALF_UP))

    @classmethod
    def _quantize_weight(cls, v: Any) -> float:
        return float(cls._to_decimal(v).quantize(cls.WEIGHT_QUANT, rounding=ROUND_HALF_UP))

    @staticmethod
    def _dt_str(v):
        if not v:
            return None
        if isinstance(v, datetime):
            return v.strftime(DATETIME_FORMAT)
        if isinstance(v, date):
            return v.isoformat()
        return str(v)

    # ========== holdings ==========

    def get_holding(self, asset_id: str, account: str, market: Optional[str] = None) -> Optional[Holding]:
        with self._connect() as conn:
            if market is None:
                row = conn.execute(
                    "SELECT * FROM holdings WHERE asset_id=? AND account=? ORDER BY CASE WHEN market='' THEN 0 ELSE 1 END, rowid LIMIT 1",
                    (asset_id, account),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM holdings WHERE asset_id=? AND account=? AND market=? LIMIT 1",
                    (asset_id, account, market or ''),
                ).fetchone()
        return self._row_to_holding(row) if row else None

    def get_holdings(self, account: Optional[str] = None, asset_type: Optional[str] = None, include_empty: bool = False) -> List[Holding]:
        sql = "SELECT * FROM holdings WHERE 1=1"
        params: List[Any] = []
        if account:
            sql += " AND account=?"
            params.append(account)
        if asset_type:
            sql += " AND asset_type=?"
            params.append(asset_type)
        if not include_empty:
            sql += " AND quantity > 0"
        sql += " ORDER BY asset_type, asset_id"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._row_to_holding(r) for r in rows]

    def upsert_holding(self, holding: Holding) -> Holding:
        now = datetime.now()
        existing = self.get_holding(holding.asset_id, holding.account, holding.market)
        with self._connect() as conn:
            if existing and existing.record_id:
                is_cash_like = (existing.asset_type and existing.asset_type.value in ('cash', 'mmf'))
                new_quantity = self._quantize_money(existing.quantity + holding.quantity) if is_cash_like else (existing.quantity + holding.quantity)
                new_name = holding.asset_name or existing.asset_name
                conn.execute(
                    """
                    UPDATE holdings
                    SET asset_name=?, asset_type=?, quantity=?, currency=?, asset_class=?, industry=?, updated_at=?
                    WHERE record_id=?
                    """,
                    (
                        new_name,
                        holding.asset_type.value if holding.asset_type else existing.asset_type.value,
                        new_quantity,
                        holding.currency or existing.currency,
                        holding.asset_class.value if holding.asset_class else (existing.asset_class.value if existing.asset_class else None),
                        holding.industry.value if holding.industry else (existing.industry.value if existing.industry else None),
                        now.strftime(DATETIME_FORMAT),
                        existing.record_id,
                    ),
                )
                holding.record_id = existing.record_id
            else:
                record_id = holding.record_id or self._gen_id('hld')
                conn.execute(
                    """
                    INSERT INTO holdings(
                        record_id, asset_id, asset_name, asset_type, market, account, quantity, avg_cost,
                        currency, asset_class, industry, tag, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record_id,
                        holding.asset_id,
                        holding.asset_name,
                        holding.asset_type.value,
                        holding.market or '',
                        holding.account,
                        holding.quantity,
                        holding.avg_cost,
                        holding.currency,
                        holding.asset_class.value if holding.asset_class else None,
                        holding.industry.value if holding.industry else None,
                        json.dumps(holding.tag or [], ensure_ascii=False),
                        now.strftime(DATETIME_FORMAT),
                        now.strftime(DATETIME_FORMAT),
                    ),
                )
                holding.record_id = record_id
            conn.commit()
        return holding

    def update_holding_quantity(self, asset_id: str, account: str, quantity_change: float, market: Optional[str] = None):
        holding = self.get_holding(asset_id, account, market)
        if not holding or not holding.record_id:
            return
        is_cash_like = (holding.asset_type and holding.asset_type.value in ('cash', 'mmf'))
        new_quantity = self._quantize_money(holding.quantity + quantity_change) if is_cash_like else (holding.quantity + quantity_change)
        with self._connect() as conn:
            conn.execute(
                "UPDATE holdings SET quantity=?, updated_at=? WHERE record_id=?",
                (new_quantity, datetime.now().strftime(DATETIME_FORMAT), holding.record_id),
            )
            conn.commit()

    def delete_holding_if_zero(self, asset_id: str, account: str, market: Optional[str] = None):
        holding = self.get_holding(asset_id, account, market)
        if holding and holding.record_id and abs(holding.quantity) <= 1e-8:
            self.delete_holding_by_record_id(holding.record_id)

    def delete_holding_by_record_id(self, record_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM holdings WHERE record_id=?", (record_id,))
            conn.commit()
            return cur.rowcount > 0

    # ========== transactions ==========

    def add_transaction(self, tx: Transaction) -> Transaction:
        if tx.request_id:
            existing = self._find_by_request_id(tx.request_id)
            if existing:
                tx.record_id = existing.record_id
                return tx
        if tx.dedup_key:
            existing_id = self._find_by_dedup_key('transactions', tx.dedup_key)
            if existing_id:
                tx.record_id = existing_id
                return tx
        record_id = tx.record_id or self._gen_id('tx')
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO transactions(
                    record_id, request_id, dedup_key, tx_date, tx_type, asset_id, asset_name, asset_type,
                    market, account, quantity, price, amount, currency, fee, tax, related_account, remark, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record_id, tx.request_id, tx.dedup_key, tx.tx_date.isoformat(), tx.tx_type.value,
                    tx.asset_id, tx.asset_name, tx.asset_type.value if tx.asset_type else None,
                    tx.market or '', tx.account, tx.quantity, tx.price, tx.amount,
                    tx.currency, tx.fee, tx.tax, tx.related_account, tx.remark, tx.source,
                ),
            )
            conn.commit()
        tx.record_id = record_id
        return tx

    def _find_by_request_id(self, request_id: str) -> Optional[Transaction]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM transactions WHERE request_id=? LIMIT 1", (request_id,)).fetchone()
        return self._row_to_transaction(row) if row else None

    def _find_by_dedup_key(self, table: str, dedup_key: str) -> Optional[str]:
        if table not in ('transactions', 'cash_flow'):
            return None
        with self._connect() as conn:
            row = conn.execute(f"SELECT record_id FROM {table} WHERE dedup_key=? LIMIT 1", (dedup_key,)).fetchone()
        return row['record_id'] if row else None

    def get_transaction(self, record_id: str) -> Optional[Transaction]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM transactions WHERE record_id=?", (record_id,)).fetchone()
        return self._row_to_transaction(row) if row else None

    def get_transactions(self, account: Optional[str] = None, start_date: Optional[date] = None,
                         end_date: Optional[date] = None, tx_type: Optional[str] = None) -> List[Transaction]:
        sql = "SELECT * FROM transactions WHERE 1=1"
        params: List[Any] = []
        if account:
            sql += " AND account=?"
            params.append(account)
        if tx_type:
            sql += " AND tx_type=?"
            params.append(tx_type)
        if start_date:
            sql += " AND tx_date>=?"
            params.append(start_date.isoformat())
        if end_date:
            sql += " AND tx_date<=?"
            params.append(end_date.isoformat())
        sql += " ORDER BY tx_date DESC"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._row_to_transaction(r) for r in rows]

    def delete_transaction_by_record_id(self, record_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM transactions WHERE record_id=?", (record_id,))
            conn.commit()
            return cur.rowcount > 0

    # ========== cash_flow ==========

    def add_cash_flow(self, cf: CashFlow) -> CashFlow:
        if cf.dedup_key:
            existing_id = self._find_by_dedup_key('cash_flow', cf.dedup_key)
            if existing_id:
                cf.record_id = existing_id
                return cf
        record_id = cf.record_id or self._gen_id('cf')
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO cash_flow(
                    record_id, dedup_key, flow_date, account, amount, currency,
                    cny_amount, exchange_rate, flow_type, source, remark
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record_id, cf.dedup_key, cf.flow_date.isoformat(), cf.account, cf.amount, cf.currency,
                    cf.cny_amount, cf.exchange_rate, cf.flow_type, cf.source, cf.remark,
                ),
            )
            conn.commit()
        cf.record_id = record_id
        return cf

    def get_cash_flow(self, record_id: str) -> Optional[CashFlow]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM cash_flow WHERE record_id=?", (record_id,)).fetchone()
        return self._row_to_cash_flow(row) if row else None

    def get_cash_flows(self, account: Optional[str] = None, start_date: Optional[date] = None,
                       end_date: Optional[date] = None) -> List[CashFlow]:
        sql = "SELECT * FROM cash_flow WHERE 1=1"
        params: List[Any] = []
        if account:
            sql += " AND account=?"
            params.append(account)
        if start_date:
            sql += " AND flow_date>=?"
            params.append(start_date.isoformat())
        if end_date:
            sql += " AND flow_date<=?"
            params.append(end_date.isoformat())
        sql += " ORDER BY flow_date DESC"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._row_to_cash_flow(r) for r in rows]

    def get_total_cash_flow_cny(self, account: str) -> float:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(COALESCE(cny_amount, amount)), 0) AS total FROM cash_flow WHERE account=?",
                (account,),
            ).fetchone()
        return float(row['total'] or 0)

    def delete_cash_flow_by_record_id(self, record_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM cash_flow WHERE record_id=?", (record_id,))
            conn.commit()
            return cur.rowcount > 0

    # ========== nav_history ==========

    def save_nav(self, nav: NAVHistory, overwrite_existing: bool = True, dry_run: bool = False):
        existing = self.get_nav_on_date(nav.account, nav.date)
        if existing and existing.record_id and not overwrite_existing:
            raise ValueError(f"nav_history 已存在同日记录，拒绝覆盖: account={nav.account}, date={nav.date}")
        details = json.dumps(nav.details, ensure_ascii=False) if nav.details is not None else None
        if dry_run:
            return {"existing": bool(existing and existing.record_id), "date": nav.date.isoformat(), "account": nav.account}
        with self._connect() as conn:
            if existing and existing.record_id:
                conn.execute(
                    """
                    UPDATE nav_history SET total_value=?, cash_value=?, stock_value=?, fund_value=?,
                    cn_stock_value=?, us_stock_value=?, hk_stock_value=?, stock_weight=?, cash_weight=?,
                    shares=?, nav=?, cash_flow=?, share_change=?, mtd_nav_change=?, ytd_nav_change=?,
                    pnl=?, mtd_pnl=?, ytd_pnl=?, details=? WHERE record_id=?
                    """,
                    (
                        nav.total_value, nav.cash_value, nav.stock_value, nav.fund_value,
                        nav.cn_stock_value, nav.us_stock_value, nav.hk_stock_value, nav.stock_weight,
                        nav.cash_weight, nav.shares, nav.nav, nav.cash_flow, nav.share_change,
                        nav.mtd_nav_change, nav.ytd_nav_change, nav.pnl, nav.mtd_pnl, nav.ytd_pnl,
                        details, existing.record_id,
                    ),
                )
                nav.record_id = existing.record_id
            else:
                record_id = nav.record_id or self._gen_id('nav')
                conn.execute(
                    """
                    INSERT INTO nav_history(
                        record_id, date, account, total_value, cash_value, stock_value, fund_value,
                        cn_stock_value, us_stock_value, hk_stock_value, stock_weight, cash_weight,
                        shares, nav, cash_flow, share_change, mtd_nav_change, ytd_nav_change,
                        pnl, mtd_pnl, ytd_pnl, details
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record_id, nav.date.isoformat(), nav.account, nav.total_value, nav.cash_value,
                        nav.stock_value, nav.fund_value, nav.cn_stock_value, nav.us_stock_value,
                        nav.hk_stock_value, nav.stock_weight, nav.cash_weight, nav.shares, nav.nav,
                        nav.cash_flow, nav.share_change, nav.mtd_nav_change, nav.ytd_nav_change,
                        nav.pnl, nav.mtd_pnl, nav.ytd_pnl, details,
                    ),
                )
                nav.record_id = record_id
            conn.commit()

    def get_nav_history(self, account: str, days: int = 365) -> List[NAVHistory]:
        from datetime import timedelta
        start_date = date.today() - timedelta(days=days)
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM nav_history WHERE account=? AND date>=? ORDER BY date ASC",
                (account, start_date.isoformat()),
            ).fetchall()
        return [self._row_to_nav(r) for r in rows]

    def update_nav_fields(self, record_id: str, fields: Dict[str, Any], dry_run: bool = False):
        allowed = ['mtd_nav_change', 'ytd_nav_change', 'mtd_pnl', 'ytd_pnl', 'pnl', 'cash_flow', 'share_change', 'details']
        update_keys = [k for k in allowed if k in fields]
        normalized = {}
        for k in update_keys:
            v = fields[k]
            if k in ('mtd_nav_change', 'ytd_nav_change') and v is not None:
                normalized[k] = self._quantize_nav(v)
            elif k in ('mtd_pnl', 'ytd_pnl', 'pnl', 'cash_flow', 'share_change') and v is not None:
                normalized[k] = self._quantize_money(v)
            else:
                normalized[k] = v
        if dry_run:
            return {"record_id": record_id, "fields": normalized}
        if not update_keys:
            return {"record_id": record_id, "fields": {}}
        assigns = ', '.join(f"{k}=?" for k in update_keys)
        values = [normalized[k] for k in update_keys] + [record_id]
        with self._connect() as conn:
            conn.execute(f"UPDATE nav_history SET {assigns} WHERE record_id=?", values)
            conn.commit()
        return {"record_id": record_id, "fields": normalized}

    def get_latest_nav(self, account: str) -> Optional[NAVHistory]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM nav_history WHERE account=? ORDER BY date DESC LIMIT 1",
                (account,),
            ).fetchone()
        return self._row_to_nav(row) if row else None

    def get_nav_on_date(self, account: str, nav_date: date) -> Optional[NAVHistory]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM nav_history WHERE account=? AND date=? LIMIT 1",
                (account, nav_date.isoformat()),
            ).fetchone()
        return self._row_to_nav(row) if row else None

    def get_latest_nav_before(self, account: str, before_date: date) -> Optional[NAVHistory]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM nav_history WHERE account=? AND date<? ORDER BY date DESC LIMIT 1",
                (account, before_date.isoformat()),
            ).fetchone()
        return self._row_to_nav(row) if row else None

    def get_total_shares(self, account: str) -> float:
        latest = self.get_latest_nav(account)
        return latest.shares if latest and latest.shares else 0.0

    def delete_nav_by_record_id(self, record_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM nav_history WHERE record_id=?", (record_id,))
            conn.commit()
            return cur.rowcount > 0

    # ========== prices ==========

    def get_price(self, asset_id: str) -> Optional[PriceCache]:
        return self._local_price_cache.get(asset_id)

    def save_price(self, price: PriceCache):
        self._local_price_cache.save(price)

    def get_all_prices(self) -> List[PriceCache]:
        return self._local_price_cache.get_all()

    # ========== row mappers ==========

    def _row_to_holding(self, row) -> Holding:
        tag = json.loads(row['tag']) if row['tag'] else []
        created_at = datetime.strptime(row['created_at'], DATETIME_FORMAT) if row['created_at'] else None
        updated_at = datetime.strptime(row['updated_at'], DATETIME_FORMAT) if row['updated_at'] else None
        return Holding(
            record_id=row['record_id'],
            asset_id=row['asset_id'],
            asset_name=row['asset_name'],
            asset_type=AssetType(row['asset_type']) if row['asset_type'] else AssetType.OTHER,
            market=row['market'] or '',
            account=row['account'],
            quantity=float(row['quantity'] or 0),
            avg_cost=float(row['avg_cost']) if row['avg_cost'] is not None else None,
            currency=row['currency'],
            asset_class=AssetClass(row['asset_class']) if row['asset_class'] else None,
            industry=Industry(row['industry']) if row['industry'] else None,
            tag=tag,
            created_at=created_at,
            updated_at=updated_at,
        )

    def _row_to_transaction(self, row) -> Transaction:
        return Transaction(
            record_id=row['record_id'],
            request_id=row['request_id'],
            dedup_key=row['dedup_key'],
            tx_date=datetime.strptime(row['tx_date'], '%Y-%m-%d').date(),
            tx_type=TransactionType(row['tx_type']),
            asset_id=row['asset_id'],
            asset_name=row['asset_name'],
            asset_type=AssetType(row['asset_type']) if row['asset_type'] else None,
            market=row['market'] or '',
            account=row['account'],
            quantity=float(row['quantity'] or 0),
            price=float(row['price'] or 0),
            amount=float(row['amount']) if row['amount'] is not None else None,
            currency=row['currency'],
            fee=float(row['fee'] or 0),
            tax=float(row['tax'] or 0),
            related_account=row['related_account'],
            remark=row['remark'],
            source=row['source'] or 'manual',
        )

    def _row_to_cash_flow(self, row) -> CashFlow:
        return CashFlow(
            record_id=row['record_id'],
            dedup_key=row['dedup_key'],
            flow_date=datetime.strptime(row['flow_date'], '%Y-%m-%d').date(),
            account=row['account'],
            amount=float(row['amount'] or 0),
            currency=row['currency'],
            cny_amount=float(row['cny_amount']) if row['cny_amount'] is not None else None,
            exchange_rate=float(row['exchange_rate']) if row['exchange_rate'] is not None else None,
            flow_type=row['flow_type'],
            source=row['source'],
            remark=row['remark'],
        )

    def _row_to_nav(self, row) -> NAVHistory:
        return NAVHistory(
            record_id=row['record_id'],
            date=datetime.strptime(row['date'], '%Y-%m-%d').date(),
            account=row['account'],
            total_value=float(row['total_value'] or 0),
            cash_value=float(row['cash_value'] or 0),
            stock_value=float(row['stock_value'] or 0),
            fund_value=float(row['fund_value'] or 0),
            cn_stock_value=float(row['cn_stock_value'] or 0),
            us_stock_value=float(row['us_stock_value'] or 0),
            hk_stock_value=float(row['hk_stock_value'] or 0),
            stock_weight=float(row['stock_weight']) if row['stock_weight'] is not None else None,
            cash_weight=float(row['cash_weight']) if row['cash_weight'] is not None else None,
            shares=float(row['shares']) if row['shares'] is not None else None,
            nav=float(row['nav']) if row['nav'] is not None else None,
            cash_flow=float(row['cash_flow']) if row['cash_flow'] is not None else None,
            share_change=float(row['share_change']) if row['share_change'] is not None else None,
            mtd_nav_change=float(row['mtd_nav_change']) if row['mtd_nav_change'] is not None else None,
            ytd_nav_change=float(row['ytd_nav_change']) if row['ytd_nav_change'] is not None else None,
            pnl=float(row['pnl']) if row['pnl'] is not None else None,
            mtd_pnl=float(row['mtd_pnl']) if row['mtd_pnl'] is not None else None,
            ytd_pnl=float(row['ytd_pnl']) if row['ytd_pnl'] is not None else None,
            details=json.loads(row['details']) if row['details'] else None,
        )
