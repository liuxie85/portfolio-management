"""Transactions CRUD mixin for FeishuStorage."""
from datetime import date, datetime
from typing import Dict, List, Optional

from ..models import (
    Transaction, TransactionType, AssetType,
    make_tx_dedup_key, make_request_id, DATETIME_FORMAT,
)


class TransactionsMixin:
    """Transactions table operations."""

    @staticmethod
    def _is_missing_field_error(error: Exception) -> bool:
        msg = str(error)
        lowered = msg.lower()
        return (
            'fieldnamenotfound' in lowered or
            ('field' in lowered and 'not found' in lowered) or
            '字段不存在' in msg or
            '不存在' in msg
        )

    def add_transaction(self, tx: Transaction) -> Transaction:
        """添加交易记录（自动防止重复提交）

        防重机制（按优先级）：
        1. request_id: 调用方传入的幂等键
        2. dedup_key: 内容指纹，自动生成
        """
        if not tx.request_id:
            tx.request_id = make_request_id(prefix="tx")
        if not tx.dedup_key:
            tx.dedup_key = make_tx_dedup_key(tx)

        # 1. request_id 幂等性检查
        if tx.request_id:
            existing = self._find_by_request_id(tx.request_id)
            if existing:
                print(f"[幂等性保护] 发现重复请求(request_id={tx.request_id})，跳过创建")
                tx.record_id = existing.record_id
                return tx

        # 2. dedup_key 内容指纹检查
        if tx.dedup_key:
            existing = self._find_by_dedup_key('transactions', tx.dedup_key)
            if existing:
                print(f"[防重保护] 发现相同内容交易(dedup_key={tx.dedup_key})，跳过创建")
                tx.record_id = existing
                return tx

        fields = self._transaction_to_dict(tx)
        feishu_fields = self._to_feishu_fields(fields, 'transactions')

        try:
            result = self.client.create_record('transactions', feishu_fields)
        except Exception as e:
            if self._is_missing_field_error(e):
                raise ValueError("Feishu transactions 表缺少 request_id/dedup_key 等幂等字段，已拒绝降级写入；请先补齐表字段") from e
            raise

        tx.record_id = result['record_id']

        # 写入防重缓存
        if tx.request_id:
            self._request_id_cache[tx.request_id] = tx.record_id
        if tx.dedup_key:
            self._dedup_key_cache[f"transactions:{tx.dedup_key}"] = tx.record_id

        return tx

    def _find_by_request_id(self, request_id: str) -> Optional[Transaction]:
        """通过 request_id 查找交易记录（用于幂等性检查，带本地缓存）"""
        if not request_id:
            return None

        cached_record_id = self._request_id_cache.get(request_id)
        if cached_record_id:
            try:
                record = self.client.get_record_strict('transactions', cached_record_id)
                fields = self._from_feishu_fields(record['fields'], 'transactions')
                fields['record_id'] = record['record_id']
                return self._dict_to_transaction(fields)
            except Exception:
                self._request_id_cache.pop(request_id, None)

        filter_str = f'CurrentValue.[request_id] = "{self._escape_filter_value(request_id)}"'
        try:
            records = self.client.list_records('transactions', filter_str=filter_str)
            if records:
                record_id = records[0]['record_id']
                self._request_id_cache[request_id] = record_id
                fields = self._from_feishu_fields(records[0]['fields'], 'transactions')
                fields['record_id'] = record_id
                return self._dict_to_transaction(fields)
        except Exception as e:
            if self._is_missing_field_error(e):
                raise ValueError("Feishu transactions 表缺少 request_id 字段，无法保证幂等性；请先补齐表字段") from e
            print(f"[警告] 幂等性检查失败: {e}")

        return None

    def _find_by_dedup_key(self, table: str, dedup_key: str) -> Optional[str]:
        """通过 dedup_key 查找记录（用于内容指纹防重，带本地缓存）

        Returns:
            record_id if found, else None
        """
        if not dedup_key:
            return None

        cache_key = f"{table}:{dedup_key}"
        cached_record_id = self._dedup_key_cache.get(cache_key)
        if cached_record_id:
            try:
                record = self.client.get_record_strict(table, cached_record_id)
                if record:
                    return cached_record_id
            except Exception:
                self._dedup_key_cache.pop(cache_key, None)

        filter_str = f'CurrentValue.[dedup_key] = "{self._escape_filter_value(dedup_key)}"'
        try:
            records = self.client.list_records(table, filter_str=filter_str)
            if records:
                record_id = records[0]['record_id']
                self._dedup_key_cache[cache_key] = record_id
                return record_id
        except Exception as e:
            if self._is_missing_field_error(e):
                raise ValueError(f"Feishu {table} 表缺少 dedup_key 字段，无法保证防重；请先补齐表字段") from e
            raise

        return None

    def get_transaction(self, record_id: str) -> Optional[Transaction]:
        """获取单条交易记录（通过 record_id）"""
        try:
            record = self.client.get_record_strict('transactions', record_id)
        except Exception:
            return None

        fields = self._from_feishu_fields(record['fields'], 'transactions')
        fields['record_id'] = record['record_id']
        return self._dict_to_transaction(fields)

    def get_transactions(self, account: Optional[str] = None,
                        start_date: Optional[date] = None,
                        end_date: Optional[date] = None,
                        tx_type: Optional[str] = None) -> List[Transaction]:
        """获取交易记录列表（日期过滤推到飞书服务端）"""
        conditions = []

        if account:
            conditions.append(f'CurrentValue.[account] = "{self._escape_filter_value(account)}"')
        if tx_type:
            conditions.append(f'CurrentValue.[tx_type] = "{self._escape_filter_value(tx_type)}"')
        if start_date:
            conditions.append(f'CurrentValue.[tx_date] >= "{start_date.strftime("%Y-%m-%d")}"')
        if end_date:
            conditions.append(f'CurrentValue.[tx_date] <= "{end_date.strftime("%Y-%m-%d")}"')

        filter_str = ' AND '.join(conditions) if conditions else None
        records = self.client.list_records('transactions', filter_str=filter_str)

        transactions = []
        for record in records:
            fields = self._from_feishu_fields(record['fields'], 'transactions')
            fields['record_id'] = record['record_id']
            tx = self._dict_to_transaction(fields)
            transactions.append(tx)

        transactions.sort(key=lambda t: t.tx_date or date.min, reverse=True)
        return transactions

    def _transaction_to_dict(self, tx: Transaction) -> Dict:
        """Transaction 转字典"""
        result = {
            'tx_date': tx.tx_date,
            'tx_type': tx.tx_type,
            'asset_id': tx.asset_id,
            'asset_name': tx.asset_name,
            'asset_type': tx.asset_type,
            'market': tx.market,
            'account': tx.account,
            'quantity': tx.quantity,
            'price': tx.price,
            'amount': tx.amount,
            'currency': tx.currency,
            'fee': tx.fee,
            'remark': tx.remark,
            'request_id': tx.request_id,
            'dedup_key': tx.dedup_key,
        }
        return {k: v for k, v in result.items() if v is not None and v != ''}

    def _dict_to_transaction(self, data: Dict) -> Transaction:
        """字典转 Transaction"""
        tx_date = data.get('tx_date')
        if isinstance(tx_date, (int, float)):
            tx_date = datetime.fromtimestamp(tx_date / 1000, tz=self.FEISHU_DATE_TZ).date()
        elif isinstance(tx_date, str):
            tx_date = datetime.strptime(tx_date, '%Y-%m-%d').date()

        return Transaction(
            record_id=data.get('record_id'),
            request_id=data.get('request_id'),
            tx_date=tx_date,
            tx_type=TransactionType(data.get('tx_type')) if data.get('tx_type') else TransactionType.BUY,
            asset_id=data.get('asset_id', ''),
            asset_name=data.get('asset_name'),
            asset_type=AssetType(data.get('asset_type')) if data.get('asset_type') else None,
            market=data.get('market'),
            account=data.get('account', ''),
            quantity=float(data.get('quantity', 0)),
            price=float(data.get('price', 0)),
            amount=float(data.get('amount')) if data.get('amount') is not None else None,
            currency=data.get('currency', 'CNY'),
            fee=float(data.get('fee', 0)),
            tax=float(data.get('tax', 0)),
            related_account=data.get('related_account'),
            remark=data.get('remark'),
            source=data.get('source', 'manual')
        )

    def delete_transaction_by_record_id(self, record_id: str) -> bool:
        """通过记录ID删除交易"""
        return self.client.delete_record('transactions', record_id)
