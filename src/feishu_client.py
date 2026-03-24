"""
飞书多维表 API 客户端
支持读写 5 张核心表：holdings, transactions, price_cache, nav_history, cash_flow
"""
import json
import time
import requests
import requests.adapters
import threading
from typing import Dict, List, Optional, Any, Union
from datetime import datetime

from src import config


class FeishuClient:
    """飞书多维表 API 客户端"""

    BASE_URL = "https://open.feishu.cn/open-apis"

    def __init__(self, app_id: str = None, app_secret: str = None, user_token: str = None):
        """
        初始化飞书客户端

        Args:
            app_id: 飞书自建应用 App ID
            app_secret: 飞书自建应用 App Secret
            user_token: 个人访问令牌（与 app_id/app_secret 二选一）
        """
        self.app_id = app_id or config.get("feishu.app_id")
        self.app_secret = app_secret or config.get("feishu.app_secret")
        self.user_token = user_token or config.get("feishu.user_token")

        # 应用级 token 缓存（带线程安全锁）
        self._tenant_token = None
        self._token_expire_time = 0
        self._token_lock = threading.Lock()  # 用于双重检查锁

        # 限流保护：飞书 API 限制 20 QPS
        self._last_request_time = 0
        self._min_interval = 0.06  # 60ms = 约 16 QPS，留有余量

        # 表配置映射（支持两种配置方式）
        # 方式1（统一base）：FEISHU_APP_TOKEN=bascnxxx + FEISHU_TABLE_HOLDINGS=tblxxx
        # 方式2（分表base）：FEISHU_TABLE_HOLDINGS=bascnxxx/tblxxx
        self.table_configs = {}
        for table_name in ['holdings', 'transactions', 'price_cache', 'nav_history', 'cash_flow']:
            value = config.get(f"feishu.tables.{table_name}")
            if value:
                if '/' in value:
                    # 分表base配置: bascnxxx/tblxxx
                    parts = value.split('/')
                    self.table_configs[table_name] = {
                        'app_token': parts[0],
                        'table_id': parts[1] if len(parts) > 1 else value
                    }
                else:
                    # 统一base配置，table_id单独存储
                    self.table_configs[table_name] = {
                        'app_token': None,  # 使用统一的 FEISHU_APP_TOKEN
                        'table_id': value
                    }

        # 统一 base token（方式1使用，方式2中各表有自己的）
        self.default_app_token = config.get("feishu.app_token")

        # 连接池配置（提升HTTP请求效率）
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=3,
            pool_block=False
        )
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

    def _get_headers(self) -> Dict[str, str]:
        """获取请求头"""
        if self.user_token:
            # 使用个人访问令牌
            return {
                'Authorization': f'Bearer {self.user_token}',
                'Content-Type': 'application/json'
            }
        else:
            # 使用应用级 token
            return {
                'Authorization': f'Bearer {self._get_tenant_token()}',
                'Content-Type': 'application/json'
            }

    def _get_tenant_token(self) -> str:
        """获取应用级 tenant access token（带缓存，线程安全 DCL）"""
        now = time.time()

        # 第一重检查（无锁）
        if self._tenant_token and now < self._token_expire_time - 300:
            return self._tenant_token

        # 获取锁进行第二重检查
        with self._token_lock:
            # 第二重检查（有锁）- 防止多个线程同时通过第一重检查后重复请求
            if self._tenant_token and now < self._token_expire_time - 300:
                return self._tenant_token

            if not self.app_id or not self.app_secret:
                raise ValueError("需要提供 app_id 和 app_secret，请在 config.json 或环境变量中配置")

            url = f"{self.BASE_URL}/auth/v3/tenant_access_token/internal"
            response = requests.post(url, json={
                'app_id': self.app_id,
                'app_secret': self.app_secret
            })
            response.raise_for_status()
            data = response.json()

            if data.get('code') != 0:
                raise Exception(f"获取 token 失败: {data.get('msg')}")

            self._tenant_token = data['tenant_access_token']
            self._token_expire_time = now + data['expire']
            return self._tenant_token

    def _rate_limit(self):
        """限流控制"""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.time()

    def _request(self, method: str, endpoint: str, _retry_count: int = 0, **kwargs) -> Dict:
        """发送请求（带限流和错误处理）"""
        self._rate_limit()

        url = f"{self.BASE_URL}{endpoint}"
        headers = self._get_headers()

        response = self.session.request(method, url, headers=headers, **kwargs)

        # 处理限流错误（最多重试3次）
        if response.status_code == 429:
            if _retry_count >= 3:
                response.raise_for_status()
            time.sleep(1 * (2 ** _retry_count))  # 指数退避
            return self._request(method, endpoint, _retry_count=_retry_count + 1, **kwargs)

        response.raise_for_status()
        data = response.json()

        if data.get('code') != 0:
            raise Exception(f"飞书 API 错误: {data.get('msg')} (code={data.get('code')})")

        return data.get('data', {})

    def _get_table_config(self, table_name: str) -> tuple:
        """获取表的配置 (app_token, table_id)"""
        config = self.table_configs.get(table_name)
        if not config:
            raise ValueError(f"未配置表 {table_name}，请设置 FEISHU_TABLE_{table_name.upper()}")

        app_token = config['app_token'] or self.default_app_token
        table_id = config['table_id']

        if not app_token:
            raise ValueError(f"未配置 FEISHU_APP_TOKEN 或表 {table_name} 的分表 token")
        if not table_id:
            raise ValueError(f"未配置表 {table_name} 的 table ID")

        return app_token, table_id

    def list_records(self, table_name: str, filter_str: str = None,
                     field_names: List[str] = None, page_size: int = 500) -> List[Dict]:
        """
        查询记录列表

        Args:
            table_name: 表名（holdings/transactions/price_cache/nav_history/cash_flow）
            filter_str: 筛选条件（飞书 filter 语法）
            field_names: 指定返回的字段列表（减少数据传输）
            page_size: 每页数量
        """
        app_token, table_id = self._get_table_config(table_name)

        records = []
        page_token = None

        while True:
            endpoint = f"/bitable/v1/apps/{app_token}/tables/{table_id}/records"
            params = {'page_size': page_size}
            if page_token:
                params['page_token'] = page_token
            if filter_str:
                params['filter'] = filter_str
            if field_names:
                # 飞书API使用field_names参数指定返回字段
                params['field_names'] = json.dumps(field_names)

            data = self._request('GET', endpoint, params=params)
            items = data.get('items', [])

            for item in items:
                record = {
                    'record_id': item['record_id'],
                    'fields': item['fields']
                }
                records.append(record)

            page_token = data.get('page_token')
            if not page_token or not items:
                break

        return records

    def get_record(self, table_name: str, record_id: str) -> Optional[Dict]:
        """获取单条记录（宽松模式）。

        Notes:
        - This method returns None on errors to preserve backward compatibility.
        - Prefer get_record_strict() for write-paths where "None" is dangerous.
        """
        try:
            return self.get_record_strict(table_name, record_id)
        except Exception:
            return None

    def get_record_strict(self, table_name: str, record_id: str) -> Dict:
        """获取单条记录（严格模式）：任何错误直接抛出。"""
        app_token, table_id = self._get_table_config(table_name)
        endpoint = f"/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"

        data = self._request('GET', endpoint)
        # API returns {'record': {...}} for get_record
        rec = data.get('record') if isinstance(data, dict) else None
        if rec and isinstance(rec, dict):
            return {
                'record_id': rec.get('record_id'),
                'fields': rec.get('fields')
            }
        # fallback (defensive)
        if isinstance(data, dict) and ('record_id' in data or 'fields' in data):
            return {
                'record_id': data.get('record_id'),
                'fields': data.get('fields')
            }
        raise ValueError(f"Unexpected get_record response shape for table={table_name}: {data}")

    # 各表必填字段定义（用于验证）
    REQUIRED_FIELDS = {
        'holdings': ['asset_id', 'account', 'quantity'],
        'transactions': ['tx_date', 'tx_type', 'asset_id', 'account', 'quantity', 'price'],
        'cash_flow': ['flow_date', 'account', 'amount', 'currency'],
        'nav_history': ['date', 'account', 'total_value', 'shares', 'nav'],
        'price_cache': ['asset_id', 'price', 'currency', 'cny_price']
    }

    def create_record(self, table_name: str, fields: Dict[str, Any]) -> Dict:
        """
        创建记录

        Args:
            table_name: 表名
            fields: 字段值字典
        """
        app_token, table_id = self._get_table_config(table_name)

        # 验证必填字段
        required = self.REQUIRED_FIELDS.get(table_name, [])
        for field in required:
            if field not in fields or fields[field] is None or fields[field] == '':
                raise ValueError(f"表 {table_name} 缺少必填字段: {field}")

        # 过滤空值字段（避免创建空记录）
        filtered_fields = {k: v for k, v in fields.items() if v is not None and v != ''}

        endpoint = f"/bitable/v1/apps/{app_token}/tables/{table_id}/records"

        data = self._request('POST', endpoint, json={'fields': filtered_fields})
        return {
            'record_id': data['record']['record_id'],
            'fields': data['record']['fields']
        }

    def update_record(self, table_name: str, record_id: str, fields: Dict[str, Any]) -> Dict:
        """更新记录"""
        app_token, table_id = self._get_table_config(table_name)

        endpoint = f"/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"

        data = self._request('PUT', endpoint, json={'fields': fields})
        return {
            'record_id': data['record']['record_id'],
            'fields': data['record']['fields']
        }

    def delete_record(self, table_name: str, record_id: str) -> bool:
        """删除记录"""
        try:
            app_token, table_id = self._get_table_config(table_name)
        except ValueError:
            return False

        endpoint = f"/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"

        try:
            self._request('DELETE', endpoint)
            return True
        except Exception:
            return False

    def batch_create_records(self, table_name: str, records: List[Dict[str, Any]]) -> List[Dict]:
        """
        批量创建记录（减少 API 调用次数）

        Args:
            table_name: 表名
            records: 字段值字典列表
        """
        if not records:
            return []

        app_token, table_id = self._get_table_config(table_name)

        endpoint = f"/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"

        # 飞书限制单次最多 500 条
        batch_size = 500
        results = []

        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            data = self._request('POST', endpoint, json={'records': batch})
            results.extend(data.get('records', []))

        return results

    def batch_update_records(self, table_name: str, records: List[Dict]) -> List[Dict]:
        """
        批量更新记录

        Args:
            table_name: 表名
            records: [{'record_id': str, 'fields': dict}, ...]
        """
        if not records:
            return []

        app_token, table_id = self._get_table_config(table_name)

        endpoint = f"/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update"

        batch_size = 500
        results = []

        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            data = self._request('POST', endpoint, json={'records': batch})
            results.extend(data.get('records', []))

        return results

    def batch_delete_records(self, table_name: str, record_ids: List[str]) -> int:
        """
        批量删除记录

        Args:
            table_name: 表名
            record_ids: 记录ID列表

        Returns:
            删除的记录数
        """
        if not record_ids:
            return 0

        app_token, table_id = self._get_table_config(table_name)

        endpoint = f"/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_delete"

        batch_size = 500
        deleted_count = 0

        for i in range(0, len(record_ids), batch_size):
            batch = record_ids[i:i + batch_size]
            data = self._request('POST', endpoint, json={'records': batch})
            deleted_count += len(data.get('records', []))

        return deleted_count
