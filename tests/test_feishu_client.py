"""测试飞书客户端"""
import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import json
import os

from src.feishu_client import FeishuClient


class TestFeishuClientInitialization:
    """测试飞书客户端初始化"""

    def test_init_with_env_vars(self):
        """测试使用环境变量初始化"""
        with patch.dict(os.environ, {
            'FEISHU_APP_ID': 'test_app_id',
            'FEISHU_APP_SECRET': 'test_secret',
            'FEISHU_APP_TOKEN': 'test_token'
        }):
            client = FeishuClient()
            assert client.app_id == 'test_app_id'
            assert client.app_secret == 'test_secret'
            assert client.default_app_token == 'test_token'

    def test_init_with_params(self):
        """测试使用参数初始化"""
        client = FeishuClient(
            app_id='param_app_id',
            app_secret='param_secret',
            user_token='user_token_123'
        )
        assert client.app_id == 'param_app_id'
        assert client.app_secret == 'param_secret'
        assert client.user_token == 'user_token_123'

    def test_init_with_user_token(self):
        """测试使用个人访问令牌初始化"""
        with patch.dict(os.environ, {
            'FEISHU_USER_TOKEN': 'user_token_env'
        }):
            client = FeishuClient()
            assert client.user_token == 'user_token_env'

    def test_table_configs_unified_base(self):
        """测试统一base配置方式"""
        with patch.dict(os.environ, {
            'FEISHU_APP_TOKEN': 'base_token',
            'FEISHU_TABLE_HOLDINGS': 'tbl_holdings',
            'FEISHU_TABLE_TRANSACTIONS': 'tbl_transactions'
        }):
            client = FeishuClient()
            assert client.table_configs['holdings']['table_id'] == 'tbl_holdings'
            assert client.table_configs['holdings']['app_token'] is None
            assert client.table_configs['transactions']['table_id'] == 'tbl_transactions'

    def test_table_configs_separate_base(self):
        """测试分表base配置方式"""
        with patch.dict(os.environ, {
            'FEISHU_TABLE_HOLDINGS': 'base1/tbl1',
            'FEISHU_TABLE_TRANSACTIONS': 'base2/tbl2'
        }):
            client = FeishuClient()
            assert client.table_configs['holdings']['app_token'] == 'base1'
            assert client.table_configs['holdings']['table_id'] == 'tbl1'
            assert client.table_configs['transactions']['app_token'] == 'base2'
            assert client.table_configs['transactions']['table_id'] == 'tbl2'


class TestFeishuClientToken:
    """测试飞书客户端Token管理"""

    @patch('src.feishu_client.requests.post')
    def test_get_tenant_token_success(self, mock_post):
        """测试获取tenant token成功"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'code': 0,
            'tenant_access_token': 'test_token_123',
            'expire': 7200
        }
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        client = FeishuClient(app_id='test_id', app_secret='test_secret')
        token = client._get_tenant_token()

        assert token == 'test_token_123'
        assert client._tenant_token == 'test_token_123'

    @patch('src.feishu_client.requests.post')
    def test_get_tenant_token_failure(self, mock_post):
        """测试获取tenant token失败"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'code': 99991663,
            'msg': 'app_id or app_secret is invalid'
        }
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        client = FeishuClient(app_id='invalid', app_secret='invalid')
        with pytest.raises(Exception) as exc_info:
            client._get_tenant_token()
        assert '获取 token 失败' in str(exc_info.value)

    @patch('src.feishu_client.requests.post')
    def test_get_tenant_token_cache(self, mock_post):
        """测试token缓存"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'code': 0,
            'tenant_access_token': 'cached_token',
            'expire': 7200
        }
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        client = FeishuClient(app_id='test_id', app_secret='test_secret')

        # 第一次调用
        token1 = client._get_tenant_token()
        # 第二次调用应该使用缓存
        token2 = client._get_tenant_token()

        assert token1 == token2 == 'cached_token'
        # 只调用了一次API
        assert mock_post.call_count == 1

    def test_get_headers_with_user_token(self):
        """测试使用个人访问令牌的请求头"""
        client = FeishuClient(user_token='user_token_123')
        headers = client._get_headers()

        assert headers['Authorization'] == 'Bearer user_token_123'
        assert headers['Content-Type'] == 'application/json'

    @patch('src.feishu_client.requests.post')
    def test_get_headers_with_tenant_token(self, mock_post):
        """测试使用tenant token的请求头"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'code': 0,
            'tenant_access_token': 'tenant_token_123',
            'expire': 7200
        }
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        client = FeishuClient(app_id='test_id', app_secret='test_secret')
        headers = client._get_headers()

        assert headers['Authorization'] == 'Bearer tenant_token_123'


class TestFeishuClientRequest:
    """测试飞书客户端请求"""

    @patch('src.feishu_client.requests.Session.request')
    @patch('src.feishu_client.FeishuClient._get_headers')
    def test_request_success(self, mock_headers, mock_request):
        """测试请求成功"""
        mock_headers.return_value = {'Authorization': 'Bearer token'}

        mock_response = Mock()
        mock_response.json.return_value = {
            'code': 0,
            'data': {'records': [{'record_id': 'rec123'}]}
        }
        mock_response.raise_for_status = Mock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response

        client = FeishuClient(app_id='test', app_secret='test')
        result = client._request('POST', '/test/endpoint', json={'test': 'data'})

        assert result == {'records': [{'record_id': 'rec123'}]}

    @patch('src.feishu_client.requests.Session.request')
    @patch('src.feishu_client.FeishuClient._get_headers')
    def test_request_api_error(self, mock_headers, mock_request):
        """测试API返回错误"""
        mock_headers.return_value = {'Authorization': 'Bearer token'}

        mock_response = Mock()
        mock_response.json.return_value = {
            'code': 99991661,
            'msg': 'tenant token is invalid'
        }
        mock_response.raise_for_status = Mock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response

        client = FeishuClient(app_id='test', app_secret='test')
        with pytest.raises(Exception) as exc_info:
            client._request('POST', '/test/endpoint')
        assert '飞书 API 错误' in str(exc_info.value)

    @patch('src.feishu_client.time.sleep')
    @patch('src.feishu_client.requests.Session.request')
    @patch('src.feishu_client.FeishuClient._get_headers')
    def test_request_rate_limit(self, mock_headers, mock_request, mock_sleep):
        """测试限流重试"""
        mock_headers.return_value = {'Authorization': 'Bearer token'}

        # 第一次返回429，第二次成功
        error_response = Mock()
        error_response.status_code = 429
        error_response.raise_for_status = Mock()

        success_response = Mock()
        success_response.json.return_value = {
            'code': 0,
            'data': {'success': True}
        }
        success_response.raise_for_status = Mock()
        success_response.status_code = 200

        mock_request.side_effect = [error_response, success_response]

        client = FeishuClient(app_id='test', app_secret='test')
        result = client._request('POST', '/test/endpoint')

        assert result == {'success': True}
        assert mock_request.call_count == 2


class TestFeishuClientTableOperations:
    """测试飞书客户端表操作"""

    def test_get_table_config_success(self):
        """测试获取表配置成功"""
        with patch.dict(os.environ, {
            'FEISHU_APP_TOKEN': 'base_token',
            'FEISHU_TABLE_HOLDINGS': 'tbl_holdings'
        }):
            client = FeishuClient()
            app_token, table_id = client._get_table_config('holdings')
            assert app_token == 'base_token'
            assert table_id == 'tbl_holdings'

    def test_get_table_config_separate_base(self):
        """测试分表配置"""
        with patch.dict(os.environ, {
            'FEISHU_TABLE_HOLDINGS': 'separate_base/tbl_holdings'
        }):
            client = FeishuClient()
            app_token, table_id = client._get_table_config('holdings')
            assert app_token == 'separate_base'
            assert table_id == 'tbl_holdings'

    def test_get_table_config_not_configured(self):
        """测试表未配置"""
        with patch.dict(os.environ, {}, clear=True):
            client = FeishuClient()
            client.table_configs = {}
            client.default_app_token = None
            with pytest.raises(ValueError) as exc_info:
                client._get_table_config('holdings')
            assert '未配置表 holdings' in str(exc_info.value)

    def test_get_table_config_missing_app_token(self):
        """测试缺少app token"""
        with patch.dict(os.environ, {
            'FEISHU_TABLE_HOLDINGS': 'tbl_holdings'
        }):
            client = FeishuClient()
            with pytest.raises(ValueError) as exc_info:
                client._get_table_config('holdings')
            assert '未配置 FEISHU_APP_TOKEN' in str(exc_info.value)


class TestFeishuClientRecords:
    """测试飞书客户端记录操作"""

    @patch('src.feishu_client.FeishuClient._request')
    @patch('src.feishu_client.FeishuClient._get_table_config')
    def test_list_records(self, mock_config, mock_request):
        """测试查询记录列表"""
        mock_config.return_value = ('app_token', 'table_id')
        mock_request.return_value = {
            'items': [
                {
                    'record_id': 'rec1',
                    'fields': {'asset_id': '000001', 'quantity': 100}
                },
                {
                    'record_id': 'rec2',
                    'fields': {'asset_id': '000002', 'quantity': 200}
                }
            ],
            'page_token': None
        }

        client = FeishuClient(app_id='test', app_secret='test')
        records = client.list_records('holdings')

        assert len(records) == 2
        assert records[0]['record_id'] == 'rec1'
        assert records[0]['fields']['asset_id'] == '000001'

    @patch('src.feishu_client.FeishuClient._request')
    @patch('src.feishu_client.FeishuClient._get_table_config')
    def test_list_records_with_filter(self, mock_config, mock_request):
        """测试带筛选条件的记录查询"""
        mock_config.return_value = ('app_token', 'table_id')
        mock_request.return_value = {
            'items': [{'record_id': 'rec1', 'fields': {}}],
            'page_token': None
        }

        client = FeishuClient(app_id='test', app_secret='test')
        records = client.list_records('holdings', filter_str='asset_id = "000001"')

        # 验证_filter参数被正确传递
        call_args = mock_request.call_args
        assert 'params' in call_args.kwargs
        assert call_args.kwargs['params']['filter'] == 'asset_id = "000001"'

    @patch('src.feishu_client.requests.Session.request')
    @patch('src.feishu_client.FeishuClient._get_headers')
    @patch('src.feishu_client.FeishuClient._get_table_config')
    def test_create_record(self, mock_config, mock_headers, mock_request):
        """测试创建记录"""
        mock_config.return_value = ('app_token', 'table_id')
        mock_headers.return_value = {'Authorization': 'Bearer token'}

        mock_response = Mock()
        mock_response.json.return_value = {
            'code': 0,
            'data': {
                'record': {
                    'record_id': 'new_rec_123',
                    'fields': {'asset_id': '000001', 'quantity': 100}
                }
            }
        }
        mock_response.raise_for_status = Mock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response

        client = FeishuClient(app_id='test', app_secret='test')
        result = client.create_record('holdings', {
            'asset_id': '000001',
            'account': '测试账户',
            'quantity': 100
        })

        assert result['record_id'] == 'new_rec_123'

    @patch('src.feishu_client.FeishuClient._request')
    @patch('src.feishu_client.FeishuClient._get_table_config')
    def test_create_record_missing_required_field(self, mock_config, mock_request):
        """测试创建记录缺少必填字段"""
        mock_config.return_value = ('app_token', 'table_id')

        client = FeishuClient(app_id='test', app_secret='test')
        with pytest.raises(ValueError) as exc_info:
            client.create_record('holdings', {'asset_name': '测试'})
        assert '缺少必填字段' in str(exc_info.value)

    @patch('src.feishu_client.FeishuClient._request')
    @patch('src.feishu_client.FeishuClient._get_table_config')
    def test_update_record(self, mock_config, mock_request):
        """测试更新记录"""
        mock_config.return_value = ('app_token', 'table_id')
        mock_request.return_value = {
            'record': {
                'record_id': 'rec_123',
                'fields': {'quantity': 200}
            }
        }

        client = FeishuClient(app_id='test', app_secret='test')
        result = client.update_record('holdings', 'rec_123', {'quantity': 200})

        assert result['record_id'] == 'rec_123'

    @patch('src.feishu_client.FeishuClient._request')
    @patch('src.feishu_client.FeishuClient._get_table_config')
    def test_delete_record(self, mock_config, mock_request):
        """测试删除记录"""
        mock_config.return_value = ('app_token', 'table_id')
        mock_request.return_value = {}

        client = FeishuClient(app_id='test', app_secret='test')
        result = client.delete_record('holdings', 'rec_123')

        assert result == True

    @patch('src.feishu_client.FeishuClient._request')
    @patch('src.feishu_client.FeishuClient._get_table_config')
    def test_delete_record_failure(self, mock_config, mock_request):
        """测试删除记录失败"""
        mock_config.return_value = ('app_token', 'table_id')
        mock_request.side_effect = Exception('Record not found')

        client = FeishuClient(app_id='test', app_secret='test')
        result = client.delete_record('holdings', 'invalid_rec')

        assert result == False

    @patch('src.feishu_client.FeishuClient._request')
    @patch('src.feishu_client.FeishuClient._get_table_config')
    def test_get_record(self, mock_config, mock_request):
        """测试获取单条记录"""
        mock_config.return_value = ('app_token', 'table_id')
        mock_request.return_value = {
            'record_id': 'rec_123',
            'fields': {'asset_id': '000001', 'quantity': 100}
        }

        client = FeishuClient(app_id='test', app_secret='test')
        result = client.get_record('holdings', 'rec_123')

        assert result['record_id'] == 'rec_123'
        assert result['fields']['asset_id'] == '000001'

    @patch('src.feishu_client.FeishuClient._get_table_config')
    def test_get_record_not_configured(self, mock_config):
        """测试获取未配置表的记录"""
        mock_config.side_effect = ValueError('未配置表')

        client = FeishuClient(app_id='test', app_secret='test')
        result = client.get_record('holdings', 'rec_123')

        assert result is None


class TestFeishuClientBatchOperations:
    """测试飞书客户端批量操作"""

    @patch('src.feishu_client.FeishuClient._request')
    @patch('src.feishu_client.FeishuClient._get_table_config')
    def test_batch_create_records(self, mock_config, mock_request):
        """测试批量创建记录"""
        mock_config.return_value = ('app_token', 'table_id')
        mock_request.return_value = {
            'records': [
                {'record_id': 'rec1'},
                {'record_id': 'rec2'}
            ]
        }

        client = FeishuClient(app_id='test', app_secret='test')
        records = [
            {'fields': {'asset_id': '000001'}},
            {'fields': {'asset_id': '000002'}}
        ]
        results = client.batch_create_records('holdings', records)

        assert len(results) == 2

    @patch('src.feishu_client.FeishuClient._request')
    @patch('src.feishu_client.FeishuClient._get_table_config')
    def test_batch_create_records_empty(self, mock_config, mock_request):
        """测试批量创建空记录列表"""
        client = FeishuClient(app_id='test', app_secret='test')
        results = client.batch_create_records('holdings', [])

        assert results == []
        mock_request.assert_not_called()

    @patch('src.feishu_client.FeishuClient._request')
    @patch('src.feishu_client.FeishuClient._get_table_config')
    def test_batch_update_records(self, mock_config, mock_request):
        """测试批量更新记录"""
        mock_config.return_value = ('app_token', 'table_id')
        mock_request.return_value = {
            'records': [
                {'record_id': 'rec1', 'fields': {'quantity': 200}},
                {'record_id': 'rec2', 'fields': {'quantity': 300}}
            ]
        }

        client = FeishuClient(app_id='test', app_secret='test')
        records = [
            {'record_id': 'rec1', 'fields': {'quantity': 200}},
            {'record_id': 'rec2', 'fields': {'quantity': 300}}
        ]
        results = client.batch_update_records('holdings', records)

        assert len(results) == 2

    @patch('src.feishu_client.FeishuClient._request')
    @patch('src.feishu_client.FeishuClient._get_table_config')
    def test_batch_delete_records(self, mock_config, mock_request):
        """测试批量删除记录"""
        mock_config.return_value = ('app_token', 'table_id')
        mock_request.return_value = {
            'records': ['rec1', 'rec2']
        }

        client = FeishuClient(app_id='test', app_secret='test')
        deleted_count = client.batch_delete_records('holdings', ['rec1', 'rec2', 'rec3'])

        assert deleted_count == 2

    @patch('src.feishu_client.FeishuClient._request')
    @patch('src.feishu_client.FeishuClient._get_table_config')
    def test_batch_delete_records_empty(self, mock_config, mock_request):
        """测试批量删除空记录列表"""
        client = FeishuClient(app_id='test', app_secret='test')
        deleted_count = client.batch_delete_records('holdings', [])

        assert deleted_count == 0
        mock_request.assert_not_called()


class TestFeishuClientRateLimit:
    """测试飞书客户端限流"""

    @patch('src.feishu_client.time.time')
    @patch('src.feishu_client.time.sleep')
    def test_rate_limit(self, mock_sleep, mock_time):
        """测试限流控制"""
        client = FeishuClient(app_id='test', app_secret='test')

        # 第一次调用：足够间隔，不需要sleep
        mock_time.side_effect = [
            1000.1,   # now in _rate_limit
            1000.1,   # update _last_request_time
        ]
        client._last_request_time = 1000.0  # 上次请求在0.1秒前
        client._rate_limit()
        assert mock_sleep.call_count == 0  # 间隔>=0.06，不需要sleep

        # 第二次调用：间隔不足，需要sleep
        mock_time.side_effect = [
            1000.12,  # now in _rate_limit (距上次0.02秒)
            1000.16,  # update _last_request_time
        ]
        client._last_request_time = 1000.1  # 上次请求在0.02秒前
        client._rate_limit()
        mock_sleep.assert_called_once()  # 间隔<0.06，需要sleep
