from unittest.mock import patch

import pytest

from src.storage import create_storage


@patch('src.storage._feishu_healthcheck', side_effect=Exception('boom'))
@patch('src.storage.FeishuStorage')
def test_create_storage_auto_raises_when_feishu_unavailable(mock_feishu, mock_healthcheck):
    with pytest.raises(RuntimeError, match='拒绝静默回退 SQLite'):
        create_storage('auto')


@patch('src.storage.SQLiteStorage')
def test_create_storage_sqlite_explicit(mock_sqlite):
    create_storage('sqlite')
    mock_sqlite.assert_called_once()
