import unittest

from src.feishu.bitable_client import BitableClient, BitableClientError


class _FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def request(self, method, url, headers=None, **kwargs):
        self.calls.append((method, url, headers, kwargs))
        if not self._responses:
            raise AssertionError("no more fake responses")
        return self._responses.pop(0)


class _FakeFeishuClient:
    BASE_URL = "https://open.feishu.cn/open-apis"

    def __init__(self, responses):
        self.session = _FakeSession(responses)
        self.request_timeout = 3

    def _rate_limit(self):
        return None

    def _get_headers(self):
        return {"Authorization": "Bearer fake"}

    def _get_table_config(self, table_name):
        self._last_table_name = table_name
        return "app_from_table", "tbl_from_table"


class TestBitableClient(unittest.TestCase):
    def test_parse_bitable_url(self):
        app_token, table_id = BitableClient.parse_bitable_url(
            "https://xxx.feishu.cn/base/AbCdEf?table=tbl123&view=vew456"
        )
        self.assertEqual(app_token, "AbCdEf")
        self.assertEqual(table_id, "tbl123")

    def test_list_records_hides_page_token_from_caller(self):
        fake = _FakeFeishuClient(
            responses=[
                _FakeResponse(
                    200,
                    {
                        "code": 0,
                        "data": {
                            "items": [{"record_id": "rec1", "fields": {"x": 1}}],
                            "has_more": True,
                            "page_token": "p2",
                        },
                    },
                ),
                _FakeResponse(
                    200,
                    {
                        "code": 0,
                        "data": {
                            "items": [{"record_id": "rec2", "fields": {"x": 2}}],
                            "has_more": False,
                        },
                    },
                ),
            ]
        )
        client = BitableClient(app_token="app1", table_id="tbl1", feishu_client=fake)

        records = client.list_records(filter_str='CurrentValue.[account] = "lx"')

        self.assertEqual([r["record_id"] for r in records], ["rec1", "rec2"])
        self.assertEqual(len(fake.session.calls), 2)
        self.assertNotIn("page_token", fake.session.calls[0][3]["params"])
        self.assertEqual(fake.session.calls[1][3]["params"]["page_token"], "p2")

    def test_get_record_ret_error_contains_context(self):
        fake = _FakeFeishuClient(
            responses=[
                _FakeResponse(
                    200,
                    {
                        "ret": 10001,
                        "msg": "bad request",
                    },
                )
            ]
        )
        client = BitableClient(app_token="appX", table_id="tblY", feishu_client=fake)

        with self.assertRaises(BitableClientError) as ctx:
            client.get_record("rec_123")

        msg = str(ctx.exception)
        self.assertIn("app_token=appX", msg)
        self.assertIn("table_id=tblY", msg)
        self.assertIn("record_id=rec_123", msg)


if __name__ == "__main__":
    unittest.main()
