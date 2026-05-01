"""Thin Feishu Bitable wrapper: one entry, normalized params, explicit errors."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from src import config
from src.feishu_client import FeishuClient


class BitableClientError(RuntimeError):
    """Raised when Bitable API returns non-success or HTTP error."""


@dataclass(frozen=True)
class BitableTarget:
    app_token: str
    table_id: str


class BitableClient:
    """Thin wrapper for Feishu Bitable only."""

    def __init__(
        self,
        app_token: Optional[str] = None,
        table_id: Optional[str] = None,
        bitable_url: Optional[str] = None,
        table_name: Optional[str] = None,
        feishu_client: Optional[FeishuClient] = None,
    ):
        self._client = feishu_client or FeishuClient()
        self._target = self._resolve_target(
            app_token=app_token,
            table_id=table_id,
            bitable_url=bitable_url,
            table_name=table_name,
        )

    @property
    def app_token(self) -> str:
        return self._target.app_token

    @property
    def table_id(self) -> str:
        return self._target.table_id

    @classmethod
    def from_table_name(cls, table_name: str, feishu_client: Optional[FeishuClient] = None) -> "BitableClient":
        return cls(table_name=table_name, feishu_client=feishu_client)

    @staticmethod
    def parse_bitable_url(url: str) -> Tuple[str, str]:
        """Parse bitable URL (or app_token/table_id) to (app_token, table_id)."""
        if not url:
            raise ValueError("bitable url is empty")

        text = str(url).strip()
        if "/" in text and "http" not in text:
            app_token, table_id = text.split("/", 1)
            if app_token and table_id:
                return app_token.strip(), table_id.strip()

        parsed = urlparse(text)
        path_parts = [p for p in parsed.path.split("/") if p]

        app_token = ""
        table_id = ""

        if "base" in path_parts:
            idx = path_parts.index("base")
            if idx + 1 < len(path_parts):
                app_token = path_parts[idx + 1]

        query = parse_qs(parsed.query)
        if query.get("table"):
            table_id = query["table"][0]

        if (not app_token or not table_id) and len(path_parts) >= 2:
            if not app_token:
                app_token = path_parts[-2]
            if not table_id:
                table_id = path_parts[-1]

        if not app_token or not table_id:
            raise ValueError(f"unable to parse bitable url: {url}")

        return app_token, table_id

    def list_fields(self, page_size: int = 500) -> List[Dict[str, Any]]:
        endpoint = f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields"
        items: List[Dict[str, Any]] = []
        page_token: Optional[str] = None

        while True:
            params: Dict[str, Any] = {"page_size": page_size}
            if page_token:
                params["page_token"] = page_token

            data = self._request("GET", endpoint, op="list_fields", params=params)
            batch = data.get("items", []) if isinstance(data, dict) else []
            items.extend(batch)

            page_token = data.get("page_token") if isinstance(data, dict) else None
            if not page_token:
                break

        return items

    def list_records_iter(
        self,
        filter_str: Optional[str] = None,
        field_names: Optional[List[str]] = None,
        sort: Optional[List[str]] = None,
        view_id: Optional[str] = None,
        page_size: int = 500,
    ) -> Iterator[Dict[str, Any]]:
        endpoint = f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        page_token: Optional[str] = None

        while True:
            params: Dict[str, Any] = {"page_size": page_size}
            if page_token:
                params["page_token"] = page_token
            if filter_str:
                params["filter"] = filter_str
            if field_names:
                params["field_names"] = json.dumps(field_names, ensure_ascii=False)
            if sort:
                params["sort"] = json.dumps(sort, ensure_ascii=False)
            if view_id:
                params["view_id"] = view_id

            data = self._request("GET", endpoint, op="list_records", params=params)
            items = data.get("items", []) if isinstance(data, dict) else []
            for item in items:
                yield item

            has_more = bool(data.get("has_more")) if isinstance(data, dict) else False
            page_token = data.get("page_token") if isinstance(data, dict) else None
            if not has_more and not page_token:
                break
            if has_more and not page_token:
                break

    def list_records(
        self,
        filter_str: Optional[str] = None,
        field_names: Optional[List[str]] = None,
        sort: Optional[List[str]] = None,
        view_id: Optional[str] = None,
        page_size: int = 500,
    ) -> List[Dict[str, Any]]:
        return list(
            self.list_records_iter(
                filter_str=filter_str,
                field_names=field_names,
                sort=sort,
                view_id=view_id,
                page_size=page_size,
            )
        )

    def get_record(self, record_id: str) -> Dict[str, Any]:
        endpoint = f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/{record_id}"
        data = self._request("GET", endpoint, op="get_record", record_id=record_id)
        if isinstance(data, dict) and "record" in data:
            return data["record"]
        return data

    def create_record(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        endpoint = f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        data = self._request("POST", endpoint, op="create_record", json={"fields": fields})
        if isinstance(data, dict) and "record" in data:
            return data["record"]
        return data

    def update_record(self, record_id: str, fields: Dict[str, Any]) -> Dict[str, Any]:
        endpoint = f"/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/{record_id}"
        data = self._request(
            "PUT",
            endpoint,
            op="update_record",
            record_id=record_id,
            json={"fields": fields},
        )
        if isinstance(data, dict) and "record" in data:
            return data["record"]
        return data

    def _resolve_target(
        self,
        app_token: Optional[str],
        table_id: Optional[str],
        bitable_url: Optional[str],
        table_name: Optional[str],
    ) -> BitableTarget:
        if bitable_url:
            parsed_app_token, parsed_table_id = self.parse_bitable_url(bitable_url)
            return BitableTarget(app_token=parsed_app_token, table_id=parsed_table_id)

        if app_token and table_id:
            return BitableTarget(app_token=app_token, table_id=table_id)

        if table_name:
            a_token, t_id = self._client.get_table_config(table_name)
            return BitableTarget(app_token=a_token, table_id=t_id)

        cfg_url = (
            config.get("feishu.bitable.url")
            or config.get("feishu.bitable_url")
            or config.get("feishu.base_url")
        )
        if cfg_url:
            parsed_app_token, parsed_table_id = self.parse_bitable_url(cfg_url)
            return BitableTarget(app_token=parsed_app_token, table_id=parsed_table_id)

        cfg_app_token = config.get("feishu.bitable.app_token") or config.get("feishu.app_token")
        cfg_table_id = config.get("feishu.bitable.table_id") or config.get("feishu.table_id")
        if cfg_app_token and cfg_table_id:
            return BitableTarget(app_token=cfg_app_token, table_id=cfg_table_id)

        raise ValueError(
            "missing bitable target: provide (app_token + table_id), bitable_url, table_name, "
            "or config feishu.bitable.url / feishu.bitable.app_token+table_id"
        )

    def _request(
        self,
        method: str,
        endpoint: str,
        op: str,
        record_id: Optional[str] = None,
        _retry_count: int = 0,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        rate_limit = getattr(self._client, "rate_limit", None) or getattr(self._client, "_rate_limit", None)
        if callable(rate_limit):
            rate_limit()
        url = f"{self._client.BASE_URL}{endpoint}"
        get_headers = getattr(self._client, "get_headers", None) or getattr(self._client, "_get_headers", None)
        headers = get_headers() if callable(get_headers) else {}

        request_kwargs = dict(kwargs)
        request_kwargs.setdefault("timeout", self._client.request_timeout)

        response = self._client.session.request(method, url, headers=headers, **request_kwargs)

        if response.status_code == 429 and _retry_count < 3:
            time.sleep(1 * (2 ** _retry_count))
            return self._request(
                method,
                endpoint,
                op=op,
                record_id=record_id,
                _retry_count=_retry_count + 1,
                **request_kwargs,
            )

        if response.status_code < 200 or response.status_code >= 300:
            raise BitableClientError(
                self._format_error(
                    op=op,
                    record_id=record_id,
                    message=f"http_status={response.status_code}",
                )
            )

        try:
            payload = response.json()
        except ValueError:
            raise BitableClientError(
                self._format_error(op=op, record_id=record_id, message="response is not valid json")
            )

        if not isinstance(payload, dict):
            raise BitableClientError(
                self._format_error(op=op, record_id=record_id, message=f"unexpected payload type={type(payload)}")
            )

        api_code = payload.get("code")
        if api_code is None:
            api_code = payload.get("ret")

        if api_code is not None and api_code != 0:
            msg = payload.get("msg") or payload.get("errmsg") or "unknown api error"
            raise BitableClientError(
                self._format_error(op=op, record_id=record_id, message=f"api_code={api_code} msg={msg}")
            )

        data = payload.get("data")
        if isinstance(data, dict):
            return data
        if data is None:
            return payload
        return {"data": data}

    def _format_error(self, op: str, message: str, record_id: Optional[str] = None) -> str:
        parts = [
            f"bitable op={op} failed",
            f"app_token={self.app_token}",
            f"table_id={self.table_id}",
        ]
        if record_id:
            parts.append(f"record_id={record_id}")
        parts.append(message)
        return " | ".join(parts)
