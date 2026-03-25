"""Tencent batch quote helper.

Tencent quote API supports multiple codes per request:
  http://qt.gtimg.cn/q=sh600519,sz000651,hk00700,jj007722

This module:
- chunks codes to avoid overly long URLs
- parses response into a mapping {query_code: parts[]}

We keep it minimal (requests + stdlib) and avoid coupling to project models.
"""

from __future__ import annotations

from typing import Dict, List, Iterable, Tuple, Any
import re
import time


def chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def parse_multi_payload(text: str) -> Dict[str, List[str]]:
    """Parse Tencent multi-line payload.

    Each line is like: v_sh600519="...~...";
    """
    out: Dict[str, List[str]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r"^v_([a-z0-9\.]+)=\"([^\"]*)\";?", line, flags=re.IGNORECASE)
        if not m:
            continue
        code = m.group(1)
        payload = m.group(2)
        out[code] = payload.split('~') if payload is not None else []
    return out


def fetch_batch(
    session,
    query_codes: List[str],
    timeout: int = 8,
    chunk_size: int = 50,
) -> Tuple[Dict[str, List[str]], Dict[str, Any]]:
    """Fetch Tencent quotes in batches.

    Returns:
        (mapping query_code -> parts list, meta)

    meta fields:
        - requests: number of HTTP requests performed
        - chunk_size
        - timeout
        - elapsed_ms
        - requested_codes
        - returned_codes
    """
    started = time.time()
    results: Dict[str, List[str]] = {}
    if not query_codes:
        return results, {
            'requests': 0,
            'chunk_size': chunk_size,
            'timeout': timeout,
            'elapsed_ms': 0,
            'requested_codes': 0,
            'returned_codes': 0,
        }

    req_count = 0
    for batch in chunked(query_codes, chunk_size):
        url = "http://qt.gtimg.cn/q=" + ",".join(batch)
        resp = session.get(url, timeout=timeout)
        resp.encoding = 'gb2312'
        parsed = parse_multi_payload(resp.text)
        results.update(parsed)
        req_count += 1

    elapsed_ms = int((time.time() - started) * 1000)
    meta = {
        'requests': req_count,
        'chunk_size': chunk_size,
        'timeout': timeout,
        'elapsed_ms': elapsed_ms,
        'requested_codes': len(query_codes),
        'returned_codes': len(results),
    }
    return results, meta
