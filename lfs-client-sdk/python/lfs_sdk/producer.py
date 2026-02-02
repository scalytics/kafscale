from __future__ import annotations

import requests
from typing import Dict, Optional


def produce_lfs(endpoint: str, topic: str, payload: bytes, key: Optional[bytes] = None, headers: Optional[Dict[str, str]] = None) -> dict:
    out_headers = {
        "X-Kafka-Topic": topic,
    }
    if key is not None:
        out_headers["X-Kafka-Key"] = key.decode("utf-8", errors="ignore")
    if headers:
        out_headers.update(headers)
    resp = requests.post(endpoint, data=payload, headers=out_headers, timeout=300)
    if resp.status_code < 200 or resp.status_code >= 300:
        raise RuntimeError(f"produce failed: {resp.status_code} {resp.text}")
    return resp.json()
