from __future__ import annotations

import hashlib
import json
from typing import Any


def wal_checksum(*, lsn: int, op: str, key: str, value: Any | None, tx_time: float) -> str:
    payload: dict[str, Any] = {
        "lsn": int(lsn),
        "op": op,
        "key": key,
        "tx_time": float(tx_time),
    }
    if op != "DELETE":
        payload["value"] = value
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
