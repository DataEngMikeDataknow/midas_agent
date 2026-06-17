from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

from .config import Stage4Config


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_logs(spark, cfg: Stage4Config, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    normalized = []
    for row in rows:
        normalized.append(
            {
                "run_id": row.get("run_id"),
                "event_ts": row.get("event_ts"),
                "order_id": row.get("order_id"),
                "event_type": row.get("event_type"),
                "technical_status": row.get("technical_status"),
                "duration_ms": row.get("duration_ms"),
                "prompt_version": row.get("prompt_version"),
                "model_endpoint": row.get("model_endpoint"),
                "error_code": row.get("error_code"),
                "error_message": row.get("error_message"),
                "metadata_json": json.dumps(row.get("metadata") or {}, ensure_ascii=False, default=str),
            }
        )
    spark.createDataFrame(normalized).write.mode("append").saveAsTable(cfg.table("logs"))
