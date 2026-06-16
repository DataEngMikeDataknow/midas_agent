from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


class InferenceLogger:
    """Helper liviano para generar registros de auditoría por inferencia."""

    @staticmethod
    def build_record(
        *,
        run_id: str,
        order_id: str,
        case_id: str,
        technical_status: str,
        latency_ms: int | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "order_id": str(order_id),
            "case_id": case_id,
            "technical_status": technical_status,
            "latency_ms": latency_ms,
            "error_code": error_code,
            "error_message": error_message,
            "logged_at_utc": datetime.now(timezone.utc).isoformat(),
            "extra": extra or {},
        }
