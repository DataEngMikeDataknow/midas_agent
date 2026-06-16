from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class AgentResult:
    """Contrato interno normalizado para resultados del agente.

    Este objeto separa explícitamente la decisión funcional del estado técnico de
    procesamiento. Esa separación es obligatoria para operación productiva: una
    falla de endpoint, datos o tools no debe convertirse en una decisión de
    negocio falsa.
    """

    order_id: str
    case_id: str
    service_type: str | None = None
    business_decision: str | None = None
    technical_status: str = "PROCESADA"
    classification: str | None = None
    confidence_score: float | None = None
    requires_human_review: bool = False
    justification: str = ""
    evidence: list[dict[str, Any]] = field(default_factory=list)
    rules_applied: list[str] = field(default_factory=list)
    data_quality_warnings: list[str] = field(default_factory=list)
    error_code: str | None = None
    error_message: str | None = None
    model_version: str | None = None
    prompt_version: str | None = None
    run_id: str | None = None
    created_at_utc: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def technical_error(
        cls,
        *,
        order_id: str,
        case_id: str,
        technical_status: str,
        error_code: str,
        error_message: str,
        run_id: str | None = None,
    ) -> "AgentResult":
        return cls(
            order_id=str(order_id),
            case_id=case_id,
            business_decision=None,
            technical_status=technical_status,
            classification="ERROR_TECNICO",
            confidence_score=0.0,
            requires_human_review=True,
            justification="La orden no pudo ser evaluada por una falla técnica.",
            error_code=error_code,
            error_message=error_message,
            run_id=run_id,
        )
