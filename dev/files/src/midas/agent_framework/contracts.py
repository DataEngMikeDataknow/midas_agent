from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class BusinessDecision(str, Enum):
    """Catálogo funcional común para todas las casuísticas.

    Este catálogo puede ajustarse cuando EPM cierre oficialmente las decisiones
    válidas. Mantenerlo centralizado evita que cada agente invente etiquetas.
    """

    SIN_NOVEDAD = "SIN_NOVEDAD"
    REQUIERE_AJUSTE = "REQUIERE_AJUSTE"
    REQUIERE_VISITA = "REQUIERE_VISITA"
    REVISION_MANUAL = "REVISION_MANUAL"


class TechnicalStatus(str, Enum):
    """Estado técnico del procesamiento.

    No debe mezclarse con la decisión de negocio. Una falla técnica no es una
    decisión funcional del agente.
    """

    PROCESADA = "PROCESADA"
    ERROR_DATOS = "ERROR_DATOS"
    ERROR_ENDPOINT = "ERROR_ENDPOINT"
    ERROR_TIMEOUT = "ERROR_TIMEOUT"
    ERROR_TOOL = "ERROR_TOOL"
    ERROR_VALIDACION_JSON = "ERROR_VALIDACION_JSON"
    NO_SOPORTADA = "NO_SOPORTADA"
    PENDIENTE_REINTENTO = "PENDIENTE_REINTENTO"


@dataclass(frozen=True)
class EvidenceItem:
    source: str
    field: str
    value: Any
    description: Optional[str] = None


@dataclass(frozen=True)
class AgentDecision:
    """Contrato de salida normalizado hacia Gold."""

    order_id: str
    case_id: str
    service_type: Optional[str]
    business_decision: Optional[str]
    justification: str
    confidence_score: Optional[float] = None
    requires_human_review: bool = False
    cause_category: Optional[str] = None
    recommended_action: Optional[str] = None
    evidence: list[dict[str, Any]] = field(default_factory=list)
    rules_applied: list[str] = field(default_factory=list)
    data_quality_warnings: list[str] = field(default_factory=list)
    technical_status: str = TechnicalStatus.PROCESADA.value
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    model_version: Optional[str] = None
    prompt_version: Optional[str] = None
    run_id: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
