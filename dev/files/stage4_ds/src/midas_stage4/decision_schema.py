from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class BusinessDecision(str, Enum):
    SIN_NOVEDAD = "SIN_NOVEDAD"
    REQUIERE_AJUSTE = "REQUIERE_AJUSTE"
    REQUIERE_VISITA = "REQUIERE_VISITA"
    REVISION_MANUAL = "REVISION_MANUAL"


class ClassificationCategory(str, Enum):
    NORMAL = "NORMAL"
    CONSTANTE_MAL_CONFIGURADA = "CONSTANTE_MAL_CONFIGURADA"
    PNO = "PNO"
    ESTACIONAL = "ESTACIONAL"
    OBRA_NUEVA = "OBRA_NUEVA"
    ERROR_LECTURA = "ERROR_LECTURA"
    LECTURA_ESTIMADA_O_CORREGIDA = "LECTURA_ESTIMADA_O_CORREGIDA"
    CAMBIO_PATRON_CONSUMO = "CAMBIO_PATRON_CONSUMO"
    HISTORIAL_INSUFICIENTE = "HISTORIAL_INSUFICIENTE"
    DATOS_INCONSISTENTES = "DATOS_INCONSISTENTES"
    VARIACION_NO_EXPLICADA = "VARIACION_NO_EXPLICADA"
    RECLAMO_OBSERVACION = "RECLAMO_OBSERVACION"
    OTRO = "OTRO"


class TechnicalStatus(str, Enum):
    PROCESADA = "PROCESADA"
    ERROR_VALIDACION_JSON = "ERROR_VALIDACION_JSON"
    ERROR_ENDPOINT = "ERROR_ENDPOINT"
    ERROR_DATOS = "ERROR_DATOS"
    PENDIENTE_REINTENTO = "PENDIENTE_REINTENTO"


@dataclass
class Evidence:
    source: str
    field: str
    value: Any = None
    description: str = ""


@dataclass
class AgentDecision:
    order_id: str
    case_id: str
    service_type: Optional[str]
    business_decision: str
    classification_category: Optional[str]
    justification: str
    confidence_score: Optional[float]
    requires_human_review: bool
    recommended_action: Optional[str] = None
    evidence: List[Dict[str, Any]] = field(default_factory=list)
    rules_applied: List[str] = field(default_factory=list)
    data_quality_warnings: List[str] = field(default_factory=list)
    model_version: Optional[str] = None
    prompt_version: Optional[str] = None
    technical_status: str = TechnicalStatus.PROCESADA.value
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    raw_response: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"))


_JSON_BLOCK_RE = re.compile(r"\{(?:[^{}]|(?R))*\}", re.DOTALL) if False else None


def extract_json_object(text: str) -> Dict[str, Any]:
    """Extract the first valid JSON object from a model response.

    The prompt asks for raw JSON only, but this defensive parser prevents one bad
    response from breaking the whole Databricks batch.
    """
    if not isinstance(text, str) or not text.strip():
        raise ValueError("empty model response")
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        cleaned = cleaned.replace("json\n", "", 1).strip()
    try:
        value = json.loads(cleaned)
        if isinstance(value, dict):
            return value
    except json.JSONDecodeError:
        pass

    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start >= 0 and end > start:
        candidate = cleaned[start : end + 1]
        value = json.loads(candidate)
        if isinstance(value, dict):
            return value
    raise ValueError("model response does not contain a valid JSON object")


class DecisionValidator:
    REQUIRED_FIELDS = {
        "order_id",
        "case_id",
        "service_type",
        "business_decision",
        "classification_category",
        "justification",
        "confidence_score",
        "requires_human_review",
        "recommended_action",
        "evidence",
        "rules_applied",
        "data_quality_warnings",
        "model_version",
        "prompt_version",
    }

    def validate(self, raw_response: str, fallback_order_id: str, fallback_case_id: str, prompt_version: str) -> AgentDecision:
        payload = extract_json_object(raw_response)
        missing = self.REQUIRED_FIELDS - set(payload.keys())
        if missing:
            raise ValueError(f"missing fields in agent JSON: {sorted(missing)}")

        decision = str(payload.get("business_decision") or "").strip().upper()
        if decision not in {x.value for x in BusinessDecision}:
            raise ValueError(f"invalid business_decision: {decision!r}")

        category = payload.get("classification_category")
        if category is not None:
            category = str(category).strip().upper()
            if category not in {x.value for x in ClassificationCategory}:
                raise ValueError(f"invalid classification_category: {category!r}")

        confidence = payload.get("confidence_score")
        if confidence is not None:
            confidence = float(confidence)
            if confidence < 0 or confidence > 1:
                raise ValueError("confidence_score must be between 0 and 1")

        evidence = payload.get("evidence") or []
        if not isinstance(evidence, list):
            raise ValueError("evidence must be a list")
        rules = payload.get("rules_applied") or []
        warnings = payload.get("data_quality_warnings") or []
        if not isinstance(rules, list) or not isinstance(warnings, list):
            raise ValueError("rules_applied and data_quality_warnings must be lists")

        return AgentDecision(
            order_id=str(payload.get("order_id") or fallback_order_id),
            case_id=str(payload.get("case_id") or fallback_case_id),
            service_type=payload.get("service_type"),
            business_decision=decision,
            classification_category=category,
            justification=str(payload.get("justification") or "")[:2000],
            confidence_score=confidence,
            requires_human_review=bool(payload.get("requires_human_review")),
            recommended_action=payload.get("recommended_action"),
            evidence=evidence,
            rules_applied=[str(x) for x in rules],
            data_quality_warnings=[str(x) for x in warnings],
            model_version=payload.get("model_version"),
            prompt_version=str(payload.get("prompt_version") or prompt_version),
            raw_response=raw_response,
        )


def manual_review_decision(
    order_id: str,
    case_id: str,
    prompt_version: str,
    reason: str,
    status: str = TechnicalStatus.ERROR_DATOS.value,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
) -> AgentDecision:
    return AgentDecision(
        order_id=str(order_id),
        case_id=case_id,
        service_type=None,
        business_decision=BusinessDecision.REVISION_MANUAL.value,
        classification_category=ClassificationCategory.DATOS_INCONSISTENTES.value,
        justification=reason[:2000],
        confidence_score=0.0,
        requires_human_review=True,
        recommended_action="Enviar a revisión funcional por analista.",
        evidence=[],
        rules_applied=["fallback_revision_manual"],
        data_quality_warnings=[reason[:500]],
        model_version=None,
        prompt_version=prompt_version,
        technical_status=status,
        error_code=error_code,
        error_message=error_message,
    )
