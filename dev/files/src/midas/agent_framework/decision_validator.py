from __future__ import annotations

import json
import re
from typing import Any

from .contracts import AgentDecision, BusinessDecision, TechnicalStatus


class DecisionValidationError(ValueError):
    pass


class DecisionValidator:
    """Normaliza y valida la salida JSON del agente.

    Objetivos:
    - impedir texto libre no parseable en Gold;
    - separar decisión funcional de estado técnico;
    - estandarizar campos obligatorios para todas las casuísticas.
    """

    REQUIRED_FIELDS = {
        "order_id",
        "case_id",
        "business_decision",
        "justification",
        "requires_human_review",
    }

    def __init__(self, allowed_decisions: list[str] | None = None):
        self.allowed_decisions = set(allowed_decisions or [item.value for item in BusinessDecision])

    @staticmethod
    def _strip_code_fence(raw_output: str) -> str:
        cleaned = (raw_output or "").strip()
        cleaned = re.sub(r"^```json\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"^```\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
        return cleaned.strip()

    def parse(self, raw_output: str) -> dict[str, Any]:
        cleaned = self._strip_code_fence(raw_output)
        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise DecisionValidationError(f"La salida del agente no es JSON válido: {exc}") from exc
        if not isinstance(parsed, dict):
            raise DecisionValidationError("La salida del agente debe ser un objeto JSON.")
        return parsed

    def validate(self, raw_output: str, fallback_order_id: str | None = None, fallback_case_id: str | None = None) -> AgentDecision:
        payload = self.parse(raw_output)

        missing = self.REQUIRED_FIELDS - set(payload.keys())
        if missing:
            raise DecisionValidationError(f"Faltan campos obligatorios en salida del agente: {sorted(missing)}")

        business_decision = str(payload.get("business_decision", "")).strip().upper()
        if business_decision not in self.allowed_decisions:
            raise DecisionValidationError(
                f"Decisión funcional inválida: {business_decision!r}. Permitidas: {sorted(self.allowed_decisions)}"
            )

        confidence = payload.get("confidence_score")
        if confidence is not None:
            try:
                confidence = float(confidence)
            except Exception as exc:
                raise DecisionValidationError("confidence_score debe ser numérico o null.") from exc
            if not 0.0 <= confidence <= 1.0:
                raise DecisionValidationError("confidence_score debe estar entre 0 y 1.")

        return AgentDecision(
            order_id=str(payload.get("order_id") or fallback_order_id or ""),
            case_id=str(payload.get("case_id") or fallback_case_id or ""),
            service_type=payload.get("service_type"),
            business_decision=business_decision,
            justification=str(payload.get("justification") or "").strip(),
            confidence_score=confidence,
            requires_human_review=bool(payload.get("requires_human_review", False)),
            cause_category=payload.get("cause_category"),
            recommended_action=payload.get("recommended_action"),
            evidence=list(payload.get("evidence") or []),
            rules_applied=list(payload.get("rules_applied") or []),
            data_quality_warnings=list(payload.get("data_quality_warnings") or []),
            technical_status=TechnicalStatus.PROCESADA.value,
            model_version=payload.get("model_version"),
            prompt_version=payload.get("prompt_version"),
        )

    def build_error_decision(
        self,
        order_id: str,
        case_id: str,
        technical_status: TechnicalStatus,
        error_code: str,
        error_message: str,
    ) -> AgentDecision:
        return AgentDecision(
            order_id=str(order_id),
            case_id=case_id,
            service_type=None,
            business_decision=None,
            justification="No se emitió decisión funcional por error técnico o de datos.",
            requires_human_review=True,
            technical_status=technical_status.value,
            error_code=error_code,
            error_message=error_message[:4000],
        )
