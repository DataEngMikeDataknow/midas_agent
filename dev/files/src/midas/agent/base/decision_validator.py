from __future__ import annotations

from typing import Any


class DecisionValidationError(ValueError):
    """Error de validación de contrato de salida del agente."""


class DecisionValidator:
    """Valida la respuesta estructurada del agente.

    La validación se mantiene liviana para evitar dependencias externas en
    Databricks. Se validan campos requeridos, catálogos, tipo de confianza y la
    separación entre errores técnicos y decisiones funcionales.
    """

    REQUIRED_FIELDS = {
        "order_id",
        "case_id",
        "business_decision",
        "technical_status",
        "classification",
        "confidence_score",
        "requires_human_review",
        "justification",
        "evidence",
        "rules_applied",
        "data_quality_warnings",
    }

    BUSINESS_DECISIONS = {
        "SIN_NOVEDAD",
        "REQUIERE_AJUSTE",
        "REQUIERE_VISITA",
        "REVISION_MANUAL",
    }

    TECHNICAL_STATUSES = {
        "PROCESADA",
        "ERROR_DATOS",
        "ERROR_ENDPOINT",
        "ERROR_TOOL",
        "ERROR_VALIDACION_JSON",
        "NO_SOPORTADA",
        "PENDIENTE_REINTENTO",
    }

    CLASSIFICATIONS = {
        "NORMAL",
        "VARIACION_NO_JUSTIFICADA",
        "PNO",
        "ESTACIONALIDAD",
        "OBRA_NUEVA",
        "ERROR_LECTURA",
        "CONSTANTE_MAL_CONFIGURADA",
        "DATOS_INSUFICIENTES",
        "ERROR_TECNICO",
    }

    def __init__(self, expected_case_id: str | None = None):
        self.expected_case_id = expected_case_id

    def validate(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise DecisionValidationError("La respuesta del agente debe ser un objeto JSON.")

        missing = sorted(self.REQUIRED_FIELDS - set(payload))
        if missing:
            raise DecisionValidationError(f"Faltan campos obligatorios: {missing}")

        if self.expected_case_id and payload.get("case_id") != self.expected_case_id:
            raise DecisionValidationError(
                f"case_id inválido. Esperado={self.expected_case_id!r}, "
                f"recibido={payload.get('case_id')!r}"
            )

        technical_status = payload.get("technical_status")
        if technical_status not in self.TECHNICAL_STATUSES:
            raise DecisionValidationError(f"technical_status no permitido: {technical_status!r}")

        business_decision = payload.get("business_decision")
        if technical_status == "PROCESADA":
            if business_decision not in self.BUSINESS_DECISIONS:
                raise DecisionValidationError(
                    f"business_decision no permitida: {business_decision!r}"
                )
        else:
            if business_decision not in {None, ""}:
                raise DecisionValidationError(
                    "Si technical_status no es PROCESADA, business_decision debe ser null o vacío."
                )

        classification = payload.get("classification")
        if classification not in self.CLASSIFICATIONS:
            raise DecisionValidationError(f"classification no permitida: {classification!r}")

        confidence = payload.get("confidence_score")
        if not isinstance(confidence, (int, float)) or not (0 <= confidence <= 1):
            raise DecisionValidationError("confidence_score debe ser numérico entre 0 y 1.")

        if not isinstance(payload.get("requires_human_review"), bool):
            raise DecisionValidationError("requires_human_review debe ser booleano.")

        for array_field in ["evidence", "rules_applied", "data_quality_warnings"]:
            if not isinstance(payload.get(array_field), list):
                raise DecisionValidationError(f"{array_field} debe ser una lista.")

        if not isinstance(payload.get("justification"), str):
            raise DecisionValidationError("justification debe ser texto.")

        return payload
