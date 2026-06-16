import json

import pytest

from midas.agent_framework.contracts import TechnicalStatus
from midas.agent_framework.decision_validator import DecisionValidationError, DecisionValidator


def test_decision_validator_accepts_valid_payload():
    payload = {
        "order_id": "123",
        "case_id": "variacion_significativa_mes_anterior",
        "service_type": "ENERGIA",
        "business_decision": "REVISION_MANUAL",
        "justification": "Historial insuficiente para emitir una decisión automática confiable.",
        "confidence_score": 0.55,
        "requires_human_review": True,
        "cause_category": "HISTORIAL_INSUFICIENTE",
        "recommended_action": "Revisión del analista",
        "evidence": [],
        "rules_applied": ["historial_insuficiente"],
        "data_quality_warnings": ["sin_consumo_anterior"],
    }

    decision = DecisionValidator().validate(json.dumps(payload))

    assert decision.order_id == "123"
    assert decision.business_decision == "REVISION_MANUAL"
    assert decision.technical_status == TechnicalStatus.PROCESADA.value
    assert decision.requires_human_review is True


def test_decision_validator_rejects_invalid_decision():
    payload = {
        "order_id": "123",
        "case_id": "variacion_significativa_mes_anterior",
        "business_decision": "AJUSTAR",
        "justification": "Decisión inválida",
        "requires_human_review": False,
    }

    with pytest.raises(DecisionValidationError):
        DecisionValidator().validate(json.dumps(payload))


def test_decision_validator_builds_error_decision():
    decision = DecisionValidator().build_error_decision(
        order_id="123",
        case_id="variacion_significativa_mes_anterior",
        technical_status=TechnicalStatus.ERROR_ENDPOINT,
        error_code="HTTP_503",
        error_message="Endpoint no disponible",
    )

    assert decision.business_decision is None
    assert decision.technical_status == "ERROR_ENDPOINT"
    assert decision.requires_human_review is True
