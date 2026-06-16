import pytest

from midas.agent.base.decision_validator import DecisionValidationError, DecisionValidator
from midas.agent.registry.agent_registry import CASE_VARIACION_SIGNIFICATIVA


def valid_payload():
    return {
        "order_id": "1",
        "case_id": CASE_VARIACION_SIGNIFICATIVA,
        "service_type": "agua",
        "business_decision": "SIN_NOVEDAD",
        "technical_status": "PROCESADA",
        "classification": "NORMAL",
        "confidence_score": 0.82,
        "requires_human_review": False,
        "justification": "Consumo dentro del patrón histórico.",
        "evidence": [],
        "rules_applied": [],
        "data_quality_warnings": [],
    }


def test_validate_valid_payload():
    validator = DecisionValidator(expected_case_id=CASE_VARIACION_SIGNIFICATIVA)
    assert validator.validate(valid_payload())["business_decision"] == "SIN_NOVEDAD"


def test_reject_business_decision_when_technical_error():
    payload = valid_payload()
    payload["technical_status"] = "ERROR_ENDPOINT"
    with pytest.raises(DecisionValidationError):
        DecisionValidator(expected_case_id=CASE_VARIACION_SIGNIFICATIVA).validate(payload)


def test_accept_technical_error_with_null_business_decision():
    payload = valid_payload()
    payload["technical_status"] = "ERROR_ENDPOINT"
    payload["business_decision"] = None
    payload["classification"] = "ERROR_TECNICO"
    assert DecisionValidator(expected_case_id=CASE_VARIACION_SIGNIFICATIVA).validate(payload)["technical_status"] == "ERROR_ENDPOINT"
