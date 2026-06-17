import json

from midas_stage4.decision_schema import DecisionValidator, BusinessDecision


def test_validate_valid_json():
    payload = {
        "order_id": "123",
        "case_id": "variacion_significativa_mes_anterior",
        "service_type": "energia",
        "business_decision": "SIN_NOVEDAD",
        "classification_category": "NORMAL",
        "justification": "Variación explicada por histórico estable.",
        "confidence_score": 0.8,
        "requires_human_review": False,
        "recommended_action": None,
        "evidence": [],
        "rules_applied": ["consumo_actual_y_anterior_disponibles"],
        "data_quality_warnings": [],
        "model_version": None,
        "prompt_version": "vsma_prompt_v1",
    }
    decision = DecisionValidator().validate(json.dumps(payload), "123", "variacion_significativa_mes_anterior", "vsma_prompt_v1")
    assert decision.business_decision == BusinessDecision.SIN_NOVEDAD.value


def test_reject_invalid_decision():
    payload = {
        "order_id": "123",
        "case_id": "variacion_significativa_mes_anterior",
        "service_type": None,
        "business_decision": "CERRAR",
        "classification_category": "NORMAL",
        "justification": "x",
        "confidence_score": 0.5,
        "requires_human_review": True,
        "recommended_action": None,
        "evidence": [],
        "rules_applied": [],
        "data_quality_warnings": [],
        "model_version": None,
        "prompt_version": "vsma_prompt_v1",
    }
    try:
        DecisionValidator().validate(json.dumps(payload), "123", "variacion_significativa_mes_anterior", "vsma_prompt_v1")
    except ValueError as exc:
        assert "invalid business_decision" in str(exc)
    else:
        raise AssertionError("Expected ValueError")
