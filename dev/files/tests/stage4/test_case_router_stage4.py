from midas.agent_framework.case_config import CaseConfig
from midas.agent_framework.case_router import CaseRouter


def test_case_router_resolves_variacion_significativa():
    case = CaseConfig(
        case_id="variacion_significativa_mes_anterior",
        display_name="Variación significativa contra mes anterior",
        enabled=True,
        activity_filters=["VARIACION SIGNIFICATIVA"],
        supported_services=["ENERGIA", "GAS", "AGUA"],
        prompt_file="variacion_significativa/prompt.md",
        tool_names=[],
        decision_catalog=[],
        manual_review_conditions=[],
        metadata={},
    )
    router = CaseRouter({case.case_id: case})

    result = router.resolve({"actividad": "Orden por VARIACION SIGNIFICATIVA", "servicio": "ENERGIA"})

    assert result == "variacion_significativa_mes_anterior"


def test_case_router_returns_unsupported_case_when_no_match():
    case = CaseConfig(
        case_id="variacion_significativa_mes_anterior",
        display_name="Variación significativa contra mes anterior",
        enabled=True,
        activity_filters=["VARIACION SIGNIFICATIVA"],
        supported_services=["ENERGIA"],
        prompt_file="variacion_significativa/prompt.md",
        tool_names=[],
        decision_catalog=[],
        manual_review_conditions=[],
        metadata={},
    )
    router = CaseRouter({case.case_id: case})

    result = router.resolve({"actividad": "DIFERENCIA ACUEDUCTO ALCANTARILLADO", "servicio": "ACUEDUCTO"})

    assert result == "unsupported_case"
