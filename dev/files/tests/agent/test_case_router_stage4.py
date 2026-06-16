from midas.agent.registry.agent_registry import (
    CASE_DIFERENCIA_ACUEDUCTO_ALCANTARILLADO,
    CASE_VARIACION_SIGNIFICATIVA,
)
from midas.agent.registry.case_router import CaseRouter


def test_route_variacion_significativa():
    router = CaseRouter()
    case_id = router.resolve({"actividad": "Variación significativa contra mes anterior"})
    assert case_id == CASE_VARIACION_SIGNIFICATIVA


def test_route_legacy_diferencia_acueducto_alcantarillado():
    router = CaseRouter()
    case_id = router.resolve({"actividad": "1019 - DIFERENCIA ACUEDUCTO Y ALCANTARILLADO"})
    assert case_id == CASE_DIFERENCIA_ACUEDUCTO_ALCANTARILLADO


def test_unknown_activity_returns_none():
    router = CaseRouter()
    assert router.resolve({"actividad": "Otra actividad"}) is None
