"""Registro y enrutamiento de casuísticas MIDAS."""

from .agent_registry import (
    AgentConfig,
    AgentRegistry,
    CASE_DIFERENCIA_ACUEDUCTO_ALCANTARILLADO,
    CASE_VARIACION_SIGNIFICATIVA,
)
from .case_router import CaseRouter

__all__ = [
    "AgentConfig",
    "AgentRegistry",
    "CaseRouter",
    "CASE_DIFERENCIA_ACUEDUCTO_ALCANTARILLADO",
    "CASE_VARIACION_SIGNIFICATIVA",
]
