"""Framework Stage 4 para agentes MIDAS por casuística.

Este paquete agrega una capa modular y extensible sobre el agente actual:
- configuración por casuística;
- routing de órdenes hacia agentes;
- validación estricta de salida JSON;
- separación entre decisión funcional y estado técnico;
- soporte para auditoría e idempotencia en inferencia batch.
"""

from .case_config import CaseConfig, CaseConfigLoader
from .case_router import CaseRouter
from .contracts import AgentDecision, BusinessDecision, TechnicalStatus
from .decision_validator import DecisionValidationError, DecisionValidator

__all__ = [
    "AgentDecision",
    "BusinessDecision",
    "CaseConfig",
    "CaseConfigLoader",
    "CaseRouter",
    "DecisionValidationError",
    "DecisionValidator",
    "TechnicalStatus",
]
