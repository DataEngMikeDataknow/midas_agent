from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

CASE_VARIACION_SIGNIFICATIVA = "variacion_significativa_mes_anterior"
CASE_DIFERENCIA_ACUEDUCTO_ALCANTARILLADO = "diferencia_acueducto_alcantarillado"


@dataclass(frozen=True)
class AgentConfig:
    case_id: str
    name: str
    prompt_path: Path
    schema_path: Path
    tool_function_names: tuple[str, ...]
    activity_keywords: tuple[str, ...] = ()


class AgentRegistry:
    """Registro central de agentes/casuísticas.

    La etapa 4 agrega la casuística de variación significativa sin modificar el
    agente legacy. Futuras casuísticas se registran aquí con sus prompts, schema
    y tools.
    """

    def __init__(self, base_dir: str | Path | None = None):
        if base_dir is None:
            # .../src/midas/agent/registry/agent_registry.py -> .../src/midas/agent
            base_dir = Path(__file__).resolve().parents[1]
        self.base_dir = Path(base_dir)
        self._configs = self._build_default_configs()

    def _build_default_configs(self) -> dict[str, AgentConfig]:
        case_root = self.base_dir / "cases" / "variacion_significativa"
        return {
            CASE_VARIACION_SIGNIFICATIVA: AgentConfig(
                case_id=CASE_VARIACION_SIGNIFICATIVA,
                name="Agente de variación significativa contra mes anterior",
                prompt_path=case_root / "prompt.md",
                schema_path=case_root / "schema.json",
                tool_function_names=(
                    "get_contexto_variacion_significativa",
                    "get_historial_consumo_producto",
                    "get_ordenes_calidad_previas_producto",
                ),
                activity_keywords=(
                    "VARIACION",
                    "VARIACIÓN",
                    "MES ANTERIOR",
                    "CONSUMO",
                    "SIGNIFICATIVA",
                ),
            ),
            CASE_DIFERENCIA_ACUEDUCTO_ALCANTARILLADO: AgentConfig(
                case_id=CASE_DIFERENCIA_ACUEDUCTO_ALCANTARILLADO,
                name="Agente legacy diferencia acueducto alcantarillado",
                prompt_path=self.base_dir / "prompts.py",
                schema_path=case_root / "schema.json",
                tool_function_names=("get_hist_fact", "get_ordenes_critica"),
                activity_keywords=("DIFERENCIA", "ACUEDUCTO", "ALCANTARILLADO"),
            ),
        }

    def get(self, case_id: str) -> AgentConfig:
        try:
            return self._configs[case_id]
        except KeyError as exc:
            raise ValueError(f"Casuística no registrada: {case_id}") from exc

    def has_case(self, case_id: str) -> bool:
        return case_id in self._configs

    def list_cases(self) -> list[str]:
        return sorted(self._configs)
