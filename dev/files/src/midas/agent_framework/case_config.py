from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CaseConfig:
    case_id: str
    display_name: str
    enabled: bool
    activity_filters: list[str]
    supported_services: list[str]
    prompt_file: str
    tool_names: list[str]
    decision_catalog: list[str]
    manual_review_conditions: list[str]
    metadata: dict[str, Any]

    def matches_activity(self, activity: str | None) -> bool:
        if not activity:
            return False
        activity_upper = activity.strip().upper()
        return any(token.upper() in activity_upper for token in self.activity_filters)

    def supports_service(self, service_type: str | None) -> bool:
        if not service_type:
            return True
        service_upper = service_type.strip().upper()
        return any(token.upper() in service_upper for token in self.supported_services)


class CaseConfigLoader:
    """Carga configuración de casuísticas desde JSON.

    Ruta por defecto:
      dev/files/config/midas_cases.json

    En Databricks puede sobreescribirse con:
      MIDAS_CASES_CONFIG_PATH=/Workspace/.../midas_cases.json
    """

    def __init__(self, config_path: str | None = None):
        self.config_path = Path(
            config_path
            or os.environ.get("MIDAS_CASES_CONFIG_PATH", "")
            or self._default_config_path()
        )

    @staticmethod
    def _default_config_path() -> Path:
        current = Path(__file__).resolve()
        # .../src/midas/agent_framework/case_config.py -> .../dev/files/config
        for parent in current.parents:
            candidate = parent / "config" / "midas_cases.json"
            if candidate.exists():
                return candidate
            candidate = parent.parent / "config" / "midas_cases.json"
            if candidate.exists():
                return candidate
        return Path("config/midas_cases.json")

    def load_all(self) -> dict[str, CaseConfig]:
        if not self.config_path.exists():
            raise FileNotFoundError(f"No existe archivo de configuración de casuísticas: {self.config_path}")

        payload = json.loads(self.config_path.read_text(encoding="utf-8"))
        cases = payload.get("cases", [])
        result: dict[str, CaseConfig] = {}
        for item in cases:
            case = CaseConfig(
                case_id=item["case_id"],
                display_name=item.get("display_name", item["case_id"]),
                enabled=bool(item.get("enabled", True)),
                activity_filters=list(item.get("activity_filters", [])),
                supported_services=list(item.get("supported_services", [])),
                prompt_file=item["prompt_file"],
                tool_names=list(item.get("tool_names", [])),
                decision_catalog=list(item.get("decision_catalog", [])),
                manual_review_conditions=list(item.get("manual_review_conditions", [])),
                metadata=dict(item.get("metadata", {})),
            )
            result[case.case_id] = case
        return result

    def load_enabled(self) -> dict[str, CaseConfig]:
        return {case_id: cfg for case_id, cfg in self.load_all().items() if cfg.enabled}
