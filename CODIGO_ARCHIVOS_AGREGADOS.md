# Código de archivos agregados - MIDAS Etapa 4

Este documento contiene el código de los archivos nuevos incluidos en el ZIP. Las rutas son relativas a la raíz del repositorio.

## dev/files/agent/stage4_agent.py

```python
import json
import os
import re
import sys
import warnings
from pathlib import Path
from typing import Any, Callable, Generator, Optional

import mlflow
from databricks.sdk import WorkspaceClient
from databricks_openai import UCFunctionToolkit
from mlflow.entities import SpanType
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    output_to_responses_items_stream,
    to_chat_completions_input,
)
from openai import OpenAI
from pydantic import BaseModel
from unitycatalog.ai.core.base import get_uc_function_client

# Permite importar midas.* cuando MLflow valida el modelo desde /Workspace/.../agent
try:
    _agent_dir = Path(__file__).resolve().parent
    _bundle_root = _agent_dir.parent
    _src_path = _bundle_root / "src"
    if str(_src_path) not in sys.path:
        sys.path.insert(0, str(_src_path))
except Exception:
    pass

try:
    from midas.agent_framework.prompt_loader import PromptLoader
except Exception:
    PromptLoader = None  # type: ignore


LLM_ENDPOINT_NAME = os.environ.get("MIDAS_LLM_ENDPOINT_NAME", "databricks-gpt-oss-120b")
CATALOG_NAME = os.environ.get("MIDAS_CATALOG", "epm_datalabs_catalog_dllo")
SCHEMA_NAME = os.environ.get("MIDAS_SCHEMA", "facturacion")
LLM_TIMEOUT_SECONDS = int(os.environ.get("MIDAS_LLM_TIMEOUT_SECONDS", "60"))
AGENT_MAX_ITERATIONS = int(os.environ.get("MIDAS_AGENT_MAX_ITERATIONS", "10"))
STAGE4_CASE_ID = os.environ.get("MIDAS_STAGE4_CASE_ID", "variacion_significativa_mes_anterior")
STAGE4_PROMPT_FILE = os.environ.get("MIDAS_STAGE4_PROMPT_FILE", "variacion_significativa/prompt.md")
STAGE4_TOOL_NAMES_RAW = os.environ.get(
    "MIDAS_STAGE4_TOOL_NAMES",
    "get_contexto_variacion_significativa,get_historial_consumo_producto,get_contexto_observaciones_calidad",
)


def _load_system_prompt() -> str:
    if PromptLoader is not None:
        try:
            return PromptLoader().load(STAGE4_PROMPT_FILE)
        except Exception:
            pass

    # Fallback mínimo para que el modelo siga funcionando si el prompt markdown no fue empacado.
    return f"""
Eres un Analista Senior de Calidad de Facturación para EPM.
Analiza la casuística {STAGE4_CASE_ID} usando las herramientas disponibles.
Devuelve únicamente JSON válido con los campos: order_id, case_id, service_type,
business_decision, justification, confidence_score, requires_human_review,
cause_category, recommended_action, evidence, rules_applied y data_quality_warnings.
""".strip()


SYSTEM_PROMPT = _load_system_prompt()


class ToolInfo(BaseModel):
    name: str
    spec: dict
    exec_fn: Callable


def _parse_tool_names(raw_value: str) -> list[str]:
    raw_value = (raw_value or "").strip()
    if not raw_value:
        return []
    try:
        parsed = json.loads(raw_value)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except Exception:
        pass
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def _to_full_uc_name(tool_name: str) -> str:
    if tool_name.count(".") == 2:
        return tool_name
    if tool_name.count(".") == 1:
        return f"{CATALOG_NAME}.{tool_name}"
    return f"{CATALOG_NAME}.{SCHEMA_NAME}.{tool_name}"


def create_tool_info(tool_spec, exec_fn_param: Optional[Callable] = None):
    tool_spec["function"].pop("strict", None)
    tool_name = tool_spec["function"]["name"]

    if tool_name.startswith("on__"):
        tool_name = tool_name.replace("on__", f"{SCHEMA_NAME}__", 1)
        tool_spec["function"]["name"] = tool_name

    udf_name = tool_name.replace("__", ".")
    udf_name_parts = udf_name.split(".")

    if len(udf_name_parts) == 2:
        udf_name = f"{CATALOG_NAME}.{udf_name}"
    elif len(udf_name_parts) != 3:
        raise ValueError(
            f"Nombre de herramienta inválido '{tool_name}'. Se convirtió en '{udf_name}'."
        )

    def _coerce_uc_value(value: Any) -> Any:
        if isinstance(value, str) and re.fullmatch(r"-?\d+", value):
            return int(value)
        if isinstance(value, str) and re.fullmatch(r"-?\d+\.\d+", value):
            return float(value)
        if isinstance(value, list):
            return [_coerce_uc_value(item) for item in value]
        if isinstance(value, dict):
            return {key: _coerce_uc_value(val) for key, val in value.items()}
        return value

    def exec_fn(**kwargs):
        function_result = uc_function_client.execute_function(
            udf_name, {key: _coerce_uc_value(val) for key, val in kwargs.items()}
        )
        if function_result.error is not None:
            return function_result.error
        return function_result.value

    return ToolInfo(name=tool_name, spec=tool_spec, exec_fn=exec_fn_param or exec_fn)


UC_TOOL_NAMES = [_to_full_uc_name(name) for name in _parse_tool_names(STAGE4_TOOL_NAMES_RAW)]
uc_toolkit = UCFunctionToolkit(function_names=UC_TOOL_NAMES)
uc_function_client = get_uc_function_client()
TOOL_INFOS = [create_tool_info(tool_spec) for tool_spec in uc_toolkit.tools]


class Stage4ToolCallingAgent(ResponsesAgent):
    """Agente Stage 4 configurable por casuística y tools Unity Catalog."""

    def __init__(self, llm_endpoint: str, tools: list[ToolInfo]):
        self.llm_endpoint = llm_endpoint
        self.workspace_client = WorkspaceClient()
        self._model_serving_client: Optional[OpenAI] = None
        self._tools_dict = {tool.name: tool for tool in tools}

    @property
    def model_serving_client(self) -> OpenAI:
        if self._model_serving_client is None:
            try:
                self._model_serving_client = self.workspace_client.serving_endpoints.get_open_ai_client()
            except AttributeError:
                cfg = self.workspace_client.config
                try:
                    auth_headers = cfg.authenticate()
                except TypeError:
                    auth_headers = {}
                    cfg.authenticate(auth_headers)
                token = auth_headers.get("Authorization", "Bearer ").split()[-1]
                self._model_serving_client = OpenAI(api_key=token, base_url=f"{cfg.host}/serving-endpoints")
        return self._model_serving_client

    def get_tool_specs(self) -> list[dict]:
        return [tool_info.spec for tool_info in self._tools_dict.values()]

    @mlflow.trace(span_type=SpanType.TOOL)
    def execute_tool(self, tool_name: str, args: dict) -> Any:
        return self._tools_dict[tool_name].exec_fn(**args)

    def call_llm(self, messages: list[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="PydanticSerializationUnexpectedValue")
            for chunk in self.model_serving_client.chat.completions.create(
                model=self.llm_endpoint,
                messages=to_chat_completions_input(messages),
                tools=self.get_tool_specs(),
                stream=True,
                temperature=0.0,
                timeout=LLM_TIMEOUT_SECONDS,
            ):
                chunk_dict = chunk.to_dict()
                if len(chunk_dict.get("choices", [])) > 0:
                    yield chunk_dict

    def handle_tool_call(self, tool_call: dict[str, Any], messages: list[dict[str, Any]]) -> ResponsesAgentStreamEvent:
        arguments_str = tool_call.get("arguments")
        args = json.loads(arguments_str) if arguments_str else {}
        result = str(self.execute_tool(tool_name=tool_call["name"], args=args))
        tool_call_output = self.create_function_call_output_item(tool_call["call_id"], result)
        messages.append(tool_call_output)
        return ResponsesAgentStreamEvent(type="response.output_item.done", item=tool_call_output)

    def call_and_run_tools(
        self,
        messages: list[dict[str, Any]],
        max_iter: int = 10,
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        for _ in range(max_iter):
            last_msg = messages[-1]
            if last_msg.get("role") == "assistant":
                return
            if last_msg.get("type") == "function_call":
                yield self.handle_tool_call(last_msg, messages)
            else:
                yield from output_to_responses_items_stream(chunks=self.call_llm(messages), aggregator=messages)
        raise RuntimeError(f"Agent max iterations reached ({max_iter}). Stopping.")

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [
            event.item
            for event in self.predict_stream(request)
            if event.type == "response.output_item.done"
        ]
        return ResponsesAgentResponse(output=outputs, custom_outputs=request.custom_inputs)

    def predict_stream(self, request: ResponsesAgentRequest) -> Generator[ResponsesAgentStreamEvent, None, None]:
        messages = to_chat_completions_input([item.model_dump() for item in request.input])
        if SYSTEM_PROMPT:
            messages.insert(0, {"role": "system", "content": SYSTEM_PROMPT})
        yield from self.call_and_run_tools(messages=messages, max_iter=AGENT_MAX_ITERATIONS)


try:
    mlflow.openai.autolog()
except Exception:
    pass

AGENT = Stage4ToolCallingAgent(llm_endpoint=LLM_ENDPOINT_NAME, tools=TOOL_INFOS)
mlflow.models.set_model(AGENT)

```

## dev/files/config/midas_cases.json

```json
{
  "version": "stage4.0.1",
  "default_case_id": "variacion_significativa_mes_anterior",
  "cases": [
    {
      "case_id": "variacion_significativa_mes_anterior",
      "display_name": "Variación significativa contra el mes anterior",
      "enabled": true,
      "activity_filters": [
        "VARIACION SIGNIFICATIVA",
        "VARIACIÓN SIGNIFICATIVA",
        "VARIACION MES ANTERIOR",
        "VARIACIÓN MES ANTERIOR"
      ],
      "supported_services": [
        "AGUA",
        "ACUEDUCTO",
        "ENERGIA",
        "ENERGÍA",
        "GAS"
      ],
      "prompt_file": "variacion_significativa/prompt.md",
      "tool_names": [
        "get_contexto_variacion_significativa",
        "get_historial_consumo_producto",
        "get_contexto_observaciones_calidad"
      ],
      "decision_catalog": [
        "SIN_NOVEDAD",
        "REQUIERE_AJUSTE",
        "REQUIERE_VISITA",
        "REVISION_MANUAL"
      ],
      "manual_review_conditions": [
        "historial_insuficiente",
        "lecturas_inconsistentes",
        "servicio_no_identificado",
        "orden_duplicada",
        "contexto_no_disponible",
        "diferencia_no_explicada"
      ],
      "metadata": {
        "owner_role": "Científico de datos",
        "stage": "Etapa 4 - Construcción del nuevo agente inteligente desde cero",
        "prompt_version": "vsma_prompt_v1",
        "output_schema_version": "vsma_schema_v1"
      }
    }
  ]
}

```

## dev/files/config/midas_decision_catalog.json

```json
{
  "version": "1.0.0",
  "business_decisions": [
    {
      "code": "SIN_NOVEDAD",
      "description": "La orden no presenta evidencia suficiente para recomendar ajuste o visita."
    },
    {
      "code": "REQUIERE_AJUSTE",
      "description": "La evidencia disponible sugiere inconsistencia de facturación que debe ser ajustada."
    },
    {
      "code": "REQUIERE_VISITA",
      "description": "La evidencia sugiere necesidad de validación en campo o inspección técnica."
    },
    {
      "code": "REVISION_MANUAL",
      "description": "La información es insuficiente, contradictoria o no permite una decisión automática confiable."
    }
  ],
  "technical_statuses": [
    "PROCESADA",
    "ERROR_DATOS",
    "ERROR_ENDPOINT",
    "ERROR_TIMEOUT",
    "ERROR_TOOL",
    "ERROR_VALIDACION_JSON",
    "NO_SOPORTADA",
    "PENDIENTE_REINTENTO"
  ]
}

```

## dev/files/docs/10_ETAPA_4_AGENTE_INTELIGENTE_DESDE_CERO.md

```markdown
# Etapa 4 - Construcción del nuevo agente inteligente desde cero

## Objetivo

Agregar una capa modular para construir el agente de la casuística **variación significativa contra el mes anterior**, sin romper el agente actual de diferencia acueducto-alcantarillado.

La propuesta del proyecto indica que el sistema debe evolucionar hacia un enfoque multiagente/multi-casuística en Databricks/Mosaic AI, con resultado final en tablas Databricks y sin comportamiento conversacional para usuarios funcionales.

## Qué agrega esta etapa

1. Configuración por casuística.
2. Prompt versionable en markdown.
3. SQL Functions específicas para el contexto de variación significativa.
4. Agente Stage 4 configurable por tools.
5. Inferencia batch resiliente por orden.
6. Separación entre decisión funcional y estado técnico.
7. Validación JSON estricta antes de escribir en Gold.
8. Tests unitarios iniciales.

## Decisión de arquitectura

No se recomienda iniciar con muchos modelos independientes. Se recomienda iniciar con:

- un modelo base aprobado por EPM;
- múltiples agentes/casuísticas configurables;
- tools y prompts especializados;
- una salida Gold común y auditable.

Esto reduce costo, complejidad operativa y riesgo de gobierno.

## Componentes nuevos

| Componente | Ruta | Propósito |
|---|---|---|
| Framework de agentes | `src/midas/agent_framework/` | Contratos, routing, config y validación |
| Casuística VSMA | `src/midas/agents/variacion_significativa/` | Prompt, reglas, schema y SQL tools |
| Agente MLflow | `agent/stage4_agent.py` | Agente ResponsesAgent configurable |
| SQL Functions Stage 4 | `src/midas/main_tools_stage4.py` | Crea tools UC para el nuevo agente |
| Deploy Stage 4 | `src/midas/main_deploy_stage4.py` | Registra y despliega el agente nuevo |
| Inferencia Stage 4 | `src/midas/main_inference_stage4.py` | Ejecuta batch inference resiliente |
| Snippet DAB | `resources/jobs/stage4_databricks_job_snippet.yml` | Fragmento para integrar al `databricks.yml` |

## Contrato Gold recomendado

La tabla `midas_predicciones_agente_stage4_gold` debería conservar, como mínimo:

- `id_orden`
- `servicio_suscrito`
- `contrato`
- `ciclo`
- `actividad`
- `servicio`
- `case_id`
- `decision_payload`
- `business_decision`
- `technical_status`
- `requires_human_review`
- `confidence_score`
- `cause_category`
- `error_code`
- `error_message`
- `run_id`
- `fecha_inferencia`

## Manejo de errores

Una orden fallida no debe detener el lote completo.

- Si falla una orden individual: se marca con `technical_status` de error y `requires_human_review=true`.
- Si falla el endpoint: se reintenta y, si persiste, queda `PENDIENTE_REINTENTO` o `ERROR_ENDPOINT`.
- Si el JSON del agente no es válido: queda `ERROR_VALIDACION_JSON`.
- Una falla técnica no debe convertirse en decisión funcional.

## Pendientes funcionales con EPM

Antes de endurecer la etapa 4, se debe confirmar:

1. Nombre/código exacto de la actividad para variación significativa.
2. Umbrales oficiales de variación por servicio.
3. Catálogo final de decisiones de negocio.
4. Criterios de visita, ajuste, sin novedad y revisión manual.
5. Estructura final de la tabla Gold.
6. Dataset histórico validado por analista para medir desempeño.
7. Criterio de aceptación de la POC.

```

## dev/files/resources/jobs/stage4_databricks_job_snippet.yml

```yaml
# Fragmento para integrar manualmente en dev/files/databricks.yml.
# No se incluye automáticamente para evitar sobrescribir el bundle actual.
# Recomendación: agregar como nuevo job o adaptar el job midas_agent_deploy_ops existente.

midas_stage4_agent_deploy_ops:
  name: midas_stage4_agent_deploy_ops
  permissions:
    - level: CAN_MANAGE
      service_principal_name: ${var.pipeline_sp}
  job_clusters:
    - job_cluster_key: midas_cluster
      new_cluster:
        spark_version: "16.4.x-scala2.12"
        node_type_id: Standard_DS4_v2
        data_security_mode: USER_ISOLATION
        num_workers: 2
  tasks:
    - task_key: create_stage4_sql_functions
      job_cluster_key: midas_cluster
      spark_python_task:
        python_file: ./src/midas/main_tools_stage4.py
        parameters:
          - "--catalog"
          - "${var.catalog_name}"
          - "--schema"
          - "${var.schema_name}"
          - "--pipeline_sp"
          - "${var.pipeline_sp}"
          - "--grant_account_users"
          - "false"

    - task_key: deploy_stage4_agent_version
      depends_on:
        - task_key: create_stage4_sql_functions
      job_cluster_key: midas_cluster
      spark_python_task:
        python_file: ./src/midas/main_deploy_stage4.py
        parameters:
          - "--catalog"
          - "${var.catalog_name}"
          - "--schema"
          - "${var.schema_name}"
          - "--model_name"
          - "midas_stage4_agent_model"
          - "--endpoint_name"
          - "${var.endpoint_name}"
          - "--experiment_path"
          - "${var.experiment_path}"
          - "--endpoint_workload_size"
          - "${var.endpoint_workload_size}"
          - "--endpoint_scale_to_zero_enabled"
          - "${var.endpoint_scale_to_zero_enabled}"
          - "--agent_llm_timeout_seconds"
          - "${var.agent_llm_timeout_seconds}"
          - "--agent_max_iterations"
          - "${var.agent_max_iterations}"
          - "--pipeline_sp"
          - "${var.pipeline_sp}"
          - "--case_id"
          - "variacion_significativa_mes_anterior"
          - "--prompt_file"
          - "variacion_significativa/prompt.md"

midas_stage4_agent_inference:
  name: midas_stage4_agent_inference
  permissions:
    - level: CAN_MANAGE
      service_principal_name: ${var.pipeline_sp}
  job_clusters:
    - job_cluster_key: midas_cluster
      new_cluster:
        spark_version: "16.4.x-scala2.12"
        node_type_id: Standard_DS4_v2
        data_security_mode: USER_ISOLATION
        num_workers: 2
  tasks:
    - task_key: stage4_batch_inference
      job_cluster_key: midas_cluster
      spark_python_task:
        python_file: ./src/midas/main_inference_stage4.py
        parameters:
          - "--catalog"
          - "${var.catalog_name}"
          - "--schema"
          - "${var.schema_name}"
          - "--model_endpoint"
          - "${var.endpoint_name}"
          - "--case_id"
          - "variacion_significativa_mes_anterior"
          - "--activity_filter"
          - "VARIACION SIGNIFICATIVA"
          - "--request_timeout_seconds"
          - "60"
          - "--max_retries"
          - "3"
          - "--source_table"
          - "midas_ordenes_calidad_pendientes_silver"
          - "--target_table"
          - "midas_predicciones_agente_stage4_gold"

```

## dev/files/src/midas/agent_framework/__init__.py

```python
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

```

## dev/files/src/midas/agent_framework/case_config.py

```python
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

```

## dev/files/src/midas/agent_framework/case_router.py

```python
from __future__ import annotations

from .case_config import CaseConfig


class CaseRouter:
    """Resuelve qué casuística debe procesar una orden.

    El router funciona por metadata, no por condicionales hardcodeados en el
    pipeline principal. Esto permite agregar casuísticas nuevas sin reescribir
    la inferencia batch.
    """

    def __init__(self, cases: dict[str, CaseConfig], default_case_id: str | None = None):
        self.cases = cases
        self.default_case_id = default_case_id

    def resolve(self, order: dict) -> str:
        activity = order.get("actividad") or order.get("activity") or order.get("activity_filter")
        service_type = order.get("servicio") or order.get("service_type")

        for case_id, case in self.cases.items():
            if case.matches_activity(activity) and case.supports_service(service_type):
                return case_id

        if self.default_case_id and self.default_case_id in self.cases:
            return self.default_case_id

        return "unsupported_case"

```

## dev/files/src/midas/agent_framework/contracts.py

```python
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class BusinessDecision(str, Enum):
    """Catálogo funcional común para todas las casuísticas.

    Este catálogo puede ajustarse cuando EPM cierre oficialmente las decisiones
    válidas. Mantenerlo centralizado evita que cada agente invente etiquetas.
    """

    SIN_NOVEDAD = "SIN_NOVEDAD"
    REQUIERE_AJUSTE = "REQUIERE_AJUSTE"
    REQUIERE_VISITA = "REQUIERE_VISITA"
    REVISION_MANUAL = "REVISION_MANUAL"


class TechnicalStatus(str, Enum):
    """Estado técnico del procesamiento.

    No debe mezclarse con la decisión de negocio. Una falla técnica no es una
    decisión funcional del agente.
    """

    PROCESADA = "PROCESADA"
    ERROR_DATOS = "ERROR_DATOS"
    ERROR_ENDPOINT = "ERROR_ENDPOINT"
    ERROR_TIMEOUT = "ERROR_TIMEOUT"
    ERROR_TOOL = "ERROR_TOOL"
    ERROR_VALIDACION_JSON = "ERROR_VALIDACION_JSON"
    NO_SOPORTADA = "NO_SOPORTADA"
    PENDIENTE_REINTENTO = "PENDIENTE_REINTENTO"


@dataclass(frozen=True)
class EvidenceItem:
    source: str
    field: str
    value: Any
    description: Optional[str] = None


@dataclass(frozen=True)
class AgentDecision:
    """Contrato de salida normalizado hacia Gold."""

    order_id: str
    case_id: str
    service_type: Optional[str]
    business_decision: Optional[str]
    justification: str
    confidence_score: Optional[float] = None
    requires_human_review: bool = False
    cause_category: Optional[str] = None
    recommended_action: Optional[str] = None
    evidence: list[dict[str, Any]] = field(default_factory=list)
    rules_applied: list[str] = field(default_factory=list)
    data_quality_warnings: list[str] = field(default_factory=list)
    technical_status: str = TechnicalStatus.PROCESADA.value
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    model_version: Optional[str] = None
    prompt_version: Optional[str] = None
    run_id: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

```

## dev/files/src/midas/agent_framework/decision_validator.py

```python
from __future__ import annotations

import json
import re
from typing import Any

from .contracts import AgentDecision, BusinessDecision, TechnicalStatus


class DecisionValidationError(ValueError):
    pass


class DecisionValidator:
    """Normaliza y valida la salida JSON del agente.

    Objetivos:
    - impedir texto libre no parseable en Gold;
    - separar decisión funcional de estado técnico;
    - estandarizar campos obligatorios para todas las casuísticas.
    """

    REQUIRED_FIELDS = {
        "order_id",
        "case_id",
        "business_decision",
        "justification",
        "requires_human_review",
    }

    def __init__(self, allowed_decisions: list[str] | None = None):
        self.allowed_decisions = set(allowed_decisions or [item.value for item in BusinessDecision])

    @staticmethod
    def _strip_code_fence(raw_output: str) -> str:
        cleaned = (raw_output or "").strip()
        cleaned = re.sub(r"^```json\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"^```\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
        return cleaned.strip()

    def parse(self, raw_output: str) -> dict[str, Any]:
        cleaned = self._strip_code_fence(raw_output)
        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise DecisionValidationError(f"La salida del agente no es JSON válido: {exc}") from exc
        if not isinstance(parsed, dict):
            raise DecisionValidationError("La salida del agente debe ser un objeto JSON.")
        return parsed

    def validate(self, raw_output: str, fallback_order_id: str | None = None, fallback_case_id: str | None = None) -> AgentDecision:
        payload = self.parse(raw_output)

        missing = self.REQUIRED_FIELDS - set(payload.keys())
        if missing:
            raise DecisionValidationError(f"Faltan campos obligatorios en salida del agente: {sorted(missing)}")

        business_decision = str(payload.get("business_decision", "")).strip().upper()
        if business_decision not in self.allowed_decisions:
            raise DecisionValidationError(
                f"Decisión funcional inválida: {business_decision!r}. Permitidas: {sorted(self.allowed_decisions)}"
            )

        confidence = payload.get("confidence_score")
        if confidence is not None:
            try:
                confidence = float(confidence)
            except Exception as exc:
                raise DecisionValidationError("confidence_score debe ser numérico o null.") from exc
            if not 0.0 <= confidence <= 1.0:
                raise DecisionValidationError("confidence_score debe estar entre 0 y 1.")

        return AgentDecision(
            order_id=str(payload.get("order_id") or fallback_order_id or ""),
            case_id=str(payload.get("case_id") or fallback_case_id or ""),
            service_type=payload.get("service_type"),
            business_decision=business_decision,
            justification=str(payload.get("justification") or "").strip(),
            confidence_score=confidence,
            requires_human_review=bool(payload.get("requires_human_review", False)),
            cause_category=payload.get("cause_category"),
            recommended_action=payload.get("recommended_action"),
            evidence=list(payload.get("evidence") or []),
            rules_applied=list(payload.get("rules_applied") or []),
            data_quality_warnings=list(payload.get("data_quality_warnings") or []),
            technical_status=TechnicalStatus.PROCESADA.value,
            model_version=payload.get("model_version"),
            prompt_version=payload.get("prompt_version"),
        )

    def build_error_decision(
        self,
        order_id: str,
        case_id: str,
        technical_status: TechnicalStatus,
        error_code: str,
        error_message: str,
    ) -> AgentDecision:
        return AgentDecision(
            order_id=str(order_id),
            case_id=case_id,
            service_type=None,
            business_decision=None,
            justification="No se emitió decisión funcional por error técnico o de datos.",
            requires_human_review=True,
            technical_status=technical_status.value,
            error_code=error_code,
            error_message=error_message[:4000],
        )

```

## dev/files/src/midas/agent_framework/prompt_loader.py

```python
from __future__ import annotations

from pathlib import Path


class PromptLoader:
    """Carga prompts desde archivos versionables.

    El prompt ya no debe vivir como un string gigante dentro de código Python.
    Mantenerlo en markdown facilita revisión funcional, versionamiento y
    comparación entre versiones del agente.
    """

    def __init__(self, base_path: str | Path | None = None):
        self.base_path = Path(base_path).resolve() if base_path else self._discover_base_path()

    @staticmethod
    def _discover_base_path() -> Path:
        current = Path(__file__).resolve()
        for parent in current.parents:
            if (parent / "midas" / "agents").exists():
                return parent / "midas" / "agents"
            if (parent / "agents").exists():
                return parent / "agents"
        return Path("src/midas/agents")

    def load(self, prompt_file: str) -> str:
        prompt_path = Path(prompt_file)
        if not prompt_path.is_absolute():
            prompt_path = self.base_path / prompt_file

        if not prompt_path.exists():
            raise FileNotFoundError(f"Prompt no encontrado: {prompt_path}")

        return prompt_path.read_text(encoding="utf-8")

```

## dev/files/src/midas/agents/variacion_significativa/__init__.py

```python
"""Casuística: variación significativa contra el mes anterior."""

```

## dev/files/src/midas/agents/variacion_significativa/prompt.md

```markdown
# Rol
Eres un Analista Senior de Calidad de Facturación para EPM. Tu tarea es analizar órdenes de calidad de la casuística **variación significativa contra el mes anterior**, aplicable a agua/acueducto, energía y gas.

El agente NO es conversacional. Debe procesar una orden y devolver únicamente una decisión estructurada en JSON para ser persistida en una tabla Gold de Databricks.

# Casuística
`variacion_significativa_mes_anterior`

Analiza si la orden presenta una variación de consumo relevante contra el mes anterior y si esa variación tiene explicación operativa, comercial o de calidad de datos.

# Herramientas disponibles
Usa las herramientas en este orden lógico:

1. `get_contexto_variacion_significativa(order_id)`
   - Devuelve contexto consolidado de la orden, servicio, periodo actual, periodo anterior, consumo actual, consumo anterior, variación absoluta, variación porcentual, límites de lectura, observación de lectura y datos básicos.

2. `get_historial_consumo_producto(servicio_suscrito, limite_periodos)`
   - Devuelve histórico reciente de consumo/facturación para analizar tendencia, estacionalidad, consumos cero, saltos anómalos o falta de historial.

3. `get_contexto_observaciones_calidad(servicio_suscrito, id_periodo_consumo)`
   - Devuelve órdenes de crítica, comentarios, observaciones o antecedentes asociados al producto y periodo.

# Reglas obligatorias
- Si no puedes obtener contexto mínimo de la orden, devuelve `REVISION_MANUAL`.
- Si falta consumo actual o consumo anterior, devuelve `REVISION_MANUAL`.
- Si hay datos contradictorios o insuficientes, devuelve `REVISION_MANUAL`.
- Si existe evidencia clara de error de lectura, lectura estimada/corregida, reclamo, PNO u observación relevante, inclúyela en `evidence`.
- No inventes datos, umbrales, comentarios, órdenes, reclamos ni causas.
- No ejecutes acciones operativas. Solo recomienda decisión.
- No incluyas razonamiento paso a paso. La justificación debe ser breve, trazable y basada en evidencias.

# Criterios de decisión
Usa el siguiente catálogo:

- `SIN_NOVEDAD`: la variación está explicada o no hay evidencia suficiente de inconsistencia.
- `REQUIERE_AJUSTE`: hay evidencia fuerte de inconsistencia de facturación o lectura que justifica ajuste.
- `REQUIERE_VISITA`: hay señales de posible condición física/operativa que requiere validación en campo.
- `REVISION_MANUAL`: faltan datos, hay contradicciones, la casuística no es concluyente o el riesgo de decisión automática es alto.

# Causas sugeridas
Usa una de estas categorías cuando aplique:

- `NORMAL`
- `VARIACION_NO_EXPLICADA`
- `LECTURA_ESTIMADA_O_CORREGIDA`
- `ERROR_LECTURA`
- `CAMBIO_PATRON_CONSUMO`
- `PNO_RECLAMO_OBSERVACION`
- `HISTORIAL_INSUFICIENTE`
- `DATOS_INCONSISTENTES`

# Formato de salida obligatorio
Devuelve únicamente JSON válido, sin markdown, sin texto adicional, con esta estructura exacta:

```json
{
  "order_id": "string",
  "case_id": "variacion_significativa_mes_anterior",
  "service_type": "string|null",
  "business_decision": "SIN_NOVEDAD|REQUIERE_AJUSTE|REQUIERE_VISITA|REVISION_MANUAL",
  "justification": "Texto breve y trazable basado en datos consultados.",
  "confidence_score": 0.0,
  "requires_human_review": true,
  "cause_category": "NORMAL|VARIACION_NO_EXPLICADA|LECTURA_ESTIMADA_O_CORREGIDA|ERROR_LECTURA|CAMBIO_PATRON_CONSUMO|PNO_RECLAMO_OBSERVACION|HISTORIAL_INSUFICIENTE|DATOS_INCONSISTENTES|null",
  "recommended_action": "string|null",
  "evidence": [
    {
      "source": "string",
      "field": "string",
      "value": "string|number|null",
      "description": "string"
    }
  ],
  "rules_applied": ["string"],
  "data_quality_warnings": ["string"],
  "model_version": "string|null",
  "prompt_version": "vsma_prompt_v1"
}
```

```

## dev/files/src/midas/agents/variacion_significativa/rules.json

```json
{
  "case_id": "variacion_significativa_mes_anterior",
  "version": "vsma_rules_v1",
  "thresholds": {
    "default_percentage_variation_review": 0.30,
    "default_minimum_absolute_delta": 5
  },
  "deterministic_rules": [
    "Si no existe consumo actual o consumo anterior, enviar a REVISION_MANUAL.",
    "Si el consumo anterior es cero y el actual es mayor que cero, evaluar como variación crítica y exigir explicación contextual.",
    "Si hay lectura estimada/corregida, reclamo, PNO u observación relevante, usar esa evidencia antes de recomendar ajuste.",
    "Si la información es contradictoria, no emitir ajuste automático; enviar a REVISION_MANUAL."
  ],
  "notes": [
    "Los umbrales deben ser validados con EPM. Estos valores son placeholders técnicos para la primera iteración del agente."
  ]
}

```

## dev/files/src/midas/agents/variacion_significativa/schema.json

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "MidasAgentDecision",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "order_id",
    "case_id",
    "service_type",
    "business_decision",
    "justification",
    "confidence_score",
    "requires_human_review",
    "cause_category",
    "recommended_action",
    "evidence",
    "rules_applied",
    "data_quality_warnings"
  ],
  "properties": {
    "order_id": {"type": "string"},
    "case_id": {"const": "variacion_significativa_mes_anterior"},
    "service_type": {"type": ["string", "null"]},
    "business_decision": {
      "type": "string",
      "enum": ["SIN_NOVEDAD", "REQUIERE_AJUSTE", "REQUIERE_VISITA", "REVISION_MANUAL"]
    },
    "justification": {"type": "string", "minLength": 20},
    "confidence_score": {"type": ["number", "null"], "minimum": 0, "maximum": 1},
    "requires_human_review": {"type": "boolean"},
    "cause_category": {
      "type": ["string", "null"],
      "enum": [
        "NORMAL",
        "VARIACION_NO_EXPLICADA",
        "LECTURA_ESTIMADA_O_CORREGIDA",
        "ERROR_LECTURA",
        "CAMBIO_PATRON_CONSUMO",
        "PNO_RECLAMO_OBSERVACION",
        "HISTORIAL_INSUFICIENTE",
        "DATOS_INCONSISTENTES",
        null
      ]
    },
    "recommended_action": {"type": ["string", "null"]},
    "evidence": {"type": "array", "items": {"type": "object"}},
    "rules_applied": {"type": "array", "items": {"type": "string"}},
    "data_quality_warnings": {"type": "array", "items": {"type": "string"}},
    "model_version": {"type": ["string", "null"]},
    "prompt_version": {"type": ["string", "null"]}
  }
}

```

## dev/files/src/midas/agents/variacion_significativa/tools.py

```python
from __future__ import annotations

import logging
from pyspark.sql import SparkSession

log = logging.getLogger(__name__)


class VariacionSignificativaToolBuilder:
    """Crea SQL Functions para la casuística de variación significativa.

    Las funciones se apoyan en las tablas Silver ya creadas en etapas 1-3:
    - midas_ordenes_calidad_pendientes_silver
    - midas_historial_facturacion_silver
    - midas_historial_critica_silver

    Nota: los nombres de actividades/umbrales deben validarse con EPM. La lógica
    SQL aquí prioriza contexto trazable para el agente, no decisiones finales.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def build_sql_functions(
        self,
        catalog: str,
        schema: str,
        pipeline_sp: str | None = None,
        grant_account_users: bool = False,
    ) -> None:
        self.spark.sql(f"USE CATALOG {catalog}")
        self.spark.sql(f"USE SCHEMA {schema}")

        self._create_context_function(catalog, schema)
        self._create_consumption_history_function(catalog, schema)
        self._create_quality_observations_function(catalog, schema)
        self._grant_permissions(catalog, schema, pipeline_sp, grant_account_users)

    def _create_context_function(self, catalog: str, schema: str) -> None:
        log.info("Creando función get_contexto_variacion_significativa...")
        self.spark.sql(f"""
            CREATE OR REPLACE FUNCTION {catalog}.{schema}.get_contexto_variacion_significativa(
                order_id BIGINT COMMENT 'Identificador de la orden de calidad a analizar'
            )
            RETURNS TABLE(
                id_orden BIGINT,
                servicio_suscrito BIGINT,
                contrato BIGINT,
                ciclo STRING,
                actividad STRING,
                servicio STRING,
                categoria STRING,
                subcategoria STRING,
                localidad STRING,
                plan_facturacion STRING,
                id_periodo_facturacion_actual BIGINT,
                id_periodo_consumo_actual BIGINT,
                consumo_actual DOUBLE,
                lectura_anterior_actual DOUBLE,
                lectura_actual_actual DOUBLE,
                limite_inferior_actual DOUBLE,
                limite_superior_actual DOUBLE,
                observacion_lectura_actual STRING,
                id_periodo_facturacion_anterior BIGINT,
                id_periodo_consumo_anterior BIGINT,
                consumo_anterior DOUBLE,
                variacion_absoluta DOUBLE,
                variacion_porcentual DOUBLE,
                historial_periodos_disponibles BIGINT,
                advertencias_datos ARRAY<STRING>
            )
            COMMENT 'Contexto consolidado para analizar variación significativa contra el periodo anterior'
            RETURN (
                WITH orden AS (
                    SELECT
                        id_orden,
                        servicio_suscrito,
                        contrato,
                        CAST(ciclo AS STRING) AS ciclo,
                        actividad,
                        servicio,
                        categoria,
                        subcategoria,
                        localidad,
                        plan_facturacion
                    FROM {catalog}.{schema}.midas_ordenes_calidad_pendientes_silver
                    WHERE id_orden = order_id
                ), hist AS (
                    SELECT
                        h.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY h.servicio_suscrito
                            ORDER BY h.id_periodo_facturacion DESC
                        ) AS rn,
                        COUNT(*) OVER (PARTITION BY h.servicio_suscrito) AS total_periodos
                    FROM {catalog}.{schema}.midas_historial_facturacion_silver h
                    INNER JOIN orden o
                        ON h.servicio_suscrito = o.servicio_suscrito
                ), actual AS (
                    SELECT * FROM hist WHERE rn = 1
                ), anterior AS (
                    SELECT * FROM hist WHERE rn = 2
                )
                SELECT
                    o.id_orden,
                    o.servicio_suscrito,
                    o.contrato,
                    o.ciclo,
                    o.actividad,
                    o.servicio,
                    o.categoria,
                    o.subcategoria,
                    o.localidad,
                    o.plan_facturacion,
                    a.id_periodo_facturacion AS id_periodo_facturacion_actual,
                    a.id_periodo_consumo AS id_periodo_consumo_actual,
                    CAST(COALESCE(a.consumo_facturado_periodo, a.consumo_facturado_lectura, a.consumo_calculado) AS DOUBLE) AS consumo_actual,
                    CAST(a.lectura_anterior AS DOUBLE) AS lectura_anterior_actual,
                    CAST(a.lectura_actual AS DOUBLE) AS lectura_actual_actual,
                    CAST(a.limite_inferior AS DOUBLE) AS limite_inferior_actual,
                    CAST(a.limite_superior AS DOUBLE) AS limite_superior_actual,
                    CAST(a.observacion_Lectura AS STRING) AS observacion_lectura_actual,
                    p.id_periodo_facturacion AS id_periodo_facturacion_anterior,
                    p.id_periodo_consumo AS id_periodo_consumo_anterior,
                    CAST(COALESCE(p.consumo_facturado_periodo, p.consumo_facturado_lectura, p.consumo_calculado) AS DOUBLE) AS consumo_anterior,
                    CAST(
                        COALESCE(a.consumo_facturado_periodo, a.consumo_facturado_lectura, a.consumo_calculado)
                        - COALESCE(p.consumo_facturado_periodo, p.consumo_facturado_lectura, p.consumo_calculado)
                        AS DOUBLE
                    ) AS variacion_absoluta,
                    CASE
                        WHEN COALESCE(p.consumo_facturado_periodo, p.consumo_facturado_lectura, p.consumo_calculado) IS NULL THEN NULL
                        WHEN COALESCE(p.consumo_facturado_periodo, p.consumo_facturado_lectura, p.consumo_calculado) = 0 THEN NULL
                        ELSE CAST(
                            (
                                COALESCE(a.consumo_facturado_periodo, a.consumo_facturado_lectura, a.consumo_calculado)
                                - COALESCE(p.consumo_facturado_periodo, p.consumo_facturado_lectura, p.consumo_calculado)
                            ) / ABS(COALESCE(p.consumo_facturado_periodo, p.consumo_facturado_lectura, p.consumo_calculado))
                            AS DOUBLE
                        )
                    END AS variacion_porcentual,
                    CAST(COALESCE(a.total_periodos, 0) AS BIGINT) AS historial_periodos_disponibles,
                    FILTER(ARRAY(
                        CASE WHEN a.servicio_suscrito IS NULL THEN 'sin_consumo_actual' END,
                        CASE WHEN p.servicio_suscrito IS NULL THEN 'sin_consumo_anterior' END,
                        CASE WHEN COALESCE(a.consumo_facturado_periodo, a.consumo_facturado_lectura, a.consumo_calculado) IS NULL THEN 'consumo_actual_nulo' END,
                        CASE WHEN COALESCE(p.consumo_facturado_periodo, p.consumo_facturado_lectura, p.consumo_calculado) IS NULL THEN 'consumo_anterior_nulo' END,
                        CASE WHEN COALESCE(p.consumo_facturado_periodo, p.consumo_facturado_lectura, p.consumo_calculado) = 0 THEN 'consumo_anterior_cero' END,
                        CASE WHEN a.observacion_Lectura IS NOT NULL THEN 'observacion_lectura_actual' END
                    ), x -> x IS NOT NULL) AS advertencias_datos
                FROM orden o
                LEFT JOIN actual a ON o.servicio_suscrito = a.servicio_suscrito
                LEFT JOIN anterior p ON o.servicio_suscrito = p.servicio_suscrito
            )
        """)

    def _create_consumption_history_function(self, catalog: str, schema: str) -> None:
        log.info("Creando función get_historial_consumo_producto...")
        self.spark.sql(f"""
            CREATE OR REPLACE FUNCTION {catalog}.{schema}.get_historial_consumo_producto(
                serv_suscrito BIGINT COMMENT 'Servicio suscrito del producto',
                limite_periodos INT COMMENT 'Cantidad máxima de periodos recientes a retornar'
            )
            RETURNS TABLE(
                servicio_suscrito BIGINT,
                id_periodo_facturacion BIGINT,
                id_periodo_consumo BIGINT,
                anio_facturacion BIGINT,
                mes_facturacion BIGINT,
                consumo_facturado_periodo DOUBLE,
                consumo_facturado_lectura DOUBLE,
                consumo_calculado DOUBLE,
                lectura_anterior DOUBLE,
                lectura_actual DOUBLE,
                limite_inferior DOUBLE,
                limite_superior DOUBLE,
                observacion_lectura STRING
            )
            COMMENT 'Histórico reciente de consumo del producto para análisis de tendencia y estacionalidad'
            RETURN (
                SELECT
                    servicio_suscrito,
                    id_periodo_facturacion,
                    id_periodo_consumo,
                    anio_facturacion,
                    mes_facturacion,
                    CAST(consumo_facturado_periodo AS DOUBLE) AS consumo_facturado_periodo,
                    CAST(consumo_facturado_lectura AS DOUBLE) AS consumo_facturado_lectura,
                    CAST(consumo_calculado AS DOUBLE) AS consumo_calculado,
                    CAST(lectura_anterior AS DOUBLE) AS lectura_anterior,
                    CAST(lectura_actual AS DOUBLE) AS lectura_actual,
                    CAST(limite_inferior AS DOUBLE) AS limite_inferior,
                    CAST(limite_superior AS DOUBLE) AS limite_superior,
                    CAST(observacion_Lectura AS STRING) AS observacion_lectura
                FROM {catalog}.{schema}.midas_historial_facturacion_silver
                WHERE servicio_suscrito = serv_suscrito
                ORDER BY id_periodo_facturacion DESC
                LIMIT limite_periodos
            )
        """)

    def _create_quality_observations_function(self, catalog: str, schema: str) -> None:
        log.info("Creando función get_contexto_observaciones_calidad...")
        self.spark.sql(f"""
            CREATE OR REPLACE FUNCTION {catalog}.{schema}.get_contexto_observaciones_calidad(
                serv_suscrito BIGINT COMMENT 'Servicio suscrito del producto',
                periodo_consumo BIGINT COMMENT 'Periodo de consumo relacionado con la orden'
            )
            RETURNS TABLE(
                id_orden BIGINT,
                servicio_suscrito BIGINT,
                tipo_consumo BIGINT,
                id_periodo_consumo BIGINT,
                tipo_trabajo STRING,
                actividad STRING,
                fecha_creacion_orden STRING,
                fecha_legalizacion_orden STRING,
                estado STRING,
                analista_legaliza STRING,
                lista_comentarios ARRAY<STRUCT<fecha_registro:TIMESTAMP_NTZ,tipo_comentario:STRING,comentario:STRING>>
            )
            COMMENT 'Observaciones, órdenes de crítica y antecedentes de calidad asociados al producto y periodo'
            RETURN (
                SELECT
                    id_orden,
                    servicio_suscrito,
                    tipo_consumo,
                    id_periodo_consumo,
                    tipo_trabajo,
                    actividad,
                    fecha_creacion_orden,
                    fecha_legalizacion_orden,
                    estado,
                    analista_legaliza,
                    lista_comentarios
                FROM {catalog}.{schema}.midas_historial_critica_silver
                WHERE servicio_suscrito = serv_suscrito
                  AND (periodo_consumo IS NULL OR id_periodo_consumo = periodo_consumo)
            )
        """)

    def _grant_permissions(
        self,
        catalog: str,
        schema: str,
        pipeline_sp: str | None,
        grant_account_users: bool,
    ) -> None:
        function_names = [
            "get_contexto_variacion_significativa",
            "get_historial_consumo_producto",
            "get_contexto_observaciones_calidad",
        ]
        for fn in function_names:
            full_name = f"{catalog}.{schema}.{fn}"
            if pipeline_sp:
                self.spark.sql(f"GRANT EXECUTE ON FUNCTION {full_name} TO `{pipeline_sp}`")
                log.info("GRANT EXECUTE concedido sobre %s a %s", full_name, pipeline_sp)
            if grant_account_users:
                self.spark.sql(f"GRANT EXECUTE ON FUNCTION {full_name} TO `account users`")
                log.info("GRANT EXECUTE concedido sobre %s a account users", full_name)

```

## dev/files/src/midas/main_deploy_stage4.py

```python
import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

import mlflow
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

_LLM_ENDPOINT_NAME = "databricks-gpt-oss-120b"
_STAGE4_TOOL_FUNCTION_NAMES = [
    "get_contexto_variacion_significativa",
    "get_historial_consumo_producto",
    "get_contexto_observaciones_calidad",
]


def _parse_bool(value):
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "y", "si", "sí"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError(f"Valor booleano inválido: {value!r}")


def _ensure_runtime_compatibility_stubs() -> None:
    """Compatibilidad con clusters DEV que no tienen MLflow ResponsesAgent reciente.

    Este patrón replica la intención del deploy actual del proyecto para evitar
    fallos durante la validación local de mlflow.pyfunc.log_model.
    """

    from unittest.mock import MagicMock

    for mod_name in [
        "databricks.vector_search",
        "databricks.vector_search.client",
        "databricks.vector_search.reranker",
        "mlflow.types.responses",
    ]:
        if mod_name not in sys.modules:
            sys.modules[mod_name] = MagicMock()

    import mlflow.pyfunc as pyfunc

    if not hasattr(pyfunc, "ResponsesAgent"):
        class ResponsesAgentStub(pyfunc.PythonModel):
            pass

        pyfunc.ResponsesAgent = ResponsesAgentStub

    try:
        import mlflow.models.signature as mlflow_sig
        import mlflow.pyfunc as mlflow_pyfunc_mod

        original_infer_signature = mlflow_sig._infer_signature_from_type_hints

        def safe_infer_signature(func, input_arg_index, input_example=None):
            try:
                return original_infer_signature(func, input_arg_index, input_example)
            except Exception:
                return None

        mlflow_sig._infer_signature_from_type_hints = safe_infer_signature
        if hasattr(mlflow_pyfunc_mod, "_infer_signature_from_type_hints"):
            mlflow_pyfunc_mod._infer_signature_from_type_hints = safe_infer_signature
    except Exception as exc:
        log.warning("No se pudo aplicar patch de signature MLflow: %s", exc)


def main() -> None:
    parser = argparse.ArgumentParser(description="Midas Stage 4 Agent Deployment Runner")
    parser.add_argument("--catalog", default=os.environ.get("MIDAS_CATALOG"))
    parser.add_argument("--schema", default=os.environ.get("MIDAS_SCHEMA"))
    parser.add_argument("--model_name", default=os.environ.get("MIDAS_MODEL_NAME", "midas_stage4_agent_model"))
    parser.add_argument("--endpoint_name", default=os.environ.get("MIDAS_ENDPOINT_NAME"))
    parser.add_argument("--experiment_path", default=os.environ.get("MIDAS_EXPERIMENT_PATH"))
    parser.add_argument("--endpoint_workload_size", default=os.environ.get("MIDAS_ENDPOINT_WORKLOAD_SIZE", "Small"))
    parser.add_argument("--endpoint_scale_to_zero_enabled", type=_parse_bool, default=_parse_bool(os.environ.get("MIDAS_ENDPOINT_SCALE_TO_ZERO_ENABLED", "true")))
    parser.add_argument("--agent_llm_timeout_seconds", type=int, default=int(os.environ.get("MIDAS_LLM_TIMEOUT_SECONDS", "60")))
    parser.add_argument("--agent_max_iterations", type=int, default=int(os.environ.get("MIDAS_AGENT_MAX_ITERATIONS", "10")))
    parser.add_argument("--pipeline_sp", required=False, default=None)
    parser.add_argument("--case_id", default="variacion_significativa_mes_anterior")
    parser.add_argument("--prompt_file", default="variacion_significativa/prompt.md")
    args, _ = parser.parse_known_args()

    missing = [key for key, value in vars(args).items() if value is None and key not in {"pipeline_sp"}]
    if missing:
        parser.error(f"Argumentos requeridos no encontrados: {missing}")

    if not args.experiment_path or args.experiment_path == "None" or str(args.experiment_path).startswith("${"):
        if "prod" in (args.catalog or "").lower():
            args.experiment_path = "/Shared/midas_agent/experiments/midas_stage4_agent_experiment_prod"
        elif "np" in (args.catalog or "").lower():
            args.experiment_path = "/Shared/midas_agent/experiments/midas_stage4_agent_experiment_uat"
        else:
            args.experiment_path = "/Shared/midas_agent/experiments/midas_stage4_agent_experiment_dev"

    log.info("Instalando dependencias mínimas para validación MLflow...")
    subprocess.run(
        ["pip", "install", "databricks-openai", "databricks-vectorsearch", "-q"],
        check=True,
        capture_output=True,
    )

    cwd = os.getcwd()
    bundle_root = os.path.normpath(os.path.join(cwd, "../.."))
    src_path = os.path.join(bundle_root, "src")
    agent_path = os.path.join(bundle_root, "agent")
    config_path = os.path.join(bundle_root, "config")

    for key in list(sys.modules.keys()):
        if key == "midas" or key.startswith("midas.") or key == "src" or key.startswith("src."):
            del sys.modules[key]

    for path in [src_path, bundle_root, agent_path, config_path]:
        if path not in sys.path:
            sys.path.insert(0, path)

    _ensure_runtime_compatibility_stubs()

    uc_model_full_name = f"{args.catalog}.{args.schema}.{args.model_name}"
    uc_tool_names = [f"{args.catalog}.{args.schema}.{fn}" for fn in _STAGE4_TOOL_FUNCTION_NAMES]

    resources = [DatabricksServingEndpoint(endpoint_name=_LLM_ENDPOINT_NAME)]
    resources.extend(DatabricksFunction(function_name=tool_name) for tool_name in uc_tool_names)

    os.environ["MIDAS_CATALOG"] = args.catalog
    os.environ["MIDAS_SCHEMA"] = args.schema
    os.environ["MIDAS_STAGE4_CASE_ID"] = args.case_id
    os.environ["MIDAS_STAGE4_PROMPT_FILE"] = args.prompt_file
    os.environ["MIDAS_STAGE4_TOOL_NAMES"] = ",".join(_STAGE4_TOOL_FUNCTION_NAMES)
    os.environ["MIDAS_LLM_TIMEOUT_SECONDS"] = str(args.agent_llm_timeout_seconds)
    os.environ["MIDAS_AGENT_MAX_ITERATIONS"] = str(args.agent_max_iterations)

    responses_signature = mlflow.models.infer_signature(
        model_input={"input": [{"role": "user", "content": "123"}]},
        model_output={"output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "{}"}]}]},
    )

    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(args.experiment_path)

    import mlflow.openai as mlflow_openai

    original_autolog = mlflow_openai.autolog
    mlflow_openai.autolog = lambda *_a, **_kw: None
    try:
        with mlflow.start_run(run_name="deploy_stage4_agent") as run:
            logged_agent_info = mlflow.pyfunc.log_model(
                artifact_path="agent",
                python_model=os.path.join(agent_path, "stage4_agent.py"),
                code_paths=[src_path, agent_path, config_path],
                pip_requirements=[
                    "databricks-openai",
                    "databricks-vectorsearch",
                    "backoff",
                    "mlflow",
                ],
                signature=responses_signature,
                resources=resources,
            )
            log.info("Modelo Stage 4 logueado en MLflow run_id=%s", run.info.run_id)
            registered_model_info = mlflow.register_model(
                model_uri=logged_agent_info.model_uri,
                name=uc_model_full_name,
            )
            log.info("Modelo Stage 4 registrado: %s versión %s", uc_model_full_name, registered_model_info.version)
    finally:
        mlflow_openai.autolog = original_autolog

    workspace = WorkspaceClient()
    served_entities = [
        ServedEntityInput(
            entity_name=uc_model_full_name,
            entity_version=str(registered_model_info.version),
            scale_to_zero_enabled=args.endpoint_scale_to_zero_enabled,
            workload_size=args.endpoint_workload_size,
            environment_vars={
                "MIDAS_CATALOG": args.catalog,
                "MIDAS_SCHEMA": args.schema,
                "MIDAS_LLM_TIMEOUT_SECONDS": str(args.agent_llm_timeout_seconds),
                "MIDAS_AGENT_MAX_ITERATIONS": str(args.agent_max_iterations),
                "MIDAS_STAGE4_CASE_ID": args.case_id,
                "MIDAS_STAGE4_PROMPT_FILE": args.prompt_file,
                "MIDAS_STAGE4_TOOL_NAMES": ",".join(_STAGE4_TOOL_FUNCTION_NAMES),
            },
        )
    ]

    try:
        workspace.serving_endpoints.get(args.endpoint_name)
    except Exception as exc:
        msg = str(exc)
        if "does not exist" in msg or "RESOURCE_DOES_NOT_EXIST" in msg:
            workspace.serving_endpoints.create(
                name=args.endpoint_name,
                config=EndpointCoreConfigInput(served_entities=served_entities),
            )
            log.info("Endpoint Stage 4 creado: %s", args.endpoint_name)
        else:
            raise
    else:
        workspace.serving_endpoints.update_config(
            name=args.endpoint_name,
            served_entities=served_entities,
        )
        log.info("Endpoint Stage 4 actualizado: %s", args.endpoint_name)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    for fn in _STAGE4_TOOL_FUNCTION_NAMES:
        full_name = f"{args.catalog}.{args.schema}.{fn}"
        if args.pipeline_sp:
            spark.sql(f"GRANT EXECUTE ON FUNCTION {full_name} TO `{args.pipeline_sp}`")
            log.info("GRANT EXECUTE otorgado: %s -> %s", full_name, args.pipeline_sp)

    log.info("Deploy Stage 4 finalizado correctamente.")


if __name__ == "__main__":
    main()

```

## dev/files/src/midas/main_inference_stage4.py

```python
import argparse
import json
import logging
import os
import sys
from uuid import uuid4

try:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
_src_path = os.path.join(_script_dir, "..")
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, get_json_object, lit, udf
from pyspark.sql.types import StringType

from midas.agent_framework.contracts import TechnicalStatus

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

MAX_ENDPOINT_ERROR_BODY_CHARS = 8000


def _error_payload(order_id: str, case_id: str, status: str, code: str, message: str) -> str:
    return json.dumps(
        {
            "order_id": str(order_id),
            "case_id": case_id,
            "service_type": None,
            "business_decision": None,
            "justification": "No se emitió decisión funcional por error técnico o de datos.",
            "confidence_score": None,
            "requires_human_review": True,
            "cause_category": None,
            "recommended_action": None,
            "evidence": [],
            "rules_applied": [],
            "data_quality_warnings": [],
            "technical_status": status,
            "error_code": code,
            "error_message": str(message)[:4000],
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _extract_output_text(endpoint_response: dict) -> str:
    for item in reversed(endpoint_response.get("output", [])):
        if item.get("type") == "message":
            content = item.get("content", [])
            if isinstance(content, list):
                for content_item in content:
                    if isinstance(content_item, dict) and content_item.get("type") == "output_text":
                        return content_item.get("text", "")
            if isinstance(content, str):
                return content
    return json.dumps(endpoint_response, ensure_ascii=False)


def build_stage4_agent_udf(
    endpoint_name: str,
    host: str,
    token: str,
    request_timeout_seconds: int,
    max_retries: int,
    case_id: str,
):
    """Construye UDF resiliente para inferencia Stage 4.

    Diferencia clave frente a la inferencia anterior:
    - una orden fallida no detiene todo el lote;
    - se devuelve un payload JSON con technical_status y error_code;
    - la decisión funcional solo existe si la inferencia fue válida.
    """

    def call_agent(order_id: str, activity: str, service_type: str, servicio_suscrito: str) -> str:
        import requests
        import time

        if max_retries < 1:
            return _error_payload(order_id, case_id, TechnicalStatus.ERROR_ENDPOINT.value, "INVALID_RETRY_CONFIG", "max_retries debe ser >= 1")

        order_payload = {
            "order_id": str(order_id),
            "case_id": case_id,
            "activity": activity,
            "service_type": service_type,
            "servicio_suscrito": str(servicio_suscrito) if servicio_suscrito is not None else None,
        }
        payload = {
            "input": [
                {
                    "role": "user",
                    "content": "Analiza la siguiente orden de calidad y responde únicamente el JSON solicitado: "
                    + json.dumps(order_payload, ensure_ascii=False),
                }
            ]
        }

        last_error = None
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    f"{host}/serving-endpoints/{endpoint_name}/invocations",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                    timeout=request_timeout_seconds,
                )
                response.raise_for_status()
                raw_text = _extract_output_text(response.json())

                try:
                    from midas.agent_framework.decision_validator import DecisionValidator

                    decision = DecisionValidator().validate(
                        raw_text,
                        fallback_order_id=str(order_id),
                        fallback_case_id=case_id,
                    )
                    payload_dict = decision.to_dict()
                    payload_dict["technical_status"] = TechnicalStatus.PROCESADA.value
                    return json.dumps(payload_dict, ensure_ascii=False, separators=(",", ":"))
                except Exception as validation_exc:
                    return _error_payload(
                        order_id,
                        case_id,
                        TechnicalStatus.ERROR_VALIDACION_JSON.value,
                        "INVALID_AGENT_JSON",
                        f"{validation_exc}. raw_output={raw_text[:1500]}",
                    )

            except requests.exceptions.Timeout as exc:
                last_error = exc
                if attempt < max_retries - 1:
                    time.sleep(min(300, 10 * (2 ** attempt)))
                    continue
                return _error_payload(
                    order_id,
                    case_id,
                    TechnicalStatus.PENDIENTE_REINTENTO.value,
                    "ENDPOINT_TIMEOUT",
                    f"Timeout tras {max_retries} intentos: {last_error}",
                )
            except requests.exceptions.HTTPError as exc:
                response = exc.response
                status_code = response.status_code if response is not None else "unknown"
                body = response.text if response is not None else ""
                if len(body) > MAX_ENDPOINT_ERROR_BODY_CHARS:
                    body = body[:MAX_ENDPOINT_ERROR_BODY_CHARS] + "... [truncated]"
                retryable = str(status_code) in {"429", "500", "502", "503", "504"}
                if retryable and attempt < max_retries - 1:
                    time.sleep(min(300, 10 * (2 ** attempt)))
                    continue
                return _error_payload(
                    order_id,
                    case_id,
                    TechnicalStatus.ERROR_ENDPOINT.value,
                    f"HTTP_{status_code}",
                    body,
                )
            except Exception as exc:
                return _error_payload(
                    order_id,
                    case_id,
                    TechnicalStatus.ERROR_ENDPOINT.value,
                    "ENDPOINT_CALL_ERROR",
                    str(exc),
                )

        return _error_payload(order_id, case_id, TechnicalStatus.ERROR_ENDPOINT.value, "UNKNOWN_ENDPOINT_ERROR", str(last_error))

    return udf(call_agent, StringType())


def main() -> None:
    parser = argparse.ArgumentParser(description="Midas Stage 4 Batch Inference Runner")
    parser.add_argument("--source_table", required=True)
    parser.add_argument("--target_table", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--model_endpoint", required=True)
    parser.add_argument("--case_id", default="variacion_significativa_mes_anterior")
    parser.add_argument("--activity_filter", required=False, default=None)
    parser.add_argument("--request_timeout_seconds", type=int, default=60)
    parser.add_argument("--max_retries", type=int, default=3)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"USE CATALOG {args.catalog}")
    spark.sql(f"USE SCHEMA {args.schema}")

    from databricks.sdk import WorkspaceClient

    workspace = WorkspaceClient()
    host = workspace.config.host.rstrip("/")
    try:
        auth_headers = workspace.config.authenticate()
    except TypeError:
        auth_headers = {}
        workspace.config.authenticate(auth_headers)
    token = auth_headers.get("Authorization", "Bearer ").split()[-1]

    run_id = str(uuid4())
    log.info("Stage 4 inference run_id=%s endpoint=%s case_id=%s", run_id, args.model_endpoint, args.case_id)

    agent_udf = build_stage4_agent_udf(
        endpoint_name=args.model_endpoint,
        host=host,
        token=token,
        request_timeout_seconds=args.request_timeout_seconds,
        max_retries=args.max_retries,
        case_id=args.case_id,
    )

    where_clause = ""
    if args.activity_filter:
        escaped = args.activity_filter.replace("'", "''")
        where_clause = f"WHERE UPPER(actividad) LIKE UPPER('%{escaped}%')"

    limit_clause = f"LIMIT {args.limit}" if args.limit else ""

    df_source = spark.sql(f"""
        SELECT
            id_orden,
            servicio_suscrito,
            contrato,
            ciclo,
            actividad,
            servicio
        FROM {args.source_table}
        {where_clause}
        {limit_clause}
    """)

    df_inference = (
        df_source
        .withColumn(
            "decision_payload",
            agent_udf(
                col("id_orden").cast("string"),
                col("actividad").cast("string"),
                col("servicio").cast("string"),
                col("servicio_suscrito").cast("string"),
            ),
        )
        .withColumn("case_id", lit(args.case_id))
        .withColumn("run_id", lit(run_id))
        .withColumn("fecha_inferencia", current_timestamp())
        .withColumn("business_decision", get_json_object(col("decision_payload"), "$.business_decision"))
        .withColumn("technical_status", get_json_object(col("decision_payload"), "$.technical_status"))
        .withColumn("requires_human_review", get_json_object(col("decision_payload"), "$.requires_human_review"))
        .withColumn("confidence_score", get_json_object(col("decision_payload"), "$.confidence_score"))
        .withColumn("cause_category", get_json_object(col("decision_payload"), "$.cause_category"))
        .withColumn("error_code", get_json_object(col("decision_payload"), "$.error_code"))
        .withColumn("error_message", get_json_object(col("decision_payload"), "$.error_message"))
    )

    (
        df_inference.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(args.target_table)
    )

    log.info("Stage 4 inference completed. Target table: %s", args.target_table)


if __name__ == "__main__":
    main()

```

## dev/files/src/midas/main_tools_stage4.py

```python
import argparse
import logging
import os
import sys

try:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
_src_path = os.path.join(_script_dir, "..")
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from pyspark.sql import SparkSession
from midas.agents.variacion_significativa.tools import VariacionSignificativaToolBuilder

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def _parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "y", "si", "sí"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError(f"Valor booleano inválido: {value!r}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Midas Stage 4 Tool Builder Runner")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--pipeline_sp", required=False, default=None)
    parser.add_argument("--grant_account_users", type=_parse_bool, default=False)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    builder = VariacionSignificativaToolBuilder(spark)
    builder.build_sql_functions(
        catalog=args.catalog,
        schema=args.schema,
        pipeline_sp=args.pipeline_sp,
        grant_account_users=args.grant_account_users,
    )
    log.info("SQL Functions Stage 4 creadas correctamente.")


if __name__ == "__main__":
    main()

```

## dev/files/tests/stage4/test_case_router_stage4.py

```python
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

```

## dev/files/tests/stage4/test_decision_validator_stage4.py

```python
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

```

