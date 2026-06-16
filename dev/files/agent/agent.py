import json
import os
import re
from typing import Any, Callable, Generator, Optional
import warnings

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

try:
    from midas.agent.prompts import SYSTEM_PROMPT
except ImportError:
    from src.midas.agent.prompts import SYSTEM_PROMPT

############################################
# Configuración del LLM y del catálogo/esquema.
# Se leen desde variables de entorno para soportar múltiples ambientes
# (dev/uat/prod) sin modificar el código. El job de despliegue (main_deploy.py)
# es responsable de pasar MIDAS_CATALOG y MIDAS_SCHEMA al endpoint.
############################################
LLM_ENDPOINT_NAME = "databricks-gpt-oss-120b"

CATALOG_NAME = os.environ.get("MIDAS_CATALOG", "epm_datalabs_catalog_dllo")
SCHEMA_NAME = os.environ.get("MIDAS_SCHEMA", "facturacion")
LLM_TIMEOUT_SECONDS = int(os.environ.get("MIDAS_LLM_TIMEOUT_SECONDS", "60"))
AGENT_MAX_ITERATIONS = int(os.environ.get("MIDAS_AGENT_MAX_ITERATIONS", "5"))

###############################################################################
## Tools del agente: SQL Functions registradas en Unity Catalog.
## Se construyen dinámicamente desde las variables de entorno.
## https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/agent-tool
###############################################################################
class ToolInfo(BaseModel):
    """
    Representa una herramienta (tool) del agente.
    - name: Nombre de la herramienta.
    - spec: Especificación JSON (formato OpenAI Responses).
    - exec_fn: Función que ejecuta la herramienta.
    """

    name: str
    spec: dict
    exec_fn: Callable


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
            f"Error al procesar el nombre de la herramienta '{tool_name}'. "
            f"Se convirtió en '{udf_name}', que no tiene 2 o 3 partes separadas por '.'."
        )

    def _coerce_uc_value(value: Any) -> Any:
        # UC functions tipadas fallan si el LLM devuelve un entero como texto.
        if isinstance(value, str) and re.fullmatch(r"-?\d+", value):
            return int(value)
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


TOOL_INFOS = []

UC_TOOL_NAMES = [
    f"{CATALOG_NAME}.{SCHEMA_NAME}.get_hist_fact",
    f"{CATALOG_NAME}.{SCHEMA_NAME}.get_ordenes_critica",
]

uc_toolkit = UCFunctionToolkit(function_names=UC_TOOL_NAMES)
uc_function_client = get_uc_function_client()
for tool_spec in uc_toolkit.tools:
    TOOL_INFOS.append(create_tool_info(tool_spec))

VECTOR_SEARCH_TOOLS = []
for vs_tool in VECTOR_SEARCH_TOOLS:
    TOOL_INFOS.append(create_tool_info(vs_tool.tool, vs_tool.execute))


class ToolCallingAgent(ResponsesAgent):
    """Agente que llama herramientas (SQL Functions) para analizar órdenes de calidad."""

    def __init__(self, llm_endpoint: str, tools: list[ToolInfo]):
        """Inicializa el agente con el endpoint LLM y sus herramientas."""
        self.llm_endpoint = llm_endpoint
        self.workspace_client = WorkspaceClient()
        self._model_serving_client: Optional[OpenAI] = None  # lazy init
        self._tools_dict = {tool.name: tool for tool in tools}

    @property
    def model_serving_client(self) -> OpenAI:
        """Crea el cliente OpenAI en el primer uso (evita fallo en validación MLflow)."""
        if self._model_serving_client is None:
            try:
                self._model_serving_client = (
                    self.workspace_client.serving_endpoints.get_open_ai_client()
                )
            except AttributeError:
                # Fallback compatible con varias versiones del databricks-sdk:
                # algunas exponen authenticate() -> headers y otras requieren
                # authenticate(headers) mutando el dict recibido.
                cfg = self.workspace_client.config
                try:
                    _auth_headers = cfg.authenticate()
                except TypeError:
                    _auth_headers = {}
                    cfg.authenticate(_auth_headers)
                _token = _auth_headers.get("Authorization", "Bearer ").split()[-1]
                self._model_serving_client = OpenAI(
                    api_key=_token,
                    base_url=f"{cfg.host}/serving-endpoints",
                )
        return self._model_serving_client

    def get_tool_specs(self) -> list[dict]:
        """Retorna las especificaciones de herramientas en formato OpenAI."""
        return [tool_info.spec for tool_info in self._tools_dict.values()]

    @mlflow.trace(span_type=SpanType.TOOL)
    def execute_tool(self, tool_name: str, args: dict) -> Any:
        """Ejecuta la herramienta especificada con los argumentos dados."""
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

    def handle_tool_call(
        self,
        tool_call: dict[str, Any],
        messages: list[dict[str, Any]],
    ) -> ResponsesAgentStreamEvent:
        """Ejecuta un tool call, lo agrega al historial y retorna el evento de resultado."""
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
            elif last_msg.get("type") == "function_call":
                yield self.handle_tool_call(last_msg, messages)
            else:
                yield from output_to_responses_items_stream(
                    chunks=self.call_llm(messages), aggregator=messages
                )

        raise RuntimeError(f"Agent max iterations reached ({max_iter}). Stopping.")

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [
            event.item
            for event in self.predict_stream(request)
            if event.type == "response.output_item.done"
        ]
        return ResponsesAgentResponse(output=outputs, custom_outputs=request.custom_inputs)

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        messages = to_chat_completions_input([i.model_dump() for i in request.input])
        if SYSTEM_PROMPT:
            messages.insert(0, {"role": "system", "content": SYSTEM_PROMPT})
        yield from self.call_and_run_tools(messages=messages, max_iter=AGENT_MAX_ITERATIONS)


# Registro del modelo en MLflow
# Guard against uninitialized trace provider during MLflow validation import
try:
    mlflow.openai.autolog()
except Exception:
    pass
AGENT = ToolCallingAgent(llm_endpoint=LLM_ENDPOINT_NAME, tools=TOOL_INFOS)
mlflow.models.set_model(AGENT)
