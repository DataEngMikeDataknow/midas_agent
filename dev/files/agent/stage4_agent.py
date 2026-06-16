import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Callable, Generator, Optional
import warnings

# Permite importar src/midas cuando el archivo se valida localmente o en MLflow.
_AGENT_DIR = Path(__file__).resolve().parent
_BUNDLE_ROOT = _AGENT_DIR.parent
_SRC_PATH = _BUNDLE_ROOT / "src"
if str(_SRC_PATH) not in sys.path:
    sys.path.insert(0, str(_SRC_PATH))

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

from midas.agent.base.prompt_loader import PromptLoader
from midas.agent.registry.agent_registry import AgentRegistry, CASE_VARIACION_SIGNIFICATIVA

LLM_ENDPOINT_NAME = os.environ.get("MIDAS_LLM_ENDPOINT_NAME", "databricks-gpt-oss-120b")
CATALOG_NAME = os.environ.get("MIDAS_CATALOG", "epm_datalabs_catalog_dllo")
SCHEMA_NAME = os.environ.get("MIDAS_SCHEMA", "facturacion")
CASE_ID = os.environ.get("MIDAS_CASE_ID", CASE_VARIACION_SIGNIFICATIVA)
LLM_TIMEOUT_SECONDS = int(os.environ.get("MIDAS_LLM_TIMEOUT_SECONDS", "60"))
AGENT_MAX_ITERATIONS = int(os.environ.get("MIDAS_AGENT_MAX_ITERATIONS", "6"))

registry = AgentRegistry(base_dir=_SRC_PATH / "midas" / "agent")
agent_config = registry.get(CASE_ID)
SYSTEM_PROMPT = PromptLoader.load(agent_config.prompt_path)


class ToolInfo(BaseModel):
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
            f"Nombre de herramienta inválido: {tool_name}. Convertido a {udf_name}."
        )

    def _coerce_uc_value(value: Any) -> Any:
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


UC_TOOL_NAMES = [
    f"{CATALOG_NAME}.{SCHEMA_NAME}.{fn}" for fn in agent_config.tool_function_names
]

uc_toolkit = UCFunctionToolkit(function_names=UC_TOOL_NAMES)
uc_function_client = get_uc_function_client()
TOOL_INFOS = [create_tool_info(tool_spec) for tool_spec in uc_toolkit.tools]


class Stage4ToolCallingAgent(ResponsesAgent):
    """Agente multi-tool para Etapa 4: variación significativa."""

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
                self._model_serving_client = OpenAI(
                    api_key=token,
                    base_url=f"{cfg.host}/serving-endpoints",
                )
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

    def handle_tool_call(self, tool_call: dict[str, Any], messages: list[dict[str, Any]]):
        arguments_str = tool_call.get("arguments")
        args = json.loads(arguments_str) if arguments_str else {}
        result = str(self.execute_tool(tool_name=tool_call["name"], args=args))
        tool_call_output = self.create_function_call_output_item(tool_call["call_id"], result)
        messages.append(tool_call_output)
        return ResponsesAgentStreamEvent(type="response.output_item.done", item=tool_call_output)

    def call_and_run_tools(self, messages: list[dict[str, Any]], max_iter: int = 10):
        for _ in range(max_iter):
            last_msg = messages[-1]
            if last_msg.get("role") == "assistant":
                return
            if last_msg.get("type") == "function_call":
                yield self.handle_tool_call(last_msg, messages)
            else:
                yield from output_to_responses_items_stream(
                    chunks=self.call_llm(messages), aggregator=messages
                )
        raise RuntimeError(f"Agent max iterations reached ({max_iter}).")

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [
            event.item
            for event in self.predict_stream(request)
            if event.type == "response.output_item.done"
        ]
        return ResponsesAgentResponse(output=outputs, custom_outputs=request.custom_inputs)

    def predict_stream(self, request: ResponsesAgentRequest):
        messages = to_chat_completions_input([i.model_dump() for i in request.input])
        messages.insert(0, {"role": "system", "content": SYSTEM_PROMPT})
        yield from self.call_and_run_tools(messages=messages, max_iter=AGENT_MAX_ITERATIONS)


try:
    mlflow.openai.autolog()
except Exception:
    pass

AGENT = Stage4ToolCallingAgent(llm_endpoint=LLM_ENDPOINT_NAME, tools=TOOL_INFOS)
mlflow.models.set_model(AGENT)
