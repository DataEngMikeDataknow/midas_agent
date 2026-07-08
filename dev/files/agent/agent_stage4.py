import os
import sys
from pathlib import Path


def _bootstrap_midas_import_path() -> str:
    """Ensure src/ is on sys.path in Databricks Workspace, DAB jobs and notebooks."""
    candidates: list[Path] = []

    def _add(value: object) -> None:
        if not value:
            return
        try:
            path = Path(str(value).replace("file:", "")).expanduser()
            if not path.is_absolute():
                path = (Path.cwd() / path).resolve()
            candidates.append(path)
        except Exception:
            pass

    try:
        _add(__file__)  # type: ignore[name-defined]
    except Exception:
        pass
    _add(sys.argv[0] if sys.argv else None)
    _add(os.getcwd())
    for env_name in ("MIDAS_PROJECT_ROOT", "PROJECT_ROOT", "DATABRICKS_REPO_ROOT"):
        _add(os.environ.get(env_name))

    visited: set[str] = set()
    for candidate in list(candidates):
        current = candidate if candidate.is_dir() else candidate.parent
        for _ in range(14):
            current_key = str(current)
            if current_key in visited:
                break
            visited.add(current_key)

            src_dir = current / "src"
            package_init = src_dir / "midas" / "__init__.py"
            if package_init.exists():
                for path in (src_dir, current):
                    path_str = str(path)
                    if path_str not in sys.path:
                        sys.path.insert(0, path_str)
                return str(src_dir)

            package_init_direct = current / "midas" / "__init__.py"
            if package_init_direct.exists():
                parent = current.parent
                parent_str = str(parent)
                if parent_str not in sys.path:
                    sys.path.insert(0, parent_str)
                return parent_str

            parent = current.parent
            if parent == current:
                break
            current = parent

    for base in (Path("/Workspace/Repos"), Path("/Workspace/Users"), Path("/Workspace/Shared")):
        if not base.exists():
            continue
        base_depth = len(base.parts)
        for walk_root, dirs, _files in os.walk(base):
            walk_path = Path(walk_root)
            if len(walk_path.parts) - base_depth > 8:
                dirs[:] = []
                continue
            dirs[:] = [d for d in dirs if d not in {".git", ".venv", "__pycache__", ".pytest_cache"}]
            src_dir = walk_path / "src"
            if (src_dir / "midas" / "__init__.py").exists():
                for path in (src_dir, walk_path):
                    path_str = str(path)
                    if path_str not in sys.path:
                        sys.path.insert(0, path_str)
                return str(src_dir)

    raise ModuleNotFoundError(
        "No se pudo ubicar el paquete 'midas'. Ejecuta desde la raíz del repo "
        "o define MIDAS_PROJECT_ROOT=/Workspace/.../midas_agent/dev/files."
    )


_bootstrap_midas_import_path()

"""Modelo MLflow ResponsesAgent para Etapa 4.

Este agente usa únicamente SQL Functions controladas de Unity Catalog. No acepta
SQL libre ni ejecuta consultas generadas por el modelo.
"""
import json
import os
import re
import warnings
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

try:
    from midas.stage4.constants import STAGE4_SQL_FUNCTION_NAMES
    from midas.stage4.prompts import SYSTEM_PROMPT_STAGE4
except ImportError:
    from src.midas.stage4.constants import STAGE4_SQL_FUNCTION_NAMES
    from src.midas.stage4.prompts import SYSTEM_PROMPT_STAGE4

LLM_ENDPOINT_NAME = os.environ.get("MIDAS_STAGE4_LLM_ENDPOINT", "databricks-gpt-oss-120b")
CATALOG_NAME = os.environ.get("MIDAS_CATALOG", "epm_datalabs_catalog_dllo")
SCHEMA_NAME = os.environ.get("MIDAS_SCHEMA", "facturacion")
LLM_TIMEOUT_SECONDS = int(os.environ.get("MIDAS_LLM_TIMEOUT_SECONDS", "60"))
AGENT_MAX_ITERATIONS = int(os.environ.get("MIDAS_AGENT_MAX_ITERATIONS", "8"))


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
    parts = udf_name.split(".")
    if len(parts) == 2:
        udf_name = f"{CATALOG_NAME}.{udf_name}"
    elif len(parts) != 3:
        raise ValueError(f"Nombre de tool inválido: {tool_name} -> {udf_name}")

    def _coerce_uc_value(value: Any) -> Any:
        if isinstance(value, str) and re.fullmatch(r"-?\d+", value):
            return int(value)
        if isinstance(value, list):
            return [_coerce_uc_value(item) for item in value]
        if isinstance(value, dict):
            return {key: _coerce_uc_value(val) for key, val in value.items()}
        return value

    def exec_fn(**kwargs):
        result = uc_function_client.execute_function(
            udf_name, {key: _coerce_uc_value(val) for key, val in kwargs.items()}
        )
        if result.error is not None:
            return result.error
        return result.value

    return ToolInfo(name=tool_name, spec=tool_spec, exec_fn=exec_fn_param or exec_fn)


UC_TOOL_NAMES = [f"{CATALOG_NAME}.{SCHEMA_NAME}.{name}" for name in STAGE4_SQL_FUNCTION_NAMES]
uc_toolkit = UCFunctionToolkit(function_names=UC_TOOL_NAMES)
uc_function_client = get_uc_function_client()
TOOL_INFOS = [create_tool_info(tool_spec) for tool_spec in uc_toolkit.tools]


class Stage4ToolCallingAgent(ResponsesAgent):
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
                    headers = cfg.authenticate()
                except TypeError:
                    headers = {}
                    cfg.authenticate(headers)
                token = headers.get("Authorization", "Bearer ").split()[-1]
                self._model_serving_client = OpenAI(api_key=token, base_url=f"{cfg.host}/serving-endpoints")
        return self._model_serving_client

    def get_tool_specs(self) -> list[dict]:
        return [tool.spec for tool in self._tools_dict.values()]

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
                if chunk_dict.get("choices"):
                    yield chunk_dict

    def handle_tool_call(self, tool_call: dict[str, Any], messages: list[dict[str, Any]]) -> ResponsesAgentStreamEvent:
        arguments = json.loads(tool_call.get("arguments") or "{}")
        result = str(self.execute_tool(tool_name=tool_call["name"], args=arguments))
        output_item = self.create_function_call_output_item(tool_call["call_id"], result)
        messages.append(output_item)
        return ResponsesAgentStreamEvent(type="response.output_item.done", item=output_item)

    def call_and_run_tools(self, messages: list[dict[str, Any]], max_iter: int) -> Generator[ResponsesAgentStreamEvent, None, None]:
        for _ in range(max_iter):
            last = messages[-1]
            if last.get("role") == "assistant":
                return
            if last.get("type") == "function_call":
                yield self.handle_tool_call(last, messages)
            else:
                yield from output_to_responses_items_stream(chunks=self.call_llm(messages), aggregator=messages)
        raise RuntimeError(f"Agent max iterations reached ({max_iter}).")

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [event.item for event in self.predict_stream(request) if event.type == "response.output_item.done"]
        return ResponsesAgentResponse(output=outputs, custom_outputs=request.custom_inputs)

    def predict_stream(self, request: ResponsesAgentRequest) -> Generator[ResponsesAgentStreamEvent, None, None]:
        messages = to_chat_completions_input([item.model_dump() for item in request.input])
        messages.insert(0, {"role": "system", "content": SYSTEM_PROMPT_STAGE4})
        yield from self.call_and_run_tools(messages, AGENT_MAX_ITERATIONS)


try:
    mlflow.openai.autolog()
except Exception:
    pass

AGENT = Stage4ToolCallingAgent(LLM_ENDPOINT_NAME, TOOL_INFOS)
mlflow.models.set_model(AGENT)
