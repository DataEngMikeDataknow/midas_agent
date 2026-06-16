import json
from typing import Any, Callable, Generator, Optional
from uuid import uuid4
import warnings

import backoff
import mlflow
import openai
from databricks.sdk import WorkspaceClient
from databricks_openai import UCFunctionToolkit, VectorSearchRetrieverTool
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

############################################
# Define your LLM endpoint and system prompt
############################################
LLM_ENDPOINT_NAME = "databricks-gpt-oss-120b"
#LLM_ENDPOINT_NAME = "databricks-gpt-5-2"

SYSTEM_PROMPT = """
### ROL Y CONTEXTO
Eres un Analista Senior de Calidad de Facturación para una empresa de servicios públicos (Acueducto y Alcantarillado). Tu objetivo es auditar órdenes de servicio siguiendo estrictamente un diagrama de flujo de decisión para determinar si requieren ajuste.

### TUS HERRAMIENTAS (SQL FUNCTIONS)
1. `get_hist_fact(order_id)`: Retorna el histórico de facturación y detalles de la orden.
2. `get_ordenes_critica(ss_orden, id_periodo_consumo_agua)`: Retorna novedades de crítica y órdenes de campo asociadas.

### PROTOCOLO DE PENSAMIENTO (LEAST-TO-MOST)
Sigue estos pasos secuenciales. No saltes ninguno.

**PASO 1: OBTENCIÓN Y PREPARACIÓN DE VARIABLES**
- Ejecuta `get_hist_fact(order_id)`.
- Define las siguientes variables internas (sin imprimirlas aún):
  - `VAR_ACUEDUCTO`: Consumo facturado de Acueducto.
  - `VAR_ALCANTARILLADO_TOTAL`: Consumo facturado total de Alcantarillado.
  - `VAR_ALCANTARILLADO_BASE`: Consumo facturado de Alcantarillado *excluyendo* los valores de los tipos 19 y 21.
  - `VAR_PLAN_FACTURACION_ACUEDUCTO`: Plan de facturación del acueducto, se toma del campo `plan_facturacion_orden` de la respuesta de la información que devuelve `get_hist_fact(order_id)`.
  - `VAR_PLAN_PR_PRODUCT_FACTURACION_ACUEDUCTO`:  Plan de facturación de pr_product del acueducto, se toma del campo `plan_facturacion_pr_product_orden` de la información que devuelve `get_hist_fact(order_id)`.
  - `VAR_PLAN_FACTURACION_ALCANTARILLADO`: Plan de facturación del alcantarillado, se toma del campo `plan_facturacion_alcantarillado` de la información que devuelve `get_hist_fact(order_id)`.
  - `VAR_PLAN_PR_PRODUCT_FACTURACION_ALCANTARILLADO`: Plan de facturación de pr_product del alcantarillado, se toma del campo `plan_facturacion_pr_product_alcantarillado` de la información que devuelve `get_hist_fact(order_id)`.
  

**PASO 2: CLASIFICACIÓN DEL FLUJO (Nodo Raíz)**
- Verifica: ¿Existe algún registro con `tipo_consumo_facturado_alcantarillado` igual a **19** o **21**?
  - **SÍ:** Activa el modo **[RAMA A: FLUJO ESPECIAL]**.
  - **NO:** Activa el modo **[RAMA B: FLUJO ESTÁNDAR]**.

---

### [RAMA A: FLUJO ESPECIAL 19/21]
**Validación de Igualdad (Acueducto vs Base Alcantarillado):**
- Compara: ¿Es `VAR_ACUEDUCTO` **DIFERENTE (<>)** a `VAR_ALCANTARILLADO_BASE`?
  - **NO (Son IGUALES):**
    - DECISIÓN FINAL = **SIN AJUSTE**.
    - *Justificación:* El consumo de acueducto es igual al consumo base de alcantarillado (excluyendo tipos especiales). La diferencia total se justifica por la presencia del tipo de consumo 19/21.
    -> **FIN DEL PROCESO.**
  - **SÍ (Son DIFERENTES):**
    - Existe una inconsistencia no explicada por los tipos 19/21.
    -> **SALTA AL PASO 3 (VALIDACIÓN DE PLANES).**

---

### [RAMA B: FLUJO ESTÁNDAR]
**Validación de Igualdad (Acueducto vs Alcantarillado):**
- Compara: ¿Es `VAR_ACUEDUCTO` **DIFERENTE (<>)** a `VAR_ALCANTARILLADO_TOTAL`?
  - **NO (Son IGUALES):**
    - DECISIÓN FINAL = **SIN AJUSTE**.
    - *Justificación:* El consumo de acueducto ([X] m3) es igual al consumo de alcantarillado ([X] m3).
    -> **FIN DEL PROCESO.**
  - **SÍ (Son DIFERENTES):**
    - Continúa a la **Búsqueda de Justificación Operativa**.

**Búsqueda de Justificación Operativa (Errores y Crítica):**
- Ejecuta `get_ordenes_critica`.
- **Validación B.1:** ¿Existe comentario explícito sobre "Error de Lectura"?
  - **SÍ:** DECISIÓN FINAL = **CON AJUSTE**.
    *Justificación:* Se confirma error de lectura: "[Citar comentario]". Se debe igualar el consumo de acueducto y alcantarillado. Consumo del acueducto: ([X] m3) consumo del alcantarillado ([X] m3), informar el consumo de los tipos de consumo 19 o 21 con su descripción en caso de aplicar ([X] m3).
    -> **FIN DEL PROCESO.**
- **Validación B.2:** ¿Existe orden de crítica (Actividad 102010) que ajustó acueducto?
  - **SÍ:** DECISIÓN FINAL = **CON AJUSTE**.
    *Justificación:* Se identifica orden de crítica atendida por [analista]. Se debe igualar el consumo del alcantarillado al del acueducto. Consumo del acueducto: ([X] m3) consumo del alcantarillado ([X] m3), informar el consumo de los tipos de consumo 19 o 21 con su descripción en caso de aplicar ([X] m3)
    -> **FIN DEL PROCESO.**
- **Validación B.3:** ¿Existe orden decisión analista (Actividad 7400027) que ajustó acueducto?
  - **SÍ:** DECISIÓN FINAL = **CON AJUSTE**.
    *Justificación:* Se identifica orden de decisión de analista atendida por [analista]. Se debe igualar el consumo del alcantarillado al del acueducto. Consumo del acueducto: ([X] m3) consumo del alcantarillado ([X] m3), informar el consumo de los tipos de consumo 19 o 21 con su descripción en caso de aplicar ([X] m3)
    -> **FIN DEL PROCESO.**
  - **NO:**
    -> **SALTA AL PASO 3 (VALIDACIÓN DE PLANES).**

---

### PASO 3: VALIDACIÓN DE PLANES DE FACTURACIÓN
*Ejecuta esto SOLO si la lógica anterior te envió aquí.*

1. **Validación Plan vs Plan (Acue vs Alcan):**
   - Compara: ¿Es `VAR_PLAN_FACTURACION_ACUEDUCTO` **DIFERENTE (<>)** a `VAR_PLAN_FACTURACION_ALCANTARILLADO`?
   - **SÍ (Diferentes):** DECISIÓN FINAL = **CON AJUSTE**.
     *Justificación:* Planes de facturación inconsistentes. Acueducto: [VAR_PLAN_FACTURACION_ACUEDUCTO] vs Alcantarillado: [VAR_PLAN_FACTURACION_ALCANTARILLADO]. Consumo del acueducto: ([X] m3) consumo del alcantarillado ([X] m3), informar el consumo de los tipos de consumo 19 o 21 con su descripción en caso de aplicar ([X] m3).
     *NO INFORMAR EL FLUJO QUE SIGUIÓ, SOLO LO QUE SE ESPECIFICA EN LA JUSTIFICACIÓN*
     -> **FIN DEL PROCESO.**
   - **NO (Iguales):** Continúa.

2. **Validación Plan Facturado vs Pr_product:**
   - Compara: ¿Es `VAR_PLAN_FACTURACION_ALCANTARILLADO` **DIFERENTE (<>)** a `VAR_PLAN_PR_PRODUCT_FACTURACION_ALCANTARILLADO`?
   - **SÍ (Diferentes):** DECISIÓN FINAL = **CON AJUSTE**.
     *Justificación:* Plan de facturación del alcantarillado ([VAR_PLAN_FACTURACION_ALCANTARILLADO]) difiere del plan de pr_product del alcantarillado ([VAR_PLAN_PR_PRODUCT_FACTURACION_ALCANTARILLADO]). Consumo del acueducto: ([X] m3) consumo del alcantarillado ([X] m3), informar el consumo de los tipos de consumo 19 o 21 con su descripción en caso de aplicar ([X] m3).
     *NO INFORMAR EL FLUJO QUE SIGUIÓ, SOLO LO QUE SE ESPECIFICA EN LA JUSTIFICACIÓN*
     -> **FIN DEL PROCESO.**
   - **NO (Iguales):** Continúa.

3. **Fallo de Lógica (Catch-all):**
   - Si llegaste hasta aquí: Los consumos son diferentes y los planes son iguales.
   - DECISIÓN FINAL = **INVESTIGACIÓN MANUAL REQUERIDA**.
   - *Justificación:* Diferencia de consumos sin justificación. Planes de facturación coinciden. Consumo del acueducto: ([X] m3) consumo del alcantarillado ([X] m3), informar el consumo de los tipos de consumo 19 o 21 con su descripción en caso de aplicar ([X] m3). Los planes de facturación están correctos.

---

### FORMATO DE SALIDA (JSON)
Genera únicamente este JSON.

```json
{
  "order_id": "{{order_id}}",
  "decision": "CON AJUSTE | SIN AJUSTE | INVESTIGACIÓN MANUAL REQUERIDA",
  "justification": "Texto limpio."
}
"""

###############################################################################
## Define tools for your agent, enabling it to retrieve data or take actions
## beyond text generation
## To create and see usage examples of more tools, see
## https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/agent-tool
###############################################################################
class ToolInfo(BaseModel):
    """
    Class representing a tool for the agent.
    - "name" (str): The name of the tool.
    - "spec" (dict): JSON description of the tool (matches OpenAI Responses format)
    - "exec_fn" (Callable): Function that implements the tool logic
    """

    name: str
    spec: dict
    exec_fn: Callable


def create_tool_info(tool_spec, exec_fn_param: Optional[Callable] = None):
    tool_spec["function"].pop("strict", None)
    tool_name = tool_spec["function"]["name"]
    
    if tool_name.startswith("on__"):
        tool_name = tool_name.replace("on__", "facturacion__", 1)
        tool_spec["function"]["name"] = tool_name

    udf_name = tool_name.replace("__", ".")

    udf_name_parts = udf_name.split('.')
    CATALOG_NAME = "epm_datalabs_catalog_dllo" 

    if len(udf_name_parts) == 2:
        udf_name = f"{CATALOG_NAME}.{udf_name}"
    elif len(udf_name_parts) != 3:
        raise ValueError(
            f"Error al procesar el nombre de la herramienta '{tool_name}'. "
            f"Se convirtió en '{udf_name}', que no tiene 2 o 3 partes."
        )

    def exec_fn(**kwargs):
        function_result = uc_function_client.execute_function(udf_name, kwargs)
        if function_result.error is not None:
            return function_result.error
        else:
            return function_result.value
            
    # Ahora 'name' y 'spec' están sincronizados
    return ToolInfo(name=tool_name, spec=tool_spec, exec_fn=exec_fn_param or exec_fn)


TOOL_INFOS = []

# You can use UDFs in Unity Catalog as agent tools
# TODO: Add additional tools
UC_TOOL_NAMES = [
    "epm_datalabs_catalog_dllo.facturacion.get_hist_fact",
    "epm_datalabs_catalog_dllo.facturacion.get_ordenes_critica"
]

uc_toolkit = UCFunctionToolkit(function_names=UC_TOOL_NAMES)
uc_function_client = get_uc_function_client()
for tool_spec in uc_toolkit.tools:
    TOOL_INFOS.append(create_tool_info(tool_spec))


# Use Databricks vector search indexes as tools
# See [docs](https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/unstructured-retrieval-tools) for details

# # (Optional) Use Databricks vector search indexes as tools
# # See https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/unstructured-retrieval-tools
# # for details
VECTOR_SEARCH_TOOLS = []
# # TODO: Add vector search indexes as tools or delete this block
# VECTOR_SEARCH_TOOLS.append(
#         VectorSearchRetrieverTool(
#         index_name="",
#         # filters="..."
#     )
# )
for vs_tool in VECTOR_SEARCH_TOOLS:
    TOOL_INFOS.append(create_tool_info(vs_tool.tool, vs_tool.execute))

class ToolCallingAgent(ResponsesAgent):
    """
    Class representing a tool-calling Agent
    """

    def __init__(self, llm_endpoint: str, tools: list[ToolInfo]):
        """Initializes the ToolCallingAgent with tools."""
        self.llm_endpoint = llm_endpoint
        self.workspace_client = WorkspaceClient()
        self.model_serving_client: OpenAI = (
            self.workspace_client.serving_endpoints.get_open_ai_client()
        )
        self._tools_dict = {tool.name: tool for tool in tools}

    def get_tool_specs(self) -> list[dict]:
        """Returns tool specifications in the format OpenAI expects."""
        return [tool_info.spec for tool_info in self._tools_dict.values()]

    @mlflow.trace(span_type=SpanType.TOOL)
    def execute_tool(self, tool_name: str, args: dict) -> Any:
        """Executes the specified tool with the given arguments."""
        return self._tools_dict[tool_name].exec_fn(**args)

    def call_llm(self, messages: list[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="PydanticSerializationUnexpectedValue")
            for chunk in self.model_serving_client.chat.completions.create(
                model=self.llm_endpoint,
                messages=to_chat_completions_input(messages),
                tools=self.get_tool_specs(),
                stream=True,
                temperature=0.0
            ):
                chunk_dict = chunk.to_dict()
                if len(chunk_dict.get("choices", [])) > 0:
                    yield chunk_dict

    def handle_tool_call(
        self,
        tool_call: dict[str, Any],
        messages: list[dict[str, Any]],
    ) -> ResponsesAgentStreamEvent:
        """
        Execute tool calls, add them to the running message history, and return a ResponsesStreamEvent w/ tool output
        """
        arguments_str = tool_call.get("arguments")

        if not arguments_str:
            args = {}
        else:
            args = json.loads(arguments_str)

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
            if last_msg.get("role", None) == "assistant":
                return
            elif last_msg.get("type", None) == "function_call":
                yield self.handle_tool_call(last_msg, messages)
            else:
                yield from output_to_responses_items_stream(
                    chunks=self.call_llm(messages), aggregator=messages
                )

        yield ResponsesAgentStreamEvent(
            type="response.output_item.done",
            item=self.create_text_output_item("Max iterations reached. Stopping.", str(uuid4())),
        )

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
        yield from self.call_and_run_tools(messages=messages)


# Log the model using MLflow
mlflow.openai.autolog()
AGENT = ToolCallingAgent(llm_endpoint=LLM_ENDPOINT_NAME, tools=TOOL_INFOS)
mlflow.models.set_model(AGENT)
