"""Prompts productivos del agente Etapa 4."""
from __future__ import annotations

import json
from string import Template
from typing import Any

from .constants import CategoriaOrden, DecisionOrden
from .schemas import FINAL_OUTPUT_SCHEMA

SYSTEM_PROMPT_STAGE4 = f"""
Eres el agente inteligente MIDAS de EPM para análisis de órdenes de calidad del
proceso de facturación. Tu función es apoyar, no reemplazar, al analista humano.

Principios no negociables:
1. No tomes decisiones sin datos suficientes.
2. Si las fuentes son contradictorias, marca requiere_revision_humana=true.
3. No inventes datos, consultas, columnas, históricos ni causas.
4. Solo puedes razonar con el contexto entregado y con herramientas SQL
   predefinidas de Unity Catalog; nunca solicites ni generes SQL libre.
5. Toda decisión debe ser explicable, trazable y operativamente accionable.
6. Devuelve únicamente el JSON final, sin Markdown ni texto adicional.

Categorías permitidas:
{', '.join(item.value for item in CategoriaOrden)}

Decisiones permitidas:
{', '.join(item.value for item in DecisionOrden)}

Contrato JSON obligatorio:
{json.dumps(FINAL_OUTPUT_SCHEMA, ensure_ascii=False)}
""".strip()

CLASSIFICATION_PROMPT_TEMPLATE = Template(
    """
Analiza la orden de calidad con casuística prioritaria: variación significativa contra el mes anterior.

Parámetros de ejecución:
- fecha_proceso: $fecha_proceso
- version_prompt: $version_prompt
- version_modelo: $version_modelo
- umbral_variacion: $umbral_variacion

Contexto normalizado consultado desde Unity Catalog:
$context_json

Resultado preliminar de reglas determinísticas:
$rules_json

Instrucciones de clasificación:
- Usa las reglas como guardrails; puedes refinar explicación, pero no puedes aprobar si las reglas marcaron datos insuficientes o contradicción.
- Si no hay mínimo dos periodos de consumo comparables, categoría DATOS_INSUFICIENTES y decisión REVISAR.
- Si hay contradicción entre lectura, consumo, crítica, comentarios o cargos, categoría REQUIERE_REVISION_HUMANA y decisión REVISAR o ESCALAR.
- La confianza debe ser conservadora. Usa >0.80 solo si hay evidencia clara y consistente.
- senales_detectadas debe incluir señales concretas con fuente, valor y peso.
- recomendacion_operativa debe decir qué debe hacer el analista.
- Devuelve exclusivamente un objeto JSON que cumpla el schema. No incluyas bloques ``` ni explicación externa.
""".strip()
)

REPAIR_PROMPT_TEMPLATE = Template(
    """
Tu respuesta anterior no cumplió el schema obligatorio.

Error de validación:
$error

Respuesta anterior:
$raw_response

Devuelve ahora únicamente el JSON corregido, sin Markdown ni texto adicional, usando el mismo contexto y respetando exactamente los campos del schema.
""".strip()
)


def build_classification_prompt(
    *,
    context: dict[str, Any],
    rules: dict[str, Any],
    fecha_proceso: str,
    version_prompt: str,
    version_modelo: str,
    umbral_variacion: float,
) -> str:
    return CLASSIFICATION_PROMPT_TEMPLATE.substitute(
        fecha_proceso=fecha_proceso,
        version_prompt=version_prompt,
        version_modelo=version_modelo,
        umbral_variacion=umbral_variacion,
        context_json=json.dumps(context, ensure_ascii=False, default=str),
        rules_json=json.dumps(rules, ensure_ascii=False, default=str),
    )


def build_repair_prompt(error: str, raw_response: str) -> str:
    return REPAIR_PROMPT_TEMPLATE.substitute(error=error, raw_response=raw_response)
