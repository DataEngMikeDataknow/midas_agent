"""Constantes de Etapa 4: agente inteligente para órdenes de calidad.

Este módulo concentra nombres de tablas, query_keys y taxonomía de salida para
mantener una sola fuente de verdad entre Data Access, tools SQL, validación y
persistencia.
"""
from __future__ import annotations

from enum import Enum
from typing import Final


class QueryKey(str, Enum):
    ORDENES_PENDIENTES = "QUERY_ORDENES_PENDIENTES"
    DATOS_BASICOS = "QUERY_DATOS_BASICOS"
    DATOS_LECTURA = "QUERY_DATOS_LECTURA"
    DATOS_CONSUMOS = "QUERY_DATOS_CONSUMOS"
    ORDENES_CRITICA_PREVIA = "QUERY_ORDENES_CRITICA_PREVIA"
    COMENTARIOS_ORDENES = "QUERY_COMENTARIOS_ORDENES"
    CUENTAS_COBRO = "QUERY_CUENTAS_COBRO"
    DETALLE_CARGOS = "QUERY_DETALLE_CARGOS"


class CategoriaOrden(str, Enum):
    NORMAL = "NORMAL"
    VARIACION_SIGNIFICATIVA_JUSTIFICADA = "VARIACION_SIGNIFICATIVA_JUSTIFICADA"
    VARIACION_SIGNIFICATIVA_NO_JUSTIFICADA = "VARIACION_SIGNIFICATIVA_NO_JUSTIFICADA"
    CONSTANTE_MAL_CONFIGURADA = "CONSTANTE_MAL_CONFIGURADA"
    POSIBLE_ERROR_LECTURA = "POSIBLE_ERROR_LECTURA"
    POSIBLE_PNO = "POSIBLE_PNO"
    ESTACIONALIDAD = "ESTACIONALIDAD"
    OBRA_NUEVA_O_CAMBIO_INSTALACION = "OBRA_NUEVA_O_CAMBIO_INSTALACION"
    RECLAMO_RELACIONADO = "RECLAMO_RELACIONADO"
    DATOS_INSUFICIENTES = "DATOS_INSUFICIENTES"
    REQUIERE_REVISION_HUMANA = "REQUIERE_REVISION_HUMANA"


class DecisionOrden(str, Enum):
    APROBAR = "APROBAR"
    RECHAZAR = "RECHAZAR"
    REVISAR = "REVISAR"
    ESCALAR = "ESCALAR"


class PesoSenal(str, Enum):
    ALTO = "ALTO"
    MEDIO = "MEDIO"
    BAJO = "BAJO"


DEFAULT_PROMPT_VERSION: Final[str] = "stage4-993-v2.0.0"
DEFAULT_MODEL_VERSION: Final[str] = "databricks-serving-endpoint"
DEFAULT_VARIATION_THRESHOLD: Final[float] = 30.0
DEFAULT_MAX_CONTEXT_RECORDS: Final[int] = 12

# Tablas Bronze entregadas por el catálogo de control de cargas. El nombre de
# comentarios corrige el typo histórico `cometarios`; data_access incluye
# fallback para ambientes donde aún exista la variante legacy.
TABLE_BY_QUERY_KEY: Final[dict[QueryKey, str]] = {
    QueryKey.ORDENES_PENDIENTES: "midas_ordenes_calidad_pendientes_silver",
    QueryKey.DATOS_BASICOS: "midas_datos_basicos_producto_silver",
    QueryKey.DATOS_LECTURA: "midas_datos_lecturas_producto_bronze",
    QueryKey.DATOS_CONSUMOS: "midas_datos_consumos_producto_bronze",
    QueryKey.ORDENES_CRITICA_PREVIA: "midas_historial_critica_silver",
    QueryKey.COMENTARIOS_ORDENES: "midas_datos_comentarios_ordenes_bronze",
    QueryKey.CUENTAS_COBRO: "midas_datos_cuentas_cobro_bronze",
    QueryKey.DETALLE_CARGOS: "midas_datos_detalle_cargos_bronze",
}

LEGACY_TABLE_FALLBACKS: Final[dict[str, tuple[str, ...]]] = {
    "midas_datos_comentarios_ordenes_bronze": ("midas_datos_cometarios_ordenes_bronze",),
}

STAGE4_SQL_FUNCTION_NAMES: Final[tuple[str, ...]] = (
    "midas_stage4_get_orden_pendiente",
    "midas_stage4_get_datos_basicos",
    "midas_stage4_get_lecturas",
    "midas_stage4_get_consumos",
    "midas_stage4_get_critica_previa",
    "midas_stage4_get_comentarios",
    "midas_stage4_get_cuentas_cobro",
    "midas_stage4_get_detalle_cargos",
)

STAGE4_ACTIVITY_993_CODE: Final[str] = "993"
STAGE4_ACTIVITY_993_LABEL: Final[str] = "993 - VARIACION SIGNIFICATIVA CONTRA EL MES ANTERIOR"
DEFAULT_CONTEXT_TABLE_993: Final[str] = "midas_contexto_agente_993_v0"
DEFAULT_AGENT_INPUT_TABLE_993: Final[str] = "midas_agent_input_993_v0"

OUTPUT_RESULT_TABLE = "midas_resultado_agente_ordenes_calidad_gold"
OUTPUT_LOG_TABLE = "midas_logs_agente_ordenes_calidad"
OUTPUT_METRICS_TABLE = "midas_metricas_agente_ordenes_calidad"
OUTPUT_EVALUATION_TABLE = "midas_evaluacion_agente_ordenes_calidad"
