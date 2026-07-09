"""Construcción del contexto real de entrada para Etapa 4.

Este módulo materializa el mapa de datos validado en Databricks para la
casuística prioritaria 993 - Variación significativa contra el mes anterior.
El agente no consulta tablas crudas libremente: consume un contrato estructurado
construido desde lecturas, consumos, crítica y solicitudes.
"""
from __future__ import annotations

from typing import Any

try:  # pragma: no cover - Databricks runtime
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
except Exception:  # pragma: no cover
    DataFrame = Any  # type: ignore
    SparkSession = Any  # type: ignore
    F = None  # type: ignore
    Window = None  # type: ignore

from .constants import (
    DEFAULT_CONTEXT_TABLE_993,
    DEFAULT_AGENT_INPUT_TABLE_993,
    STAGE4_ACTIVITY_993_CODE,
)
from .data_access import full_table_name, validate_identifier


class Stage4ContextBuilder:
    """Crea el contrato de entrada del agente usando las tablas reales de facturación.

    Decisiones incorporadas del análisis de datos:
    - Entrada: midas_ordenes_calidad_pendientes_silver.
    - Casuística inicial: actividad que contiene "993".
    - Contexto principal: midas_datos_lecturas_producto_bronze.
    - Llave técnica: servicio_suscrito.
    - Granularidad del consumo: servicio_suscrito + tipo_consumo + periodo.
    - No se envían PII al JSON del agente.
    """

    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        self.spark = spark
        self.catalog = validate_identifier(catalog, "catalog")
        self.schema = validate_identifier(schema, "schema")

    def table_name(self, table: str) -> str:
        return full_table_name(self.catalog, self.schema, table)

    def table(self, table: str) -> DataFrame:
        return self.spark.table(self.table_name(table))

    def build_context_993_df(
        self,
        *,
        actividad_code: str = STAGE4_ACTIVITY_993_CODE,
        include_pii: bool = False,
        consumo_extremo_threshold: float = 1_000_000.0,
    ) -> DataFrame:
        """Devuelve una fila por orden 993 con el contexto limpio del agente.

        `include_pii` existe por trazabilidad operativa, pero por defecto es False
        para evitar enviar nombre, identificación, dirección o página al LLM.
        """
        ordenes = self.table("midas_ordenes_calidad_pendientes_silver")
        lecturas = self.table("midas_datos_lecturas_producto_bronze")
        solicitudes = self.table("midas_datos_detalle_solicitudes_silver")
        critica = self.table("midas_historial_critica_silver")

        order_cols = [
            "id_orden",
            "servicio_suscrito",
            "instalacion",
            "contrato",
            "servicio",
            "actividad",
            "estado_orden",
            "comentario_orden",
            "categoria",
            "subcategoria",
            "ciclo",
            "localidad",
            "estado_corte",
            "saldo_pendiente",
            "cuentas_vencidas",
            "saldo_vencido",
        ]
        if include_pii:
            order_cols.extend(["nombre_cliente", "identificacion", "direccion", "pagina"])

        ordenes_993 = (
            ordenes
            .filter(F.col("actividad").contains(str(actividad_code)))
            .select(*[c for c in order_cols if c in ordenes.columns])
        )
        servicios_993 = ordenes_993.select("servicio_suscrito").distinct()
        lecturas_993 = lecturas.join(servicios_993, "servicio_suscrito", "inner")

        lecturas_scored = (
            lecturas_993
            .withColumn("constante_num", F.col("constante").cast("double"))
            .withColumn(
                "score_completitud",
                F.when(F.col("lectura_anterior").isNotNull(), 1).otherwise(0)
                + F.when(F.col("lectura_actual").isNotNull(), 1).otherwise(0)
                + F.when(F.col("consumo_calculado").isNotNull(), 1).otherwise(0)
                + F.when(F.col("consumo_facturado").isNotNull(), 1).otherwise(0)
                + F.when(F.col("limite_inferior").isNotNull(), 1).otherwise(0)
                + F.when(F.col("limite_superior").isNotNull(), 1).otherwise(0)
            )
            .withColumn(
                "flag_consumo_extremo",
                (
                    (F.abs(F.col("consumo_facturado")) > F.lit(consumo_extremo_threshold))
                    | (F.abs(F.col("consumo_calculado")) > F.lit(consumo_extremo_threshold))
                ),
            )
        )

        # Deduplicación: una sola lectura por servicio + tipo + periodo; preferir fila más completa.
        w_best_lectura = Window.partitionBy(
            "servicio_suscrito",
            "tipo_consumo",
            "id_periodo_consumo",
            "id_periodo_facturacion",
        ).orderBy(
            F.col("score_completitud").desc_nulls_last(),
            F.col("consumo_facturado").desc_nulls_last(),
        )

        lecturas_unicas = (
            lecturas_scored
            .withColumn("rn_best_lectura", F.row_number().over(w_best_lectura))
            .filter(F.col("rn_best_lectura") == 1)
        )

        w_periodo_tipo = Window.partitionBy("servicio_suscrito", "tipo_consumo").orderBy(
            F.col("id_periodo_facturacion").desc_nulls_last(),
            F.col("id_periodo_consumo").desc_nulls_last(),
        )
        lecturas_ranked = lecturas_unicas.withColumn("rn_periodo", F.row_number().over(w_periodo_tipo))

        lectura_actual_tipo = (
            lecturas_ranked
            .filter(F.col("rn_periodo") == 1)
            .select(
                "servicio_suscrito",
                "tipo_consumo",
                F.col("id_periodo_consumo").alias("id_periodo_consumo_actual"),
                F.col("id_periodo_facturacion").alias("id_periodo_facturacion_actual"),
                "fecha_ini_consumo",
                "fecha_fin_consumo",
                "dias_consumo",
                "medidor",
                "constante",
                "constante_num",
                "digitos_medidor",
                "lectura_anterior",
                "lectura_actual",
                F.col("consumo_calculado").alias("consumo_calculado_actual"),
                F.col("consumo_facturado").alias("consumo_facturado_actual"),
                "limite_inferior",
                "limite_superior",
                "observacion_lectura",
                "observacion_lectura_2",
                "observacion_lectura_3",
                "pno",
                "score_completitud",
                "flag_consumo_extremo",
            )
        )

        lectura_anterior_tipo = (
            lecturas_ranked
            .filter(F.col("rn_periodo") == 2)
            .select(
                "servicio_suscrito",
                "tipo_consumo",
                F.col("id_periodo_consumo").alias("id_periodo_consumo_anterior"),
                F.col("id_periodo_facturacion").alias("id_periodo_facturacion_anterior"),
                F.col("consumo_facturado").alias("consumo_facturado_anterior"),
            )
        )

        promedio_6m_tipo = (
            lecturas_ranked
            .filter((F.col("rn_periodo") >= 2) & (F.col("rn_periodo") <= 7))
            .groupBy("servicio_suscrito", "tipo_consumo")
            .agg(
                F.avg("consumo_facturado").alias("promedio_consumo_facturado_6m"),
                F.count("*").alias("n_periodos_promedio_6m"),
                F.min("consumo_facturado").alias("consumo_min_6m"),
                F.max("consumo_facturado").alias("consumo_max_6m"),
            )
        )

        variaciones_lectura_tipo = (
            lectura_actual_tipo
            .join(lectura_anterior_tipo, ["servicio_suscrito", "tipo_consumo"], "left")
            .join(promedio_6m_tipo, ["servicio_suscrito", "tipo_consumo"], "left")
            .withColumn(
                "variacion_pct_mes_anterior",
                F.when(
                    F.col("consumo_facturado_anterior").isNotNull()
                    & (F.col("consumo_facturado_anterior") != 0),
                    F.round(
                        ((F.col("consumo_facturado_actual") - F.col("consumo_facturado_anterior"))
                         / F.col("consumo_facturado_anterior")) * 100,
                        2,
                    ),
                ),
            )
            .withColumn(
                "variacion_pct_promedio_6m",
                F.when(
                    F.col("promedio_consumo_facturado_6m").isNotNull()
                    & (F.col("promedio_consumo_facturado_6m") != 0),
                    F.round(
                        ((F.col("consumo_facturado_actual") - F.col("promedio_consumo_facturado_6m"))
                         / F.col("promedio_consumo_facturado_6m")) * 100,
                        2,
                    ),
                ),
            )
            .withColumn(
                "diferencia_lectura",
                F.when(
                    F.col("lectura_actual").isNotNull() & F.col("lectura_anterior").isNotNull(),
                    F.col("lectura_actual") - F.col("lectura_anterior"),
                ),
            )
            .withColumn(
                "consumo_esperado_por_lectura",
                F.when(
                    F.col("diferencia_lectura").isNotNull() & F.col("constante_num").isNotNull(),
                    F.col("diferencia_lectura") * F.col("constante_num"),
                ),
            )
            .withColumn(
                "delta_consumo_calculado_vs_lectura",
                F.when(
                    F.col("consumo_calculado_actual").isNotNull()
                    & F.col("consumo_esperado_por_lectura").isNotNull(),
                    F.abs(F.col("consumo_calculado_actual") - F.col("consumo_esperado_por_lectura")),
                ),
            )
            .withColumn(
                "flag_fuera_limites",
                F.col("consumo_facturado_actual").isNotNull()
                & F.col("limite_inferior").isNotNull()
                & F.col("limite_superior").isNotNull()
                & (
                    (F.col("consumo_facturado_actual") < F.col("limite_inferior"))
                    | (F.col("consumo_facturado_actual") > F.col("limite_superior"))
                ),
            )
            .withColumn(
                "flag_lectura_inconsistente",
                F.when(
                    F.col("delta_consumo_calculado_vs_lectura").isNotNull()
                    & (F.col("delta_consumo_calculado_vs_lectura") > 0.01),
                    F.lit(True),
                ).otherwise(F.lit(False)),
            )
            .withColumn("abs_variacion_mes_anterior", F.abs(F.col("variacion_pct_mes_anterior")))
            .withColumn("abs_variacion_promedio_6m", F.abs(F.col("variacion_pct_promedio_6m")))
        )

        variaciones_con_orden = (
            ordenes_993.select("servicio_suscrito", "servicio")
            .join(variaciones_lectura_tipo, "servicio_suscrito", "left")
            .withColumn(
                "tipo_relevante_para_servicio",
                F.when(
                    F.upper(F.col("servicio")).contains("ENERG")
                    & F.upper(F.col("tipo_consumo")).contains("ACTIVA")
                    & (~F.upper(F.col("tipo_consumo")).contains("REACTIVA")),
                    F.lit(1),
                )
                .when(
                    F.upper(F.col("servicio")).contains("AGUA")
                    & F.upper(F.col("tipo_consumo")).contains("AGUA"),
                    F.lit(1),
                )
                .when(
                    F.upper(F.col("servicio")).contains("ALCANTARILLADO")
                    & F.upper(F.col("tipo_consumo")).contains("AGUA"),
                    F.lit(1),
                )
                .when(
                    F.upper(F.col("servicio")).contains("GAS")
                    & F.upper(F.col("tipo_consumo")).contains("GAS"),
                    F.lit(1),
                )
                .otherwise(F.lit(0)),
            )
        )

        w_tipo_principal = Window.partitionBy("servicio_suscrito").orderBy(
            F.col("tipo_relevante_para_servicio").desc_nulls_last(),
            F.col("abs_variacion_mes_anterior").desc_nulls_last(),
            F.col("abs_variacion_promedio_6m").desc_nulls_last(),
        )
        tipo_principal = (
            variaciones_con_orden
            .withColumn("rn_tipo_principal", F.row_number().over(w_tipo_principal))
            .filter(F.col("rn_tipo_principal") == 1)
            .drop("servicio")
        )

        consumos_por_tipo = (
            variaciones_lectura_tipo
            .groupBy("servicio_suscrito")
            .agg(
                F.collect_list(
                    F.struct(
                        "tipo_consumo",
                        "consumo_facturado_actual",
                        "consumo_facturado_anterior",
                        "promedio_consumo_facturado_6m",
                        "variacion_pct_mes_anterior",
                        "variacion_pct_promedio_6m",
                        "flag_consumo_extremo",
                        "flag_fuera_limites",
                        "flag_lectura_inconsistente",
                        "id_periodo_consumo_actual",
                        "id_periodo_consumo_anterior",
                    )
                ).alias("consumos_por_tipo")
            )
        )

        solicitudes_agg = (
            solicitudes
            .join(servicios_993, "servicio_suscrito", "inner")
            .groupBy("servicio_suscrito")
            .agg(
                F.count("*").alias("total_solicitudes"),
                F.collect_list(
                    F.struct(
                        "id_solicitud",
                        "tipo_solicitud",
                        "estado_solicitud",
                        "fecha_solicitud",
                        "fecha_atencion_solicitud",
                        "comentario",
                    )
                ).alias("solicitudes"),
            )
        )

        critica_agg = (
            critica
            .join(servicios_993, "servicio_suscrito", "inner")
            .groupBy("servicio_suscrito")
            .agg(
                F.count("*").alias("total_criticas"),
                F.collect_list(
                    F.struct(
                        F.col("id_orden").alias("id_orden_critica"),
                        "tipo_trabajo",
                        "actividad",
                        "estado",
                        "fecha_creacion_orden",
                        "fecha_legalizacion_orden",
                        "analista_legaliza",
                        "lista_comentarios",
                    )
                ).alias("criticas"),
            )
        )

        contexto = (
            ordenes_993
            .join(tipo_principal, "servicio_suscrito", "left")
            .join(consumos_por_tipo, "servicio_suscrito", "left")
            .join(solicitudes_agg, "servicio_suscrito", "left")
            .join(critica_agg, "servicio_suscrito", "left")
            .withColumn("flag_consumo_extremo", F.coalesce(F.col("flag_consumo_extremo"), F.lit(False)))
            .withColumn("flag_fuera_limites", F.coalesce(F.col("flag_fuera_limites"), F.lit(False)))
            .withColumn("flag_lectura_inconsistente", F.coalesce(F.col("flag_lectura_inconsistente"), F.lit(False)))
            .withColumn("total_solicitudes", F.coalesce(F.col("total_solicitudes"), F.lit(0)))
            .withColumn("total_criticas", F.coalesce(F.col("total_criticas"), F.lit(0)))
            .withColumn("tiene_pno", F.when(F.col("pno").isNotNull() & (F.trim(F.col("pno")) != ""), F.lit(True)).otherwise(F.lit(False)))
            .withColumn("tiene_solicitudes", F.col("total_solicitudes") > 0)
            .withColumn("tiene_criticas", F.col("total_criticas") > 0)
            .withColumn(
                "existe_consumo_extremo_en_algun_tipo",
                F.exists(F.col("consumos_por_tipo"), lambda x: x["flag_consumo_extremo"] == F.lit(True)),
            )
            .withColumn(
                "existe_fuera_limites_en_algun_tipo",
                F.exists(F.col("consumos_por_tipo"), lambda x: x["flag_fuera_limites"] == F.lit(True)),
            )
            .withColumn(
                "requiere_revision_por_calidad_dato",
                (F.col("flag_consumo_extremo") == True)
                | (F.col("flag_lectura_inconsistente") == True)
                | (F.col("existe_consumo_extremo_en_algun_tipo") == True),
            )
        )
        return contexto

    def build_agent_input_df(self, contexto_df: DataFrame) -> DataFrame:
        """Crea la tabla de JSON de entrada, libre de PII por construcción."""
        return (
            contexto_df
            .select(
                "id_orden",
                "servicio_suscrito",
                "actividad",
                "tipo_consumo",
                F.to_json(
                    F.struct(
                        F.struct(
                            "id_orden",
                            "servicio_suscrito",
                            "contrato",
                            "instalacion",
                            "servicio",
                            "actividad",
                            "estado_orden",
                            "categoria",
                            "subcategoria",
                            "ciclo",
                            "localidad",
                            "estado_corte",
                        ).alias("orden"),
                        F.struct(
                            "tipo_consumo",
                            "id_periodo_consumo_actual",
                            "id_periodo_consumo_anterior",
                            "id_periodo_facturacion_actual",
                            "id_periodo_facturacion_anterior",
                            "consumo_facturado_actual",
                            "consumo_facturado_anterior",
                            "promedio_consumo_facturado_6m",
                            "n_periodos_promedio_6m",
                            "consumo_min_6m",
                            "consumo_max_6m",
                            "variacion_pct_mes_anterior",
                            "variacion_pct_promedio_6m",
                            "limite_inferior",
                            "limite_superior",
                            "flag_fuera_limites",
                        ).alias("consumo_principal"),
                        F.struct(
                            "lectura_anterior",
                            "lectura_actual",
                            "diferencia_lectura",
                            "consumo_calculado_actual",
                            "consumo_esperado_por_lectura",
                            "constante",
                            "pno",
                            "tiene_pno",
                            "observacion_lectura",
                            "observacion_lectura_2",
                            "flag_lectura_inconsistente",
                        ).alias("lectura"),
                        F.struct(
                            "score_completitud",
                            "flag_consumo_extremo",
                            "existe_consumo_extremo_en_algun_tipo",
                            "flag_lectura_inconsistente",
                            "requiere_revision_por_calidad_dato",
                        ).alias("calidad_dato"),
                        F.struct(
                            "total_solicitudes",
                            "total_criticas",
                            "tiene_solicitudes",
                            "tiene_criticas",
                            "solicitudes",
                            "criticas",
                        ).alias("antecedentes"),
                        F.struct("consumos_por_tipo").alias("evidencia_adicional"),
                    )
                ).alias("agent_input_json"),
            )
        )

    def save_context_993(
        self,
        *,
        context_table: str = DEFAULT_CONTEXT_TABLE_993,
        agent_input_table: str = DEFAULT_AGENT_INPUT_TABLE_993,
        mode: str = "overwrite",
    ) -> tuple[str, str]:
        validate_identifier(context_table, "context_table")
        validate_identifier(agent_input_table, "agent_input_table")
        contexto_df = self.build_context_993_df()
        agent_input_df = self.build_agent_input_df(contexto_df)
        contexto_df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(
            self.table_name(context_table)
        )
        agent_input_df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(
            self.table_name(agent_input_table)
        )
        return self.table_name(context_table), self.table_name(agent_input_table)
