# Databricks notebook source
# MAGIC %md
# MAGIC # Validacion del framework de control de cargas (esquema real)
# MAGIC
# MAGIC Ejecutar tras cada corrida de `midas_load_transform_data`.
# MAGIC Estructura real: el estado vive en midas_log_cargas (no en control).

# COMMAND ----------
# Cambiar segun ambiente:
#   DEV: epm_datalabs_catalog_dllo | UAT: epm_datalake_catalog_np | PROD: epm_datalake_catalog_prod
CATALOG = "epm_datalabs_catalog_dllo"
SCHEMA  = "facturacion"

# COMMAND ----------
# MAGIC %md ## 1. Configuracion: las 8 filas FULL_CHAINED en control

# COMMAND ----------
display(spark.sql(f"""
    SELECT id_carga, tabla_destino, tipo_carga, query_key,
           orden_ejecucion, query_padre_id, activa
    FROM {CATALOG}.{SCHEMA}.midas_control_cargas
    WHERE tipo_carga = 'FULL_CHAINED'
    ORDER BY orden_ejecucion
"""))

# COMMAND ----------
# MAGIC %md ## 2. Ultima corrida (run_id mas reciente)

# COMMAND ----------
ult = spark.sql(f"""
    SELECT run_id
    FROM {CATALOG}.{SCHEMA}.midas_log_cargas
    ORDER BY fecha_inicio DESC
    LIMIT 1
""").collect()
run_id = ult[0]["run_id"] if ult else None
print("Ultimo run_id:", run_id)

# COMMAND ----------
if run_id:
    display(spark.sql(f"""
        SELECT id_carga, tabla_destino, query_key, estado,
               fecha_inicio, fecha_fin, duracion_segundos,
               filas_leidas, filas_escritas,
               LEFT(COALESCE(mensaje_error, ''), 120) AS error
        FROM {CATALOG}.{SCHEMA}.midas_log_cargas
        WHERE run_id = '{run_id}'
        ORDER BY fecha_inicio, tabla_destino
    """))

# COMMAND ----------
# MAGIC %md ## 3. Estado derivado por tabla (ultima fila de log)

# COMMAND ----------
display(spark.sql(f"""
    WITH ult AS (
        SELECT tabla_destino, estado, fecha_inicio,
               ROW_NUMBER() OVER (PARTITION BY tabla_destino ORDER BY fecha_inicio DESC) AS rn
        FROM {CATALOG}.{SCHEMA}.midas_log_cargas
    )
    SELECT tabla_destino, estado, fecha_inicio
    FROM ult WHERE rn = 1
    ORDER BY tabla_destino
"""))

# COMMAND ----------
# MAGIC %md ## 4. Conteo en Bronze

# COMMAND ----------
TABLAS = [
    "midas_ordenes_calidad_pendientes_c2_bronze",
    "midas_datos_basicos_producto_c2_bronze",
    "midas_datos_lecturas_producto_c2_bronze",
    "midas_datos_consumos_producto_c2_bronze",
    "midas_datos_ordenes_previa_critica_c2_bronze",
    "midas_datos_cometarios_ordenes_c2_bronze",
    "midas_datos_cuentas_cobro_c2_bronze",
    "midas_datos_detalle_cargos_c2_bronze",
]
for t in TABLAS:
    try:
        print(f"{t:60s}  {spark.table(f'{CATALOG}.{SCHEMA}.{t}').count():>10,}")
    except Exception as e:
        print(f"{t:60s}  ERROR: {e}")

# COMMAND ----------
# MAGIC %md ## 5. Fallidos ultimos 7 dias

# COMMAND ----------
display(spark.sql(f"""
    SELECT run_id, tabla_destino, estado, fecha_inicio,
           LEFT(mensaje_error, 200) AS error
    FROM {CATALOG}.{SCHEMA}.midas_log_cargas
    WHERE estado = 'FALLIDO'
      AND fecha_inicio >= current_timestamp() - INTERVAL 7 DAYS
    ORDER BY fecha_inicio DESC LIMIT 50
"""))

