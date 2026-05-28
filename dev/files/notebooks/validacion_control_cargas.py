# Databricks notebook source
# MAGIC %md
# MAGIC # Validacion del framework de control de cargas
# MAGIC
# MAGIC Ejecutar despues de cada corrida del job `midas_load_transform_data`
# MAGIC para verificar que:
# MAGIC
# MAGIC 1. Las 8 tablas estan registradas en `midas_control_cargas`.
# MAGIC 2. La ultima corrida quedo en `EXITOSA` en las 8.
# MAGIC 3. `midas_log_cargas` tiene los registros del intento.
# MAGIC 4. Los conteos del log coinciden razonablemente con las tablas Bronze.

# COMMAND ----------

# Cambiar segun ambiente:
#   DEV  : epm_datalabs_catalog_dllo
#   UAT  : epm_datalake_catalog_np
#   PROD : epm_datalake_catalog_prod
CATALOG = "epm_datalabs_catalog_dllo"
SCHEMA  = "facturacion"

# COMMAND ----------
# MAGIC %md ## 1. Estado actual de midas_control_cargas

# COMMAND ----------
display(spark.sql(f"""
    SELECT tabla_nombre, tipo_carga, estado_ultima_carga,
           fecha_ultima_carga, activa
    FROM {CATALOG}.{SCHEMA}.midas_control_cargas
    ORDER BY tabla_nombre
"""))

# Aserciones suaves
estados = spark.sql(f"""
    SELECT estado_ultima_carga, COUNT(*) AS n
    FROM {CATALOG}.{SCHEMA}.midas_control_cargas
    GROUP BY estado_ultima_carga
""").collect()
print("Resumen de estados:", {r.estado_ultima_carga: r.n for r in estados})

# COMMAND ----------
# MAGIC %md ## 2. Ultima corrida (id_ejecucion mas reciente)

# COMMAND ----------
ult_id = spark.sql(f"""
    SELECT id_ejecucion
    FROM {CATALOG}.{SCHEMA}.midas_log_cargas
    ORDER BY fecha_inicio DESC
    LIMIT 1
""").collect()
ult_id = ult_id[0]["id_ejecucion"] if ult_id else None
print("Ultima id_ejecucion:", ult_id)

# COMMAND ----------
if ult_id:
    display(spark.sql(f"""
        SELECT tabla_nombre, intento, estado,
               fecha_inicio, fecha_fin, duracion_seg,
               registros_leidos, registros_escritos,
               LEFT(COALESCE(mensaje_error, ''), 120) AS error
        FROM {CATALOG}.{SCHEMA}.midas_log_cargas
        WHERE id_ejecucion = '{ult_id}'
        ORDER BY fecha_inicio, tabla_nombre, intento
    """))

# COMMAND ----------
# MAGIC %md ## 3. Conteo en Bronze vs log

# COMMAND ----------
TABLAS = [
    "midas_ordenes_calidad_pendientes_bronze",
    "midas_datos_basicos_producto_bronze",
    "midas_datos_lecturas_producto_bronze",
    "midas_datos_consumos_producto_bronze",
    "midas_datos_ordenes_previa_critica_bronze",
    "midas_datos_cometarios_ordenes_bronze",
    "midas_datos_cuentas_cobro_bronze",
    "midas_datos_detalle_cargos_bronze",
]
for t in TABLAS:
    try:
        n = spark.table(f"{CATALOG}.{SCHEMA}.{t}").count()
        print(f"{t:60s}  {n:>10,}")
    except Exception as e:
        print(f"{t:60s}  ERROR: {e}")

# COMMAND ----------
# MAGIC %md ## 4. Cualquier fallida historica (ultimos 7 dias)

# COMMAND ----------
display(spark.sql(f"""
    SELECT id_ejecucion, tabla_nombre, intento, estado,
           fecha_inicio, LEFT(mensaje_error, 200) AS error
    FROM {CATALOG}.{SCHEMA}.midas_log_cargas
    WHERE estado = 'FALLIDA'
      AND fecha_inicio >= current_timestamp() - INTERVAL 7 DAYS
    ORDER BY fecha_inicio DESC
    LIMIT 50
"""))
