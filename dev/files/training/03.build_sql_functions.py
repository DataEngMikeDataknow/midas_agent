# Databricks notebook source
# MAGIC %pip install databricks-agents mlflow==3.1.0 databricks-sdk==0.55.0 'unitycatalog-ai[databricks]'
# MAGIC # Restart to load the packages into the Python environment
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"Usando {catalog}.{schema}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_hist_fact(order_id BIGINT COMMENT 'Id o código de la orden de diferencia de acueducto y alcantarillado')
# MAGIC RETURNS TABLE(
# MAGIC     id_orden BIGINT,
# MAGIC     ss_orden BIGINT,
# MAGIC     instalacion_orden BIGINT,
# MAGIC     contrato_orden BIGINT,
# MAGIC     servicio_orden STRING,
# MAGIC     categoria_orden STRING,
# MAGIC     subcategoria_orden STRING,
# MAGIC     plan_facturacion_orden STRING,
# MAGIC     plan_facturacion_pr_product_orden STRING,
# MAGIC     localidad_orden STRING,
# MAGIC     id_periodo_consumo_agua BIGINT,
# MAGIC     id_periodo_facturacion_agua BIGINT,
# MAGIC     tipo_consumo_facturado_agua STRING,
# MAGIC     lectura_anterior_agua DOUBLE,
# MAGIC     lectura_actual_agua DOUBLE,
# MAGIC     consumo_facturado_lectura_agua DOUBLE,
# MAGIC     consumo_calculado_agua DOUBLE,
# MAGIC     detalle_cargos_agua array<struct<concepto:string,causal:string,signo:string,documento_soporte:string,valor:double,unidades:double>> COMMENT 'Cargos o cobros generados para el servicio de acueducto',
# MAGIC     ss_alcantarillado BIGINT,
# MAGIC     plan_facturacion_alcantarillado STRING,
# MAGIC     plan_facturacion_pr_product_alcantarillado STRING,
# MAGIC     categoria_alcantarillado STRING,
# MAGIC     subcategoria_alcantarillado STRING,
# MAGIC     tipo_consumo_facturado_alcantarillado STRING,
# MAGIC     lectura_anterior_alcantarillado DOUBLE,
# MAGIC     lectura_actual_alcantarillado DOUBLE,
# MAGIC     consumo_facturado_lectura_alcantarillado DOUBLE,
# MAGIC     consumo_calculado_alcantarillado DOUBLE,
# MAGIC     detalle_cargos_alcantarillado array<struct<concepto:string,causal:string,signo:string,documento_soporte:string,valor:double,unidades:double>> COMMENT 'Cargos o cobros generados para el servicio de alcantarillado'
# MAGIC )
# MAGIC COMMENT 'Retorna el historico de facturación con la información basica, lectura, consumo y cuentas de cobro y cargos'
# MAGIC RETURN (
# MAGIC     WITH ORDENES_PENDIENTES AS (
# MAGIC         SELECT id_orden, 
# MAGIC                 servicio_suscrito as ss_orden,
# MAGIC                 instalacion as instalacion_orden,
# MAGIC                 contrato as contrato_orden,
# MAGIC                 servicio as servicio_orden,
# MAGIC                 categoria as categoria_orden,
# MAGIC                 subcategoria as subcategoria_orden,
# MAGIC                 plan_facturacion as plan_facturacion_orden,
# MAGIC                 plan_facturacion_pr_product as plan_facturacion_pr_product_orden,
# MAGIC                 localidad as localidad_orden
# MAGIC         FROM epm_datalabs_catalog_dllo.facturacion.midas_ordenes_calidad_pendientes_silver
# MAGIC         WHERE actividad = '1019 - DIFERENCIA ACUEDUCTO Y ALCANTARILLADO'
# MAGIC     ), DATOS_CONSUMOS_AGUA AS (
# MAGIC         SELECT id_orden,
# MAGIC                 ss_orden,
# MAGIC                 instalacion_orden,
# MAGIC                 contrato_orden,
# MAGIC                 servicio_orden,
# MAGIC                 categoria_orden,
# MAGIC                 subcategoria_orden,
# MAGIC                 plan_facturacion_orden,
# MAGIC                 plan_facturacion_pr_product_orden,
# MAGIC                 localidad_orden,
# MAGIC                 id_periodo_consumo as id_periodo_consumo_agua,
# MAGIC                 id_periodo_facturacion as id_periodo_facturacion_agua,
# MAGIC                 tipo_consumo_facturado as tipo_consumo_facturado_agua,
# MAGIC                 lectura_anterior as lectura_anterior_agua,
# MAGIC                 lectura_actual as lectura_actual_agua,
# MAGIC                 consumo_facturado_lectura as consumo_facturado_lectura_agua,
# MAGIC                 consumo_calculado as consumo_calculado_agua,
# MAGIC                 detalle_cargos as detalle_cargos_agua
# MAGIC         FROM midas_historial_facturacion_silver hist
# MAGIC         INNER JOIN ORDENES_PENDIENTES op ON hist.servicio_suscrito = op.ss_orden
# MAGIC         WHERE hist.id_periodo_facturacion = (select max(id_periodo_facturacion) from midas_historial_facturacion_silver where servicio_suscrito = op.ss_orden )
# MAGIC     ), DATOS_BASICOS_ALCANTARILLADO AS (
# MAGIC         SELECT id_orden,
# MAGIC                 ss_orden,
# MAGIC                 instalacion_orden,
# MAGIC                 contrato_orden,
# MAGIC                 servicio_orden,
# MAGIC                 categoria_orden,
# MAGIC                 subcategoria_orden,
# MAGIC                 plan_facturacion_orden,
# MAGIC                 plan_facturacion_pr_product_orden,
# MAGIC                 localidad_orden,
# MAGIC                 id_periodo_consumo_agua,
# MAGIC                 id_periodo_facturacion_agua,
# MAGIC                 tipo_consumo_facturado_agua,
# MAGIC                 lectura_anterior_agua,
# MAGIC                 lectura_actual_agua,
# MAGIC                 consumo_facturado_lectura_agua,
# MAGIC                 consumo_calculado_agua,
# MAGIC                 detalle_cargos_agua,
# MAGIC                 db.servicio_suscrito as ss_alcantarillado,
# MAGIC                 db.plan_facturacion as plan_facturacion_alcantarillado,
# MAGIC                 db.plan_facturacion_pr_product as plan_facturacion_pr_product_alcantarillado,
# MAGIC                 db.categoria as categoria_alcantarillado,
# MAGIC                 db.subcategoria as subcategoria_alcantarillado
# MAGIC         FROM midas_datos_basicos_producto_silver db
# MAGIC         INNER JOIN DATOS_CONSUMOS_AGUA ca ON db.instalacion = ca.instalacion_orden
# MAGIC         WHERE db.servicio like '%ALCANTA%'
# MAGIC     ), DATOS_CONSOLIDADO AS (
# MAGIC         SELECT id_orden,
# MAGIC                 ss_orden,
# MAGIC                 instalacion_orden,
# MAGIC                 contrato_orden,
# MAGIC                 servicio_orden,
# MAGIC                 categoria_orden,
# MAGIC                 subcategoria_orden,
# MAGIC                 plan_facturacion_orden,
# MAGIC                 plan_facturacion_pr_product_orden,
# MAGIC                 localidad_orden,
# MAGIC                 id_periodo_consumo_agua,
# MAGIC                 id_periodo_facturacion_agua,
# MAGIC                 tipo_consumo_facturado_agua,
# MAGIC                 lectura_anterior_agua,
# MAGIC                 lectura_actual_agua,
# MAGIC                 consumo_facturado_lectura_agua,
# MAGIC                 consumo_calculado_agua,
# MAGIC                 detalle_cargos_agua,
# MAGIC                 ss_alcantarillado,
# MAGIC                 plan_facturacion_alcantarillado,
# MAGIC                 plan_facturacion_pr_product_alcantarillado,
# MAGIC                 categoria_alcantarillado,
# MAGIC                 subcategoria_alcantarillado,
# MAGIC                 hist.tipo_consumo_facturado as tipo_consumo_facturado_alcantarillado,
# MAGIC                 hist.lectura_actual as lectura_actual_alcantarillado,
# MAGIC                 hist.lectura_anterior as lectura_anterior_alcantarillado,
# MAGIC                 hist.consumo_facturado_lectura as consumo_facturado_lectura_alcantarillado,
# MAGIC                 hist.consumo_calculado as consumo_calculado_alcantarillado,
# MAGIC                 hist.detalle_cargos as detalle_cargos_alcantarillado
# MAGIC         FROM midas_historial_facturacion_silver hist
# MAGIC         INNER JOIN DATOS_BASICOS_ALCANTARILLADO dba ON hist.servicio_suscrito = dba.ss_alcantarillado
# MAGIC         WHERE hist.id_periodo_facturacion = (select max(id_periodo_facturacion) from midas_historial_facturacion_silver where servicio_suscrito = dba.ss_alcantarillado)
# MAGIC     )
# MAGIC     SELECT *
# MAGIC     FROM DATOS_CONSOLIDADO
# MAGIC     where id_orden = order_id
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_ordenes_critica(serv_suscrito BIGINT COMMENT 'Id o código de la orden de diferencia de acueducto y alcantarillado',
# MAGIC                                                periodo_consumo BIGINT COMMENT 'Id o código del periodo de consumo')
# MAGIC RETURNS TABLE(
# MAGIC     id_orden BIGINT,
# MAGIC     servicio_suscrito BIGINT,
# MAGIC     tipo_consumo BIGINT,
# MAGIC     id_periodo_consumo BIGINT,
# MAGIC     tipo_trabajo STRING,
# MAGIC     actividad STRING,
# MAGIC     fecha_creacion_orden STRING,
# MAGIC     fecha_legalizacion_orden STRING,
# MAGIC     estado STRING,
# MAGIC     analista_legaliza STRING,
# MAGIC     lista_comentarios array<struct<fecha_registro:timestamp_ntz,tipo_comentario:string,comentario:string>> COMMENT 'Lista de comentarios'
# MAGIC )
# MAGIC COMMENT 'Retoran las ordenes de ordenes de critica y de previa generadas para un periodo de consumo'
# MAGIC RETURN (
# MAGIC     SELECT *
# MAGIC     FROM epm_datalabs_catalog_dllo.facturacion.midas_historial_critica_silver
# MAGIC     WHERE servicio_suscrito = serv_suscrito 
# MAGIC       AND id_periodo_consumo = periodo_consumo
# MAGIC );