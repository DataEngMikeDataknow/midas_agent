-- Validación operativa: la Etapa 4 debe consumir exclusivamente tablas Silver.
-- Ajusta ${catalog} y ${schema} según el ambiente Databricks.

SHOW TABLES IN ${catalog}.${schema} LIKE 'midas_*_silver';

-- Tablas fuente esperadas por defecto:
-- ${catalog}.${schema}.midas_ordenes_calidad_pendientes_silver
-- ${catalog}.${schema}.midas_datos_basicos_producto_silver
-- ${catalog}.${schema}.midas_datos_lecturas_producto_silver
-- ${catalog}.${schema}.midas_datos_consumos_producto_silver
-- ${catalog}.${schema}.midas_datos_ordenes_previa_critica_silver
-- ${catalog}.${schema}.midas_datos_comentarios_ordenes_silver
-- ${catalog}.${schema}.midas_datos_cuentas_cobro_silver
-- ${catalog}.${schema}.midas_datos_detalle_cargos_silver
