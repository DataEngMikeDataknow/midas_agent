# MIDAS Stage 4 - Agente inteligente en Databricks

Implementación de ciencia de datos para la Etapa 4 del agente de órdenes de calidad.

## Contrato de consumo

Esta carpeta está diseñada para que Stage 4 consuma **exclusivamente tablas Silver** producidas por Ingeniería de Datos.

No se consumen tablas Silver, no se ejecuta ingesta y no se corrigen fuentes crudas dentro del agente. La responsabilidad de Stage 4 es:

1. leer tablas Silver;
2. construir features e insumos del agente;
3. ejecutar clasificación/inferencia batch;
4. validar salida JSON;
5. registrar resultados, logs, métricas y discrepancias en Delta.

## Tablas Silver fuente esperadas

- `midas_ordenes_calidad_pendientes_silver`
- `midas_datos_basicos_producto_silver`
- `midas_datos_lecturas_producto_silver`
- `midas_datos_consumos_producto_silver`
- `midas_datos_ordenes_previa_critica_silver`
- `midas_datos_comentarios_ordenes_silver`
- `midas_datos_cuentas_cobro_silver`
- `midas_datos_detalle_cargos_silver`

Si los nombres reales de Silver cambian en el ambiente de EPM, actualiza `conf/stage4_config.json` manteniendo el sufijo `_silver`.

## Orden recomendado de ejecución en Databricks

1. `notebooks/stage4/00_setup_stage4.py`
2. `notebooks/stage4/01_build_stage4_features.py`
3. `notebooks/stage4/02_create_sql_functions.py`
4. `notebooks/stage4/03_run_batch_inference.py`
5. `notebooks/stage4/04_evaluate_accuracy.py`
6. `notebooks/stage4/05_build_discrepancy_backlog.py`
