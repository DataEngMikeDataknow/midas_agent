-- Replace values before running.
SELECT * FROM ${catalog}.${schema}.fn_stage4_contexto_orden_calidad('${order_id}') LIMIT 1;
SELECT * FROM ${catalog}.${schema}.fn_stage4_historial_consumo('${servicio_suscrito}', 12);
SELECT * FROM ${catalog}.${schema}.fn_stage4_observaciones_calidad('${servicio_suscrito}', ${id_periodo_consumo});
