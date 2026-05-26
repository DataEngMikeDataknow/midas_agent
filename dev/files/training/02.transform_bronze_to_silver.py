# Databricks notebook source
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"Usando catálogo '{catalog}' y esquema '{schema}'")

# COMMAND ----------

print("Creando midas_ordenes_calidad_pendientes_silver...")

spark.sql(f"""
    CREATE OR REPLACE TABLE midas_ordenes_calidad_pendientes_silver
    AS
    SELECT
        -- Todas las columnas de la orden original
        o.*,
        
        -- Columnas clave de enriquecimiento desde la tabla de producto
        p.servicio,
        p.categoria,
        p.subcategoria,
        p.nombre_cliente,
        p.identificacion,
        p.ciclo,
        p.plan_facturacion,
        p.plan_facturacion_pr_product,
        p.pagina,
        p.localidad,
        p.direccion,
        p.estado_corte,
        p.saldo_pendiente,
        p.cuentas_vencidas,
        p.saldo_vencido
        
    FROM midas_ordenes_calidad_pendientes_bronze AS o
    LEFT JOIN midas_datos_basicos_producto_bronze AS p
        ON o.servicio_suscrito = p.servicio_suscrito
""")

print("Tabla 'midas_ordenes_calidad_pendientes_silver' creada exitosamente.")

# COMMAND ----------

print("Creando midas_historial_facturacion_silver")

# 1. Agregar los detalles de cargos en un array
df_cargos_agg = spark.sql("""
    SELECT
        id_cuenta_cobro,
        COLLECT_LIST(
            STRUCT(
                concepto,
                causal,
                signo,
                documento_soporte,
                valor,
                unidades
            )
        ) AS detalle_cargos
    FROM midas_datos_detalle_cargos_bronze
    GROUP BY id_cuenta_cobro
""")
df_cargos_agg.createOrReplaceTempView("cargos_agregados_temp")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW consumos_agg_temp
    AS
    SELECT
        servicio_suscrito,
        id_periodo_facturacion,
        FIRST(id_periodo_consumo) AS id_periodo_consumo,
        tipo_consumo AS tipo_consumo_facturado,
        SUM(consumo) AS consumo_facturado_periodo
    FROM midas_datos_consumos_producto_bronze
    WHERE metodo_calculo = '4-Consumo facturado'
    GROUP BY
        servicio_suscrito,
        id_periodo_facturacion,
        tipo_consumo
""")

# 2. Crear la tabla Silver uniendo todas las fuentes de historial
spark.sql(f"""
    CREATE OR REPLACE TABLE midas_historial_facturacion_silver
    AS
    SELECT
        -- Claves
        cc.servicio_suscrito,
        cc.id_periodo_facturacion,
        cc.id_cuenta_cobro,
        cons.id_periodo_consumo,
        
        -- Info de Cuenta de Cobro
        cc.anio_facturacion,
        cc.mes_facturacion,
        cc.fecha_pago,
        cc.valor_total,
        cc.valor_pendiente,
        cc.fecha_vencimiento,
        
        -- Info de Consumos
        cons.tipo_consumo_facturado,
        cons.consumo_facturado_periodo,
        
        
        -- Info de Lecturas
        lect.fecha_ini_consumo,
        lect.fecha_fin_consumo,
        lect.dias_consumo,
        lect.lectura_anterior,
        lect.lectura_actual,
        lect.consumo_calculado,
        lect.consumo_facturado AS consumo_facturado_lectura,
        lect.limite_superior,
        lect.limite_inferior,
        lect.observacion_Lectura,
        
        -- Detalles (anidados)
        cargos.detalle_cargos

    FROM midas_datos_cuentas_cobro_bronze AS cc
    
    LEFT JOIN cargos_agregados_temp AS cargos
        ON cc.id_cuenta_cobro = cargos.id_cuenta_cobro
        
    LEFT JOIN consumos_agg_temp AS cons
        ON cc.servicio_suscrito = cons.servicio_suscrito 
        AND cc.id_periodo_facturacion = cons.id_periodo_facturacion
        
    LEFT JOIN midas_datos_lecturas_producto_bronze AS lect
        ON cc.servicio_suscrito = lect.servicio_suscrito 
        AND cc.id_periodo_facturacion = lect.id_periodo_facturacion
        AND lect.tipo_consumo = cons.tipo_consumo_facturado
        
    ORDER BY
        cc.servicio_suscrito, cc.anio_facturacion DESC, cc.mes_facturacion DESC
""")

print("Tabla midas_historial_facturacion_silver creada.")

# (Repetir un proceso similar para las otras 3 tablas Silver)

# COMMAND ----------

print("Creando midas_datos_basicos_producto_silver...")

spark.sql(f"""
    CREATE OR REPLACE TABLE midas_datos_basicos_producto_silver
    AS
    SELECT *
    FROM midas_datos_basicos_producto_bronze
""")

print("Tabla 'midas_datos_basicos_producto_silver' creada exitosamente.")

# COMMAND ----------

print("Creando midas_historial_critica_silver...")

# Paso 1: Crear una vista temporal que agrega los comentarios por 'id_orden'
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW comentarios_critica_agg_temp
    AS
    SELECT
        id_orden,
        -- Ordenamos los comentarios por fecha para que el agente los lea cronológicamente
        SORT_ARRAY(
            COLLECT_LIST(
                STRUCT(
                    fecha_registro,
                    tipo_comentario,
                    comentario
                )
            ),
            TRUE
        ) AS lista_comentarios
    FROM midas_datos_cometarios_ordenes_bronze
    GROUP BY id_orden
""")

# Paso 2: Crear la tabla Silver uniendo las órdenes de crítica con sus comentarios anidados
spark.sql(f"""
    CREATE OR REPLACE TABLE midas_historial_critica_silver
    AS
    SELECT
        o.*,
        c.lista_comentarios
    FROM midas_datos_ordenes_previa_critica_bronze AS o
    LEFT JOIN comentarios_critica_agg_temp AS c
        ON o.id_orden = c.id_orden
""")

print("Tabla 'midas_historial_critica_silver' creada exitosamente.")