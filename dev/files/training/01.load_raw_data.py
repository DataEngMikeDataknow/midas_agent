# Databricks notebook source
source_catalog = dbutils.widgets.get("source_catalog")
source_schema =  dbutils.widgets.get("source_schema")
source_volume =  dbutils.widgets.get("source_volume")
source_base_path =  dbutils.widgets.get("source_base_path")

destination_catalog = dbutils.widgets.get("destination_catalog")
destination_schema =  dbutils.widgets.get("destination_schema")

source_volume_path = f"dbfs:/Volumes/{source_catalog}/{source_schema}/{source_volume}/{source_base_path}"

# COMMAND ----------

spark.sql(f"USE CATALOG {destination_catalog}")
spark.sql(f"USE SCHEMA {destination_schema}")

# COMMAND ----------

tables_config = [
    {
        "name": "midas_ordenes_calidad_pendientes_bronze",
        "path": f"{source_volume_path}/ordenes_calidad_pendientes.parquet",
        "primary_key": "id_orden",
        "foreign_keys": [],
        "description": "Esta tabla almacenará la información de las órdenes de calidad pendientes de atención, las cuales corresponden a anomalías o eventualidades de facturación. Esta será la base de información del agente el cual tomará orden por orden para realizar un análisis y entregar una decisión de si se requiere ajuste o no, y una justificación de la decisión tomada.",
        "column_comments": [
            {"column": "id_orden", "comment": "Identificador único de la orden de calidad."},
            {"column": "servicio_suscrito", "comment": "dentificador único del producto en el cual se presentó la eventualidad o anomalía."},
            {"column": "instalacion", "comment": "Identificador único de la instalación, una instalación corresponde a un predio con una ubicación geográfica."},
            {"column": "contrato", "comment": "Identificador único de la cuenta o contrato, este número agrupa productos o servicio suscritos de una instalación u hogar en el cual se prestan los servicios."},
            {"column": "fecha_creacion", "comment": "Fecha de creación de la orden de calidad."},
            {"column": "actividad", "comment": "Corresponde al identificador único de la actividad concatenado con la descripción de la actividad con separador un guion (-), este dato hace referencia al tipo de novedad que se identificó en la orden."},
            {"column": "estado_orden", "comment": "Corresponde al identificador único del estado de la orden concatenado con su descripción cuya separación es por medio de un guion (-), por lo general se van a encontrar los estados: 0 – Registrada, 5 - Asignada"},
            {"column": "comentario_orden", "comment": "Corresponde a un comentario con información adicional asociado a la novedad identificada."}
        ]
    },
    {
        "name": "midas_datos_basicos_producto_bronze",
        "path": f"{source_volume_path}/datos_basicos_producto.parquet",
        "primary_key": "servicio_suscrito",
        "foreign_keys": [],
        "description": "Tabla que contiene la información básica del producto o servicio suscrito.",
        "column_comments": [
            {"column": "servicio_suscrito", "comment": "Identificador único del producto en el cual se presentó la eventualidad o anomalía."},
            {"column": "contrato", "comment": "Identificador único de la cuenta o contrato, este número agrupa productos o servicio suscritos de una instalación u hogar en el cual se prestan los servicios."},
            {"column": "servicio", "comment": "Corresponde al identificador único del tipo de producto concatenado con su descripción, separado por un guion (-). Este dato hace referencia al tipo de producto de un servicio suscrito. Los más comunes son: 101-AGUA POTABLE, 103-ALCANTARILLADO, 701-ENERGÍA MDO REGUALDO, 501-GAS NATURAL REGULADO, 7505-GAS NATURAL COMPRIMIDO"},
            {"column": "fecha_instalacion", "comment": "Fecha en la que se instaló el producto en la instalación u hogar."},
            {"column": "fecha_retiro", "comment": "Fecha en la que se retira el producto."},
            {"column": "periodicidad", "comment": "Periodicidad en la que se factura el contrato puede ser: 1 = Mensual, 2 = Bimestral, 3 = Trimestral. En caso de que no llegue algún valor, se asume que es de periodicidad 1."},
            {"column": "estado_corte", "comment": "Corresponde al identificador único del estado de corte del producto concatenado con su descripción, separado por un guion (-). Este dato hace referencia al tipo de producto de un servicio suscrito. Los más comunes son: 1-Conexion = indica que el producto está operando. 4- Orden suspension total = indica que el producto cuenta con una orden de suspensión del servicio. 5-Suspensión total = indica que el producto se encuentra suspendido"},
            {"column": "categoria", "comment": "Corresponde al identificador único de la categoría del producto concatenado con su descripción, separado por un guion (-). Este dato hace referencia a la categoría que pertenece un servicio suscrito y la cual corresponde a una segmentación de los clientes. Los más comunes son: 1-Residencial,, 2-Comercial, 3-Industrial"},
            {"column": "subcategoria", "comment": "Corresponde al identificador único de la subcategoría del producto concatenado con su descripción, separado por un guion (-). Este dato hace referencia a la subcategoría que pertenece un servicio suscrito y la cual corresponde a una sub-segmentación de los clientes de acuerdo con la categoría. Los más comunes son: Para la categoría 1-Residencial corresponde al estrato socio económico: 1-Estrato 1, 2-Estrato 2, 3-Estrato 3, 4-Estrato 4, 5-Estrato 5, 6-Estrato 6. Para las categorías diferentes a la residencial y que pertenezcan al un tipo de producto de energía, corresponde al nivel de tensión."},
            {"column": "ciclo", "comment": "Ciclo de facturación al cual pertenece el servicio suscrito."},
            {"column": "plan_facturacion", "comment": "Corresponde al identificador único del plan de facturación del producto concatenado con su descripción, separado por un guion (-). Este dato hace referencia a las condiciones que debe tener el producto para ser facturado."},
            {"column": "plan_facturacion_pr_product", "comment": "Corresponde al identificador único del plan de facturación del producto concatenado con su descripción, pero de la tabla PR_PRODUCT, separado por un guion (-). Este dato debe coincidir con el campo plan_facturacion"},
            {"column": "nombre_cliente", "comment": "Nombres y apellidos del usuario o cliente registrado en sistema."},
            {"column": "identificacion", "comment": "Identificación del usuario o cliente registrado en sistema."},
            {"column": "localidad", "comment": "Municipio de la instalación donde se presta el servicio.."},
            {"column": "direccion", "comment": "Dirección de la instalación donde se presta el servicio."},
            {"column": "pagina", "comment": "Codificación de la dirección donde se presta el servicio."},
            {"column": "saldo_pendiente", "comment": "Corresponde al valor total adeudado a la fecha para el contrato."},
            {"column": "cuentas_vencidas", "comment": "Corresponde a número de cuentas vencidas que tiene a la fecha el contrato."},
            {"column": "saldo_vencido", "comment": "Corresponde al valor total vencido a la fecha para el contrato, este saldo genera intereses de mora."}
        ]
    },
    {
        "name": "midas_datos_lecturas_producto_bronze",
        "path": f"{source_volume_path}/datos_lecturas_producto.parquet",
        "primary_key": "servicio_suscrito",
        "foreign_keys": [
            {
                "column": "servicio_suscrito",
                "references_table": "midas_datos_basicos_producto_bronze",
                "references_column": "servicio_suscrito",
                "constraint_name": "fk_lecturas_producto"
            }
        ],
        "description": "Esta tabla contiene toda la información asociada a las lecturas tomadas del medido para poder realizar el cálculo del consumo que se va a facturar.",
        "column_comments": [
           {"column": "servicio_suscrito", "comment": "Identificador único del producto en el cual se presentó la eventualidad o anomalía."},
           {"column": "id_periodo_consumo", "comment": "Identificador único del periodo de consumo del cliente"},
           {"column": "id_periodo_facturacion", "comment": "Identificador único del periodo de facturación del cliente."},
           {"column": "fecha_ini_consumo", "comment": "Fecha en la cual inicia el periodo de consumo del cliente."},
           {"column": "fecha_fin_consumo", "comment": "Fecha en la cual finaliza el periodo de consumo del cliente."},
           {"column": "dias_consumo", "comment": "Días comprendidos entre la fecha de inicio del consumo y la fecha de finalización del consumo."},
           {"column": "tipo_consumo", "comment": "Identificador único y su descripción del tipo de consumo leído, por ejemplo: energía activa, energía reactiva, etc."},
           {"column": "medidor", "comment": "Identificador único del medidor."},
           {"column": "constante", "comment": "Factor de corrección el cual es usado para ajustar el consumo."},
           {"column": "digitos_medidor", "comment": "Cantidad de dígitos que muestra el display del medidor."},
           {"column": "lectura_anterior", "comment": "Lectura tomada para el periodo anterior."},
           {"column": "lectura_actual", "comment": "Lectura tomada para el periodo actual."},
           {"column": "consumo_calculado", "comment": "Diferencia entre la lectura tomada para el periodo anterior y la lectura tomada para el periodo actual."}, 
           {"column": "consumo_facturado", "comment": "Consumo real facturado, este consumo puede ser diferente al “consumo_calculado” debido a múltiples factores, como la constate aplicada al consumo calculado o si se ajusta el consumo de forma manual."}, 
           {"column": "limite_superior", "comment": "Límite máximo que puede tener el consumo calculado, si el consumo supera este límite, por lo general se genera una orden de crítica de consumo."}, 
           {"column": "limite_inferior", "comment": "Límite mínimo que puede tener el consumo calculado, si el consumo sobrepasa este límite, por lo general se genera una orden de crítica de consumo."}, 
           {"column": "observacion_Lectura", "comment": "Observación que deja el lector al momento de tomar la lectura, esta observación sirve para múltiples cosas, como por ejemplo identificar la causa de no lectura de un medidor."}, 
           {"column": "observacion_Lectura_2", "comment": "Observación adicional número 1 que deja el lector al momento de tomar la lectura, esta observación sirve para múltiples cosas, como por ejemplo identificar la causa de no lectura de un medidor."}, 
           {"column": "observacion_Lectura_3", "comment": "Observación adicional número 2 que deja el lector al momento de tomar la lectura, esta observación sirve para múltiples cosas, como por ejemplo identificar la causa de no lectura de un medidor."}, 
           {"column": "PNO", "comment": "Valor cobrado por perdidas no operacionales como fraudes."}
        ]
    },
    {
        "name": "midas_datos_consumos_producto_bronze",
        "path": f"{source_volume_path}/datos_consumos_producto.parquet",
        "primary_key": "servicio_suscrito",
        "foreign_keys": [
            {
                "column": "servicio_suscrito",
                "references_table": "midas_datos_lecturas_producto_bronze",
                "references_column": "servicio_suscrito",
                "constraint_name": "fk_consumos_producto"
            }
        ],
        "description": "Tabla donde se almacena toda la información asociada al consumo facturado al cliente para un periodo de consumo y de facturación.",
        "column_comments": [
            {"column": "servicio_suscrito", "comment": "Identificador único del producto en el cual se presentó la eventualidad o anomalía."},
            {"column": "id_periodo_consumo", "comment": "Identificador único del periodo de consumo del cliente."},
            {"column": "id_periodo_facturacion", "comment": "Identificador único del periodo de facturación del cliente."},
            {"column": "anio_facturacion", "comment": "Año del periodo de facturación."},
            {"column": "mes_facturacion", "comment": "Mes del periodo de facturación."},
            {"column": "ciclo", "comment": "Ciclo al cual pertenece el producto que se va a facturar. Los ciclos del 1 al 20 corresponde a ciclos del área metropolitana del valle de Aburrá, los ciclos del 101 al 123 corresponden a ciclos regionales distribuidos en todo el territorio antioqueño. El ciclo 24 corresponde a un ciclo especial donde se facturan muchos servicios suscritos, estos tienen un ciclo operativo."},
            {"column": "ciclo_operativo", "comment": "Ciclo operativo al cual pertenece el producto que se va a facturar, esto aplica para el ciclo 24 debido a que este ciclo se factura siempre a final de mes y para esperar a que todos los ciclos metropolitanos y regionales se hayan facturado."},
            {"column": "fecha_registro", "comment": "Fecha en que se registra el consumo."},
            {"column": "metodo_calculo", "comment": "Esta información indica el método de cálculo de consumo. El método de cálculo 4 es el que se usa para facturar y los diferentes a 4 indican si el consumo fue ajustado o estimado con la lectura, etc."},
            {"column": "tipo_consumo", "comment": "Identificador único y su descripción del tipo de consumo leído, por ejemplo: energía activa, energía reactiva, etc."},
            {"column": "consumo", "comment": "Consumo aplicado, si su método de cálculo es 4, corresponde al consumo que se le va a facturar al cliente, para métodos de calculo diferente a 4 corresponde a información para identificar los ajustes que se le aplican al consumo facturado."},
            {"column": "funcion_calculo", "comment": "Función de cálculo del consumo, esta función corresponde a una lógica aplicada para identificar que consumo se le debe aplicar al cliente."},
            {"column": "calificacion", "comment": "Calificación del análisis de consumo."}
        ]
    },
    {
        "name": "midas_datos_ordenes_previa_critica_bronze",
        "path": f"{source_volume_path}/datos_ordenes_previa_critica.parquet",
        "primary_key": "id_orden",
        "foreign_keys": [
            {
                "column": "servicio_suscrito",
                "references_table": "midas_datos_lecturas_producto_bronze",
                "references_column": "servicio_suscrito",
                "constraint_name": "fk_ordenes_critica_producto"
            }
        ],
        "description": "Esta tabla contiene toda la información asociada a las órdenes de crítica y de previa generada para un periodo de consumo. Las ordenes de critica corresponden a consumos anormales muy altos o bajos y suelen generarse cuando el consumo se sale de los límites de consumo para el producto, las ordenes de previa, corresponden a visitas que se deben realizar a la instalación para verificar la causa del consumo desviado.",
        "column_comments": [
            {"column": "id_orden", "comment": "Identificador único de la orden de crítica o previa."},
            {"column": "servicio_suscrito", "comment": "Identificador único del producto en el cual se presentó la eventualidad o anomalía."},
            {"column": "tipo_consumo", "comment": "Identificador único y su descripción del tipo de consumo leído, por ejemplo: energía activa, energía reactiva, etc."},
            {"column": "id_periodo_consumo", "comment": "Identificador único del periodo de consumo del cliente."},
            {"column": "tipo_trabajo", "comment": "Tipo de trabajo al cual pertenece la orden de crítica o previa."},
            {"column": "actividad", "comment": "Actividad de la orden de crítica o previa."},
            {"column": "fecha_creacion_orden", "comment": "Fecha de creación de la orden de crítica o previa."},
            {"column": "fecha_legalizacion_orden", "comment": "Fecha en la que se legaliza la orden de crítica o previa."},
            {"column": "estado", "comment": "Estado de la orden de crítica o previa."},
            {"column": "analista_legaliza", "comment": "Analista que legaliza la orden de crítica o previa."}
        ]
    },
    {
        "name": "midas_datos_cometarios_ordenes_bronze",
        "path": f"{source_volume_path}/datos_comentarios_ordenes.parquet",
        "primary_key": "id_orden",
        "foreign_keys": [
            {
                "column": "id_orden",
                "references_table": "midas_datos_ordenes_previa_critica_bronze",
                "references_column": "id_orden",
                "constraint_name": "fk_comentarios_orden_critica"
            }
        ],
        "description": "Esta tabla contiene toda la información asociada a los comentarios que presenta una orden de critica o previa.",
        "column_comments": [
           {"column": "id_orden", "comment": "Identificador único de la orden de crítica o previa."},
           {"column": "servicio_suscrito", "comment": "Identificador único del producto en el cual se presentó la eventualidad o anomalía."},
           {"column": "fecha_registro", "comment": "Fecha en que se registró el comentario."},
           {"column": "tipo_comentario", "comment": "Tipo del comentario."},
           {"column": "comentario", "comment": "Comentario que se realizó."}
        ]
    },
    {
        "name": "midas_datos_cuentas_cobro_bronze",
        "path": f"{source_volume_path}/datos_cuentas_cobro.parquet",
        "primary_key": "id_cuenta_cobro",
        "foreign_keys": [
            {
                "column": "servicio_suscrito",
                "references_table": "midas_datos_basicos_producto_bronze",
                "references_column": "servicio_suscrito",
                "constraint_name": "fk_cuentas_cobro_producto"
            }
        ],
        "description": "Esta tabla contiene toda la información asociada a las cuentas por pagar de un cliente por un producto.",
        "column_comments": [
            {"column": "servicio_suscrito", "comment": "Identificador único del producto en el cual se presentó la eventualidad o anomalía."},
            {"column": "id_cuenta_cobro", "comment": "Identificador único de la cuenta de cobro."},
            {"column": "id_periodo_facturacion", "comment": "Identificador único del periodo de facturación del cliente."},
            {"column": "anio_facturacion", "comment": "Año del periodo de facturación"},
            {"column": "mes_facturacion", "comment": "Mes del periodo de facturación"},
            {"column": "fecha_pago", "comment": "Fecha en que el cliente realizó el pago de la cuenta de cobro."},
            {"column": "valor_total", "comment": "Valor total de la cuenta de cobro en pesos colombianos."},
            {"column": "valor_abonado", "comment": "Valor abonado a la cuenta de cobro en pesos colombianos en caso de que el cliente haya realizado un pago parcial."},
            {"column": "valor_reclamo", "comment": "Valor que se deja en reclamo en caso de que un cliente presente una reclamación, este valor se resta del valor total mientras se resuelve el reclamo."},
            {"column": "valor_pendiente", "comment": "Valor real que el cliente tiene sobre el total del valor de la cuenta de cobro."},
            {"column": "fecha_vencimiento", "comment": "Fecha en que se vence la cuenta de cobro, a partir de esta fecha se empieza a contabilizar recargo por mora."},
            {"column": "valor_periodo", "comment": "Valor de todo el periodo de facturación el cual incluye a otros productos del contrato."},
            {"column": "valor_recuperado", "comment": "Valor recuperado en caso de que se haya presentando alguna eventualidad en meses anteriores y se requiera cobrar o devolver valores al cliente."}
        ]
    },
    {
        "name": "midas_datos_detalle_cargos_bronze",
        "path": f"{source_volume_path}/datos_detalle_cargos.parquet",
        "primary_key": "id_cuenta_cobro", 
        "foreign_keys": [
            {
                "column": "id_cuenta_cobro",
                "references_table": "midas_datos_cuentas_cobro_bronze",
                "references_column": "id_cuenta_cobro",
                "constraint_name": "fk_detalle_cargos_cuenta"
            }
        ],
        "description": "Esta tabla contiene toda la información asociada a los cargos que contiene una cuenta de cobro, un cargo es un valor monetario en pesos colombianos asociado a un concepto de cobro.",
        "column_comments": [
            {"column": "servicio_suscrito", "comment": "Identificador único del producto en el cual se presentó la eventualidad o anomalía."},
            {"column": "id_cuenta_cobro", "comment": "Identificador único de la cuenta de cobro."},
            {"column": "id_periodo_facturacion", "comment": "Identificador único del periodo de facturación del cliente."},
            {"column": "id_periodo_consumo", "comment": "Identificador único del periodo de consumo del cliente."},
            {"column": "concepto", "comment": "Código y nombre del concepto de cobro."},
            {"column": "causal", "comment": "Causal que genera ese concepto de cobro -1, corresponde a una causal normal."},
            {"column": "signo", "comment": "Esta codificación le da el signo al concepto así: Los más comunes son: DB (Debito): signo positivo, suma al valor a pagar del cliente.CR (Crédito): signo negativo, resta al valor a pagar del cliente."},
            {"column": "documento_soporte", "comment": "Campo de texto abierto donde se ponen observaciones al concepto para soportar el cobro y otros detalles."},
            {"column": "fecha_creacion_cargo", "comment": "Fecha en que se generó el cargo o concepto."},
            {"column": "programa", "comment": "Código y descripción del programa que genera el cargo o concep."},
            {"column": "id_tarifa", "comment": "Identificador de la tarifa aplicada."},
            {"column": "unidades", "comment": "Unidades de consumo facturadas."},
            {"column": "valor", "comment": "Valor del cargo en pesos colombianos."}
        ]
    }
]

# COMMAND ----------

for config in tables_config:
    table_name = config["name"]
    path = config["path"]
    
    print(f"Procesando y creando la tabla: {table_name}")
    
    # Carga desde Parquet
    df_spark = spark.read.format("parquet").load(path)
    
    # Convertir todas las columnas a minúsculas
    nuevas_columnas = [columna.lower() for columna in df_spark.columns]
    df_spark = df_spark.toDF(*nuevas_columnas)

    # Eliminar columna "iteration"
    df_spark = df_spark.drop("iteration")
    
    # Guardar la tabla en formato Delta
    (df_spark.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true") 
        .saveAsTable(table_name)
    )
    print(f"Tabla creada en UC: {table_name}, cantidad de registros: {df_spark.count()}")
    
    # 1. Añadir descripción a la tabla (si existe)
    if config.get("description"):
        table_comment = config["description"]
        spark.sql(f"COMMENT ON TABLE {table_name} IS '{table_comment}'")
        print(f"Descripción añadida a la tabla {table_name}")

    # 2. Añadir comentarios a las columnas (si existen)
    if config.get("column_comments"):
        for col_comment in config.get("column_comments", []):
            column_name = col_comment["column"].lower() # Asegurarse de que el nombre de columna también esté en minúsculas
            comment = col_comment["comment"]
            try:
                spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column_name} COMMENT '{comment}'")
                print(f"Comentario añadido a la columna '{column_name}'.")
            except Exception as e:
                print(f"No se pudo añadir comentario a la columna '{column_name}'. Error: {e}")

    # 3. Añadir Clave Primaria (si está definida)
    if config["primary_key"]:
        pk_column = config["primary_key"]
        # Es buena práctica que las claves primarias no sean nulas
        try:
            spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {pk_column} SET NOT NULL")
        except Exception as e:
            print(f"No se pudo establecer la clave primaria como no nula: {pk_column}. Error: {e}")
            
        pk_constraint_name = f"pk_{table_name}"
        sql_pk = f"ALTER TABLE {table_name} ADD CONSTRAINT {pk_constraint_name} PRIMARY KEY({pk_column})"
        spark.sql(sql_pk)
        print(f"Clave primaria '{pk_column}' añadida a la tabla {table_name}")

    # 4. Añadir Claves Foráneas (si existen)
    if config["foreign_keys"]:
        for fk in config["foreign_keys"]:
            sql_fk = f"""
            ALTER TABLE {table_name} ADD CONSTRAINT {fk['constraint_name']}
            FOREIGN KEY ({fk['column']}) REFERENCES {fk['references_table']}({fk['references_column']})
            """
            spark.sql(sql_fk)
            print(f"Clave foránea '{fk['constraint_name']}' de '{fk['column']}' a '{fk['references_table']}' añadida.")

    print("-" * 60)

print("Proceso completado.")
#spark.stop()