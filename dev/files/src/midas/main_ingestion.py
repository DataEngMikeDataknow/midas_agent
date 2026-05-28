import os
import sys

# Agrega src/ al path para que el paquete `midas` sea importable en Databricks
# cuando el job se ejecuta como spark_python_task.
# __file__ no está definido en contextos ipykernel de Databricks; sys.argv[0] es el fallback.
try:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
_src_path = os.path.join(_script_dir, "..")
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

import logging
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from midas.ingestion import DataIngestor
from midas.framework.control_cargas import (
    ControlCargasClient,
    ESTADO_EXITOSA,
    ESTADO_FALLIDA,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Midas Ingestion Runner")
    parser.add_argument("--source_catalog", required=True)
    parser.add_argument("--source_schema", required=True)
    parser.add_argument("--source_volume", required=True)
    parser.add_argument("--source_base_path", required=True)
    parser.add_argument("--destination_catalog", required=True)
    parser.add_argument("--destination_schema", required=True)
    # ─── Etapa 2: parametros nuevos para el framework de control de cargas ───
    parser.add_argument("--control_catalog", required=True,
                        help="Catalogo donde viven midas_control_cargas y midas_log_cargas")
    parser.add_argument("--control_schema", required=True,
                        help="Schema donde viven midas_control_cargas y midas_log_cargas")
    parser.add_argument("--id_ejecucion", default=None,
                        help="UUID de la corrida. Idealmente el mismo de la etapa de extraccion.")

    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    ingestor = DataIngestor(spark)

    # ─── Etapa 2: cliente de control para el paso Parquet -> Bronze ───
    usuario = None
    try:
        usuario = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
    except Exception:
        usuario = "midas_framework"
    control = ControlCargasClient(
        spark=spark,
        catalog=args.control_catalog,
        schema=args.control_schema,
        id_ejecucion=args.id_ejecucion,
        usuario_ejecutor=usuario,
    )
    log.info("Control de cargas activo. id_ejecucion=%s", control.id_ejecucion)

    source_volume_path = f"dbfs:/Volumes/{args.source_catalog}/{args.source_schema}/{args.source_volume}/{args.source_base_path}"
    
    # Configuración de tablas (esto podría externalizarse a un JSON)
    tables_config = [
        {
            "name": "midas_ordenes_calidad_pendientes_bronze",
            "path": f"{source_volume_path}/ordenes_calidad_pendientes.parquet",
            "primary_key": "id_orden",
            "description": "Información de órdenes de calidad pendientes.",
            "column_comments": [
                {"column": "id_orden", "comment": "Identificador único de la orden."}
            ]
        },
        {
            "name": "midas_datos_basicos_producto_bronze",
            "path": f"{source_volume_path}/datos_basicos_producto.parquet",
            "primary_key": "servicio_suscrito",
            "description": "Información básica del producto."
        },
        {
            "name": "midas_datos_lecturas_producto_bronze",
            "path": f"{source_volume_path}/datos_lecturas_producto.parquet",
            "primary_key": "servicio_suscrito",
            "description": "Lecturas del medidor."
        },
        {
            "name": "midas_datos_consumos_producto_bronze",
            "path": f"{source_volume_path}/datos_consumos_producto.parquet",
            "primary_key": "servicio_suscrito",
            "description": "Consumos facturados."
        },
        {
            "name": "midas_datos_ordenes_previa_critica_bronze",
            "path": f"{source_volume_path}/datos_ordenes_previa_critica.parquet",
            "primary_key": "id_orden",
            "description": "Órdenes de crítica y previa."
        },
        {
            "name": "midas_datos_cometarios_ordenes_bronze",
            "path": f"{source_volume_path}/datos_comentarios_ordenes.parquet",
            "primary_key": "id_orden",
            "description": "Comentarios de órdenes."
        },
        {
            "name": "midas_datos_cuentas_cobro_bronze",
            "path": f"{source_volume_path}/datos_cuentas_cobro.parquet",
            "primary_key": "id_cuenta_cobro",
            "description": "Cuentas de cobro."
        },
        {
            "name": "midas_datos_detalle_cargos_bronze",
            "path": f"{source_volume_path}/datos_detalle_cargos.parquet",
            "primary_key": "id_cuenta_cobro",
            "description": "Detalle de cargos."
        }
    ]
    
    # Garantiza que el schema destino existe antes de crear/cargar tablas.
    # En primera ejecución lo crea; en cargas diarias es un no-op.
    # Requiere CREATE SCHEMA en el catálogo (permiso del Service Principal run_as).
    ingestor.ensure_schema_exists(args.destination_catalog, args.destination_schema)

    # ─── Etapa 2: cada tabla del Parquet->Bronze tambien se registra ───
    # NOTA: control aqui registra el SEGUNDO intento de la tabla. Ya hubo un
    # registro EXITOSA durante la extraccion (chain_runner). El log_cargas
    # tendra 2 filas por tabla por corrida: una de extraccion, otra de
    # ingesta a Bronze. Es deliberado: separa fallos de Oracle de fallos de
    # escritura en Delta.
    errores = []
    for config in tables_config:
        tabla = config["name"]
        fecha_inicio = datetime.utcnow()
        # No marcamos EN_PROCESO aqui para no pisar el EXITOSA de la extraccion
        # mientras Bronze esta corriendo. Solo logueamos el intento al final.
        try:
            ingestor.load_parquet_to_delta(config, args.destination_catalog, args.destination_schema)
            control.log_attempt(
                tabla_nombre=tabla,
                intento=2,  # 1 = extraccion, 2 = ingesta Bronze
                fecha_inicio=fecha_inicio,
                fecha_fin=datetime.utcnow(),
                estado=ESTADO_EXITOSA,
            )
        except Exception as exc:
            log.exception("[%s] fallo en ingesta a Bronze", tabla)
            control.mark_failed(tabla)
            control.log_attempt(
                tabla_nombre=tabla,
                intento=2,
                fecha_inicio=fecha_inicio,
                fecha_fin=datetime.utcnow(),
                estado=ESTADO_FALLIDA,
                mensaje_error=str(exc),
            )
            errores.append((tabla, str(exc)))

    if errores:
        # Falla la tarea para que Databricks marque el run como fallido y
        # las tareas downstream (silver, tools, inferencia) no corran.
        raise RuntimeError(f"Ingesta Bronze incompleta: {errores}")


if __name__ == "__main__":
    main()
