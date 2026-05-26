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
from pyspark.sql import SparkSession
from midas.ingestion import DataIngestor

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
    
    args = parser.parse_args()
    
    spark = SparkSession.builder.getOrCreate()
    ingestor = DataIngestor(spark)
    
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

    for config in tables_config:
        ingestor.load_parquet_to_delta(config, args.destination_catalog, args.destination_schema)

if __name__ == "__main__":
    main()
