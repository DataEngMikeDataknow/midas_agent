import os
import sys
from pathlib import Path


def _bootstrap_midas_import_path() -> str:
    """Ensure src/ is on sys.path in Databricks Workspace, DAB jobs and notebooks."""
    candidates: list[Path] = []

    def _add(value: object) -> None:
        if not value:
            return
        try:
            path = Path(str(value).replace("file:", "")).expanduser()
            if not path.is_absolute():
                path = (Path.cwd() / path).resolve()
            candidates.append(path)
        except Exception:
            pass

    try:
        _add(__file__)  # type: ignore[name-defined]
    except Exception:
        pass
    _add(sys.argv[0] if sys.argv else None)
    _add(os.getcwd())
    for env_name in ("MIDAS_PROJECT_ROOT", "PROJECT_ROOT", "DATABRICKS_REPO_ROOT"):
        _add(os.environ.get(env_name))

    visited: set[str] = set()
    for candidate in list(candidates):
        current = candidate if candidate.is_dir() else candidate.parent
        for _ in range(14):
            current_key = str(current)
            if current_key in visited:
                break
            visited.add(current_key)

            src_dir = current / "src"
            package_init = src_dir / "midas" / "__init__.py"
            if package_init.exists():
                for path in (src_dir, current):
                    path_str = str(path)
                    if path_str not in sys.path:
                        sys.path.insert(0, path_str)
                return str(src_dir)

            package_init_direct = current / "midas" / "__init__.py"
            if package_init_direct.exists():
                parent = current.parent
                parent_str = str(parent)
                if parent_str not in sys.path:
                    sys.path.insert(0, parent_str)
                return parent_str

            parent = current.parent
            if parent == current:
                break
            current = parent

    for base in (Path("/Workspace/Repos"), Path("/Workspace/Users"), Path("/Workspace/Shared")):
        if not base.exists():
            continue
        base_depth = len(base.parts)
        for walk_root, dirs, _files in os.walk(base):
            walk_path = Path(walk_root)
            if len(walk_path.parts) - base_depth > 8:
                dirs[:] = []
                continue
            dirs[:] = [d for d in dirs if d not in {".git", ".venv", "__pycache__", ".pytest_cache"}]
            src_dir = walk_path / "src"
            if (src_dir / "midas" / "__init__.py").exists():
                for path in (src_dir, walk_path):
                    path_str = str(path)
                    if path_str not in sys.path:
                        sys.path.insert(0, path_str)
                return str(src_dir)

    raise ModuleNotFoundError(
        "No se pudo ubicar el paquete 'midas'. Ejecuta desde la raíz del repo "
        "o define MIDAS_PROJECT_ROOT=/Workspace/.../midas_agent/dev/files."
    )


_bootstrap_midas_import_path()

import logging
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from midas.ingestion import DataIngestor
from midas.framework.control_cargas import ControlCargasClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# Mapeo tabla -> (query_key, nombre_parquet) para enriquecer el log.
_QUERY_KEY = {
    "midas_ordenes_calidad_pendientes_bronze":   "QUERY_ORDENES_PENDIENTES",
    "midas_datos_basicos_producto_bronze":       "QUERY_DATOS_BASICOS",
    "midas_datos_lecturas_producto_bronze":      "QUERY_DATOS_LECTURA",
    "midas_datos_consumos_producto_bronze":      "QUERY_DATOS_CONSUMOS",
    "midas_datos_ordenes_previa_critica_bronze": "QUERY_ORDENES_CRITICA_PEVIA",
    "midas_datos_cometarios_ordenes_bronze":     "QUERY_COMENTARIOS_ORDENES",
    "midas_datos_cuentas_cobro_bronze":          "QUERY_CUENTAS_COBRO",
    "midas_datos_detalle_cargos_bronze":         "QUERY_DETALLE_CARGOS",
}


def main():
    parser = argparse.ArgumentParser(description="Midas Ingestion Runner")
    parser.add_argument("--source_catalog", required=True)
    parser.add_argument("--source_schema", required=True)
    parser.add_argument("--source_volume", required=True)
    parser.add_argument("--source_base_path", required=True)
    parser.add_argument("--destination_catalog", required=True)
    parser.add_argument("--destination_schema", required=True)
    # ─── Etapa 2: framework de control de cargas ───
    parser.add_argument("--control_catalog", required=True)
    parser.add_argument("--control_schema", required=True)
    parser.add_argument("--run_id", default=None,
                        help="UUID de la corrida. Idealmente el mismo de la extraccion.")

    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    ingestor = DataIngestor(spark)

    try:
        usuario = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
    except Exception:
        usuario = "midas_framework"

    control = ControlCargasClient(
        spark=spark,
        catalog=args.control_catalog,
        schema=args.control_schema,
        run_id=args.run_id,
        usuario_ejecutor=usuario,
    )
    log.info("Control de cargas activo. run_id=%s", control.run_id)

    source_volume_path = f"dbfs:/Volumes/{args.source_catalog}/{args.source_schema}/{args.source_volume}/{args.source_base_path}"

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

    # En primera ejecución crea el schema; en cargas diarias es no-op.
    ingestor.ensure_schema_exists(args.destination_catalog, args.destination_schema)

    # Cada carga Parquet -> Bronze se registra como una fila adicional de log
    # con la fase de ingesta. Misma run_id que la extraccion si se pasa --run_id.
    errores = []
    for config in tables_config:
        tabla = config["name"]
        query_key = _QUERY_KEY.get(tabla)
        id_carga = control.get_id_carga(tabla)
        fecha_inicio = datetime.utcnow()
        parquet_path = config["path"]

        control.log_inicio(tabla, query_key, id_carga, fecha_inicio)
        try:
            ingestor.load_parquet_to_delta(config, args.destination_catalog, args.destination_schema)
            # Contar filas escritas en Bronze para el log.
            full = f"{args.destination_catalog}.{args.destination_schema}.{tabla}"
            try:
                n = spark.table(full).count()
            except Exception:
                n = None
            control.log_exito(
                tabla_destino=tabla,
                query_key=query_key,
                id_carga=id_carga,
                fecha_inicio=fecha_inicio,
                filas_escritas=n,
                parquet_path=parquet_path,
            )
        except Exception as exc:
            log.exception("[%s] fallo en ingesta a Bronze", tabla)
            control.log_fallo(
                tabla_destino=tabla,
                query_key=query_key,
                id_carga=id_carga,
                fecha_inicio=fecha_inicio,
                mensaje_error=str(exc),
            )
            errores.append((tabla, str(exc)))

    if errores:
        raise RuntimeError(f"Ingesta Bronze incompleta: {errores}")


if __name__ == "__main__":
    main()
