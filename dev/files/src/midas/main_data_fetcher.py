import os
import sys
import logging
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


def _get_dbutils():
    from pyspark.sql import SparkSession  # type: ignore[import-untyped]
    from pyspark.dbutils import DBUtils   # type: ignore[import-untyped]
    spark = SparkSession.builder.getOrCreate()
    return DBUtils(spark)


def fetch_db_password(secret_scope: str, secret_key: str) -> str:
    log.info(f"Obteniendo contraseña Oracle desde Databricks Secret Scope '{secret_scope}'...")
    dbutils = _get_dbutils()
    return dbutils.secrets.get(scope=secret_scope, key=secret_key)


def parse_args():
    parser = argparse.ArgumentParser(description="Extracción Oracle → Parquet en volumen UC")
    parser.add_argument("--db_user", required=True)
    parser.add_argument("--db_dsn", required=True)
    parser.add_argument("--secret_scope", required=True,
                        help="Databricks Secret Scope que contiene la contraseña de Oracle")
    parser.add_argument("--db_password_secret_key", required=True,
                        help="Nombre de la clave en el Secret Scope que almacena la contraseña de Oracle")
    parser.add_argument("--oracle_client_lib_dir", default=None,
                        help="Ruta al Oracle Instant Client. Omitir para usar modo Thin.")
    parser.add_argument("--output_path", required=True,
                        help="Ruta /Volumes/... donde se guardarán los archivos Parquet")
    # ─── Etapa 2: parametros nuevos para el framework de control de cargas ───
    parser.add_argument("--control_catalog", required=True,
                        help="Catalogo donde viven midas_control_cargas y midas_log_cargas")
    parser.add_argument("--control_schema", required=True,
                        help="Schema donde viven midas_control_cargas y midas_log_cargas")
    parser.add_argument("--id_ejecucion", default=None,
                        help="UUID de la corrida. Si se omite, se genera uno nuevo.")
    return parser.parse_args()


def main():
    args = parse_args()

    db_password = fetch_db_password(
        secret_scope=args.secret_scope,
        secret_key=args.db_password_secret_key,
    )

    # Inyectar credenciales y ruta de salida como variables de entorno ANTES de importar
    # midas.db, ya que database.py y processing.py las leen en tiempo de uso.
    os.environ["DB_USER"] = args.db_user
    os.environ["DB_PASSWORD"] = db_password
    os.environ["DB_DSN"] = args.db_dsn
    os.environ["OUTPUT_PATH"] = args.output_path
    if args.oracle_client_lib_dir:
        os.environ["ORACLE_CLIENT_LIB_DIR"] = args.oracle_client_lib_dir

    # Calcular la raíz del proyecto para hacer importable el paquete data_fetcher.
    # __file__ no está definido en contextos ipykernel de Databricks; sys.argv[0] es el fallback.
    try:
        _script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        _script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

    # src/midas/ → src/ → raíz del proyecto
    _project_root = os.path.normpath(os.path.join(_script_dir, "..", ".."))
    if _project_root not in sys.path:
        sys.path.insert(0, _project_root)

    # src/ debe estar en el path para que el paquete midas sea importable
    _src_dir = os.path.join(_project_root, "src")
    if _src_dir not in sys.path:
        sys.path.insert(0, _src_dir)

    # Importar después de ajustar el path y las variables de entorno
    from midas.db import database as db
    from midas.db import processing
    from midas.framework.control_cargas import ControlCargasClient
    from midas.framework.chain_runner import ejecutar_cadena_extraccion
    from pyspark.sql import SparkSession

    log.info(f"Iniciando extracción Oracle → {args.output_path}")

    # ─── Etapa 2: cliente de control compartido por todos los pasos ───
    spark = SparkSession.builder.getOrCreate()
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

    try:
        processing.ensure_data_dir()
        db.init_database()

        # ─── Cadena envuelta en control (reemplaza el bloque anterior de llamadas sueltas) ───
        ejecutar_cadena_extraccion(processing, control)

    finally:
        db.close_pool()

    log.info(f"Extracción completada. Parquet disponibles en: {args.output_path}")


if __name__ == "__main__":
    main()
