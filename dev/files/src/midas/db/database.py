import os
import sys
import logging

import oracledb
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

pool = None

def init_database():
    global pool
    oracle_lib_dir = os.environ.get("ORACLE_CLIENT_LIB_DIR")
    try:
        if oracle_lib_dir:
            log.info(f"Inicializando cliente Oracle desde: {oracle_lib_dir}")
            oracledb.init_oracle_client(lib_dir=oracle_lib_dir)
        else:
            log.info("ORACLE_CLIENT_LIB_DIR no está configurado. Intentando modo 'Thin'.")

    except Exception as e:
        log.error(f"Error al inicializar el cliente Oracle: {e}")
        log.error("Asegúrate que 'ORACLE_CLIENT_LIB_DIR' apunta a tu Instant Client.")
        sys.exit(1)

    try:
        dsn = os.environ["DB_DSN"]
        log.info(f"Creando pool de conexiones para DSN: {dsn}")
        pool = oracledb.create_pool(
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            dsn=dsn,
            min=2,
            max=5,
            increment=1
        )
        log.info("Pool de conexiones creado exitosamente.")

    except Exception as e:
        log.error(f"Error al crear el pool de conexiones: {e}")
        log.error("Verifica tus credenciales (DB_USER, DB_PASSWORD) y el DSN.")
        sys.exit(1)

def execute_query(query: str, params: dict = None) -> pd.DataFrame:
    global pool
    if pool is None:
        log.error("El pool no está inicializado. Llama a init_database() primero.")
        return pd.DataFrame()

    if params is None:
        params = {}

    try:
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                log.debug(f"Ejecutando query con params: {params}")
                cursor.execute(query, params)
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                df = pd.DataFrame(rows, columns=columns)
                log.debug(f"Query retornó {len(df)} filas.")
                return df

    except Exception as e:
        log.error(f"Error al ejecutar query: {e}")
        log.error(f"Query (primeros 200 chars): {query[:200]}...")
        log.error(f"Parámetros: {params}")
        return pd.DataFrame()

def close_pool():
    global pool
    if pool:
        pool.close()
        log.info("Pool de conexiones cerrado.")
