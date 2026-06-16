# src/database.py
import oracledb
import pandas as pd
from . import config
import sys
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# Variable global para el pool
pool = None

def init_database():
    """
    Inicializa el cliente de Oracle (si es necesario) y crea el pool de conexiones.
    """
    global pool
    try:
        # 1. Inicializar el cliente (Modo "Thick") si se proveyó la ruta
        if config.ORACLE_CLIENT_LIB_DIR:
            log.info(f"Inicializando cliente Oracle desde: {config.ORACLE_CLIENT_LIB_DIR}")
            oracledb.init_oracle_client(lib_dir=config.ORACLE_CLIENT_LIB_DIR)
        else:
            log.info("ORACLE_CLIENT_LIB_DIR no está configurado. Intentando modo 'Thin'.")
            
    except Exception as e:
        log.error(f"Error al inicializar el cliente Oracle: {e}")
        log.error("Asegúrate que 'ORACLE_CLIENT_LIB_DIR' en .env apunta a tu Instant Client.")
        sys.exit(1)

    try:
        # 2. Crear el pool de conexiones
        log.info(f"Creando pool de conexiones para DSN: {config.DB_DSN}")
        pool = oracledb.create_pool(
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            dsn=config.DB_DSN,
            min=2,  # Conexiones mínimas a mantener abiertas
            max=5,  # Máximo de conexiones
            increment=1
        )
        log.info("Pool de conexiones creado exitosamente.")
        
    except Exception as e:
        log.error(f"Error al crear el pool de conexiones: {e}")
        log.error("Verifica tus credenciales (DB_USER, DB_PASSWORD) y el DSN en .env")
        sys.exit(1)

def execute_query(query: str, params: dict = None) -> pd.DataFrame:
    """
    Ejecuta una query usando una conexión del pool y retorna un DataFrame.
    """
    global pool
    if pool is None:
        log.error("El pool no está inicializado. Llama a init_database() primero.")
        return pd.DataFrame() # Retorna DF vacío

    if params is None:
        params = {}

    try:
        # Adquiere una conexión del pool
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                log.debug(f"Ejecutando query con params: {params}")
                cursor.execute(query, params)
                
                # Obtener nombres de columnas
                columns = [col[0] for col in cursor.description]
                # Obtener todas las filas
                rows = cursor.fetchall()
                
                # Crear DataFrame
                df = pd.DataFrame(rows, columns=columns)
                log.debug(f"Query retornó {len(df)} filas.")
                return df
                
    except Exception as e:
        log.error(f"Error al ejecutar query: {e}")
        log.error(f"Query (primeros 200 chars): {query[:200]}...")
        log.error(f"Parámetros: {params}")
        return pd.DataFrame() # Retorna DF vacío

def close_pool():
    """
    Cierra el pool de conexiones de forma segura.
    """
    global pool
    if pool:
        pool.close()
        log.info("Pool de conexiones cerrado.")