# src/config.py
import os
from dotenv import load_dotenv

# Carga las variables de entorno desde el archivo .env
load_dotenv()

# --- Configuración de Base de Datos ---
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DSN = os.getenv("DB_DSN")

# Ruta al Instant Client (si no se define, oracledb intentará usar el modo "Thin")
ORACLE_CLIENT_LIB_DIR = os.getenv("ORACLE_CLIENT_LIB_DIR")

# --- Configuración de Salida ---
DATA_DIR = "data"
QUERY_ORDENES_PENDIENTES_OUTPUT_FILE = os.path.join(DATA_DIR, "ordenes_calidad_pendientes.parquet")
QUERY_DATOS_BASICOS_OUTPUT_FILE = os.path.join(DATA_DIR, "datos_basicos_producto.parquet")
QUERY_LECTURAS_OUTPUT_FILE = os.path.join(DATA_DIR, "datos_lecturas_producto.parquet")
QUERY_CONSUMOS_OUTPUT_FILE = os.path.join(DATA_DIR, "datos_consumos_producto.parquet")
QUERY_ORDENES_CRITICA_PREVIA_OUTPUT_FILE = os.path.join(DATA_DIR, "datos_ordenes_previa_critica.parquet")
QUERY_COMENTARIOS_ORDEN_OUTPUT_FILE = os.path.join(DATA_DIR, "datos_comentarios_ordenes.parquet")
QUERY_CUENTAS_COBRO_FILE = os.path.join(DATA_DIR, "datos_cuentas_cobro.parquet")
QUERY_DETALLE_CARGOS_FILE = os.path.join(DATA_DIR, "datos_detalle_cargos.parquet")
QUERY_DETALLE_SOLICITUDES_FILE = os.path.join(DATA_DIR, "datos_detalle_solicitudes.parquet")

# --- Validación simple ---
if not all([DB_USER, DB_PASSWORD, DB_DSN]):
    raise ValueError("Error: Faltan variables de entorno (DB_USER, DB_PASSWORD, DB_DSN). Revisa tu archivo .env")