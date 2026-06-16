# src/processing.py
import pandas as pd
from . import database as db
from . import queries
from . import config
import os
import logging
from tqdm import tqdm # Para las barras de progreso

log = logging.getLogger(__name__)

def ensure_data_dir():
    """Asegura que el directorio 'data' exista."""
    os.makedirs(config.DATA_DIR, exist_ok=True)
    log.info(f"Directorio de salida asegurado: {config.DATA_DIR}")

def save_to_parquet(df: pd.DataFrame, file_path: str):
    """Guarda un DataFrame en formato Parquet."""
    if df.empty:
        log.warning(f"No hay datos para guardar en {file_path}. Archivo no creado.")
        return
        
    try:
        df.to_parquet(
            file_path, 
            index=False, 
            engine='pyarrow', 
            coerce_timestamps='us',         
            allow_truncated_timestamps=True 
        )
        log.info(f"Datos guardados exitosamente en {file_path} ({len(df)} filas)")
    except Exception as e:
        log.error(f"Error al guardar archivo Parquet en {file_path}: {e}")

def run_query_ordenes_pendientes() -> pd.DataFrame:
    """
    Ejecuta Query [ordenes pendientes] y guarda el resultado.

    Args:
        None

    Returns:
        Dataframe con el resultado del query.
    """
    log.info("--- Iniciando proceso extracción: [ordenes_calidad_pendientes] ---")
    df_ordenes_pendientes = db.execute_query(queries.QUERY_ORDENES_PENDIENTES)
    
    if df_ordenes_pendientes.empty:
        log.warning("Query [ordenes pendientes] no retornó resultados.")
    
    save_to_parquet(df_ordenes_pendientes, config.QUERY_ORDENES_PENDIENTES_OUTPUT_FILE)
    log.info("--- Proceso extracción [datos_basicos_producto] Completado ---")
    return df_ordenes_pendientes

def run_query_datos_basicos(df_ordenes_pendientes: pd.DataFrame) -> pd.DataFrame:
    """
    Ejecuta Query datos basicos del producto iterando sobre los resultados de de ordenes pendientes.

    Args:
        Dataframe con la información de ordenes pendientes.

    Returns:
        Dataframe con el resultado del query con los datos básicos de los productos.
    """
    log.info("--- Iniciando proceso extracción: [datos_basicos_producto] ---")
    if df_ordenes_pendientes is None or df_ordenes_pendientes.empty:
        log.warning("No hay datos de ordenes pendientes para procesar. Saltando Proceso 2.")
        return pd.DataFrame()

    # Validar que la columna 'INSTALACION' exista
    if 'INSTALACION' not in df_ordenes_pendientes.columns:
        log.error("Columna 'INSTALACION' no encontrada en resultados de Query ordenes pendientes. Abortando Proceso 2.")
        return pd.DataFrame()
    
    unique_instalaciones = df_ordenes_pendientes['INSTALACION'].dropna().unique()
        
    all_results_datos_basicos = []
    log.info(f"Iterando {len(unique_instalaciones)} filas de Query ordenes pendientes para ejecutar Query datos basicos...")
    
    # tqdm envuelve el iterador para mostrar una barra de progreso
    for address_id in tqdm(unique_instalaciones, desc="Procesando intalaciones del query de ordenes pendientes"):
        params = {'address_id': int(address_id)}
        df_row_q2 = db.execute_query(queries.QUERY_DATOS_BASICOS, params)
        
        if not df_row_q2.empty:
            all_results_datos_basicos.append(df_row_q2)

    if not all_results_datos_basicos:
        log.warning("Query datos basicos no retornó resultados para ninguna iteración.")
        return pd.DataFrame()
    
    # Concatenar todos los resultados en un solo DataFrame
    df_datos_basicos_final = pd.concat(all_results_datos_basicos, ignore_index=True)
    save_to_parquet(df_datos_basicos_final, config.QUERY_DATOS_BASICOS_OUTPUT_FILE)
    log.info("--- Proceso extracción: [datos_basicos_producto] Completado ---")
    return df_datos_basicos_final

def run_query_datos_lectura(df_datos_basicos: pd.DataFrame):
    """
    Ejecuta Query datos lectura iterando sobre los resultados de Query datos básicos.

    Args:
        Dataframe con la información de datos básicos de los productos.

    Returns:
        Dataframe con el resultado del query con la información de lectura de los productos.
    """
    log.info("--- Iniciando proceso extracción: [datos_lecturas_producto] ---")
    if df_datos_basicos is None or df_datos_basicos.empty:
        log.warning("No hay datos de Query 2 para procesar. Saltando Proceso 3.")
        return

    if 'SERVICIO_SUSCRITO' not in df_datos_basicos.columns:
        log.error("Columna 'SERVICIO_SUSCRITO' no encontrada en resultados de Query 2. Abortando Proceso 3.")
        return
    
    unique_servicios = df_datos_basicos['SERVICIO_SUSCRITO'].dropna().unique()
        
    all_results_datos_lectura = []
    log.info(f"Iterando {len(unique_servicios)} filas de Query 2 para ejecutar Query 3...")
    
    for servicio_suscrito in tqdm(unique_servicios, desc="Procesando Q3"):
        params = {'servicio_suscrito': int(servicio_suscrito)}
        df_row_q3 = db.execute_query(queries.QUERY_DATOS_LECTURA, params)
        if not df_row_q3.empty:
            all_results_datos_lectura.append(df_row_q3)

    if not all_results_datos_lectura:
        log.warning("Query 3 no retornó resultados para ninguna iteración.")
        return

    df_datos_lectura_final = pd.concat(all_results_datos_lectura, ignore_index=True)
    save_to_parquet(df_datos_lectura_final, config.QUERY_LECTURAS_OUTPUT_FILE)
    log.info("--- Proceso extracción: [datos_lecturas_producto] Completado ---")
    return df_datos_lectura_final

def run_query_datos_consumos(df_datos_lecturas: pd.DataFrame) -> pd.DataFrame:
    """
    Ejecuta Query datos consumos iterando sobre los resultados de Query datos lecturas.

    Args:
        Dataframe con la información de datos de lectura de los productos.

    Returns:
        Dataframe con el resultado del query con la información de consumos de los productos.
    """
    log.info("--- Iniciando proceso extracción: [datos_consumos_producto] ---")
    if df_datos_lecturas is None or df_datos_lecturas.empty:
        log.warning("No hay datos de Query 3 para procesar. Saltando Proceso 4.")
        return pd.DataFrame()

    # Validar columnas necesarias de Q3
    required_cols = ['SERVICIO_SUSCRITO', 'ID_PERIODO_CONSUMO']
    if not all(col in df_datos_lecturas.columns for col in required_cols):
        log.error(f"Columnas requeridas {required_cols} no encontradas en resultados de Query 3. Abortando Proceso 4.")
        return pd.DataFrame()
    
    unique_combos = df_datos_lecturas[required_cols].drop_duplicates().dropna()
        
    all_results_datos_consumos = []
    log.info(f"Iterando {len(unique_combos)} filas de Query 3 para ejecutar Query 4...")
    
    for row in tqdm(unique_combos.itertuples(index=False), total=len(unique_combos), desc="Procesando Q4"):
        params = {
            'p_servicio_suscrito': row.SERVICIO_SUSCRITO,
            'p_id_periodo_consumo': row.ID_PERIODO_CONSUMO
        }
        df_row_q4 = db.execute_query(queries.QUERY_DATOS_CONSUMOS, params)
        if not df_row_q4.empty:
            all_results_datos_consumos.append(df_row_q4)

    if not all_results_datos_consumos:
        log.warning("Query 4 no retornó resultados para ninguna iteración.")
        return pd.DataFrame()
    
    df_datos_consumos_final = pd.concat(all_results_datos_consumos, ignore_index=True)
    save_to_parquet(df_datos_consumos_final, config.QUERY_CONSUMOS_OUTPUT_FILE)
    log.info("--- Proceso extracción: [datos_consumos_producto] Completado ---")
    return df_datos_consumos_final


def run_query_ordenes_critica_previa(df_datos_lectura: pd.DataFrame) -> pd.DataFrame:
    """
    Ejecuta Query ordenes de critica y previa iterando sobre los resultados de Query los datos de lectura.

    Args:
        Dataframe con la información de datos de lectura de los productos.

    Returns:
        Dataframe con el resultado del query con la información de ordenes de critica y previa.
    """
    log.info("--- Iniciando proceso extracción: [datos_ordenes_previa_critica] ---")
    if df_datos_lectura is None or df_datos_lectura.empty:
        log.warning("No hay datos de Query 3 para procesar. Saltando Proceso 5.")
        return pd.DataFrame()

    # Validar columnas (TIPOCONS es la que añadimos en Q3)
    required_cols = ['ID_PERIODO_FACTURACION', 'SERVICIO_SUSCRITO', 'TIPOCONS']
    if not all(col in df_datos_lectura.columns for col in required_cols):
        log.error(f"Columnas requeridas {required_cols} no encontradas en Q3. ¿Olvidaste modificar QUERY_3? Abortando Proceso 5.")
        return pd.DataFrame()
    
    unique_combos = df_datos_lectura[required_cols].drop_duplicates().dropna()
        
    all_results_ordenes_critica_previa = []
    log.info(f"Iterando {len(unique_combos)} filas de Query 3 para ejecutar Query 5...")
    
    for row in tqdm(unique_combos.itertuples(index=False), total=len(unique_combos), desc="Procesando Q5 (Ordenes Crítica)"):
        params = {
            'p_id_periodo_facturacion': row.ID_PERIODO_FACTURACION,
            'p_servicio_suscrito': row.SERVICIO_SUSCRITO,
            'p_tipo_consumo': row.TIPOCONS
        }
        df_row_q5 = db.execute_query(queries.QUERY_ORDENES_CRITICA_PEVIA, params)
        if not df_row_q5.empty:
            # "Arrastramos" el tipo de consumo (TCONCODI_RAW ya no es necesario si Q5 lo retorna)
            # Revisando Q5, sí retorna 'TIPO_CONSUMO'. Usaremos ese.
            all_results_ordenes_critica_previa.append(df_row_q5)
            
    if not all_results_ordenes_critica_previa:
        log.warning("Query 5 no retornó resultados para ninguna iteración.")
        return pd.DataFrame()
    
    df_ordenes_critica_previa_final = pd.concat(all_results_ordenes_critica_previa, ignore_index=True)
    save_to_parquet(df_ordenes_critica_previa_final, config.QUERY_ORDENES_CRITICA_PREVIA_OUTPUT_FILE)
    log.info("--- Proceso extracción: [datos_ordenes_previa_critica] Completado ---")
    return df_ordenes_critica_previa_final


def run_query_comentarios_ordenes(df_ordenes_critica_previa: pd.DataFrame):
    """Ejecuta Query 6 iterando sobre los resultados de Query 5."""
    log.info("--- Iniciando proceso extracción: [datos_comentarios_ordenes] ---")
    if df_ordenes_critica_previa is None or df_ordenes_critica_previa.empty:
        log.warning("No hay datos de Query 5 para procesar. Saltando Proceso 6.")
        return

    required_cols = ['ID_ORDEN', 'SERVICIO_SUSCRITO', 'ID_PERIODO_CONSUMO', 
                     'TIPO_CONSUMO', 'FECHA_CREACION_ORDEN', 'FECHA_LEGALIZACION_ORDEN']
    
    if not all(col in df_ordenes_critica_previa.columns for col in required_cols):
        log.error(f"Columnas requeridas {required_cols} no encontradas en Q5. Abortando.")
        return
    
    unique_combos = df_ordenes_critica_previa[required_cols].drop_duplicates().dropna(subset=['ID_ORDEN'])
        
    all_results_cometario_ordenes = []
    log.info(f"Iterando {len(unique_combos)} combinaciones ÚNICAS para Q6...")
    
    for row in tqdm(unique_combos.itertuples(index=False), total=len(unique_combos), desc="Procesando Q6 (Comentarios)"):
        fecha_legalizacion = row.FECHA_LEGALIZACION_ORDEN
        if pd.isna(fecha_legalizacion):
            fecha_legalizacion = None 
            
        params = {
            'p_id_orden': row.ID_ORDEN,
            'p_servicio_suscrito': row.SERVICIO_SUSCRITO,
            'p_id_periodo_consumo': row.ID_PERIODO_CONSUMO,
            'p_tipo_consumo': row.TIPO_CONSUMO.split('-')[0], # Usamos el TIPO_CONSUMO de Q5
            'p_fecha_creacion': row.FECHA_CREACION_ORDEN,
            'p_fecha_legalizacion': fecha_legalizacion
        }
        df_row_q6 = db.execute_query(queries.QUERY_COMENTARIOS_ORDENES, params)
        if not df_row_q6.empty:
            all_results_cometario_ordenes.append(df_row_q6)

    if not all_results_cometario_ordenes:
        log.warning("Query 6 no retornó resultados para ninguna iteración.")
        return

    df_comentario_ordenes_final = pd.concat(all_results_cometario_ordenes, ignore_index=True)
    save_to_parquet(df_comentario_ordenes_final, config.QUERY_COMENTARIOS_ORDEN_OUTPUT_FILE)
    log.info("--- Proceso extracción: [datos_comentarios_ordenes] Completado ---")

def run_query_cuentas_cobro(df_datos_basicos: pd.DataFrame) -> pd.DataFrame:
    """Ejecuta Query 7 iterando sobre los resultados de Query 2."""
    log.info("--- Iniciando proceso extracción: [datos_cuentas_cobro] ---")
    if df_datos_basicos is None or df_datos_basicos.empty:
        log.warning("No hay datos de Query 2 para procesar. Saltando Proceso 7.")
        return pd.DataFrame()

    if 'SERVICIO_SUSCRITO' not in df_datos_basicos.columns:
        log.error("Columna 'SERVICIO_SUSCRITO' no encontrada. Abortando.")
        return pd.DataFrame()
    
    unique_servicios = df_datos_basicos['SERVICIO_SUSCRITO'].dropna().unique()
        
    all_results_cuentas_cobro = []
    log.info(f"Iterando {len(unique_servicios)} filas de Query 2 para ejecutar Query 7...")
    
    for servicio_suscrito in tqdm(unique_servicios, desc="Procesando Q7 (Cuentas Cobro)"):
        params = {'p_servicio_suscrito': int(servicio_suscrito)}
        df_row_q7 = db.execute_query(queries.QUERY_CUENTAS_COBRO, params)
        if not df_row_q7.empty:
            all_results_cuentas_cobro.append(df_row_q7)

    if not all_results_cuentas_cobro:
        log.warning("Query 7 no retornó resultados para ninguna iteración.")
        return pd.DataFrame()
    
    df_cuentas_cobro_final = pd.concat(all_results_cuentas_cobro, ignore_index=True)
    save_to_parquet(df_cuentas_cobro_final, config.QUERY_CUENTAS_COBRO_FILE)
    log.info("--- Proceso extracción: [datos_cuentas_cobro] Completado ---")
    return df_cuentas_cobro_final


def run_query_detalle_cargos(df_cuentas_cobro: pd.DataFrame):
    """Ejecuta Query 8 iterando sobre los resultados de Query 7."""
    log.info("--- Iniciando proceso extracción: [datos_detalle_cargos] ---")
    if df_cuentas_cobro is None or df_cuentas_cobro.empty:
        log.warning("No hay datos de Query 7 para procesar. Saltando Proceso 8.")
        return

    if 'ID_CUENTA_COBRO' not in df_cuentas_cobro.columns:
        log.error("Columna 'ID_CUENTA_COBRO' no encontrada. Abortando.")
        return
    
    unique_cuentas = df_cuentas_cobro['ID_CUENTA_COBRO'].dropna().unique()
        
    all_results_detalle_cuentas_cobro = []
    log.info(f"Iterando {len(unique_cuentas)} filas de Query 7 para ejecutar Query 8...")
    
    for id_cuenta_cobro in tqdm(unique_cuentas, desc="Procesando Q8 (Detalle Cargos)"):
        params = {'p_id_cuenta_cobro': int(id_cuenta_cobro)}
        df_row_q8 = db.execute_query(queries.QUERY_DETALLE_CARGOS, params)
        if not df_row_q8.empty:
            all_results_detalle_cuentas_cobro.append(df_row_q8)

    if not all_results_detalle_cuentas_cobro:
        log.warning("Query 8 no retornó resultados para ninguna iteración.")
        return

    df_detalle_cargos_final = pd.concat(all_results_detalle_cuentas_cobro, ignore_index=True)
    save_to_parquet(df_detalle_cargos_final, config.QUERY_DETALLE_CARGOS_FILE)
    log.info("--- Proceso extracción: [datos_detalle_cargos] Completado ---")


def run_query_detalle_solicitudes(df_datos_basicos: pd.DataFrame) -> pd.DataFrame:
    log.info("--- Iniciando proceso extracción: [detalle_solicitudes] ---")
    if df_datos_basicos is None or df_datos_basicos.empty:
        log.warning("No hay datos en datos_basicos para procesar. Saltando Proceso 7.")
        return pd.DataFrame()

    if 'SERVICIO_SUSCRITO' not in df_datos_basicos.columns:
        log.error("Columna 'SERVICIO_SUSCRITO' no encontrada. Abortando.")
        return pd.DataFrame()
    
    unique_servicios = df_datos_basicos['SERVICIO_SUSCRITO'].dropna().unique()
        
    all_results_detalle_solicitudes = []
    log.info(f"Iterando {len(unique_servicios)} filas de Query 2 para ejecutar Query 7...")
    
    for servicio_suscrito in tqdm(unique_servicios, desc="Procesando Q7 (Cuentas Cobro)"):
        params = {'p_servicio_suscrito': int(servicio_suscrito)}
        df_row_q7 = db.execute_query(queries.QUERY_DETALLE_SOLICITUDES, params)
        if not df_row_q7.empty:
            all_results_detalle_solicitudes.append(df_row_q7)

    if not all_results_detalle_solicitudes:
        log.warning("Query 7 no retornó resultados para ninguna iteración.")
        return pd.DataFrame()
    
    df_detalle_solicitudes_final = pd.concat(all_results_detalle_solicitudes, ignore_index=True)
    save_to_parquet(df_detalle_solicitudes_final, config.QUERY_DETALLE_SOLICITUDES_FILE)
    log.info("--- Proceso extracción: [detalle_solicitudes] Completado ---")
    return df_detalle_solicitudes_final