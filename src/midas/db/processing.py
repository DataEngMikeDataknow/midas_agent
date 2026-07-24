import os
import logging

import pandas as pd

from . import database as db
from . import queries

log = logging.getLogger(__name__)

_PARQUET_FILES = {
    "ordenes_pendientes":    "ordenes_calidad_pendientes.parquet",
    "datos_basicos":         "datos_basicos_producto.parquet",
    "lecturas":              "datos_lecturas_producto.parquet",
    "consumos":              "datos_consumos_producto.parquet",
    "ordenes_critica_previa":"datos_ordenes_previa_critica.parquet",
    "comentarios_orden":     "datos_comentarios_ordenes.parquet",
    "cuentas_cobro":         "datos_cuentas_cobro.parquet",
    "detalle_cargos":        "datos_detalle_cargos.parquet",
    "detalle_solicitudes":   "datos_detalle_solicitudes.parquet",
    # ─── Caso 2: única dimensión materializada (matriz de decisión facturable) ───
    "dim_estado_corte_facturable": "dim_estado_corte_facturable.parquet",
    # ─── Caso 2: promociones a la cadena ───
    "servicios_contrato":    "datos_servicios_contrato.parquet",
    "consumos_contrato":     "datos_consumos_contrato.parquet",
    "investigacion_consumo": "datos_investigacion_consumo.parquet",
}

def _out(key: str) -> str:
    return os.path.join(os.environ.get("OUTPUT_PATH", "data"), _PARQUET_FILES[key])

def ensure_data_dir():
    output_path = os.environ.get("OUTPUT_PATH", "data")
    os.makedirs(output_path, exist_ok=True)
    log.info(f"Directorio de salida asegurado: {output_path}")

def save_to_parquet(df: pd.DataFrame, file_path: str):
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
    log.info("--- Iniciando proceso extracción: [ordenes_calidad_pendientes] ---")
    df_ordenes_pendientes = db.execute_query(queries.QUERY_ORDENES_PENDIENTES)

    if df_ordenes_pendientes.empty:
        log.warning("Query [ordenes pendientes] no retornó resultados.")

    save_to_parquet(df_ordenes_pendientes, _out("ordenes_pendientes"))
    log.info("--- Proceso extracción [ordenes_calidad_pendientes] Completado ---")
    return df_ordenes_pendientes

def run_query_datos_basicos(df_ordenes_pendientes: pd.DataFrame) -> pd.DataFrame:
    log.info("--- Iniciando proceso extracción: [datos_basicos_producto] ---")
    if df_ordenes_pendientes is None or df_ordenes_pendientes.empty:
        log.warning("No hay datos de ordenes pendientes para procesar. Saltando Proceso 2.")
        return pd.DataFrame()

    if 'INSTALACION' not in df_ordenes_pendientes.columns:
        log.error("Columna 'INSTALACION' no encontrada en resultados de Query ordenes pendientes. Abortando Proceso 2.")
        return pd.DataFrame()

    unique_instalaciones = df_ordenes_pendientes['INSTALACION'].dropna().unique()

    all_results_datos_basicos = []
    log.info(f"Iterando {len(unique_instalaciones)} filas de Query ordenes pendientes para ejecutar Query datos basicos...")

    for address_id in unique_instalaciones:
        params = {'address_id': int(address_id)}
        df_row_q2 = db.execute_query(queries.QUERY_DATOS_BASICOS, params)

        if not df_row_q2.empty:
            all_results_datos_basicos.append(df_row_q2)

    if not all_results_datos_basicos:
        log.warning("Query datos basicos no retornó resultados para ninguna iteración.")
        return pd.DataFrame()

    df_datos_basicos_final = pd.concat(all_results_datos_basicos, ignore_index=True)
    save_to_parquet(df_datos_basicos_final, _out("datos_basicos"))
    log.info("--- Proceso extracción: [datos_basicos_producto] Completado ---")
    return df_datos_basicos_final

def run_query_datos_lectura(df_datos_basicos: pd.DataFrame):
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

    for servicio_suscrito in unique_servicios:
        params = {'servicio_suscrito': int(servicio_suscrito)}
        df_row_q3 = db.execute_query(queries.QUERY_DATOS_LECTURA, params)
        if not df_row_q3.empty:
            all_results_datos_lectura.append(df_row_q3)

    if not all_results_datos_lectura:
        log.warning("Query 3 no retornó resultados para ninguna iteración.")
        return

    df_datos_lectura_final = pd.concat(all_results_datos_lectura, ignore_index=True)
    save_to_parquet(df_datos_lectura_final, _out("lecturas"))
    log.info("--- Proceso extracción: [datos_lecturas_producto] Completado ---")
    return df_datos_lectura_final

def run_query_datos_consumos(df_datos_lecturas: pd.DataFrame) -> pd.DataFrame:
    log.info("--- Iniciando proceso extracción: [datos_consumos_producto] ---")
    if df_datos_lecturas is None or df_datos_lecturas.empty:
        log.warning("No hay datos de Query 3 para procesar. Saltando Proceso 4.")
        return pd.DataFrame()

    required_cols = ['SERVICIO_SUSCRITO', 'ID_PERIODO_CONSUMO']
    if not all(col in df_datos_lecturas.columns for col in required_cols):
        log.error(f"Columnas requeridas {required_cols} no encontradas en resultados de Query 3. Abortando Proceso 4.")
        return pd.DataFrame()

    unique_combos = df_datos_lecturas[required_cols].drop_duplicates().dropna()

    all_results_datos_consumos = []
    log.info(f"Iterando {len(unique_combos)} filas de Query 3 para ejecutar Query 4...")

    for row in unique_combos.itertuples(index=False):
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
    save_to_parquet(df_datos_consumos_final, _out("consumos"))
    log.info("--- Proceso extracción: [datos_consumos_producto] Completado ---")
    return df_datos_consumos_final

def run_query_ordenes_critica_previa(df_datos_lectura: pd.DataFrame) -> pd.DataFrame:
    log.info("--- Iniciando proceso extracción: [datos_ordenes_previa_critica] ---")
    if df_datos_lectura is None or df_datos_lectura.empty:
        log.warning("No hay datos de Query 3 para procesar. Saltando Proceso 5.")
        return pd.DataFrame()

    required_cols = ['ID_PERIODO_FACTURACION', 'SERVICIO_SUSCRITO', 'TIPOCONS']
    if not all(col in df_datos_lectura.columns for col in required_cols):
        log.error(f"Columnas requeridas {required_cols} no encontradas en Q3. ¿Olvidaste modificar QUERY_3? Abortando Proceso 5.")
        return pd.DataFrame()

    unique_combos = df_datos_lectura[required_cols].drop_duplicates().dropna()

    all_results_ordenes_critica_previa = []
    log.info(f"Iterando {len(unique_combos)} filas de Query 3 para ejecutar Query 5...")

    for row in unique_combos.itertuples(index=False):
        params = {
            'p_id_periodo_facturacion': row.ID_PERIODO_FACTURACION,
            'p_servicio_suscrito': row.SERVICIO_SUSCRITO,
            'p_tipo_consumo': row.TIPOCONS
        }
        df_row_q5 = db.execute_query(queries.QUERY_ORDENES_CRITICA_PEVIA, params)
        if not df_row_q5.empty:
            all_results_ordenes_critica_previa.append(df_row_q5)

    if not all_results_ordenes_critica_previa:
        log.warning("Query 5 no retornó resultados para ninguna iteración.")
        return pd.DataFrame()

    df_ordenes_critica_previa_final = pd.concat(all_results_ordenes_critica_previa, ignore_index=True)
    save_to_parquet(df_ordenes_critica_previa_final, _out("ordenes_critica_previa"))
    log.info("--- Proceso extracción: [datos_ordenes_previa_critica] Completado ---")
    return df_ordenes_critica_previa_final

def run_query_comentarios_ordenes(df_ordenes_critica_previa: pd.DataFrame):
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

    for row in unique_combos.itertuples(index=False):
        fecha_legalizacion = row.FECHA_LEGALIZACION_ORDEN
        if pd.isna(fecha_legalizacion):
            fecha_legalizacion = None

        params = {
            'p_id_orden': row.ID_ORDEN,
            'p_servicio_suscrito': row.SERVICIO_SUSCRITO,
            'p_id_periodo_consumo': row.ID_PERIODO_CONSUMO,
            'p_tipo_consumo': row.TIPO_CONSUMO.split('-')[0],
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
    save_to_parquet(df_comentario_ordenes_final, _out("comentarios_orden"))
    log.info("--- Proceso extracción: [datos_comentarios_ordenes] Completado ---")

def run_query_cuentas_cobro(df_datos_basicos: pd.DataFrame) -> pd.DataFrame:
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

    for servicio_suscrito in unique_servicios:
        params = {'p_servicio_suscrito': int(servicio_suscrito)}
        df_row_q7 = db.execute_query(queries.QUERY_CUENTAS_COBRO, params)
        if not df_row_q7.empty:
            all_results_cuentas_cobro.append(df_row_q7)

    if not all_results_cuentas_cobro:
        log.warning("Query 7 no retornó resultados para ninguna iteración.")
        return pd.DataFrame()

    df_cuentas_cobro_final = pd.concat(all_results_cuentas_cobro, ignore_index=True)
    save_to_parquet(df_cuentas_cobro_final, _out("cuentas_cobro"))
    log.info("--- Proceso extracción: [datos_cuentas_cobro] Completado ---")
    return df_cuentas_cobro_final

def run_query_detalle_cargos(df_cuentas_cobro: pd.DataFrame):
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

    for id_cuenta_cobro in unique_cuentas:
        params = {'p_id_cuenta_cobro': int(id_cuenta_cobro)}
        df_row_q8 = db.execute_query(queries.QUERY_DETALLE_CARGOS, params)
        if not df_row_q8.empty:
            all_results_detalle_cuentas_cobro.append(df_row_q8)

    if not all_results_detalle_cuentas_cobro:
        log.warning("Query 8 no retornó resultados para ninguna iteración.")
        return

    df_detalle_cargos_final = pd.concat(all_results_detalle_cuentas_cobro, ignore_index=True)
    save_to_parquet(df_detalle_cargos_final, _out("detalle_cargos"))
    log.info("--- Proceso extracción: [datos_detalle_cargos] Completado ---")

def run_query_detalle_solicitudes(df_datos_basicos: pd.DataFrame) -> pd.DataFrame:
    log.info("--- Iniciando proceso extracción: [detalle_solicitudes] ---")
    if df_datos_basicos is None or df_datos_basicos.empty:
        log.warning("No hay datos en datos_basicos para procesar. Saltando.")
        return pd.DataFrame()

    if 'SERVICIO_SUSCRITO' not in df_datos_basicos.columns:
        log.error("Columna 'SERVICIO_SUSCRITO' no encontrada. Abortando.")
        return pd.DataFrame()

    unique_servicios = df_datos_basicos['SERVICIO_SUSCRITO'].dropna().unique()

    all_results_detalle_solicitudes = []
    log.info(f"Iterando {len(unique_servicios)} servicios para ejecutar Query detalle solicitudes...")

    for servicio_suscrito in unique_servicios:
        ss = int(servicio_suscrito)
        params = {'p_servicio_suscrito': ss}
        df_row = db.execute_query(queries.QUERY_DETALLE_SOLICITUDES, params)
        if not df_row.empty:
            # La query NO devuelve el SS (es el bind): se materializa como columna para
            # poder atribuir cada solicitud a su servicio suscrito (columna_join del control).
            df_row.insert(0, 'SERVICIO_SUSCRITO', ss)
            all_results_detalle_solicitudes.append(df_row)

    if not all_results_detalle_solicitudes:
        log.warning("Query detalle solicitudes no retornó resultados para ninguna iteración.")
        return pd.DataFrame()

    df_detalle_solicitudes_final = pd.concat(all_results_detalle_solicitudes, ignore_index=True)
    df_detalle_solicitudes_final = _adaptar_solicitudes_a_bronze(df_detalle_solicitudes_final)
    save_to_parquet(df_detalle_solicitudes_final, _out("detalle_solicitudes"))
    log.info("--- Proceso extracción: [detalle_solicitudes] Completado ---")
    return df_detalle_solicitudes_final


# Mapeo al schema de la Bronze EXISTENTE `midas_datos_detalle_solicitudes_bronze`
# (validado en dllo 21-22 jul 2026, celda F2 de 30_validacion_midas.py). La tabla ya existe con
# nombres en español y `servicio_suscrito` como primera columna; negocio confirmó que SE USA,
# así que se ADOPTA (no se recrea). El renombrado va aquí, en la capa Python: NO se modifica
# el SQL de QUERY_DETALLE_SOLICITUDES. El orden es posicional porque insertInto lo exige.
_SOLICITUDES_BRONZE_COLS = [
    ("SERVICIO_SUSCRITO",   "servicio_suscrito"),
    ("PACKAGE_ID",          "id_solicitud"),
    ("SUBSCRIBER",          "usuario"),
    ("PACKAGE_TYPE",        "tipo_solicitud"),
    ("REQUEST_DATE",        "fecha_solicitud"),
    ("PACKAGE_STATUS",      "estado_solicitud"),
    ("ATTENTION_DATE",      "fecha_atencion_solicitud"),
    ("COMMENT_",            "comentario"),
    ("RECEPTION_TYPE",      "medio_recepcion"),
    ("VENDOR",              "analista"),
    ("ORGANIZAT_AREA_ID",   "area_organizacional"),
]


def _adaptar_solicitudes_a_bronze(df: pd.DataFrame) -> pd.DataFrame:
    """Renombra y reordena al schema posicional de la Bronze existente de solicitudes."""
    if df is None or df.empty:
        return df
    faltantes = [o for (o, _) in _SOLICITUDES_BRONZE_COLS if o not in df.columns]
    if faltantes:
        log.error("Solicitudes: faltan columnas esperadas del query %s. "
                  "Se deja el DataFrame sin adaptar (revisar QUERY_DETALLE_SOLICITUDES).", faltantes)
        return df
    orden_origen = [o for (o, _) in _SOLICITUDES_BRONZE_COLS]
    df = df[orden_origen].rename(columns=dict(_SOLICITUDES_BRONZE_COLS))
    return df


# =============================================================================
# CASO 2 — Dimensiones de referencia (catalogos, full overwrite sin iteracion)
# =============================================================================

def _run_dim(query: str, out_key: str, nombre: str) -> pd.DataFrame:
    """Extrae un catalogo completo (full overwrite) y lo materializa a Parquet."""
    log.info(f"--- Iniciando extracción dimensión: [{nombre}] ---")
    df = db.execute_query(query)
    if df.empty:
        log.warning(f"Dimensión [{nombre}] no retornó filas.")
    save_to_parquet(df, _out(out_key))
    log.info(f"--- Dimensión [{nombre}] completada ({len(df)} filas) ---")
    return df


def run_query_dim_estado_corte_facturable() -> pd.DataFrame:
    """Única dimensión materializada: matriz de decisión facturable (estado × servicio).
    El resto de catálogos se resuelven INLINE en las queries (patrón del Caso 1)."""
    return _run_dim(queries.QUERY_DIM_ESTADO_CORTE_FACTURABLE,
                    "dim_estado_corte_facturable", "estado_corte_facturable")


# =============================================================================
# CASO 2 — Promociones: roster del contrato, consumos multi-servicio, investigacion
# =============================================================================

def run_query_servicios_contrato(df_datos_basicos: pd.DataFrame) -> pd.DataFrame:
    """A2 — Todos los SS del contrato, iterando por los contratos de datos_basicos."""
    log.info("--- Iniciando proceso extracción: [servicios_contrato] ---")
    if df_datos_basicos is None or df_datos_basicos.empty:
        log.warning("No hay datos_basicos para procesar. Saltando servicios_contrato.")
        return pd.DataFrame()
    if 'CONTRATO' not in df_datos_basicos.columns:
        log.error("Columna 'CONTRATO' no encontrada en datos_basicos. Abortando servicios_contrato.")
        return pd.DataFrame()

    unique_contratos = df_datos_basicos['CONTRATO'].dropna().unique()
    all_res = []
    log.info(f"Iterando {len(unique_contratos)} contratos para servicios_contrato...")
    for contrato in unique_contratos:
        params = {'p_contrato': int(contrato)}
        df_row = db.execute_query(queries.QUERY_SERVICIOS_CONTRATO, params)
        if not df_row.empty:
            all_res.append(df_row)

    if not all_res:
        log.warning("servicios_contrato no retornó resultados para ninguna iteración.")
        return pd.DataFrame()

    df_final = pd.concat(all_res, ignore_index=True)
    save_to_parquet(df_final, _out("servicios_contrato"))
    log.info("--- Proceso extracción: [servicios_contrato] Completado ---")
    return df_final


def run_query_consumos_contrato(df_servicios_contrato: pd.DataFrame) -> pd.DataFrame:
    """A3 — Consumos (6m) de cada SS del roster del contrato."""
    log.info("--- Iniciando proceso extracción: [consumos_contrato] ---")
    if df_servicios_contrato is None or df_servicios_contrato.empty:
        log.warning("No hay servicios_contrato para procesar. Saltando consumos_contrato.")
        return pd.DataFrame()
    if 'SERVICIO_SUSCRITO' not in df_servicios_contrato.columns:
        log.error("Columna 'SERVICIO_SUSCRITO' no encontrada en servicios_contrato. Abortando.")
        return pd.DataFrame()

    unique_ss = df_servicios_contrato['SERVICIO_SUSCRITO'].dropna().unique()
    all_res = []
    log.info(f"Iterando {len(unique_ss)} SS del contrato para consumos_contrato...")
    for ss in unique_ss:
        params = {'p_servicio_suscrito': int(ss)}
        df_row = db.execute_query(queries.QUERY_CONSUMOS_CONTRATO, params)
        if not df_row.empty:
            all_res.append(df_row)

    if not all_res:
        log.warning("consumos_contrato no retornó resultados para ninguna iteración.")
        return pd.DataFrame()

    df_final = pd.concat(all_res, ignore_index=True)
    save_to_parquet(df_final, _out("consumos_contrato"))
    log.info("--- Proceso extracción: [consumos_contrato] Completado ---")
    return df_final


def run_query_investigacion_consumo(df_datos_basicos: pd.DataFrame) -> pd.DataFrame:
    """A4 — Consumo en investigación (PE_INVEST_CONSUM) por SS (estado crudo)."""
    log.info("--- Iniciando proceso extracción: [investigacion_consumo] ---")
    if df_datos_basicos is None or df_datos_basicos.empty:
        log.warning("No hay datos_basicos para procesar. Saltando investigacion_consumo.")
        return pd.DataFrame()
    if 'SERVICIO_SUSCRITO' not in df_datos_basicos.columns:
        log.error("Columna 'SERVICIO_SUSCRITO' no encontrada en datos_basicos. Abortando.")
        return pd.DataFrame()

    unique_ss = df_datos_basicos['SERVICIO_SUSCRITO'].dropna().unique()
    all_res = []
    log.info(f"Iterando {len(unique_ss)} SS para investigacion_consumo...")
    for ss in unique_ss:
        params = {'p_servicio_suscrito': int(ss)}
        df_row = db.execute_query(queries.QUERY_INVESTIGACION_CONSUMO, params)
        if not df_row.empty:
            all_res.append(df_row)

    if not all_res:
        log.warning("investigacion_consumo no retornó resultados para ninguna iteración.")
        return pd.DataFrame()

    df_final = pd.concat(all_res, ignore_index=True)
    save_to_parquet(df_final, _out("investigacion_consumo"))
    log.info("--- Proceso extracción: [investigacion_consumo] Completado ---")
    return df_final
