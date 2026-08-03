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
    # ─── Caso 2: promociones a la cadena ───
    "perdidas_no_operacionales": "perdidas_no_operacionales.parquet",
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
    """Escribe el Parquet. Lanza si no puede.

    DOS cambios frente a la version anterior, los dos por la misma razon:

    1. Un DataFrame VACIO pero CON COLUMNAS **si** se escribe. Antes se devolvia sin
       escribir, y el archivo de la corrida anterior sobrevivia con el mismo nombre.
       La ingesta lo leia despues como si fuera de hoy. Escribir el vacio deja el
       estado sin ambiguedad: cero filas es cero filas, no "lo de ayer".
    2. Un fallo de escritura **lanza** en vez de loguearse y seguir. Tragarlo dejaba
       igualmente el archivo viejo en su sitio.

    Un DataFrame sin columnas si es un error de programacion: significa que alguien
    construyo un DataFrame vacio a mano en vez de dejar que el query fallara.
    """
    if df is None:
        raise ValueError(f"save_to_parquet recibio None para {file_path}.")

    if len(df.columns) == 0:
        raise ValueError(
            f"save_to_parquet recibio un DataFrame SIN COLUMNAS para {file_path}. "
            f"Eso ya no deberia ocurrir: execute_query lanza ante un fallo en vez de "
            f"devolver un DataFrame vacio."
        )

    if df.empty:
        log.warning("Resultado VACIO para %s: se escribe igual (0 filas) para no dejar "
                    "el archivo de la corrida anterior en su sitio.", file_path)

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
        raise RuntimeError(f"No se pudo escribir el Parquet {file_path}: {e}") from e

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

    # Deduplicacion OBLIGATORIA por la rama 4 (orden de decision del analista, 7400027).
    # Este bucle itera (periodo_facturacion, SS, tipocons). Las ramas 1-3 filtran por
    # :p_tipo_consumo, asi que nunca repiten entre iteraciones. La rama 4 NO puede filtrar
    # por tipo (la orden de decision no expone uno), de modo que un SS con activa Y reactiva
    # en el mismo periodo la devuelve una vez por cada tipo. El UNION de Oracle deduplica
    # DENTRO de una llamada; pd.concat entre llamadas no.
    # Detectado en dllo el 2026-07-30: 27 filas para 26 ordenes distintas.
    # Es seguro para las ramas 1-3: dos filas identicas en las 12 columnas son el mismo
    # hecho, no dos hechos distintos.
    antes = len(df_ordenes_critica_previa_final)
    df_ordenes_critica_previa_final = df_ordenes_critica_previa_final.drop_duplicates(
        ignore_index=True)
    if antes != len(df_ordenes_critica_previa_final):
        log.info("Critica: %d filas duplicadas eliminadas (rama 4 sin filtro de tipo_consumo).",
                 antes - len(df_ordenes_critica_previa_final))

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

    # dropna SOLO sobre ID_ORDEN: la rama 4 trae tipo_consumo NULL y no se puede
    # descartar por eso (perderiamos las ordenes de decision del analista).
    unique_combos = df_ordenes_critica_previa[required_cols].drop_duplicates().dropna(subset=['ID_ORDEN'])

    all_results_cometario_ordenes = []
    log.info(f"Iterando {len(unique_combos)} combinaciones ÚNICAS para Q6...")

    for row in unique_combos.itertuples(index=False):
        fecha_legalizacion = row.FECHA_LEGALIZACION_ORDEN
        if pd.isna(fecha_legalizacion):
            fecha_legalizacion = None

        # La rama 4 (orden de decision del analista, 7400027) no expone tipo de consumo:
        # llega NULL. Sin esta guarda, `.split('-')` lanzaria AttributeError y como este
        # paso corre con abortar_en_fallo=True, tumbaria la cadena ENTERA del Caso 1.
        tipo_consumo = row.TIPO_CONSUMO
        if tipo_consumo is None or pd.isna(tipo_consumo):
            tipo_consumo_cod = None
        else:
            tipo_consumo_cod = str(tipo_consumo).split('-')[0]

        params = {
            'p_id_orden': row.ID_ORDEN,
            'p_servicio_suscrito': row.SERVICIO_SUSCRITO,
            'p_id_periodo_consumo': row.ID_PERIODO_CONSUMO,
            'p_tipo_consumo': tipo_consumo_cod,
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
    # Sin este return, chain_runner recibe None, cuenta 0 y registra EXITOSO con
    # filas_escritas=0 aunque se hayan extraido miles. El log mentia.
    return df_comentario_ordenes_final

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
    # Mismo defecto que comentarios: sin return, el log reportaba 0 filas.
    return df_detalle_cargos_final

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
# CASO 2 — Promociones: roster del contrato, consumos multi-servicio, investigacion
#
# v3 R1: ya no hay extractores de dimension. `_run_dim` se elimino junto con la ultima
# dimension materializada (matriz facturable), que ahora se resuelve inline en
# QUERY_DATOS_BASICOS. Invariante I11.
# =============================================================================

def run_query_consumos_contrato(df_datos_basicos: pd.DataFrame,
                                df_ordenes_pendientes: pd.DataFrame) -> pd.DataFrame:
    """A3 (v3 R2) — Consumos (6m) de los SS hermanos del contrato.

    Antes iteraba sobre `midas_datos_servicios_contrato_bronze`. Esa tabla se retiró:
    los datos básicos ya contienen los SS y el roster se obtiene FILTRANDO por contrato.

    El filtro por CONTRATO es deliberado y NO debe reemplazarse por "todos los SS de la
    instalación": la instalación es un superconjunto que traería SS de contratos ajenos
    (otros clientes del mismo predio), lo que inflaría el volumen y contaminaría el
    análisis multi-servicio del Caso 13.

    Efecto colateral valioso: al desaparecer la tabla intermedia desaparece también la
    posibilidad de que A3 itere sobre un roster ESTANCADO de una corrida anterior (A2
    corría con abortar_en_fallo=False, así que podía quedar desactualizada mientras
    datos_basicos sí se sobrescribía).
    """
    log.info("--- Iniciando proceso extracción: [consumos_contrato] ---")
    if df_datos_basicos is None or df_datos_basicos.empty:
        log.warning("No hay datos_basicos para procesar. Saltando consumos_contrato.")
        return pd.DataFrame()
    for col in ('SERVICIO_SUSCRITO', 'CONTRATO'):
        if col not in df_datos_basicos.columns:
            log.error("Columna '%s' no encontrada en datos_basicos. Abortando consumos_contrato.", col)
            return pd.DataFrame()

    # Contratos "de las órdenes": los de los SS que efectivamente generaron una orden.
    if df_ordenes_pendientes is not None and not df_ordenes_pendientes.empty             and 'SERVICIO_SUSCRITO' in df_ordenes_pendientes.columns:
        ss_ordenes = set(df_ordenes_pendientes['SERVICIO_SUSCRITO'].dropna().astype('int64'))
        contratos = (df_datos_basicos[df_datos_basicos['SERVICIO_SUSCRITO'].isin(ss_ordenes)]
                     ['CONTRATO'].dropna().unique())
    else:
        # Sin órdenes no hay a qué acotar: se usan todos los contratos de datos_basicos.
        log.warning("Sin órdenes pendientes: se usan todos los contratos de datos_basicos.")
        contratos = df_datos_basicos['CONTRATO'].dropna().unique()

    unique_ss = (df_datos_basicos[df_datos_basicos['CONTRATO'].isin(contratos)]
                 ['SERVICIO_SUSCRITO'].dropna().unique())

    # Reporte pedido en §3.3: cuánto ahorra acotar por contrato en vez de por instalación.
    total_instalacion = df_datos_basicos['SERVICIO_SUSCRITO'].dropna().nunique()
    log.info("consumos_contrato: %d SS por contrato vs %d SS por instalación (%d contratos).",
             len(unique_ss), total_instalacion, len(contratos))

    all_res = []
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


# =============================================================================
# R4 (v3) — Perdidas No Operacionales
# =============================================================================
def run_query_perdidas_no_operacionales(df_datos_basicos: pd.DataFrame) -> pd.DataFrame:
    """PNO por servicio suscrito.

    ESPEJA exactamente el paso A1 (detalle_solicitudes, §5.5 del prompt v3): mismo conjunto
    de SS (los de datos_basicos) y mismo mecanismo de iteracion. No se inventa un driver.

    A diferencia de A1, la query SI proyecta `normalized_prod_id` como servicio_suscrito,
    asi que NO hay que insertar la columna del bind.
    """
    log.info("--- Iniciando proceso extracción: [perdidas_no_operacionales] ---")
    if df_datos_basicos is None or df_datos_basicos.empty:
        log.warning("No hay datos en datos_basicos para procesar. Saltando PNO.")
        return pd.DataFrame()

    if 'SERVICIO_SUSCRITO' not in df_datos_basicos.columns:
        log.error("Columna 'SERVICIO_SUSCRITO' no encontrada. Abortando PNO.")
        return pd.DataFrame()

    unique_servicios = df_datos_basicos['SERVICIO_SUSCRITO'].dropna().unique()

    all_results = []
    log.info(f"Iterando {len(unique_servicios)} servicios para ejecutar Query PNO...")

    for servicio_suscrito in unique_servicios:
        params = {'p_servicio_suscrito': int(servicio_suscrito)}
        df_row = db.execute_query(queries.QUERY_PERDIDAS_NO_OPERACIONALES, params)
        if not df_row.empty:
            all_results.append(df_row)

    if not all_results:
        log.warning("Query PNO no retornó resultados para ninguna iteración.")
        return pd.DataFrame()

    df_final = pd.concat(all_results, ignore_index=True)
    # Verificacion §5.4.4: volumen, para dimensionar si hace falta ventana temporal.
    log.info("PNO: %d filas sobre %d SS distintos (de %d SS iterados).",
             len(df_final),
             df_final['SERVICIO_SUSCRITO'].nunique() if 'SERVICIO_SUSCRITO' in df_final.columns else -1,
             len(unique_servicios))
    save_to_parquet(df_final, _out("perdidas_no_operacionales"))
    log.info("--- Proceso extracción: [perdidas_no_operacionales] Completado ---")
    return df_final
