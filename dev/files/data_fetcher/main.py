# main.py
import time
import logging
import os

# Importar módulos de nuestro paquete src
from src import database as db
from src import processing
from src.config import DATA_DIR # Para imprimir la ruta al final

# Configurar logging (lo hacemos aquí para que aplique a todos los módulos)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

def main():
    """
    Punto de entrada principal para el proceso de extracción de Oracle a Parquet.
    """
    start_time = time.time()
    log.info("==================================================")
    log.info("INICIANDO PROCESO DE EXTRACCIÓN ORACLE -> PARQUET")
    log.info("==================================================")
    
    try:
        # 0. Asegurar que el directorio de datos exista
        processing.ensure_data_dir()
        
        # 1. Inicializar la base de datos (Cliente y Pool)
        db.init_database()
        
        # 2. Ejecutar Query ordenes de calidad pendientes y obtener sus resultados
        df_ordenes_pendientes_results = processing.run_query_ordenes_pendientes()
        
        # 3. Ejecutar Query datos basicos (depende de los resultados de las ordenes de calidad pendientes)
        df_datos_basicos_results = processing.run_query_datos_basicos(df_ordenes_pendientes_results)
        
        # 4. Ejecutar Query datos de lectura (depende de los resultados de los datos basicos)
        df_datos_lecturas_results = processing.run_query_datos_lectura(df_datos_basicos_results)

        # 5. Ejecutar Query datos de consumos (depende de los resultados de los datos de lectura)
        processing.run_query_datos_consumos(df_datos_lecturas_results)

        # 6. Ejecutar Query ordenes de critica y previa (depende de los resultados de los datos de lectura)
        df_ordenes_critica_previa_results = processing.run_query_ordenes_critica_previa(df_datos_lecturas_results)
        
        # 7. Ejecutar Query comentarios de las ordenes de critica y previa (depende de los resultados de las ordenes de critica y previa)
        processing.run_query_comentarios_ordenes(df_ordenes_critica_previa_results)

        # 8. Ejecutar Query 7 (depende de los resultados de Q2)
        df_cuentas_cobro_results = processing.run_query_cuentas_cobro(df_datos_basicos_results)

        # 9. Ejecutar Query 8 (depende de los resultados de Q7)
        processing.run_query_detalle_cargos(df_cuentas_cobro_results)

        # 10. Ejecutar query detalle solicitudes
        processing.run_query_detalle_solicitudes(df_datos_basicos_results)

    except Exception as e:
        log.critical(f"Ha ocurrido un error fatal en el proceso principal: {e}", exc_info=True)
    finally:
        # 5. Cerrar el pool de conexiones SIEMPRE
        db.close_pool()
        
    end_time = time.time()
    log.info("=================================================")
    log.info(f"PROCESO COMPLETADO EN {end_time - start_time:.2f} SEGUNDOS")
    log.info(f"Archivos Parquet generados en: {os.path.abspath(DATA_DIR)}")
    log.info("=================================================")

if __name__ == "__main__":
    main()