"""
Tests de la Fase 2 (Bronze Caso 2): queries nuevas (binds), tables_config,
consistencia del control y pasos del chain_runner. Sin Spark ni Oracle reales.
"""
import sys
import unittest
from unittest.mock import MagicMock

# pyspark debe estar mockeado antes de importar los runners/framework.
sys.modules.setdefault("pyspark", MagicMock())
sys.modules.setdefault("pyspark.sql", MagicMock())

from src.midas.db import database, queries          # noqa: E402
from src.midas.db import processing                 # noqa: E402
import src.midas.main_ingestion as mi               # noqa: E402
from src.midas.framework import chain_runner        # noqa: E402


# v3 R1: NO hay dimensiones materializadas. Ninguna. Invariante I11.
DIMS = []
DIM_TABLES = []
# Todas las dims eliminadas: NO deben existir en el código.
DIMS_ELIMINADAS = [
    "QUERY_DIM_TIPO_CONSUMO", "QUERY_DIM_OBSERVACION_LECTURA", "QUERY_DIM_CALIFICACION",
    "QUERY_DIM_CONCEPTO", "QUERY_DIM_CAUSAL_CARGO", "QUERY_DIM_METODO_CALCULO",
    "QUERY_DIM_TIPO_SOLICITUD", "QUERY_DIM_ESTADO_INVESTIGACION",
    "QUERY_DIM_ESTADO_CORTE_FACTURABLE",
]
PROMO_TABLES = [
    "midas_datos_detalle_solicitudes_c2_bronze",
    "midas_datos_consumos_contrato_bronze", "midas_datos_investigacion_consumo_bronze",
]
# v3 R2: retirada. Un agrupador, una tabla (I12).
TABLAS_RETIRADAS_V3 = [
    "midas_dim_estado_corte_facturable_bronze",
    "midas_datos_servicios_contrato_bronze",
]
# v3 R4
PNO_TABLE = "midas_datos_perdidas_no_operacionales_bronze"
# v3 R3: columnas de periodo agregadas AL FINAL de cada query (invariante I13).
COLUMNAS_R3 = {
    "QUERY_DATOS_LECTURA":         ["anio_facturacion", "mes_facturacion", "ciclo_facturacion"],
    "QUERY_DATOS_CONSUMOS":        ["fecha_ini_consumo", "fecha_fin_consumo"],
    "QUERY_ORDENES_CRITICA_PEVIA": ["fecha_ini_consumo", "fecha_fin_consumo"],
    "QUERY_CUENTAS_COBRO":         ["id_periodo_consumo", "fecha_ini_consumo", "fecha_fin_consumo"],
    "QUERY_DETALLE_CARGOS":        ["fecha_ini_consumo", "fecha_fin_consumo",
                                    "anio_facturacion", "mes_facturacion"],
    "QUERY_CONSUMOS_CONTRATO":     ["fecha_ini_consumo", "fecha_fin_consumo",
                                    "anio_facturacion", "mes_facturacion"],
    "QUERY_INVESTIGACION_CONSUMO": ["fecha_ini_consumo", "fecha_fin_consumo"],
}


class TestNuevasQueriesBinds(unittest.TestCase):
    def test_consumos_contrato_bind(self):
        prepared, values = database._prepare(queries.QUERY_CONSUMOS_CONTRATO, {"p_servicio_suscrito": 5})
        self.assertEqual(prepared.count("?"), 1)
        self.assertEqual(values, [5])

    def test_investigacion_bind(self):
        prepared, values = database._prepare(queries.QUERY_INVESTIGACION_CONSUMO, {"p_servicio_suscrito": 5})
        self.assertEqual(prepared.count("?"), 1)
        self.assertEqual(values, [5])

    def test_dims_sin_binds(self):
        for nombre in DIMS:
            q = getattr(queries, nombre)
            prepared, values = database._prepare(q, {})
            self.assertEqual(prepared, q, f"{nombre} no debería tener binds")
            self.assertEqual(values, [])


class TestTablesConfig(unittest.TestCase):
    def setUp(self):
        self.cfg = mi.build_tables_config("/vol")
        self.by_name = {c["name"]: c for c in self.cfg}

    def test_conteo_total(self):
        # 8 cadena + 3 promociones + 1 PNO = 12. La orden de decision del analista NO
        # agrega tablas: es la rama 4 de QUERY_ORDENES_CRITICA_PEVIA.
        self.assertEqual(len(self.cfg), 12)

    def test_tablas_retiradas_no_estan(self):
        for t in TABLAS_RETIRADAS_V3:
            self.assertNotIn(t, self.by_name, f"{t} debió retirarse en v3")

    def test_pno_presente_y_configurada(self):
        pno = self.by_name[PNO_TABLE]
        self.assertEqual(pno["primary_key"], "id_pno")
        self.assertTrue(pno["path"].endswith("perdidas_no_operacionales.parquet"))
        self.assertTrue(pno.get("column_comments"), "PNO sin column_comments (§7.4)")

    def test_nombres_unicos(self):
        nombres = [c["name"] for c in self.cfg]
        self.assertEqual(len(nombres), len(set(nombres)))

    def test_nuevas_entradas_presentes(self):
        for t in DIM_TABLES + PROMO_TABLES + [PNO_TABLE]:
            self.assertIn(t, self.by_name)

    def test_pk_solicitudes_compuesta(self):
        self.assertEqual(self.by_name["midas_datos_detalle_solicitudes_c2_bronze"]["primary_key"],
                         ["servicio_suscrito", "id_solicitud"])

    def test_paths_bajo_volumen(self):
        for c in self.cfg:
            self.assertTrue(c["path"].startswith("/vol/"))

    def test_consistencia_query_key(self):
        # Toda tabla de la config debe tener su query_key mapeado.
        for c in self.cfg:
            self.assertIn(c["name"], mi._QUERY_KEY)


class TestControlYPasos(unittest.TestCase):
    def test_query_key_tiene_12(self):
        self.assertEqual(len(mi._QUERY_KEY), 12)

    def test_dims_podadas_no_existen(self):
        """v3: las 8 dims redundantes no deben quedar ni en queries ni en processing."""
        for nombre in DIMS_ELIMINADAS:
            self.assertFalse(hasattr(queries, nombre), f"quedó {nombre} en queries.py")
        for fn in ["run_query_dim_tipo_consumo", "run_query_dim_calificacion",
                   "run_query_dim_concepto", "run_query_dim_tipo_solicitud",
                   "run_query_dim_estado_investigacion",
                   "run_query_dim_estado_corte_facturable", "_run_dim"]:
            self.assertFalse(hasattr(processing, fn), f"quedó processing.{fn}")

    def test_pasos_chain_runner_existen(self):
        pasos = [
            chain_runner.PASO_SOLICITUDES,
            chain_runner.PASO_CONSUMOS_CONTRATO, chain_runner.PASO_INVESTIGACION,
            chain_runner.PASO_PNO,
        ]
        for tabla, query_key in pasos:
            self.assertTrue(tabla.startswith("midas_"))
            self.assertTrue(query_key.startswith("QUERY_"))

    def test_processing_tiene_funciones_nuevas(self):
        for fn in ["run_query_consumos_contrato",
                   "run_query_investigacion_consumo",
                   "run_query_perdidas_no_operacionales"]:
            self.assertTrue(hasattr(processing, fn), f"falta processing.{fn}")


class TestR3TraduccionPeriodos(unittest.TestCase):
    """v3 R3: cada query tocada expone sus columnas de periodo, AL FINAL y con outer join."""

    def test_columnas_presentes(self):
        for nombre, columnas in COLUMNAS_R3.items():
            sql = getattr(queries, nombre)
            for col in columnas:
                self.assertIn(col, sql, f"{nombre} no expone {col}")

    def test_critica_agrega_en_todas_las_ramas(self):
        """El UNION exige el MISMO numero de columnas en TODAS las ramas.
        Eran 3; con la rama 4 (decision del analista, 7400027) son 4."""
        sql = queries.QUERY_ORDENES_CRITICA_PEVIA
        self.assertEqual(sql.count("fecha_ini_consumo"), 4)
        self.assertEqual(sql.count("fecha_fin_consumo"), 4)

    def test_sin_inner_join_a_los_maestros(self):
        """§4.4: subconsulta escalar o (+), nunca inner join contra pericose/perifact:
        un maestro faltante no puede hacer desaparecer filas."""
        for nombre in COLUMNAS_R3:
            sql = getattr(queries, nombre)
            self.assertNotIn("JOIN pericose", sql.upper().replace("LEFT JOIN", "LEFT_JOIN"))


class TestR4PerdidasNoOperacionales(unittest.TestCase):
    def test_bind_unico(self):
        prep, vals = database._prepare(queries.QUERY_PERDIDAS_NO_OPERACIONALES,
                                       {"p_servicio_suscrito": 90858173})
        self.assertEqual(prep.count("?"), 1)
        self.assertEqual(vals, [90858173])

    def test_hint_bien_formado(self):
        """El SQL crudo traia LEADING(fm_possible_ntl)) con un parentesis de mas; Oracle
        ignora en silencio un hint mal formado."""
        sql = queries.QUERY_PERDIDAS_NO_OPERACIONALES
        self.assertIn("LEADING(fm_possible_ntl)", sql)
        self.assertNotIn("LEADING(fm_possible_ntl))", sql)

    def test_proyecta_el_servicio_suscrito(self):
        """Sin esta proyeccion el paso encadenado no puede escribir la llave."""
        self.assertIn("normalized_prod_id", queries.QUERY_PERDIDAS_NO_OPERACIONALES)
        self.assertIn("servicio_suscrito", queries.QUERY_PERDIDAS_NO_OPERACIONALES)

    def test_sin_literal_cableado(self):
        self.assertNotIn("133284722", queries.QUERY_PERDIDAS_NO_OPERACIONALES)

    def test_concatenacion_sql_plano(self):
        """El original venia con ||'' - ''|| por ser literal PL/SQL."""
        self.assertNotIn("|| - ||", queries.QUERY_PERDIDAS_NO_OPERACIONALES)
        self.assertIn("||'-'||", queries.QUERY_PERDIDAS_NO_OPERACIONALES)


class TestRama4DecisionAnalista(unittest.TestCase):
    """RAMA 4 de QUERY_ORDENES_CRITICA_PEVIA: la orden de decision del analista (7400027).

    Es la MISMA pantalla y las MISMAS columnas de "Ordenes de Critica y Previa"; el filtro
    `activity_id = 102010` de la rama 1 la dejaba afuera. Verificado en Oracle (2026-07-29):
    ninguna orden tiene a la vez 102010 y 7400027, asi que nunca aparecia en la rejilla.
    NO hace falta ninguna tabla nueva.
    """

    def test_la_rama_existe(self):
        self.assertIn("7400027", queries.QUERY_ORDENES_CRITICA_PEVIA)

    def test_cuatro_ramas_con_las_mismas_columnas_de_periodo(self):
        """El UNION exige el MISMO numero de columnas en TODAS las ramas."""
        sql = queries.QUERY_ORDENES_CRITICA_PEVIA
        self.assertEqual(sql.count("fecha_ini_consumo"), 4)
        self.assertEqual(sql.count("fecha_fin_consumo"), 4)

    def test_binds_completos(self):
        """periodo(1) + rama1(2) + rama2(2) + rama3(2) + rama4(1) = 8 marcadores."""
        prep, vals = database._prepare(
            queries.QUERY_ORDENES_CRITICA_PEVIA,
            {"p_id_periodo_facturacion": 1476, "p_servicio_suscrito": 90858173,
             "p_tipo_consumo": 3})
        self.assertEqual(prep.count("?"), 8)
        self.assertEqual(vals, [1476, 90858173, 3, 90858173, 3, 90858173, 3, 90858173])

    def test_rama4_acotada_por_la_ventana_del_periodo(self):
        """Sin la ventana, la misma orden se repetiria en cada iteracion de periodo
        (~8 por SS). Mismo patron que ya usa la rama 3 con register_date."""
        self.assertIn("o.created_date between pefafimo and pefaffmo",
                      queries.QUERY_ORDENES_CRITICA_PEVIA)

    @staticmethod
    def _rama4_sin_comentarios():
        """El SQL de la rama 4, sin lineas de comentario: estas citan
        `investigate_request` justamente para explicar por que NO se usa."""
        import re
        sql = queries.QUERY_ORDENES_CRITICA_PEVIA
        rama4 = sql[sql.index("RAMA 4"):]
        return re.sub("--.*", "", rama4)

    def test_rama4_no_depende_de_investigacion(self):
        """Se ataca directo por or_order_activity.product_id: enganchar la decision a
        `oa.package_id = p.investigate_request` supondria que toda decision cuelga de una
        solicitud de investigacion, cosa NO verificada."""
        rama4 = self._rama4_sin_comentarios()
        self.assertNotIn("investigate_request", rama4)
        self.assertIn("oa.product_id = :p_servicio_suscrito", rama4)

    def test_pericose_aliasado_en_la_rama4(self):
        """`pecscons = pecscons` seria una tautologia -> ORA-01427."""
        rama4 = self._rama4_sin_comentarios()
        self.assertIn("pc.pecscons = periodo.pecscons", rama4)
        self.assertNotIn("where pecscons = pecscons", rama4)

    def test_no_se_crearon_tablas_nuevas(self):
        """El arreglo es una rama mas en una tabla que ya existe."""
        for nombre in ("QUERY_ORDENES_DECISION_ANALISTA", "QUERY_COMENTARIOS_DECISION"):
            self.assertFalse(hasattr(queries, nombre), f"quedo {nombre}")
        for tabla in ("midas_datos_ordenes_decision_analista_bronze",
                      "midas_datos_comentarios_decision_bronze"):
            self.assertNotIn(tabla, mi._QUERY_KEY, f"quedo la carga {tabla}")

    def test_critica_conserva_sus_12_columnas(self):
        """La rama 4 NO agrega columnas: el schema de la Bronze no cambia."""
        import src.midas.main_ingestion as _mi
        cfg = {c["name"]: c for c in _mi.build_tables_config("/vol")}
        self.assertIn("midas_datos_ordenes_previa_critica_c2_bronze", cfg)


class TestCriticaDeduplicaLaRama4(unittest.TestCase):
    """La rama 4 no filtra por tipo_consumo (la orden de decision no expone uno), asi que
    un SS con activa Y reactiva en el mismo periodo la devuelve una vez por cada tipo.
    El UNION de Oracle deduplica DENTRO de una llamada; pd.concat entre llamadas no.
    Detectado en dllo el 2026-07-30: 27 filas para 26 ordenes distintas."""

    def test_elimina_la_fila_repetida(self):
        import pandas as pd
        lecturas = pd.DataFrame([
            # Mismo SS y mismo periodo de facturacion, dos tipos de consumo distintos:
            # dos iteraciones que devolveran la MISMA orden de decision.
            {"ID_PERIODO_FACTURACION": 1476, "SERVICIO_SUSCRITO": 90858173, "TIPOCONS": 3},
            {"ID_PERIODO_FACTURACION": 1476, "SERVICIO_SUSCRITO": 90858173, "TIPOCONS": 6},
        ])
        fila_decision = {
            "ID_ORDEN": 638644863, "SERVICIO_SUSCRITO": 90858173, "TIPO_CONSUMO": None,
            "ID_PERIODO_CONSUMO": 1210132993, "TIPO_TRABAJO": "10038-ORDEN DECISION ANALISTA",
            "ACTIVIDAD": "7400027-ORDEN DECISIÓN ANALISTA",
            "FECHA_CREACION_ORDEN": "2026-02-26 16:03:28",
            "FECHA_LEGALIZACION_ORDEN": "2026-03-17 10:58:38",
            "ESTADO": "8-Cerrada", "ANALISTA_LEGALIZA": "ELI",
            "FECHA_INI_CONSUMO": "2026-02-10", "FECHA_FIN_CONSUMO": "2026-03-10",
        }
        original = processing.db.execute_query
        processing.db.execute_query = lambda sql, params=None: pd.DataFrame([fila_decision])
        guardado = {}
        original_save = processing.save_to_parquet
        processing.save_to_parquet = lambda df, path: guardado.update(df=df)
        try:
            out = processing.run_query_ordenes_critica_previa(lecturas)
        finally:
            processing.db.execute_query = original
            processing.save_to_parquet = original_save

        self.assertEqual(len(out), 1, "la orden de decision quedo duplicada")
        self.assertEqual(len(guardado["df"]), 1, "el Parquet se escribio con duplicados")

    def test_no_colapsa_filas_legitimamente_distintas(self):
        """Dos ordenes distintas del mismo SS deben sobrevivir ambas."""
        import pandas as pd
        lecturas = pd.DataFrame([
            {"ID_PERIODO_FACTURACION": 1476, "SERVICIO_SUSCRITO": 1, "TIPOCONS": 3},
        ])
        filas = pd.DataFrame([
            {"ID_ORDEN": 1, "SERVICIO_SUSCRITO": 1, "TIPO_CONSUMO": "3-ACTIVA"},
            {"ID_ORDEN": 2, "SERVICIO_SUSCRITO": 1, "TIPO_CONSUMO": "3-ACTIVA"},
        ])
        original = processing.db.execute_query
        original_save = processing.save_to_parquet
        processing.db.execute_query = lambda sql, params=None: filas
        processing.save_to_parquet = lambda df, path: None
        try:
            out = processing.run_query_ordenes_critica_previa(lecturas)
        finally:
            processing.db.execute_query = original
            processing.save_to_parquet = original_save
        self.assertEqual(len(out), 2)


class TestComentariosToleraTipoConsumoNulo(unittest.TestCase):
    """La rama 4 devuelve tipo_consumo NULL. Sin guarda, `.split('-')` lanzaria
    AttributeError y, como el paso de comentarios corre con abortar_en_fallo=True,
    tumbaria la cadena ENTERA del Caso 1."""

    def test_no_revienta_con_tipo_consumo_nulo(self):
        import pandas as pd
        df = pd.DataFrame([{
            "ID_ORDEN": 648518072, "SERVICIO_SUSCRITO": 90858173,
            "ID_PERIODO_CONSUMO": 1210133131, "TIPO_CONSUMO": None,
            "FECHA_CREACION_ORDEN": "2026-01-01 00:00:00",
            "FECHA_LEGALIZACION_ORDEN": "2026-01-02 00:00:00",
        }])
        capturados = []
        original = processing.db.execute_query

        def _fake(sql, params=None):
            capturados.append(params)
            return pd.DataFrame()

        processing.db.execute_query = _fake
        try:
            processing.run_query_comentarios_ordenes(df)   # no debe lanzar
        finally:
            processing.db.execute_query = original

        self.assertEqual(len(capturados), 1)
        self.assertIsNone(capturados[0]["p_tipo_consumo"])

    def test_conserva_el_codigo_cuando_si_hay_tipo(self):
        import pandas as pd
        df = pd.DataFrame([{
            "ID_ORDEN": 1, "SERVICIO_SUSCRITO": 2, "ID_PERIODO_CONSUMO": 3,
            "TIPO_CONSUMO": "3-ENERGIA ACTIVA",
            "FECHA_CREACION_ORDEN": "2026-01-01 00:00:00",
            "FECHA_LEGALIZACION_ORDEN": None,
        }])
        capturados = []
        original = processing.db.execute_query
        processing.db.execute_query = lambda sql, params=None: (
            capturados.append(params) or pd.DataFrame())
        try:
            processing.run_query_comentarios_ordenes(df)
        finally:
            processing.db.execute_query = original
        self.assertEqual(capturados[0]["p_tipo_consumo"], "3")


# Lista oficial de facturable (confesco), entregada por negocio (prompt v2 §1.4).
# Fixture ESTRUCTURAL: valida S/N por servicio, no un conteo rígido universal.
# (escocodi, coecfact, coecserv)
FIXTURE_FACTURABLE = [
    # 101 AGUA POTABLE
    (-1, "S", 101), (1, "S", 101), (4, "S", 101), (5, "S", 101), (6, "S", 101),
    (92, "N", 101), (95, "N", 101), (110, "N", 101), (113, "N", 101), (122, "S", 101),
    # 103 ALCANTARILLADO (sin 4/5/91 según §1.4)
    (-1, "S", 103), (1, "S", 103), (6, "S", 103), (95, "N", 103), (110, "N", 103),
    (113, "N", 103), (122, "S", 103),
    # 501 GAS NATURAL REGULADO (sin 97/970)
    (-1, "S", 501), (1, "S", 501), (4, "S", 501), (95, "N", 501), (110, "N", 501),
    (113, "N", 501), (122, "S", 501),
    # 701 ENERGIA MDO REGULADO (único con 96 = N)
    (-1, "S", 701), (1, "S", 701), (4, "S", 701), (95, "N", 701), (96, "N", 701),
    (110, "N", 701), (113, "N", 701), (122, "S", 701),
]


class TestR1FacturableInline(unittest.TestCase):
    """v3 R1: la matriz facturable se resuelve inline, sin tabla de dimensión."""

    def test_columnas_en_basicos(self):
        sql = queries.QUERY_DATOS_BASICOS
        self.assertIn("estado_corte_facturable", sql)
        self.assertIn("confesco", sql)

    def test_no_reemplaza_null_por_n(self):
        """NULL = combinación no parametrizada; NO es lo mismo que 'no facturable'."""
        sql = queries.QUERY_DATOS_BASICOS
        self.assertNotIn("nvl(coecfact", sql.lower())
        self.assertNotIn("coalesce(coecfact", sql.lower())

    def test_dimension_eliminada(self):
        self.assertFalse(hasattr(queries, "QUERY_DIM_ESTADO_CORTE_FACTURABLE"))
        self.assertNotIn("midas_dim_estado_corte_facturable_bronze", mi._QUERY_KEY)


class TestR2RosterDesdeDatosBasicos(unittest.TestCase):
    """v3 R2: el roster del contrato sale de datos_basicos filtrado, sin tabla espejo."""

    def test_query_y_extractor_eliminados(self):
        self.assertFalse(hasattr(queries, "QUERY_SERVICIOS_CONTRATO"))
        self.assertFalse(hasattr(processing, "run_query_servicios_contrato"))
        self.assertFalse(hasattr(chain_runner, "PASO_SERVICIOS_CONTRATO"))

    def test_a3_recibe_basicos_y_ordenes(self):
        import inspect
        params = list(inspect.signature(processing.run_query_consumos_contrato).parameters)
        self.assertEqual(params, ["df_datos_basicos", "df_ordenes_pendientes"])

    def test_a3_acota_por_contrato_no_por_instalacion(self):
        """Los SS iterados son los del CONTRATO de las órdenes, no todos los de la
        instalación: la instalación traería SS de contratos ajenos (Caso 13)."""
        import pandas as pd
        basicos = pd.DataFrame({
            "SERVICIO_SUSCRITO": [1, 2, 3, 4],
            "CONTRATO":          [10, 10, 20, 20],   # dos contratos en la misma instalación
        })
        ordenes = pd.DataFrame({"SERVICIO_SUSCRITO": [1]})   # la orden es del contrato 10

        llamados = []
        original = processing.db.execute_query
        processing.db.execute_query = lambda q, p=None: (
            llamados.append(p["p_servicio_suscrito"]) or __import__("pandas").DataFrame()
        )
        try:
            processing.run_query_consumos_contrato(basicos, ordenes)
        finally:
            processing.db.execute_query = original

        self.assertEqual(sorted(llamados), [1, 2], "debe iterar solo el contrato 10")


class TestFacturableFixtureNegocio(unittest.TestCase):
    """Contrato ESTRUCTURAL de la matriz facturable entregada por negocio.
    Sigue vigente tras R1: lo que cambió es DÓNDE vive (inline), no QUÉ dice."""

    def _por_servicio(self, serv):
        return {code: sn for (code, sn, s) in FIXTURE_FACTURABLE if s == serv}

    def test_cuatro_servicios(self):
        servicios = {s for (_, _, s) in FIXTURE_FACTURABLE}
        self.assertEqual(servicios, {101, 103, 501, 701})

    def test_cada_servicio_tiene_S_y_N(self):
        for serv in (101, 103, 501, 701):
            vals = set(self._por_servicio(serv).values())
            self.assertIn("S", vals, f"servicio {serv} sin código facturable")
            self.assertIn("N", vals, f"servicio {serv} sin código no facturable")

    def test_invariantes_conocidos(self):
        for serv in (101, 103, 501, 701):
            m = self._por_servicio(serv)
            self.assertEqual(m.get(1), "S", f"código 1 (Conexión) debe ser S en {serv}")
            self.assertEqual(m.get(95), "N", f"código 95 (Retiro voluntario) debe ser N en {serv}")
            self.assertEqual(m.get(110), "N", f"código 110 debe ser N en {serv}")

    def test_96_solo_en_energia(self):
        con_96 = {s for (code, _, s) in FIXTURE_FACTURABLE if code == 96}
        self.assertEqual(con_96, {701})
        self.assertEqual(self._por_servicio(701).get(96), "N")


class TestAdaptacionSolicitudes(unittest.TestCase):
    """El output de la query se mapea al schema POSICIONAL de la Bronze existente
    (nombres en español + servicio_suscrito primero). Validado en dllo, celda F2."""

    ESQUEMA_BRONZE = [
        "servicio_suscrito", "id_solicitud", "usuario", "tipo_solicitud", "fecha_solicitud",
        "estado_solicitud", "fecha_atencion_solicitud", "comentario", "medio_recepcion",
        "analista", "area_organizacional",
    ]

    def _df_oracle(self):
        import pandas as pd
        # Columnas tal como las devuelve Oracle (mayúsculas) + el SS ya insertado.
        return pd.DataFrame([{
            "SERVICIO_SUSCRITO": 119687794, "PACKAGE_ID": 1, "SUBSCRIBER": "JUAN",
            "PACKAGE_TYPE": "10 - RECLAMO", "REQUEST_DATE": "2026-01-01",
            "PACKAGE_STATUS": "3 - ATENDIDA", "ATTENTION_DATE": "2026-01-02",
            "COMMENT_": "texto", "RECEPTION_TYPE": "1 - WEB", "VENDOR": "5 - ANA",
            "ORGANIZAT_AREA_ID": "7 - AREA",
        }])

    def test_mapea_al_schema_posicional(self):
        out = processing._adaptar_solicitudes_a_bronze(self._df_oracle())
        self.assertEqual(list(out.columns), self.ESQUEMA_BRONZE)

    def test_conserva_valores(self):
        out = processing._adaptar_solicitudes_a_bronze(self._df_oracle())
        self.assertEqual(out.iloc[0]["servicio_suscrito"], 119687794)
        self.assertEqual(out.iloc[0]["id_solicitud"], 1)
        self.assertEqual(out.iloc[0]["comentario"], "texto")

    def test_df_vacio_no_rompe(self):
        import pandas as pd
        vacio = pd.DataFrame()
        self.assertTrue(processing._adaptar_solicitudes_a_bronze(vacio).empty)

    def test_columnas_faltantes_no_rompe(self):
        import pandas as pd
        parcial = pd.DataFrame([{"PACKAGE_ID": 1}])
        out = processing._adaptar_solicitudes_a_bronze(parcial)
        self.assertEqual(list(out.columns), ["PACKAGE_ID"])  # se deja sin adaptar


class TestPasoNoCriticoNoAborta(unittest.TestCase):
    """Un paso con abortar_en_fallo=False registra el fallo pero NO re-lanza."""

    def test_no_relanza(self):
        control = MagicMock()
        control.get_id_carga.return_value = 1

        def _boom():
            raise RuntimeError("Oracle caido")

        df, n = chain_runner._ejecutar_paso(
            control, "midas_datos_perdidas_no_operacionales_bronze", "QUERY_PERDIDAS_NO_OPERACIONALES",
            fn=_boom, abortar_en_fallo=False,
        )
        self.assertIsNone(df)
        self.assertEqual(n, 0)
        control.log_fallo.assert_called_once()


if __name__ == "__main__":
    unittest.main()
