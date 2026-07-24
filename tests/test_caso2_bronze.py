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


# v3: ÚNICA dimensión materializada (matriz de decisión facturable). El resto de
# catálogos se resuelven INLINE en las queries (patrón del Caso 1).
DIMS = ["QUERY_DIM_ESTADO_CORTE_FACTURABLE"]
DIM_TABLES = ["midas_dim_estado_corte_facturable_bronze"]
# Nombres podados en v3: NO deben existir en el código.
DIMS_ELIMINADAS = [
    "QUERY_DIM_TIPO_CONSUMO", "QUERY_DIM_OBSERVACION_LECTURA", "QUERY_DIM_CALIFICACION",
    "QUERY_DIM_CONCEPTO", "QUERY_DIM_CAUSAL_CARGO", "QUERY_DIM_METODO_CALCULO",
    "QUERY_DIM_TIPO_SOLICITUD", "QUERY_DIM_ESTADO_INVESTIGACION",
]
PROMO_TABLES = [
    "midas_datos_detalle_solicitudes_bronze", "midas_datos_servicios_contrato_bronze",
    "midas_datos_consumos_contrato_bronze", "midas_datos_investigacion_consumo_bronze",
]


class TestNuevasQueriesBinds(unittest.TestCase):
    def test_servicios_contrato_bind(self):
        prepared, values = database._prepare(queries.QUERY_SERVICIOS_CONTRATO, {"p_contrato": 123})
        self.assertEqual(prepared.count("?"), 1)
        self.assertEqual(values, [123])
        self.assertNotIn(":p_contrato", prepared)

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
        # 8 cadena + 1 dimensión (facturable) + 4 promociones
        self.assertEqual(len(self.cfg), 13)

    def test_nombres_unicos(self):
        nombres = [c["name"] for c in self.cfg]
        self.assertEqual(len(nombres), len(set(nombres)))

    def test_nuevas_entradas_presentes(self):
        for t in DIM_TABLES + PROMO_TABLES:
            self.assertIn(t, self.by_name)

    def test_pk_compuesta_facturable(self):
        self.assertEqual(self.by_name["midas_dim_estado_corte_facturable_bronze"]["primary_key"],
                         ["escocodi", "coecserv"])

    def test_pk_solicitudes_compuesta(self):
        self.assertEqual(self.by_name["midas_datos_detalle_solicitudes_bronze"]["primary_key"],
                         ["servicio_suscrito", "id_solicitud"])

    def test_paths_bajo_volumen(self):
        for c in self.cfg:
            self.assertTrue(c["path"].startswith("/vol/"))

    def test_consistencia_query_key(self):
        # Toda tabla de la config debe tener su query_key mapeado.
        for c in self.cfg:
            self.assertIn(c["name"], mi._QUERY_KEY)


class TestControlYPasos(unittest.TestCase):
    def test_query_key_tiene_13(self):
        self.assertEqual(len(mi._QUERY_KEY), 13)

    def test_dims_podadas_no_existen(self):
        """v3: las 8 dims redundantes no deben quedar ni en queries ni en processing."""
        for nombre in DIMS_ELIMINADAS:
            self.assertFalse(hasattr(queries, nombre), f"quedó {nombre} en queries.py")
        for fn in ["run_query_dim_tipo_consumo", "run_query_dim_calificacion",
                   "run_query_dim_concepto", "run_query_dim_tipo_solicitud",
                   "run_query_dim_estado_investigacion"]:
            self.assertFalse(hasattr(processing, fn), f"quedó processing.{fn}")

    def test_pasos_chain_runner_existen(self):
        pasos = [
            chain_runner.PASO_DIM_ESTADO_CORTE,
            chain_runner.PASO_SOLICITUDES, chain_runner.PASO_SERVICIOS_CONTRATO,
            chain_runner.PASO_CONSUMOS_CONTRATO, chain_runner.PASO_INVESTIGACION,
        ]
        for tabla, query_key in pasos:
            self.assertTrue(tabla.startswith("midas_"))
            self.assertTrue(query_key.startswith("QUERY_"))

    def test_processing_tiene_funciones_nuevas(self):
        for fn in ["run_query_dim_estado_corte_facturable",
                   "run_query_servicios_contrato", "run_query_consumos_contrato",
                   "run_query_investigacion_consumo"]:
            self.assertTrue(hasattr(processing, fn), f"falta processing.{fn}")


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


class TestDimFacturableFixture(unittest.TestCase):
    """Contrato ESTRUCTURAL de la dimensión facturable (no valida la query en vivo)."""

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
            control, "midas_dim_estado_corte_facturable_bronze", "QUERY_DIM_ESTADO_CORTE_FACTURABLE",
            fn=_boom, abortar_en_fallo=False,
        )
        self.assertIsNone(df)
        self.assertEqual(n, 0)
        control.log_fallo.assert_called_once()


if __name__ == "__main__":
    unittest.main()
