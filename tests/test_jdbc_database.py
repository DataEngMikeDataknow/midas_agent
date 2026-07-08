"""
Tests del conector JDBC (ojdbc11 + JayDeBeApi) SIN arrancar la JVM.

Se prueban las piezas puras de src/midas/db/database.py:
- _prepare: conversión de binds nombrados (:param) → posicionales (?).
- _sanitize_param: coerción de tipos numpy/pandas a nativos de Python.
- _to_python: conversión de objetos Java (JPype) a tipos Python (paridad de
  schema Parquet con la versión python-oracledb).
- execute_query: sin init_database() devuelve un DataFrame vacío.
"""
import unittest
from datetime import datetime

import numpy as np
import pandas as pd

import src.midas.db.database as database
from src.midas.db import queries


# --------- Objetos Java falsos (imitan la API de JPype: getClass().getName()) ----------

class _FakeJavaClass:
    def __init__(self, name):
        self._name = name

    def getName(self):
        return self._name


class FakeJavaObject:
    """Imita un objeto Java: getClass().getName() + str()."""

    def __init__(self, class_name, string_value):
        self._class_name = class_name
        self._string_value = string_value

    def getClass(self):
        return _FakeJavaClass(self._class_name)

    def __str__(self):
        return self._string_value


class TestPrepare(unittest.TestCase):
    def test_bind_simple(self):
        prepared, values = database._prepare(
            queries.QUERY_DATOS_BASICOS, {"address_id": 999}
        )
        self.assertEqual(prepared.count("?"), 1)
        self.assertEqual(values, [999])
        self.assertNotIn(":address_id", prepared)

    def test_bind_repetido(self):
        # :p_servicio_suscrito aparece 3 veces en QUERY_ORDENES_CRITICA_PEVIA.
        n = queries.QUERY_ORDENES_CRITICA_PEVIA.count(":p_servicio_suscrito")
        self.assertGreater(n, 1)  # asegura que el caso "repetido" es real
        prepared, values = database._prepare(
            queries.QUERY_ORDENES_CRITICA_PEVIA, {"p_servicio_suscrito": 5}
        )
        # Cada ocurrencia se vuelve un '?' y repite el valor.
        self.assertEqual(len(values), n)
        self.assertEqual(values, [5] * n)
        self.assertNotIn(":p_servicio_suscrito", prepared)

    def test_no_toca_literales_de_fecha(self):
        # 'YYYY-MM-DD HH24:MI:SS' no debe romperse: :MI y :SS no son params.
        prepared, _ = database._prepare(
            queries.QUERY_ORDENES_CRITICA_PEVIA, {"p_servicio_suscrito": 5}
        )
        self.assertIn("HH24:MI:SS", prepared)

    def test_sin_params(self):
        prepared, values = database._prepare("SELECT 1 FROM dual", {})
        self.assertEqual(prepared, "SELECT 1 FROM dual")
        self.assertEqual(values, [])


class TestSanitizeParam(unittest.TestCase):
    def test_numpy_int64_a_int(self):
        out = database._sanitize_param(np.int64(7))
        self.assertIsInstance(out, int)
        self.assertEqual(out, 7)

    def test_numpy_float64_a_float(self):
        out = database._sanitize_param(np.float64(3.5))
        self.assertIsInstance(out, float)
        self.assertEqual(out, 3.5)

    def test_none(self):
        self.assertIsNone(database._sanitize_param(None))

    def test_datetime_a_str(self):
        out = database._sanitize_param(datetime(2026, 7, 1, 8, 30, 0))
        self.assertEqual(out, "2026-07-01 08:30:00")


class TestToPython(unittest.TestCase):
    def test_bigdecimal_scale0_a_int(self):
        v = FakeJavaObject("java.math.BigDecimal", "42")
        out = database._to_python(v, 0)
        self.assertIsInstance(out, int)
        self.assertEqual(out, 42)

    def test_bigdecimal_scale2_a_float(self):
        v = FakeJavaObject("java.math.BigDecimal", "42.5")
        out = database._to_python(v, 2)
        self.assertIsInstance(out, float)
        self.assertEqual(out, 42.5)

    def test_timestamp_a_datetime(self):
        v = FakeJavaObject("java.sql.Timestamp", "2026-07-01 08:30:00")
        out = database._to_python(v, None)
        self.assertIsInstance(out, datetime)
        self.assertEqual(out, datetime(2026, 7, 1, 8, 30, 0))

    def test_none(self):
        self.assertIsNone(database._to_python(None, 0))

    def test_str_passthrough(self):
        self.assertEqual(database._to_python("hola", None), "hola")


class TestExecuteQuerySinInit(unittest.TestCase):
    def test_devuelve_df_vacio(self):
        database._conn = None  # aseguramos que no hay conexión
        df = database.execute_query("SELECT 1 FROM dual")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)


if __name__ == "__main__":
    unittest.main()
