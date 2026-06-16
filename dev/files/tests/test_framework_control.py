"""
Tests del framework de control de cargas (esquema real).
Mocks de pyspark; no requieren Spark ni Oracle.
"""
import sys
import unittest
from datetime import datetime
from unittest.mock import MagicMock

sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()

from src.midas.framework.control_cargas import (  # noqa: E402
    ControlCargasClient,
    ESTADO_INICIADO,
    ESTADO_EXITOSO,
    ESTADO_FALLIDO,
)
from src.midas.framework import chain_runner  # noqa: E402


class TestControlCargasClient(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.client = ControlCargasClient(
            spark=self.spark, catalog="cat", schema="facturacion",
            run_id="run-123", usuario_ejecutor="user@test",
        )

    def test_table_names(self):
        self.assertEqual(self.client.control_table, "cat.facturacion.midas_control_cargas")
        self.assertEqual(self.client.log_table, "cat.facturacion.midas_log_cargas")

    def test_get_id_carga_resuelve(self):
        fila = MagicMock()
        fila.__getitem__.return_value = 42
        self.spark.sql.return_value.collect.return_value = [fila]
        self.assertEqual(self.client.get_id_carga("midas_x_bronze"), 42)

    def test_get_id_carga_none_si_no_existe(self):
        self.spark.sql.return_value.collect.return_value = []
        self.assertIsNone(self.client.get_id_carga("inexistente"))

    def test_log_inicio_inserta_por_columnas(self):
        self.spark.createDataFrame.return_value = MagicMock()
        self.client.log_inicio("midas_x_bronze", "QUERY_X", 7, datetime(2026, 5, 27, 7, 0, 0))
        # Debe usar INSERT INTO ... (columnas) SELECT ... (no append posicional).
        sql_calls = [c.args[0] for c in self.spark.sql.call_args_list if c.args]
        inserts = [s for s in sql_calls if "INSERT INTO cat.facturacion.midas_log_cargas" in s]
        self.assertTrue(inserts, "Debe ejecutar un INSERT INTO con columnas explicitas")
        self.assertIn("id_carga", inserts[0])
        self.assertNotIn("id_log", inserts[0])  # id_log es IDENTITY, no se inserta

    def test_log_exito_estado(self):
        self.spark.createDataFrame.return_value = MagicMock()
        self.client.log_exito("midas_x_bronze", "QUERY_X", 7,
                              datetime(2026, 5, 27, 7, 0, 0), filas_escritas=100)
        filas = self.spark.createDataFrame.call_args.args[0]
        # estado es el 8vo campo (indice 7) del tuple.
        self.assertEqual(filas[0][7], ESTADO_EXITOSO)

    def test_log_fallo_trunca_error(self):
        self.spark.createDataFrame.return_value = MagicMock()
        self.client.log_fallo("midas_x_bronze", "QUERY_X", 7,
                             datetime(2026, 5, 27, 7, 0, 0), "x" * 5000)
        filas = self.spark.createDataFrame.call_args.args[0]
        mensaje = filas[0][11]  # mensaje_error
        self.assertLessEqual(len(mensaje), 4000)
        self.assertEqual(filas[0][7], ESTADO_FALLIDO)


class TestChainRunner(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.spark.createDataFrame.return_value = MagicMock()
        # get_id_carga devuelve algo no-nulo siempre.
        fila = MagicMock()
        fila.__getitem__.return_value = 1
        self.spark.sql.return_value.collect.return_value = [fila]

        self.control = ControlCargasClient(
            spark=self.spark, catalog="cat", schema="facturacion", run_id="run-xyz",
        )

        import pandas as pd
        real_df = pd.DataFrame({"x": [1, 2, 3]})
        self.processing = MagicMock()
        for m in [
            "run_query_ordenes_pendientes", "run_query_datos_basicos",
            "run_query_datos_lectura", "run_query_datos_consumos",
            "run_query_ordenes_critica_previa", "run_query_comentarios_ordenes",
            "run_query_cuentas_cobro", "run_query_detalle_cargos",
        ]:
            getattr(self.processing, m).return_value = real_df

    def test_corre_los_8_pasos(self):
        chain_runner.ejecutar_cadena_extraccion(self.processing, self.control)
        self.processing.run_query_ordenes_pendientes.assert_called_once()
        self.processing.run_query_datos_basicos.assert_called_once()
        self.processing.run_query_datos_lectura.assert_called_once()
        self.processing.run_query_datos_consumos.assert_called_once()
        self.processing.run_query_ordenes_critica_previa.assert_called_once()
        self.processing.run_query_comentarios_ordenes.assert_called_once()
        self.processing.run_query_cuentas_cobro.assert_called_once()
        self.processing.run_query_detalle_cargos.assert_called_once()

    def test_aborta_en_primer_fallo(self):
        self.processing.run_query_ordenes_pendientes.side_effect = RuntimeError("Oracle off")
        with self.assertRaises(RuntimeError):
            chain_runner.ejecutar_cadena_extraccion(self.processing, self.control)
        self.processing.run_query_datos_basicos.assert_not_called()

    def test_aborta_a_mitad(self):
        self.processing.run_query_datos_lectura.side_effect = RuntimeError("timeout")
        with self.assertRaises(RuntimeError):
            chain_runner.ejecutar_cadena_extraccion(self.processing, self.control)
        self.processing.run_query_datos_basicos.assert_called_once()
        self.processing.run_query_datos_consumos.assert_not_called()


if __name__ == "__main__":
    unittest.main()
