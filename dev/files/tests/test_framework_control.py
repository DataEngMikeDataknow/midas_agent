"""
Tests unitarios del framework de control de cargas.

Mocks de pyspark para que corran en local sin Spark. Validan que el
cliente emite las llamadas correctas y que chain_runner ejecuta los
8 pasos en el orden esperado, registra exitos y propaga errores.
"""
import sys
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

# Mocks de Spark obligatorios antes de importar el modulo.
sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()


from src.midas.framework.control_cargas import (  # noqa: E402
    ControlCargasClient,
    ESTADO_EXITOSA,
    ESTADO_FALLIDA,
)
from src.midas.framework import chain_runner  # noqa: E402


class TestControlCargasClient(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.client = ControlCargasClient(
            spark=self.spark,
            catalog="cat",
            schema="facturacion",
            id_ejecucion="exec-123",
            usuario_ejecutor="user@test",
        )

    def test_table_names_qualified(self):
        self.assertEqual(self.client.control_table, "cat.facturacion.midas_control_cargas")
        self.assertEqual(self.client.log_table, "cat.facturacion.midas_log_cargas")

    def test_mark_in_progress_emits_update(self):
        self.client.mark_in_progress("midas_x_bronze")
        called = self.spark.sql.call_args.args[0]
        self.assertIn("UPDATE cat.facturacion.midas_control_cargas", called)
        self.assertIn("EN_PROCESO", called)
        self.assertIn("midas_x_bronze", called)

    def test_mark_success_emits_update(self):
        self.client.mark_success("midas_x_bronze")
        called = self.spark.sql.call_args.args[0]
        self.assertIn("EXITOSA", called)
        self.assertIn("midas_x_bronze", called)

    def test_mark_failed_emits_update(self):
        self.client.mark_failed("midas_x_bronze")
        called = self.spark.sql.call_args.args[0]
        self.assertIn("FALLIDA", called)

    def test_log_attempt_appends(self):
        mock_df = MagicMock()
        self.spark.createDataFrame.return_value = mock_df

        self.client.log_attempt(
            tabla_nombre="midas_x_bronze",
            intento=1,
            fecha_inicio=datetime(2026, 5, 27, 4, 0, 0),
            fecha_fin=datetime(2026, 5, 27, 4, 1, 0),
            estado=ESTADO_EXITOSA,
            registros_leidos=100,
            registros_escritos=100,
        )

        self.spark.createDataFrame.assert_called_once()
        # Verifica que el write fue append e insertInto a la tabla de log.
        mock_df.write.mode.assert_called_with("append")
        mock_df.write.mode.return_value.insertInto.assert_called_with(
            "cat.facturacion.midas_log_cargas"
        )

    def test_log_attempt_truncates_long_error(self):
        mock_df = MagicMock()
        self.spark.createDataFrame.return_value = mock_df

        mensaje_largo = "x" * 5000
        self.client.log_attempt(
            tabla_nombre="midas_x_bronze",
            intento=1,
            fecha_inicio=datetime(2026, 5, 27, 4, 0, 0),
            fecha_fin=datetime(2026, 5, 27, 4, 1, 0),
            estado=ESTADO_FALLIDA,
            mensaje_error=mensaje_largo,
        )

        # El primer argumento de createDataFrame es la lista de filas.
        filas = self.spark.createDataFrame.call_args.args[0]
        mensaje_pasado = filas[0][-2]  # penultimo es mensaje_error
        self.assertLessEqual(len(mensaje_pasado), 4000)


class TestChainRunner(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.control = ControlCargasClient(
            spark=self.spark,
            catalog="cat",
            schema="facturacion",
            id_ejecucion="exec-xyz",
        )
        # No queremos que las llamadas a Spark exploten en los tests.
        self.spark.createDataFrame.return_value = MagicMock()

        # Mock del modulo processing con las 8 funciones.
        self.processing = MagicMock()
        # Cada funcion devuelve un DataFrame "no vacio" simulado.
        df_mock = MagicMock()
        df_mock.empty = False
        # len(df) debe devolver 5 para que registros_leidos quede en algo finito.
        df_mock.__len__ = MagicMock(return_value=5)

        # Hace que isinstance(df_mock, pd.DataFrame) sea False, lo que en chain_runner
        # cae en la rama n=0. Para que sea True usamos un DataFrame real chico.
        import pandas as pd
        real_df = pd.DataFrame({"x": [1, 2, 3, 4, 5]})

        self.processing.run_query_ordenes_pendientes.return_value = real_df
        self.processing.run_query_datos_basicos.return_value = real_df
        self.processing.run_query_datos_lectura.return_value = real_df
        self.processing.run_query_datos_consumos.return_value = real_df
        self.processing.run_query_ordenes_critica_previa.return_value = real_df
        self.processing.run_query_comentarios_ordenes.return_value = real_df
        self.processing.run_query_cuentas_cobro.return_value = real_df
        self.processing.run_query_detalle_cargos.return_value = real_df

    def test_chain_runs_all_eight_steps(self):
        chain_runner.ejecutar_cadena_extraccion(self.processing, self.control)

        # Las 8 funciones de processing deben haberse llamado exactamente una vez.
        self.processing.run_query_ordenes_pendientes.assert_called_once()
        self.processing.run_query_datos_basicos.assert_called_once()
        self.processing.run_query_datos_lectura.assert_called_once()
        self.processing.run_query_datos_consumos.assert_called_once()
        self.processing.run_query_ordenes_critica_previa.assert_called_once()
        self.processing.run_query_comentarios_ordenes.assert_called_once()
        self.processing.run_query_cuentas_cobro.assert_called_once()
        self.processing.run_query_detalle_cargos.assert_called_once()

    def test_chain_propagates_error_from_first_step(self):
        self.processing.run_query_ordenes_pendientes.side_effect = RuntimeError("Oracle off")

        with self.assertRaises(RuntimeError):
            chain_runner.ejecutar_cadena_extraccion(self.processing, self.control)

        # Las funciones downstream NO deben haberse llamado.
        self.processing.run_query_datos_basicos.assert_not_called()
        self.processing.run_query_datos_lectura.assert_not_called()

    def test_chain_propagates_error_midway(self):
        self.processing.run_query_datos_lectura.side_effect = RuntimeError("timeout")

        with self.assertRaises(RuntimeError):
            chain_runner.ejecutar_cadena_extraccion(self.processing, self.control)

        # Lo que va antes del fallo si se llamo.
        self.processing.run_query_ordenes_pendientes.assert_called_once()
        self.processing.run_query_datos_basicos.assert_called_once()
        # Lo que va despues, no.
        self.processing.run_query_datos_consumos.assert_not_called()
        self.processing.run_query_cuentas_cobro.assert_not_called()


if __name__ == "__main__":
    unittest.main()
