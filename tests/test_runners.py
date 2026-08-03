import unittest
from unittest.mock import MagicMock, patch
import sys

# Mocks de Spark obligatorios antes de importar los runners
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()

# Importamos los runners (main_tools se fue al bundle del agente).
import src.midas.main_ingestion as main_ingestion
import src.midas.main_transform as main_transform

class TestRunners(unittest.TestCase):

    @patch('src.midas.main_ingestion.ControlCargasClient')
    @patch('src.midas.main_ingestion.DataIngestor')
    @patch('src.midas.main_ingestion.SparkSession.builder.getOrCreate')
    def test_main_ingestion(self, mock_spark, mock_ingestor_class, mock_control_class):
        # Mock class instance
        mock_ingestor_instance = MagicMock()
        mock_ingestor_class.return_value = mock_ingestor_instance
        mock_control_class.return_value = MagicMock()

        test_args = [
            "main_ingestion.py",
            "--source_catalog", "src_cat",
            "--source_schema", "src_schema",
            "--source_volume", "src_vol",
            "--source_base_path", "src_path",
            "--destination_catalog", "dest_cat",
            "--destination_schema", "dest_schema",
            "--control_catalog", "ctrl_cat",
            "--control_schema", "ctrl_schema",
            # La guarda de frescura se desactiva AQUI a proposito: este test comprueba
            # el cableado (spark, ingestor, control, la lista de tablas), no la guarda.
            # Sin esto abortaria antes de empezar, porque los Parquet no existen — que
            # es exactamente lo que la guarda debe hacer, y se prueba abajo.
            "--max_antiguedad_horas", "0",
        ]

        with patch.object(sys, 'argv', test_args):
            main_ingestion.main()

        mock_spark.assert_called_once()
        mock_ingestor_class.assert_called_once()
        mock_control_class.assert_called_once()

        # Debe llamar load_parquet_to_delta al menos una vez (tiene una lista de configuraciones)
        self.assertTrue(mock_ingestor_instance.load_parquet_to_delta.called)

    @patch('src.midas.main_ingestion.ControlCargasClient')
    @patch('src.midas.main_ingestion.DataIngestor')
    @patch('src.midas.main_ingestion.SparkSession.builder.getOrCreate')
    def test_main_ingestion_aborta_si_los_parquet_no_son_de_esta_corrida(
            self, mock_spark, mock_ingestor_class, mock_control_class):
        """F02: la ingesta NO debe cargar Parquet de una corrida anterior.

        Con la guarda activa y sin archivos en el Volume, debe abortar ANTES de tocar
        Bronze. Publicar los datos de ayer es peor que no publicar: el consumidor no
        tiene forma de notarlo."""
        mock_ingestor_instance = MagicMock()
        mock_ingestor_class.return_value = mock_ingestor_instance
        mock_control_class.return_value = MagicMock()

        test_args = [
            "main_ingestion.py",
            "--source_catalog", "src_cat",
            "--source_schema", "src_schema",
            "--source_volume", "src_vol",
            "--source_base_path", "src_path",
            "--destination_catalog", "dest_cat",
            "--destination_schema", "dest_schema",
            "--control_catalog", "ctrl_cat",
            "--control_schema", "ctrl_schema",
        ]

        with patch.object(sys, 'argv', test_args):
            with self.assertRaises(RuntimeError) as ctx:
                main_ingestion.main()

        mensaje = str(ctx.exception)
        self.assertIn("RANCIO", mensaje.upper())
        self.assertIn("No se cargo nada", mensaje)
        # Lo que de verdad importa: no toco Bronze.
        mock_ingestor_instance.load_parquet_to_delta.assert_not_called()

    @patch('src.midas.main_transform.ControlCargasClient')
    @patch('src.midas.main_transform.SilverTransformer')
    @patch('src.midas.main_transform.SparkSession.builder.getOrCreate')
    def test_main_transform(self, mock_spark, mock_transformer_class, mock_control_class):
        mock_transformer_instance = MagicMock()
        mock_transformer_class.return_value = mock_transformer_instance

        test_args = [
            "main_transform.py",
            "--catalog", "test_cat",
            "--schema", "test_schema"
        ]

        with patch.object(sys, 'argv', test_args):
            main_transform.main()

        mock_spark.assert_called_once()
        mock_transformer_class.assert_called_once()
        # Catálogo y schema ahora van al constructor, no al método: el transformer
        # necesita ambos para resolver los placeholders del SQL externalizado.
        args, kwargs = mock_transformer_class.call_args
        self.assertEqual(args[1], "test_cat")
        self.assertEqual(args[2], "test_schema")
        mock_transformer_instance.transform_bronze_to_silver.assert_called_once_with()

    @patch('src.midas.main_transform.ControlCargasClient')
    @patch('src.midas.main_transform.SilverTransformer')
    @patch('src.midas.main_transform.SparkSession.builder.getOrCreate')
    def test_main_transform_usa_job_name_silver(self, mock_spark, _t, mock_control_class):
        """Sin este job_name, Silver leería las 12 filas de Bronze del control."""
        test_args = ["main_transform.py", "--catalog", "c", "--schema", "s"]
        with patch.object(sys, 'argv', test_args):
            main_transform.main()
        self.assertEqual(mock_control_class.call_args.kwargs["job_name"], "midas_silver")

    @patch('src.midas.main_transform.ControlCargasClient')
    @patch('src.midas.main_transform.SilverTransformer')
    @patch('src.midas.main_transform.SparkSession.builder.getOrCreate')
    def test_main_transform_control_por_defecto_al_destino(self, mock_spark, _t, mock_control):
        """--control_catalog/--control_schema son opcionales: caen al destino."""
        test_args = ["main_transform.py", "--catalog", "cat_x", "--schema", "sch_x"]
        with patch.object(sys, 'argv', test_args):
            main_transform.main()
        self.assertEqual(mock_control.call_args.kwargs["catalog"], "cat_x")
        self.assertEqual(mock_control.call_args.kwargs["schema"], "sch_x")

if __name__ == '__main__':
    unittest.main()
