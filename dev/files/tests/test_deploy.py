import unittest
from unittest.mock import MagicMock, patch
import sys

# Mocks para dependencias de MLflow y Databricks antes de importar el runner.
# Deben estar en sys.modules ANTES del import para que el módulo los use al cargarse.
sys.modules['mlflow'] = MagicMock()
sys.modules['mlflow.models'] = MagicMock()
sys.modules['mlflow.models.resources'] = MagicMock()
sys.modules['mlflow.pyfunc'] = MagicMock()
sys.modules['databricks'] = MagicMock()
sys.modules['databricks.agents'] = MagicMock()

# Mock de importlib.metadata para evitar requerir databricks-connect instalado localmente.
# main_deploy.py llama a version('databricks-connect') para construir pip_requirements.
_mock_metadata = MagicMock()
_mock_metadata.version.return_value = "13.0.0"
sys.modules['importlib.metadata'] = _mock_metadata

import src.midas.main_deploy as main_deploy


class TestMainDeploy(unittest.TestCase):
    def setUp(self):
        # Reiniciar contadores de llamadas antes de cada test para aislar los efectos.
        main_deploy.mlflow.reset_mock()
        main_deploy.agents.reset_mock()

    def test_main_deploy(self):
        test_args = [
            "main_deploy.py",
            "--catalog", "test_cat",
            "--schema", "test_schema",
            "--model_name", "test_model",
            "--endpoint_name", "test_endpoint",
            "--experiment_path", "/test/experiment"
        ]

        with patch.object(sys, 'argv', test_args):
            main_deploy.main()

        # Verificar que se configuró el experimento de MLflow correcto
        main_deploy.mlflow.set_experiment.assert_called_with("/test/experiment")

        # Verificar que el registry apunta a Unity Catalog
        main_deploy.mlflow.set_registry_uri.assert_called_with("databricks-uc")

        # Verificar que se inició un run con el nombre del pipeline
        main_deploy.mlflow.start_run.assert_called_once_with(run_name="deploy_pipeline_run")

        # Verificar que el modelo fue logeado en MLflow
        main_deploy.mlflow.pyfunc.log_model.assert_called_once()

        # Verificar que el modelo fue registrado con el nombre calificado correcto
        # (catalog.schema.model_name)
        main_deploy.mlflow.register_model.assert_called_once()
        register_kwargs = main_deploy.mlflow.register_model.call_args.kwargs
        self.assertEqual(register_kwargs['name'], "test_cat.test_schema.test_model")

        # Verificar despliegue del agente: endpoint y variables de entorno correctos
        main_deploy.agents.deploy.assert_called_once()
        deploy_kwargs = main_deploy.agents.deploy.call_args.kwargs
        self.assertEqual(deploy_kwargs['endpoint_name'], "test_endpoint")
        self.assertEqual(deploy_kwargs['environment_vars']['MIDAS_CATALOG'], "test_cat")
        self.assertEqual(deploy_kwargs['environment_vars']['MIDAS_SCHEMA'], "test_schema")


if __name__ == '__main__':
    unittest.main()
