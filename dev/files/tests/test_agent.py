import unittest
from unittest.mock import MagicMock, patch
import sys

# Mocks para evitar errores si las librerías no están instaladas
sys.modules['mlflow'] = MagicMock()
sys.modules['mlflow.models'] = MagicMock()
sys.modules['mlflow.models.resources'] = MagicMock()
sys.modules['mlflow.openai'] = MagicMock()

# Dummy class for inheritance
class DummyResponsesAgent:
    pass

mock_pyfunc = MagicMock()
mock_pyfunc.ResponsesAgent = DummyResponsesAgent
sys.modules['mlflow.pyfunc'] = mock_pyfunc

sys.modules['mlflow.entities'] = MagicMock()
sys.modules['mlflow.types'] = MagicMock()
sys.modules['mlflow.types.responses'] = MagicMock()
sys.modules['databricks'] = MagicMock()
sys.modules['databricks.agents'] = MagicMock()
sys.modules['databricks.sdk'] = MagicMock()
sys.modules['databricks_openai'] = MagicMock()
sys.modules['unitycatalog_ai'] = MagicMock()
sys.modules['unitycatalog'] = MagicMock()
sys.modules['unitycatalog.ai'] = MagicMock()
sys.modules['unitycatalog.ai.core'] = MagicMock()
sys.modules['unitycatalog.ai.core.base'] = MagicMock()
sys.modules['backoff'] = MagicMock()
sys.modules['flask'] = MagicMock()
sys.modules['openai'] = MagicMock()
sys.modules['pydantic'] = MagicMock()

# Mock pydantic.BaseModel
class DummyBaseModel:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
    @classmethod
    def model_validate(cls, obj):
        return obj

sys.modules['pydantic'].BaseModel = DummyBaseModel

from agent.agent import ToolCallingAgent, ToolInfo, SYSTEM_PROMPT


class TestMidasAgent(unittest.TestCase):
    def setUp(self):
        self.endpoint = "test_endpoint"
        mock_tool = ToolInfo(
            name="test_tool",
            spec={"type": "function", "function": {"name": "test_tool", "description": "test"}},
            exec_fn=MagicMock()
        )
        self.tools = [mock_tool]
        self.agent = ToolCallingAgent(self.endpoint, self.tools)

    def test_agent_initialization(self):
        self.assertEqual(self.agent.llm_endpoint, self.endpoint)
        self.assertIn("test_tool", self.agent._tools_dict)

    def test_system_prompt_content(self):
        # Verificar que el prompt contiene las secciones críticas
        self.assertIn("### ROL Y CONTEXTO", SYSTEM_PROMPT)
        self.assertIn("PASO 1: OBTENCIÓN Y PREPARACIÓN DE VARIABLES", SYSTEM_PROMPT)
        self.assertIn("PASO 2: CLASIFICACIÓN DEL FLUJO", SYSTEM_PROMPT)
        self.assertIn("### FORMATO DE SALIDA (JSON)", SYSTEM_PROMPT)

    @patch('mlflow.openai.autolog')
    def test_predict_structure(self, mock_autolog):
        # Verificamos que la clase expone los métodos públicos esperados
        self.assertTrue(has_builtins_method(self.agent, 'predict'))
        self.assertTrue(has_builtins_method(self.agent, 'predict_stream'))
        self.assertTrue(has_builtins_method(self.agent, 'execute_tool'))

def has_builtins_method(obj, method_name):
    return hasattr(obj, method_name) and callable(getattr(obj, method_name))

if __name__ == '__main__':
    unittest.main()
