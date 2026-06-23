import unittest

from src.midas.stage4.prompts import SYSTEM_PROMPT_STAGE4, build_classification_prompt


class TestStage4Prompts(unittest.TestCase):
    def test_system_prompt_contains_guardrails_and_schema(self):
        self.assertIn("No tomes decisiones sin datos suficientes", SYSTEM_PROMPT_STAGE4)
        self.assertIn("VARIACION_SIGNIFICATIVA_NO_JUSTIFICADA", SYSTEM_PROMPT_STAGE4)
        self.assertIn("requiere_revision_humana", SYSTEM_PROMPT_STAGE4)

    def test_classification_prompt_embeds_context(self):
        prompt = build_classification_prompt(
            context={"orden": {"id_orden": "1"}},
            rules={"categoria": "NORMAL"},
            fecha_proceso="2026-06-23",
            version_prompt="v1",
            version_modelo="m1",
            umbral_variacion=0.30,
        )
        self.assertIn("2026-06-23", prompt)
        self.assertIn('"id_orden": "1"', prompt)


if __name__ == "__main__":
    unittest.main()
