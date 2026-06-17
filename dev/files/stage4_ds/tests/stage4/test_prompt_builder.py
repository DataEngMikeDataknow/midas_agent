from pathlib import Path

from midas_stage4.prompting import PromptBuilder, PromptRepository


def test_prompt_builder_loads_messages():
    repo = PromptRepository(Path(__file__).resolve().parents[2] / "prompts")
    builder = PromptBuilder(repo, prompt_version="vsma_prompt_v1", case_id="variacion_significativa_mes_anterior")
    messages = builder.build_messages({"orden": {"order_id": "1"}, "historial_consumo": [], "observaciones_calidad": []})
    assert messages[0]["role"] == "system"
    assert messages[1]["role"] == "user"
    assert "INPUT_JSON" in messages[1]["content"]
