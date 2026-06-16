from __future__ import annotations

from pathlib import Path


class PromptLoader:
    """Carga prompts desde archivos versionables.

    El prompt ya no debe vivir como un string gigante dentro de código Python.
    Mantenerlo en markdown facilita revisión funcional, versionamiento y
    comparación entre versiones del agente.
    """

    def __init__(self, base_path: str | Path | None = None):
        self.base_path = Path(base_path).resolve() if base_path else self._discover_base_path()

    @staticmethod
    def _discover_base_path() -> Path:
        current = Path(__file__).resolve()
        for parent in current.parents:
            if (parent / "midas" / "agents").exists():
                return parent / "midas" / "agents"
            if (parent / "agents").exists():
                return parent / "agents"
        return Path("src/midas/agents")

    def load(self, prompt_file: str) -> str:
        prompt_path = Path(prompt_file)
        if not prompt_path.is_absolute():
            prompt_path = self.base_path / prompt_file

        if not prompt_path.exists():
            raise FileNotFoundError(f"Prompt no encontrado: {prompt_path}")

        return prompt_path.read_text(encoding="utf-8")
