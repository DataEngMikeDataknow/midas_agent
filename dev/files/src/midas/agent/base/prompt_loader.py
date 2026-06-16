from __future__ import annotations

from pathlib import Path


class PromptLoader:
    """Carga prompts desde archivos Markdown.

    Se usa para sacar los prompts del código Python y versionarlos como
    artefactos de ciencia de datos por casuística.
    """

    @staticmethod
    def load(path: str | Path) -> str:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Prompt no encontrado: {p}")
        content = p.read_text(encoding="utf-8").strip()
        if not content:
            raise ValueError(f"Prompt vacío: {p}")
        return content

    @staticmethod
    def from_case(case_dir: str | Path, filename: str = "prompt.md") -> str:
        return PromptLoader.load(Path(case_dir) / filename)
