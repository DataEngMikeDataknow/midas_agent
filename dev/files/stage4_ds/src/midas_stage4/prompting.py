from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


class PromptRepository:
    def __init__(self, prompts_dir: str | Path):
        self.prompts_dir = Path(prompts_dir)

    @classmethod
    def default(cls) -> "PromptRepository":
        return cls(Path(__file__).resolve().parents[2] / "prompts")

    def read_text(self, filename: str) -> str:
        return (self.prompts_dir / filename).read_text(encoding="utf-8")

    def read_json(self, filename: str) -> Dict[str, Any]:
        return json.loads(self.read_text(filename))

    def read_jsonl(self, filename: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        path = self.prompts_dir / filename
        if not path.exists():
            return []
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            rows.append(json.loads(line))
            if limit is not None and len(rows) >= limit:
                break
        return rows


class PromptBuilder:
    def __init__(self, repo: PromptRepository, prompt_version: str, case_id: str):
        self.repo = repo
        self.prompt_version = prompt_version
        self.case_id = case_id

    def system_prompt(self) -> str:
        return self.repo.read_text("master_system_prompt.md").replace("{{PROMPT_VERSION}}", self.prompt_version)

    def classification_template(self) -> str:
        return self.repo.read_text("classification_prompt.md")

    def few_shots(self, limit: int = 6) -> str:
        examples = self.repo.read_jsonl("few_shots_variacion_significativa.jsonl", limit=limit)
        if not examples:
            return "[]"
        return json.dumps(examples, ensure_ascii=False, indent=2, default=str)

    def build_messages(self, order_context: Dict[str, Any]) -> List[Dict[str, str]]:
        user_payload = {
            "case_id": self.case_id,
            "prompt_version": self.prompt_version,
            "order_context": order_context,
            "few_shots": self.repo.read_jsonl("few_shots_variacion_significativa.jsonl", limit=6),
            "output_contract": self.repo.read_json("output_schema.json"),
        }
        return [
            {"role": "system", "content": self.system_prompt()},
            {"role": "user", "content": self.classification_template() + "\n\nINPUT_JSON:\n" + json.dumps(user_payload, ensure_ascii=False, default=str)},
        ]
