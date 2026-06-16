import json

from midas.agent.base.output_parser import AgentOutputParser


def test_parse_plain_json():
    payload = {"order_id": "1", "technical_status": "PROCESADA"}
    assert AgentOutputParser.parse(json.dumps(payload))["order_id"] == "1"


def test_parse_fenced_json():
    raw = "```json\n{\"order_id\":\"1\",\"technical_status\":\"PROCESADA\"}\n```"
    assert AgentOutputParser.parse(raw)["technical_status"] == "PROCESADA"


def test_parse_json_with_text():
    raw = "Respuesta:\n{\"order_id\":\"1\",\"technical_status\":\"PROCESADA\"}\nFin"
    assert AgentOutputParser.parse(raw)["order_id"] == "1"
