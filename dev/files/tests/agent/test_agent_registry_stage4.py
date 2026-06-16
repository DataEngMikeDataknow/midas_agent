from midas.agent.registry.agent_registry import AgentRegistry, CASE_VARIACION_SIGNIFICATIVA


def test_registry_contains_variacion_significativa():
    registry = AgentRegistry()
    assert registry.has_case(CASE_VARIACION_SIGNIFICATIVA)


def test_registry_variacion_tools():
    config = AgentRegistry().get(CASE_VARIACION_SIGNIFICATIVA)
    assert "get_contexto_variacion_significativa" in config.tool_function_names
