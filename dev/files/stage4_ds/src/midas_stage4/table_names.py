from __future__ import annotations

import re
from dataclasses import dataclass

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def assert_identifier(value: str, name: str = "identifier") -> str:
    if not isinstance(value, str) or not _IDENTIFIER_RE.match(value):
        raise ValueError(f"Invalid Databricks SQL {name}: {value!r}")
    return value


def escape_sql_string(value: object) -> str:
    return str(value).replace("'", "''")


@dataclass(frozen=True)
class UnityTable:
    catalog: str
    schema: str
    name: str

    def __post_init__(self) -> None:
        assert_identifier(self.catalog, "catalog")
        assert_identifier(self.schema, "schema")
        assert_identifier(self.name, "table")

    @property
    def fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.name}"


def fqn(catalog: str, schema: str, table: str) -> str:
    return UnityTable(catalog, schema, table).fqn
