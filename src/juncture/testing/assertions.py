"""Column-level assertions compiled to SQL.

Each function returns a SQL snippet that, when executed, yields the number of
failing rows. The test passes when that count is zero.
"""

from __future__ import annotations


class AssertionError(Exception):
    """Raised when a data test fails."""


def assert_not_null(table: str, column: str) -> str:
    return f'SELECT COUNT(*) AS failures FROM {table} WHERE "{column}" IS NULL'


def assert_unique(table: str, column: str) -> str:
    return (
        f"SELECT COUNT(*) AS failures FROM ("
        f'  SELECT "{column}" FROM {table} WHERE "{column}" IS NOT NULL '
        f'  GROUP BY "{column}" HAVING COUNT(*) > 1'
        f")"
    )


def assert_relationships(table: str, column: str, *, to_table: str, to_column: str) -> str:
    return (
        f"SELECT COUNT(*) AS failures FROM {table} t "
        f'LEFT JOIN {to_table} r ON t."{column}" = r."{to_column}" '
        f'WHERE t."{column}" IS NOT NULL AND r."{to_column}" IS NULL'
    )


def assert_accepted_values(table: str, column: str, values: list[object]) -> str:
    quoted = ", ".join(_sql_literal(v) for v in values)
    return (
        f"SELECT COUNT(*) AS failures FROM {table} "
        f'WHERE "{column}" IS NOT NULL AND "{column}" NOT IN ({quoted})'
    )


def _sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"
