"""Unit: classify_error maps DuckDB error strings to actionable buckets."""

from __future__ import annotations

from juncture.diagnostics import ErrorBucket, classify_error, classify_statement_errors


def test_conversion_error_with_sentinel_string() -> None:
    msg = "Conversion Error: Could not convert string '' to INT64"
    c = classify_error(msg)
    assert c.bucket is ErrorBucket.CONVERSION
    assert c.subcategory == "sentinel_string_to_typed"
    assert c.operands["value"] == ""
    assert c.operands["target"] == "INT64"
    assert "TRY_CAST" in c.fix_hint
    assert "NULLIF" in c.fix_hint


def test_conversion_error_with_custom_sentinel() -> None:
    msg = "Conversion Error: Could not convert string '--empty--' to INT32"
    c = classify_error(msg)
    assert c.bucket is ErrorBucket.CONVERSION
    assert c.operands["value"] == "--empty--"
    assert "--empty--" in c.fix_hint


def test_empty_string_vs_timestamp() -> None:
    msg = 'Conversion Error: invalid timestamp field format: ""'
    c = classify_error(msg)
    assert c.bucket is ErrorBucket.CONVERSION
    assert c.subcategory == "empty_string_vs_timestamp"


def test_case_mixed_types() -> None:
    msg = "Binder Error: Cannot mix values of type VARCHAR and INTEGER_LITERAL in CASE expression"
    c = classify_error(msg)
    assert c.bucket is ErrorBucket.TYPE_MISMATCH
    assert c.subcategory == "case_mixed_types"
    assert "VARCHAR" in c.fix_hint or "CAST" in c.fix_hint


def test_comparison_type_mismatch() -> None:
    msg = "Binder Error: Cannot compare values of type VARCHAR and BIGINT"
    c = classify_error(msg)
    assert c.bucket is ErrorBucket.TYPE_MISMATCH
    assert c.subcategory == "comparison_type_mismatch"
    assert c.operands["left"] == "VARCHAR"
    assert c.operands["right"] == "BIGINT"


def test_function_signature_mismatch() -> None:
    msg = "Binder Error: No function matches the given name and argument types 'sum(VARCHAR)'"
    c = classify_error(msg)
    assert c.bucket is ErrorBucket.FUNCTION_SIGNATURE
    assert c.operands["fn"] == "sum"


def test_table_already_exists_is_idempotence_bucket() -> None:
    msg = "Catalog Error: Table with name foo already exists"
    c = classify_error(msg)
    assert c.bucket is ErrorBucket.IDEMPOTENCE


def test_table_missing_is_missing_object_bucket() -> None:
    msg = "Catalog Error: Table with name foo does not exist"
    c = classify_error(msg)
    assert c.bucket is ErrorBucket.MISSING_OBJECT
    assert c.operands["name"] == "foo"


def test_parser_error_falls_into_parser_bucket() -> None:
    msg = 'Parser Error: syntax error at or near "QUALIFY"'
    c = classify_error(msg)
    assert c.bucket is ErrorBucket.PARSER


def test_unknown_error_lands_in_other() -> None:
    msg = "Runtime Error: something really weird happened"
    c = classify_error(msg)
    assert c.bucket is ErrorBucket.OTHER
    assert c.subcategory == "unclassified"


def test_classify_statement_errors_accepts_dataclass_like() -> None:
    class Fake:
        def __init__(self, error: str) -> None:
            self.error = error

    items = [Fake("Catalog Error: Table with name a already exists"), "whatever"]
    results = classify_statement_errors(items)
    assert len(results) == 2
    assert results[0].bucket is ErrorBucket.IDEMPOTENCE
    assert results[1].bucket is ErrorBucket.OTHER


def test_empty_message_is_other_bucket() -> None:
    assert classify_error("").bucket is ErrorBucket.OTHER
