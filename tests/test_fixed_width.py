"""Spec-loader tests for fairway.fixed_width.

The DuckDB and PySpark ingestion classes that the v0.2 suite exercised
(``DuckDBEngine`` / ``PySparkEngine``) were deleted in v0.3 Step 1 / 7.4.
The surviving public surface is the spec loader/validator — those tests
keep useful coverage of the cast-mode + column-overlap machinery and
remain in this module.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from fairway.fixed_width import FixedWidthSpecError, load_spec, validate_spec


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "formats" / "fixed_width"


class TestSpecLoader:
    """Tests for fixed-width spec file loading and validation."""

    def test_load_valid_spec(self):
        """Load the simple_spec.yaml fixture."""
        spec = load_spec(FIXTURES_DIR / "simple_spec.yaml")

        assert "columns" in spec
        assert len(spec["columns"]) == 3
        assert spec["line_length"] == 26  # 23 + 3 = 26

        cols = {c["name"]: c for c in spec["columns"]}
        assert cols["id"]["start"] == 0
        assert cols["id"]["length"] == 3
        assert cols["id"]["type"] == "INTEGER"
        assert cols["name"]["start"] == 3
        assert cols["name"]["length"] == 20
        assert cols["age"]["start"] == 23

    def test_spec_missing_name(self):
        with pytest.raises(FixedWidthSpecError) as exc:
            validate_spec({"columns": [{"start": 0, "length": 5}]})
        assert "Missing required 'name'" in str(exc.value)

    def test_spec_missing_start(self):
        with pytest.raises(FixedWidthSpecError) as exc:
            validate_spec({"columns": [{"name": "col1", "length": 5}]})
        assert "Missing required 'start'" in str(exc.value)

    def test_spec_missing_length(self):
        with pytest.raises(FixedWidthSpecError) as exc:
            validate_spec({"columns": [{"name": "col1", "start": 0}]})
        assert "Missing required 'length'" in str(exc.value)

    def test_spec_overlapping_columns(self):
        with pytest.raises(FixedWidthSpecError) as exc:
            validate_spec({
                "columns": [
                    {"name": "col1", "start": 0, "length": 10},
                    {"name": "col2", "start": 5, "length": 10},  # overlaps
                ]
            })
        assert "Overlaps" in str(exc.value)

    def test_spec_invalid_start(self):
        with pytest.raises(FixedWidthSpecError) as exc:
            validate_spec({"columns": [{"name": "col1", "start": -1, "length": 5}]})
        assert "non-negative integer" in str(exc.value)

    def test_spec_invalid_length(self):
        with pytest.raises(FixedWidthSpecError) as exc:
            validate_spec({"columns": [{"name": "col1", "start": 0, "length": 0}]})
        assert "positive integer" in str(exc.value)

    def test_spec_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            load_spec("/nonexistent/path/spec.yaml")

    def test_spec_invalid_column_name(self):
        """SQL-injection-style names are rejected."""
        with pytest.raises(FixedWidthSpecError) as exc:
            validate_spec({"columns": [{"name": "col; DROP TABLE", "start": 0, "length": 5}]})
        assert "Invalid column name" in str(exc.value)

    def test_spec_invalid_type(self):
        with pytest.raises(FixedWidthSpecError) as exc:
            validate_spec({"columns": [{"name": "col1", "start": 0, "length": 5, "type": "BANANA"}]})
        assert "Invalid type" in str(exc.value)


class TestSpecCastMode:
    """Validates cast_mode field parsing in fixed_width spec."""

    def test_cast_mode_defaults_to_adaptive(self):
        spec = validate_spec({"columns": [{"name": "x", "start": 0, "length": 3, "type": "BIGINT"}]})
        assert spec["columns"][0]["cast_mode"] == "adaptive"

    def test_cast_mode_strict_is_preserved(self):
        spec = validate_spec({"columns": [
            {"name": "x", "start": 0, "length": 3, "type": "BIGINT", "cast_mode": "strict"}
        ]})
        assert spec["columns"][0]["cast_mode"] == "strict"

    def test_cast_mode_invalid_value_raises(self):
        with pytest.raises(FixedWidthSpecError, match="cast_mode"):
            validate_spec({"columns": [{"name": "x", "start": 0, "length": 3, "cast_mode": "banana"}]})

    def test_cast_mode_non_string_raises(self):
        with pytest.raises(FixedWidthSpecError, match="cast_mode"):
            validate_spec({"columns": [{"name": "x", "start": 0, "length": 3, "cast_mode": 42}]})

    def test_coded_values_spec_loads(self):
        spec = load_spec(FIXTURES_DIR / "coded_values_spec.yaml")
        cols = {c["name"]: c for c in spec["columns"]}
        assert cols["rectype"]["cast_mode"] == "strict"
        assert cols["income"]["cast_mode"] == "adaptive"
        assert cols["name"]["type"] == "VARCHAR"
