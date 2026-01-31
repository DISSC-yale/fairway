"""Tests for schema merging across multiple files with different columns.

These tests validate that schema inference captures the SUPERSET of all columns
across all files, not just columns from sampled files.

TDD Approach:
- Tests in TestSchemaMergeBug are expected to FAIL before the fix (RED phase)
- After implementing the two-phase schema inference fix, they should PASS (GREEN)
"""
import pytest
from pathlib import Path


class TestSchemaMergeBug:
    """Tests that prove the schema merging bug exists.

    These tests MUST FAIL before the fix is applied.
    They demonstrate that sampling files misses columns from non-sampled files.
    """

    @pytest.fixture
    def schema_merge_dir(self, fixtures_dir):
        """Path to schema_merge fixture directory."""
        return fixtures_dir / "schema_merge"

    @pytest.mark.local
    def test_single_file_sampling_misses_columns(self, duckdb_engine, tmp_path):
        """RED TEST: Proves that sampling only one file misses columns from others.

        This test MUST FAIL before the fix is applied.
        """
        # Create files with different columns
        (tmp_path / "file_a.csv").write_text("id,name,age\n1,alice,30\n")
        (tmp_path / "file_b.csv").write_text("id,name,city\n1,bob,NYC\n")
        (tmp_path / "file_c.csv").write_text("id,name,country\n1,carol,USA\n")

        # Force single-file sampling to trigger the bug
        schema = duckdb_engine.infer_schema(
            path=str(tmp_path / "*.csv"),
            format="csv",
            sample_files=1  # This forces the bug to manifest
        )

        # These assertions WILL FAIL before fix - only columns from sampled file
        assert "id" in schema, "Missing 'id' column"
        assert "name" in schema, "Missing 'name' column"
        assert "age" in schema, "Missing 'age' column from file_a.csv"
        assert "city" in schema, "Missing 'city' column from file_b.csv"
        assert "country" in schema, "Missing 'country' column from file_c.csv"

    @pytest.mark.local
    def test_schema_captures_all_columns_from_fixtures(self, duckdb_engine, schema_merge_dir):
        """RED TEST: Schema inference should capture superset of all columns.

        Uses the schema_merge fixtures with file_a (age), file_b (city), file_c (country).
        """
        schema = duckdb_engine.infer_schema(
            path=str(schema_merge_dir / "*.csv"),
            format="csv"
        )

        # Expected: ALL columns from ALL files
        assert "id" in schema
        assert "name" in schema
        assert "age" in schema, "Missing 'age' column from file_a.csv"
        assert "city" in schema, "Missing 'city' column from file_b.csv"
        assert "country" in schema, "Missing 'country' column from file_c.csv"

    @pytest.mark.local
    def test_all_columns_get_real_types_not_defaults(self, duckdb_engine, tmp_path):
        """RED TEST: Every column must get a type from actual data, not default to STRING.

        This tests the coverage guarantee in Phase 2 sampling.
        """
        # Create files where each has a unique column with specific type
        (tmp_path / "file_a.csv").write_text("id,name,age\n1,alice,30\n")
        (tmp_path / "file_b.csv").write_text("id,name,salary\n1,bob,50000.50\n")
        (tmp_path / "file_c.csv").write_text("id,name,is_active\n1,carol,true\n")

        # Even with sample_files=1, coverage guarantee should include all unique columns
        schema = duckdb_engine.infer_schema(
            path=str(tmp_path / "*.csv"),
            format="csv",
            sample_files=1  # Restrictive, but coverage should override
        )

        # All columns present
        assert "age" in schema, "Missing 'age' column"
        assert "salary" in schema, "Missing 'salary' column"
        assert "is_active" in schema, "Missing 'is_active' column"

        # Types inferred from real data, not defaulted to STRING
        assert schema["age"] in ("INTEGER", "BIGINT", "INT"), f"'age' should be numeric, got {schema.get('age')}"
        assert schema["salary"] in ("DOUBLE", "FLOAT", "DECIMAL"), f"'salary' should be float, got {schema.get('salary')}"
        # Note: DuckDB may infer 'true'/'false' as STRING or BOOLEAN depending on version
        # We mainly care that the column exists


class TestSchemaInferenceQuality:
    """Tests for schema inference quality and edge cases."""

    @pytest.mark.local
    def test_schema_inference_deterministic(self, duckdb_engine, tmp_path):
        """Schema inference should produce consistent results across runs."""
        # Create files
        for i in range(5):
            (tmp_path / f"file_{i}.csv").write_text(f"id,col_{i}\n{i},val\n")

        # Run multiple times
        schemas = [
            duckdb_engine.infer_schema(str(tmp_path / "*.csv"), "csv")
            for _ in range(5)
        ]

        # All should be identical (when deterministic)
        first = schemas[0]
        for i, schema in enumerate(schemas[1:], start=2):
            assert schema == first, f"Run {i} differs from run 1: schema inference is non-deterministic"

    @pytest.mark.local
    def test_type_conflict_uses_broader_type(self, duckdb_engine, tmp_path):
        """When same column has different types, use broader type."""
        (tmp_path / "ints.csv").write_text("id,value\n1,100\n2,200\n")
        (tmp_path / "strings.csv").write_text("id,value\n3,hello\n4,world\n")

        schema = duckdb_engine.infer_schema(str(tmp_path / "*.csv"), "csv")

        # Should use STRING (broader) not INTEGER
        assert schema["value"] == "STRING", f"Expected STRING for mixed types, got {schema['value']}"

    @pytest.mark.local
    def test_empty_file_handled_gracefully(self, duckdb_engine, tmp_path):
        """Empty files should not break schema inference."""
        (tmp_path / "empty.csv").write_text("")
        (tmp_path / "valid.csv").write_text("id,name\n1,alice\n")

        # Should not raise - should handle empty file gracefully
        try:
            schema = duckdb_engine.infer_schema(str(tmp_path / "*.csv"), "csv")
            # If it succeeds, check the valid columns are there
            assert "id" in schema
            assert "name" in schema
        except Exception as e:
            # If it fails, the error message should be informative
            pytest.fail(f"Schema inference failed on empty file: {e}")

    @pytest.mark.local
    def test_header_only_file_handled(self, duckdb_engine, tmp_path):
        """Files with only headers (no data rows) should be handled."""
        (tmp_path / "header_only.csv").write_text("id,name,extra\n")
        (tmp_path / "with_data.csv").write_text("id,name\n1,alice\n")

        schema = duckdb_engine.infer_schema(str(tmp_path / "*.csv"), "csv")

        # Should capture columns from header-only file too
        assert "id" in schema
        assert "name" in schema
        # 'extra' column from header-only file should also be captured
        assert "extra" in schema, "Missing 'extra' column from header-only file"


class TestSchemaInferenceFormats:
    """Test schema inference works across different file formats."""

    @pytest.mark.local
    def test_csv_schema_inference(self, duckdb_engine, fixtures_dir):
        """Basic CSV schema inference."""
        schema = duckdb_engine.infer_schema(
            str(fixtures_dir / "formats" / "csv" / "simple.csv"),
            "csv"
        )
        assert "id" in schema
        assert "name" in schema
        assert "value" in schema

    @pytest.mark.local
    def test_json_schema_inference(self, duckdb_engine, fixtures_dir):
        """JSON records schema inference."""
        schema = duckdb_engine.infer_schema(
            str(fixtures_dir / "formats" / "json" / "records.json"),
            "json"
        )
        assert "id" in schema
        assert "name" in schema
        assert "value" in schema

    @pytest.mark.local
    def test_parquet_schema_inference(self, duckdb_engine, fixtures_dir):
        """Parquet schema inference."""
        schema = duckdb_engine.infer_schema(
            str(fixtures_dir / "formats" / "parquet" / "simple.parquet"),
            "parquet"
        )
        assert "id" in schema
        assert "name" in schema
        assert "value" in schema
