"""
Tests for fixed-width file format support (Chunk F).
"""
import os
import shutil
import pytest
import tempfile
from pathlib import Path

from fairway.fixed_width import load_spec, validate_spec, FixedWidthSpecError
from fairway.engines.duckdb_engine import DuckDBEngine

# Test fixtures path
FIXTURES_DIR = Path(__file__).parent / "fixtures" / "formats" / "fixed_width"


class TestSpecLoader:
    """Tests for fixed-width spec file loading and validation."""

    def test_load_valid_spec(self):
        """Load the simple_spec.yaml fixture."""
        spec = load_spec(FIXTURES_DIR / "simple_spec.yaml")

        assert "columns" in spec
        assert len(spec["columns"]) == 3
        assert spec["line_length"] == 26  # 23 + 3 = 26

        # Check column definitions
        cols = {c["name"]: c for c in spec["columns"]}
        assert cols["id"]["start"] == 0
        assert cols["id"]["length"] == 3
        assert cols["id"]["type"] == "INTEGER"
        assert cols["name"]["start"] == 3
        assert cols["name"]["length"] == 20
        assert cols["age"]["start"] == 23

    def test_spec_missing_name(self):
        """Spec validation fails when column missing name."""
        with pytest.raises(FixedWidthSpecError) as exc:
            validate_spec({"columns": [{"start": 0, "length": 5}]})
        assert "Missing required 'name'" in str(exc.value)

    def test_spec_missing_start(self):
        """Spec validation fails when column missing start."""
        with pytest.raises(FixedWidthSpecError) as exc:
            validate_spec({"columns": [{"name": "col1", "length": 5}]})
        assert "Missing required 'start'" in str(exc.value)

    def test_spec_missing_length(self):
        """Spec validation fails when column missing length."""
        with pytest.raises(FixedWidthSpecError) as exc:
            validate_spec({"columns": [{"name": "col1", "start": 0}]})
        assert "Missing required 'length'" in str(exc.value)

    def test_spec_overlapping_columns(self):
        """Spec validation fails for overlapping columns."""
        with pytest.raises(FixedWidthSpecError) as exc:
            validate_spec({
                "columns": [
                    {"name": "col1", "start": 0, "length": 10},
                    {"name": "col2", "start": 5, "length": 10},  # overlaps
                ]
            })
        assert "Overlaps" in str(exc.value)

    def test_spec_invalid_start(self):
        """Spec validation fails for negative start."""
        with pytest.raises(FixedWidthSpecError) as exc:
            validate_spec({"columns": [{"name": "col1", "start": -1, "length": 5}]})
        assert "non-negative integer" in str(exc.value)

    def test_spec_invalid_length(self):
        """Spec validation fails for zero/negative length."""
        with pytest.raises(FixedWidthSpecError) as exc:
            validate_spec({"columns": [{"name": "col1", "start": 0, "length": 0}]})
        assert "positive integer" in str(exc.value)

    def test_spec_file_not_found(self):
        """Load fails for non-existent spec file."""
        with pytest.raises(FileNotFoundError):
            load_spec("/nonexistent/path/spec.yaml")

    def test_spec_invalid_column_name(self):
        """Spec validation fails for invalid column names (SQL injection prevention)."""
        with pytest.raises(FixedWidthSpecError) as exc:
            validate_spec({"columns": [{"name": "col; DROP TABLE", "start": 0, "length": 5}]})
        assert "Invalid column name" in str(exc.value)

    def test_spec_invalid_type(self):
        """Spec validation fails for invalid types."""
        with pytest.raises(FixedWidthSpecError) as exc:
            validate_spec({"columns": [{"name": "col1", "start": 0, "length": 5, "type": "BANANA"}]})
        assert "Invalid type" in str(exc.value)


class TestDuckDBFixedWidth:
    """Tests for DuckDB fixed-width ingestion."""

    @pytest.fixture
    def output_dir(self):
        """Create temporary output directory."""
        base_dir = os.path.abspath("test_temp_data/fixed_width")
        if os.path.exists(base_dir):
            shutil.rmtree(base_dir)
        os.makedirs(base_dir)
        yield base_dir
        if os.path.exists(base_dir):
            shutil.rmtree(base_dir)

    def test_basic_read(self, output_dir):
        """Read simple.txt with spec and verify values."""
        engine = DuckDBEngine()
        input_path = str(FIXTURES_DIR / "simple.txt")
        spec_path = str(FIXTURES_DIR / "simple_spec.yaml")
        output_path = os.path.join(output_dir, "output.parquet")

        result = engine.ingest(
            input_path, output_path,
            format="fixed_width",
            fixed_width_spec=spec_path
        )
        assert result is True

        # Verify output
        df = engine.con.execute(f"SELECT * FROM '{output_path}'").df()
        assert len(df) == 3
        assert list(df.columns) == ["id", "name", "age"]

        # Check values (with trim applied)
        assert df["id"].tolist() == [1, 2, 3]
        assert df["name"].tolist() == ["Alice", "Bob", "Carol"]
        assert df["age"].tolist() == [30, 25, 28]

    def test_type_conversion(self, output_dir):
        """Verify types are correctly converted."""
        engine = DuckDBEngine()
        input_path = str(FIXTURES_DIR / "simple.txt")
        spec_path = str(FIXTURES_DIR / "simple_spec.yaml")
        output_path = os.path.join(output_dir, "output.parquet")

        engine.ingest(
            input_path, output_path,
            format="fixed_width",
            fixed_width_spec=spec_path
        )

        # Check types via DuckDB
        result = engine.con.execute(f"""
            SELECT typeof(id), typeof(name), typeof(age)
            FROM '{output_path}' LIMIT 1
        """).fetchone()

        assert "INTEGER" in result[0]
        assert "VARCHAR" in result[1]
        assert "INTEGER" in result[2]

    def test_missing_spec_fails(self, output_dir):
        """Ingestion fails without spec file."""
        engine = DuckDBEngine()
        input_path = str(FIXTURES_DIR / "simple.txt")
        output_path = os.path.join(output_dir, "output.parquet")

        with pytest.raises(ValueError, match="requires 'fixed_width_spec'"):
            engine.ingest(
                input_path, output_path,
                format="fixed_width"
            )

    def test_short_line_fails(self, output_dir):
        """Short lines trigger RULE-115 failure."""
        engine = DuckDBEngine()
        output_path = os.path.join(output_dir, "output.parquet")

        # Create a file with a short line
        short_file = os.path.join(output_dir, "short.txt")
        with open(short_file, "w") as f:
            f.write("001Alice               030\n")
            f.write("002Bob\n")  # Too short!

        spec_path = str(FIXTURES_DIR / "simple_spec.yaml")

        with pytest.raises(ValueError, match="RULE-115"):
            engine.ingest(
                short_file, output_path,
                format="fixed_width",
                fixed_width_spec=spec_path
            )

    def test_metadata_injection(self, output_dir):
        """Metadata can be injected into fixed-width output."""
        engine = DuckDBEngine()
        input_path = str(FIXTURES_DIR / "simple.txt")
        spec_path = str(FIXTURES_DIR / "simple_spec.yaml")
        output_path = os.path.join(output_dir, "output.parquet")

        engine.ingest(
            input_path, output_path,
            format="fixed_width",
            fixed_width_spec=spec_path,
            metadata={"source": "legacy_system"}
        )

        df = engine.con.execute(f"SELECT * FROM '{output_path}'").df()
        assert "source" in df.columns
        assert df["source"].unique()[0] == "legacy_system"


class TestPySparkFixedWidth:
    """Tests for PySpark fixed-width ingestion."""

    @pytest.fixture
    def output_dir(self):
        """Create temporary output directory."""
        base_dir = os.path.abspath("test_temp_data/fixed_width_spark")
        if os.path.exists(base_dir):
            shutil.rmtree(base_dir)
        os.makedirs(base_dir)
        yield base_dir
        if os.path.exists(base_dir):
            shutil.rmtree(base_dir)

    @pytest.fixture
    def spark_engine(self):
        """Create PySpark engine."""
        from fairway.engines.pyspark_engine import PySparkEngine
        return PySparkEngine()

    def test_basic_read(self, output_dir, spark_engine):
        """Read simple.txt with spec and verify values."""
        input_path = str(FIXTURES_DIR / "simple.txt")
        spec_path = str(FIXTURES_DIR / "simple_spec.yaml")
        output_path = os.path.join(output_dir, "output.parquet")

        # Test through public API
        result = spark_engine.ingest(
            input_path, output_path,
            format="fixed_width",
            fixed_width_spec=spec_path
        )
        assert result is True

        # Verify output
        df = spark_engine.spark.read.parquet(output_path)
        assert df.count() == 3
        assert set(df.columns) == {"id", "name", "age"}

        # Check values
        rows = df.orderBy("id").collect()
        assert rows[0]["id"] == 1
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == 30

    def test_short_line_fails(self, output_dir, spark_engine):
        """Short lines trigger RULE-115 failure."""
        output_path = os.path.join(output_dir, "output.parquet")

        # Create a file with a short line
        short_file = os.path.join(output_dir, "short.txt")
        with open(short_file, "w") as f:
            f.write("001Alice               030\n")
            f.write("002Bob\n")  # Too short!

        spec_path = str(FIXTURES_DIR / "simple_spec.yaml")

        with pytest.raises(ValueError, match="RULE-115"):
            spark_engine._ingest_fixed_width(
                short_file, output_path, spec_path
            )
