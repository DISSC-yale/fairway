"""
Tests for fixed-width file format support (Chunk F).
"""
import os
import shutil
import pytest
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
        """Ingest simple.txt → processed layer has correct trimmed STRING values."""
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

        df = engine.con.execute(f"SELECT * FROM '{output_path}'").df()
        assert len(df) == 3
        assert list(df.columns) == ["id", "name", "age"]

        # Processed layer: all columns are STRING (trimmed per spec)
        # id/age have leading zeros — trim removes whitespace only, not zeros
        assert df["id"].tolist() == ["001", "002", "003"]
        assert df["name"].tolist() == ["Alice", "Bob", "Carol"]
        assert df["age"].tolist() == ["030", "025", "028"]

    def test_type_conversion(self, output_dir):
        """enforce_types() produces correctly typed columns in the curated layer."""
        engine = DuckDBEngine()
        input_path = str(FIXTURES_DIR / "simple.txt")
        spec_path = str(FIXTURES_DIR / "simple_spec.yaml")
        processed_path = os.path.join(output_dir, "processed.parquet")
        curated_path = os.path.join(output_dir, "curated.parquet")

        engine.ingest(input_path, processed_path, format="fixed_width", fixed_width_spec=spec_path)

        spec = load_spec(spec_path)
        engine.enforce_types(processed_path, curated_path, spec["columns"])

        result = engine.con.execute(f"""
            SELECT typeof(id), typeof(name), typeof(age)
            FROM '{curated_path}' LIMIT 1
        """).fetchone()

        assert "INTEGER" in result[0]
        assert "VARCHAR" in result[1]
        assert "INTEGER" in result[2]

        # Curated values are correctly typed integers
        df = engine.con.execute(f"SELECT id, age FROM '{curated_path}' ORDER BY id").df()
        assert df["id"].tolist() == [1, 2, 3]
        assert df["age"].tolist() == [30, 25, 28]

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

    def test_min_line_length_skips_short_lines(self, output_dir):
        """min_line_length config filters out corrupted short lines."""
        engine = DuckDBEngine()
        output_path = os.path.join(output_dir, "output.parquet")

        # Create a file with a short/corrupted line
        mixed_file = os.path.join(output_dir, "mixed.txt")
        with open(mixed_file, "w") as f:
            f.write("001Alice               030\n")
            f.write("BAD\n")  # Corrupted line (length 3)
            f.write("002Bob                 025\n")

        spec_path = str(FIXTURES_DIR / "simple_spec.yaml")

        # Without min_line_length, this would fail with RULE-115
        # With min_line_length=20, the short line is filtered out
        engine.ingest(
            mixed_file, output_path,
            format="fixed_width",
            fixed_width_spec=spec_path,
            min_line_length=20
        )

        df = engine.con.execute(f"SELECT * FROM '{output_path}'").df()
        assert len(df) == 2  # Only valid lines kept
        assert df["id"].tolist() == ["001", "002"]

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
    def spark_engine(self, pyspark_engine):
        """Use the shared PySpark engine from conftest."""
        return pyspark_engine

    def test_basic_read(self, output_dir, spark_engine):
        """Ingest simple.txt → processed layer has correct trimmed STRING values."""
        input_path = str(FIXTURES_DIR / "simple.txt")
        spec_path = str(FIXTURES_DIR / "simple_spec.yaml")
        output_path = os.path.join(output_dir, "output.parquet")

        result = spark_engine.ingest(
            input_path, output_path,
            format="fixed_width",
            fixed_width_spec=spec_path
        )
        assert result is True

        # Read back with pandas to avoid holding a Spark DataFrame whose py4j GC
        # could interfere with the shared session in the next test.
        import pandas as pd
        df = pd.read_parquet(output_path)
        assert len(df) == 3
        assert set(df.columns) == {"id", "name", "age"}

        # Processed layer: all columns are STRING (trimmed per spec)
        df = df.sort_values("id").reset_index(drop=True)
        assert df["id"].tolist() == ["001", "002", "003"]
        assert df["name"].tolist() == ["Alice", "Bob", "Carol"]
        assert df["age"].tolist() == ["030", "025", "028"]

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


# ---------------------------------------------------------------------------
# Tests: cast_mode field in spec validator
# ---------------------------------------------------------------------------

CODED_FIXTURES_DIR = FIXTURES_DIR  # same dir


class TestSpecCastMode:
    """Validates cast_mode field parsing in fixed_width spec."""

    def test_cast_mode_defaults_to_adaptive(self):
        col = validate_spec({"columns": [{"name": "x", "start": 0, "length": 3, "type": "BIGINT"}]})
        assert col["columns"][0]["cast_mode"] == "adaptive"

    def test_cast_mode_strict_is_preserved(self):
        col = validate_spec({"columns": [{"name": "x", "start": 0, "length": 3, "type": "BIGINT", "cast_mode": "strict"}]})
        assert col["columns"][0]["cast_mode"] == "strict"

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


# ---------------------------------------------------------------------------
# Tests: DuckDB two-layer type enforcement
# ---------------------------------------------------------------------------

class TestDuckDBTypeEnforcement:
    """Tests for the STRING-preserved processed layer and typed curated layer."""

    @pytest.fixture
    def dirs(self, tmp_path):
        processed = str(tmp_path / "processed.parquet")
        curated = str(tmp_path / "curated.parquet")
        return processed, curated

    @pytest.fixture
    def engine(self):
        return DuckDBEngine()

    @pytest.fixture
    def coded_spec(self):
        return str(FIXTURES_DIR / "coded_values_spec.yaml")

    @pytest.fixture
    def coded_input(self):
        return str(FIXTURES_DIR / "coded_values.txt")

    def test_ingest_preserves_coded_value_as_string(self, engine, dirs, coded_input, coded_spec):
        """Ingestion must NOT crash and must write 'ZZZ' as a string to processed."""
        processed, _ = dirs
        result = engine.ingest(coded_input, processed, format="fixed_width", fixed_width_spec=coded_spec)
        assert result is True
        rows = engine.con.execute(f"SELECT income FROM '{processed}' ORDER BY income").fetchall()
        incomes = [r[0] for r in rows]
        assert "ZZZ" in incomes

    def test_ingest_produces_all_string_columns(self, engine, dirs, coded_input, coded_spec):
        """All fixed-width columns must be VARCHAR in the processed layer."""
        processed, _ = dirs
        engine.ingest(coded_input, processed, format="fixed_width", fixed_width_spec=coded_spec)
        types = engine.con.execute(
            f"SELECT typeof(rectype), typeof(income) FROM '{processed}' LIMIT 1"
        ).fetchone()
        assert types[0].upper() in ("VARCHAR", "TEXT")
        assert types[1].upper() in ("VARCHAR", "TEXT")

    def test_enforce_types_produces_correct_types(self, engine, dirs, coded_input, coded_spec):
        """enforce_types() must write typed columns to the curated layer."""
        processed, curated = dirs
        engine.ingest(coded_input, processed, format="fixed_width", fixed_width_spec=coded_spec)
        spec = load_spec(coded_spec)
        engine.enforce_types(processed, curated, spec["columns"])
        types = engine.con.execute(
            f"SELECT typeof(rectype), typeof(income) FROM '{curated}' LIMIT 1"
        ).fetchone()
        assert "INT" in types[0].upper()
        assert "INT" in types[1].upper()

    def test_enforce_types_nulls_coded_values(self, engine, dirs, coded_input, coded_spec):
        """Non-castable coded values must become NULL in the curated layer."""
        processed, curated = dirs
        engine.ingest(coded_input, processed, format="fixed_width", fixed_width_spec=coded_spec)
        spec = load_spec(coded_spec)
        engine.enforce_types(processed, curated, spec["columns"])
        df = engine.con.execute(f"SELECT rectype, income, name FROM '{curated}' ORDER BY name").df()
        import pandas as pd
        bob = df[df["name"] == "Bob"].iloc[0]
        assert pd.isna(bob["income"])

    def test_enforce_types_valid_values_cast_correctly(self, engine, dirs, coded_input, coded_spec):
        """Numeric values in the coded-value file must cast to integers correctly."""
        processed, curated = dirs
        engine.ingest(coded_input, processed, format="fixed_width", fixed_width_spec=coded_spec)
        spec = load_spec(coded_spec)
        engine.enforce_types(processed, curated, spec["columns"])
        df = engine.con.execute(f"SELECT income FROM '{curated}' WHERE income IS NOT NULL ORDER BY income").df()
        assert list(df["income"]) == [123456, 789012]

    def test_enforce_types_strict_raises_on_coded_value(self, engine, dirs, coded_input, coded_spec):
        """on_fail='strict' must raise when a column contains non-castable values."""
        processed, curated = dirs
        engine.ingest(coded_input, processed, format="fixed_width", fixed_width_spec=coded_spec)
        spec = load_spec(coded_spec)
        # income column has cast_mode=adaptive in spec; override globally with strict
        with pytest.raises(Exception):
            engine.enforce_types(processed, curated, spec["columns"], on_fail="strict")

    def test_enforce_types_strict_column_raises(self, engine, dirs, coded_input, coded_spec):
        """A column with cast_mode=strict must raise even when on_fail='null' globally."""
        processed, _ = dirs
        curated2 = str(Path(processed).parent / "curated2.parquet")
        # Write a version of the data where rectype has a non-numeric value
        bad_data = str(Path(processed).parent / "bad_rectype.txt")
        import subprocess
        with open(bad_data, "w") as f:
            # rectype='XX' (non-numeric), income=' 123456', name='Alice     '
            f.write("XX 123456Alice     \n")
        engine2 = DuckDBEngine()
        engine2.ingest(bad_data, processed + "_bad", format="fixed_width", fixed_width_spec=coded_spec)
        spec = load_spec(coded_spec)
        with pytest.raises(Exception):
            engine2.enforce_types(processed + "_bad", curated2, spec["columns"], on_fail="null")

    def test_clean_file_ingest_still_works(self, engine, tmp_path):
        """Regression: a file with no coded values still ingests cleanly."""
        output = str(tmp_path / "simple.parquet")
        result = engine.ingest(
            str(FIXTURES_DIR / "simple.txt"), output,
            format="fixed_width",
            fixed_width_spec=str(FIXTURES_DIR / "simple_spec.yaml"),
        )
        assert result is True
        count = engine.con.execute(f"SELECT COUNT(*) FROM '{output}'").fetchone()[0]
        assert count == 3
