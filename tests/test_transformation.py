"""Tests for transformation registry and base transformer."""
import pytest
import pandas as pd
from pathlib import Path

from fairway.transformations.registry import (
    load_transformer, add_allowed_directory, clear_allowed_directories
)
from fairway.transformations.base import BaseTransformer


@pytest.fixture(autouse=True)
def allow_tmp_path_for_transforms(tmp_path):
    """Allow tmp_path for transformation loading in tests."""
    add_allowed_directory(str(tmp_path))
    yield
    clear_allowed_directories()


class _IdentityTransformer(BaseTransformer):
    """Concrete subclass used to exercise BaseTransformer helpers in tests."""

    def transform(self) -> pd.DataFrame:
        return self.df


class TestBaseTransformer:
    """Tests for BaseTransformer functionality."""

    def test_base_transformer_is_abstract(self):
        """Instantiating BaseTransformer without overriding transform() must fail."""
        with pytest.raises(TypeError):
            BaseTransformer(pd.DataFrame({"id": [1]}))

    def test_subclass_identity(self):
        """Concrete subclass with trivial transform returns unchanged dataframe."""
        df = pd.DataFrame({"id": [1, 2], "name": ["alice", "bob"]})
        result = _IdentityTransformer(df).transform()
        pd.testing.assert_frame_equal(result, df)

    def test_rename_columns(self):
        df = pd.DataFrame({"old_name": [1, 2], "keep": ["a", "b"]})
        transformer = _IdentityTransformer(df)
        transformer.rename_columns({"old_name": "new_name"})
        result = transformer.transform()
        assert "new_name" in result.columns
        assert "old_name" not in result.columns
        assert "keep" in result.columns

    def test_cast_types(self):
        df = pd.DataFrame({"id": ["1", "2"], "value": [1.5, 2.5]})
        transformer = _IdentityTransformer(df)
        transformer.cast_types({"id": int, "value": int})
        result = transformer.transform()
        assert result["id"].dtype == int
        assert result["value"].dtype == int
        assert list(result["id"]) == [1, 2]
        assert list(result["value"]) == [1, 2]

    def test_cast_types_missing_column(self):
        df = pd.DataFrame({"id": ["1", "2"]})
        transformer = _IdentityTransformer(df)
        transformer.cast_types({"id": int, "missing": str})
        result = transformer.transform()
        assert result["id"].dtype == int

    def test_clean_strings(self):
        df = pd.DataFrame({"name": ["  ALICE  ", "BOB  ", "  Carol"]})
        transformer = _IdentityTransformer(df)
        transformer.clean_strings(["name"])
        result = transformer.transform()
        assert list(result["name"]) == ["alice", "bob", "carol"]

    def test_clean_strings_missing_column(self):
        df = pd.DataFrame({"name": ["Alice"]})
        transformer = _IdentityTransformer(df)
        transformer.clean_strings(["name", "missing"])
        result = transformer.transform()
        assert list(result["name"]) == ["alice"]

    def test_chained_operations(self):
        df = pd.DataFrame({
            "old_col": ["  VALUE1  ", "  VALUE2  "],
            "num": ["10", "20"]
        })
        transformer = _IdentityTransformer(df)
        result = (transformer
            .rename_columns({"old_col": "new_col"})
            .clean_strings(["new_col"])
            .cast_types({"num": int})
            .transform())
        assert "new_col" in result.columns
        assert list(result["new_col"]) == ["value1", "value2"]
        assert result["num"].dtype == int


class TestTransformationRegistry:
    """Tests for dynamic transformer loading."""

    def test_load_valid_transformer(self, tmp_path):
        """Test loading a valid transformer class."""
        script = tmp_path / "my_transform.py"
        script.write_text('''
from fairway.transformations.base import BaseTransformer

class MyTransformer(BaseTransformer):
    def transform(self):
        self.df["processed"] = True
        return self.df
''')

        transformer_cls = load_transformer(str(script))

        assert transformer_cls is not None
        assert transformer_cls.__name__ == "MyTransformer"

    def test_load_transformer_works(self, tmp_path):
        """Test loaded transformer can actually transform data."""
        script = tmp_path / "double_transform.py"
        script.write_text('''
from fairway.transformations.base import BaseTransformer

class DoubleTransformer(BaseTransformer):
    def transform(self):
        self.df["doubled"] = self.df["value"] * 2
        return self.df
''')

        transformer_cls = load_transformer(str(script))
        df = pd.DataFrame({"value": [1, 2, 3]})
        transformer = transformer_cls(df)
        result = transformer.transform()

        assert "doubled" in result.columns
        assert list(result["doubled"]) == [2, 4, 6]

    def test_load_nonexistent_script(self, caplog, monkeypatch):
        """Test loading from nonexistent path returns None and logs error.

        Ensures propagation is on for the fairway logger so caplog captures
        the error record regardless of which other tests may have called
        setup_logger() first (which sets propagate=False).
        """
        import logging
        monkeypatch.setattr(logging.getLogger("fairway"), "propagate", True)
        with caplog.at_level(logging.ERROR, logger="fairway.transformations.registry"):
            result = load_transformer("/nonexistent/path.py")

        assert result is None
        assert any("not found" in rec.getMessage() for rec in caplog.records)

    def test_load_script_without_transformer(self, tmp_path):
        """Test loading script without Transformer class returns None."""
        script = tmp_path / "no_transformer.py"
        script.write_text('''
def some_function():
    return "not a transformer"

class SomeOtherClass:
    pass
''')

        result = load_transformer(str(script))

        assert result is None

    def test_load_script_with_multiple_transformers_raises(self, tmp_path):
        """Two Transformer classes in one script must raise, not silently pick one."""
        script = tmp_path / "two_transformers.py"
        script.write_text(
            "from fairway.transformations.base import BaseTransformer\n"
            "class AlphaTransformer(BaseTransformer):\n"
            "    def transform(self): return self.df\n"
            "class BetaTransformer(BaseTransformer):\n"
            "    def transform(self): return self.df\n"
        )
        with pytest.raises(ValueError, match="exactly one Transformer class"):
            load_transformer(str(script))

    def test_load_script_with_imported_base(self, tmp_path):
        """Test that imported BaseTransformer is not returned."""
        script = tmp_path / "import_only.py"
        script.write_text('''
from fairway.transformations.base import BaseTransformer
# Just import, don't define custom transformer
''')

        result = load_transformer(str(script))

        # Should not return BaseTransformer since it's imported, not defined here
        assert result is None


class TestExampleTransformer:
    """Tests using the example transformer from the package."""

    def test_example_transformer_exists(self):
        """Test that example transformer file exists."""
        from fairway.data import example_transform
        assert hasattr(example_transform, 'ExampleTransformer')

    def test_example_transformer_loads(self):
        """Test that example transformer can be loaded via registry."""
        import fairway.data.example_transform as et
        script_path = et.__file__

        transformer_cls = load_transformer(script_path)

        assert transformer_cls is not None
        assert transformer_cls.__name__ == "ExampleTransformer"


# ---------------------------------------------------------------------------
# Pipeline-level transformation tests
# ---------------------------------------------------------------------------

class TestTransformationThroughPipeline:
    """Transformation scripts must run via the pipeline and affect output data."""

    def test_transform_adds_processed_column(self, engine, fixtures_dir, tmp_path):
        """simple_transform.py adds processed=True column. Must appear in output."""
        if hasattr(engine, "spark"):
            pytest.xfail("BaseTransformer receives Spark DF but test uses Pandas syntax (df['col']=...)")
        from tests.helpers import build_config, read_curated

        def engine_name(e):
            return "pyspark" if hasattr(e, "spark") else "duckdb"

        # Write a class-based transformer to tmp_path (registry requires class ending in Transformer)
        transform_script = tmp_path / "pipeline_transform.py"
        transform_script.write_text(
            "from fairway.transformations.base import BaseTransformer\n\n"
            "class PipelineTransformer(BaseTransformer):\n"
            "    def transform(self):\n"
            "        self.df['processed'] = True\n"
            "        return self.df\n"
        )

        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "transformed",
            "path": str(fixtures_dir / "formats" / "csv" / "simple.csv"),
            "format": "csv",
            "transformation": str(transform_script),
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "transformed")
        assert "processed" in df.columns, "Transform did not add 'processed' column"
        assert len(df) == 3, "Transform must not change row count"
        assert df["processed"].all(), "All rows must have processed=True"

    def test_transform_returning_none_raises(self, engine, fixtures_dir, tmp_path):
        """A transformer that returns None must raise, not silently drop data."""
        if hasattr(engine, "spark"):
            pytest.xfail("Spark path not relevant — validation is engine-agnostic")
        from tests.helpers import build_config

        def engine_name(e):
            return "pyspark" if hasattr(e, "spark") else "duckdb"

        transform_script = tmp_path / "bad_transform.py"
        transform_script.write_text(
            "from fairway.transformations.base import BaseTransformer\n\n"
            "class BadTransformer(BaseTransformer):\n"
            "    def transform(self):\n"
            "        return None\n"
        )

        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "bad",
            "path": str(fixtures_dir / "formats" / "csv" / "simple.csv"),
            "format": "csv",
            "transformation": str(transform_script),
        })
        from fairway.pipeline import IngestionPipeline
        with pytest.raises((ValueError, RuntimeError), match="returned None"):
            IngestionPipeline(config).run()

    def test_transform_row_count_unchanged_both_engines(self, engine, fixtures_dir, tmp_path):
        """Transform must not filter or duplicate rows on either engine."""
        if hasattr(engine, "spark"):
            pytest.xfail("BaseTransformer receives Spark DF but test uses Pandas syntax (df['col']=...)")
        from tests.helpers import build_config, read_curated

        def engine_name(e):
            return "pyspark" if hasattr(e, "spark") else "duckdb"

        # Write a class-based transformer to tmp_path (registry requires class ending in Transformer)
        transform_script = tmp_path / "count_transform.py"
        transform_script.write_text(
            "from fairway.transformations.base import BaseTransformer\n\n"
            "class CountTransformer(BaseTransformer):\n"
            "    def transform(self):\n"
            "        self.df['processed'] = True\n"
            "        return self.df\n"
        )

        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "count_check",
            "path": str(fixtures_dir / "formats" / "csv" / "simple.csv"),
            "format": "csv",
            "transformation": str(transform_script),
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "count_check")
        assert len(df) == 3
