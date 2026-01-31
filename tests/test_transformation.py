"""Tests for transformation registry and base transformer."""
import pytest
import pandas as pd
from pathlib import Path

from fairway.transformations.registry import load_transformer
from fairway.transformations.base import BaseTransformer


class TestBaseTransformer:
    """Tests for BaseTransformer functionality."""

    def test_base_transformer_identity(self):
        """Test base transformer returns unchanged dataframe."""
        df = pd.DataFrame({"id": [1, 2], "name": ["alice", "bob"]})
        transformer = BaseTransformer(df)

        result = transformer.transform()

        pd.testing.assert_frame_equal(result, df)

    def test_rename_columns(self):
        """Test column renaming."""
        df = pd.DataFrame({"old_name": [1, 2], "keep": ["a", "b"]})
        transformer = BaseTransformer(df)

        transformer.rename_columns({"old_name": "new_name"})
        result = transformer.transform()

        assert "new_name" in result.columns
        assert "old_name" not in result.columns
        assert "keep" in result.columns

    def test_cast_types(self):
        """Test type casting."""
        df = pd.DataFrame({"id": ["1", "2"], "value": [1.5, 2.5]})
        transformer = BaseTransformer(df)

        transformer.cast_types({"id": int, "value": int})
        result = transformer.transform()

        assert result["id"].dtype == int
        assert result["value"].dtype == int
        assert list(result["id"]) == [1, 2]
        assert list(result["value"]) == [1, 2]

    def test_cast_types_missing_column(self):
        """Test type casting ignores missing columns."""
        df = pd.DataFrame({"id": ["1", "2"]})
        transformer = BaseTransformer(df)

        # Should not raise even though 'missing' doesn't exist
        transformer.cast_types({"id": int, "missing": str})
        result = transformer.transform()

        assert result["id"].dtype == int

    def test_clean_strings(self):
        """Test string cleaning (strip and lowercase)."""
        df = pd.DataFrame({"name": ["  ALICE  ", "BOB  ", "  Carol"]})
        transformer = BaseTransformer(df)

        transformer.clean_strings(["name"])
        result = transformer.transform()

        assert list(result["name"]) == ["alice", "bob", "carol"]

    def test_clean_strings_missing_column(self):
        """Test string cleaning ignores missing columns."""
        df = pd.DataFrame({"name": ["Alice"]})
        transformer = BaseTransformer(df)

        # Should not raise
        transformer.clean_strings(["name", "missing"])
        result = transformer.transform()

        assert list(result["name"]) == ["alice"]

    def test_chained_operations(self):
        """Test chaining multiple transformer operations."""
        df = pd.DataFrame({
            "old_col": ["  VALUE1  ", "  VALUE2  "],
            "num": ["10", "20"]
        })
        transformer = BaseTransformer(df)

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

    def test_load_nonexistent_script(self, capsys):
        """Test loading from nonexistent path returns None."""
        result = load_transformer("/nonexistent/path.py")

        assert result is None
        captured = capsys.readouterr()
        assert "not found" in captured.out

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
