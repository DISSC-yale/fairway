"""Targeted tests filling pandas-branch coverage gaps in:

- fairway.validations.checks (max_rows schema-error, missing-column branches,
  empty-after-dropna branches)
- fairway.enrichments.geospatial.Enricher (pandas mock_geocode + enrich_dataframe)
- fairway.transformations.registry (security errors and missing files)

All tests run without Spark/Java (RULE-113 compliant locally) and use tmp_path
for any filesystem artifacts (RULE-114 cleanup).
"""
import os
import pandas as pd
import pytest

from fairway.validations.checks import Validator
from fairway.enrichments.geospatial import Enricher
from fairway.transformations import registry


# ===========================================================================
# Validator: schema-error & missing-column branches (pandas)
# ===========================================================================

class TestValidatorSchemaErrors:

    def test_max_rows_zero_is_schema_error(self):
        """max_rows must be a positive int — 0 should be flagged."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = Validator.run_all(df, {"max_rows": 0})
        assert any(
            "max_rows" in f["message"] and "positive" in f["message"]
            for f in result.errors
        )

    def test_max_rows_negative_is_schema_error(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = Validator.run_all(df, {"max_rows": -10})
        assert any("max_rows" in f["message"] for f in result.errors)


class TestCheckValuesPandasGaps:

    def test_check_values_missing_column_warns_not_errors(self):
        """RULE-104 spirit: missing columns warn (severity=warn), do not error."""
        df = pd.DataFrame({"present": ["a", "b"]})
        result = Validator.run_all(df, {"check_values": {"absent": ["a", "b"]}})
        # Missing column should be a warning, not an error
        assert result.errors == []
        assert any(
            f["check"] == "check_values" and f["column"] == "absent"
            for f in result.warnings
        )

    def test_check_values_all_nulls_skipped(self):
        """A column that is entirely null after dropna() should not produce findings."""
        df = pd.DataFrame({"state": [None, None, None]}, dtype=object)
        result = Validator.run_all(df, {"check_values": {"state": ["CT", "MA"]}})
        # No findings of any kind for check_values
        all_findings = result.errors + result.warnings
        assert not any(f["check"] == "check_values" for f in all_findings)


# ===========================================================================
# Enricher pandas branch
# ===========================================================================

class TestEnricherPandas:

    def test_mock_geocode_returns_lat_lon_in_range(self):
        lat, lon = Enricher.mock_geocode("123 Main St")
        assert -90 <= lat <= 90
        assert -180 <= lon <= 180

    def test_mock_h3_index_returns_15_char_hex(self):
        h3 = Enricher.mock_h3_index(40.0, -73.0)
        assert isinstance(h3, str)
        assert len(h3) == 15
        # All chars must be valid hex
        int(h3, 16)

    def test_enrich_dataframe_adds_columns_when_address_present(self):
        df = pd.DataFrame({"address": ["1 A St", "2 B St", "3 C St"]})
        out = Enricher.enrich_dataframe(df)
        for col in ("latitude", "longitude", "h3_index"):
            assert col in out.columns
        assert len(out) == 3
        # latitude/longitude must be numeric and within geographic bounds
        assert ((out["latitude"] >= -90) & (out["latitude"] <= 90)).all()
        assert ((out["longitude"] >= -180) & (out["longitude"] <= 180)).all()

    def test_enrich_dataframe_passthrough_when_no_address(self):
        df = pd.DataFrame({"id": [1, 2, 3]})
        out = Enricher.enrich_dataframe(df)
        # Unchanged — no enrichment columns added
        assert list(out.columns) == ["id"]


# ===========================================================================
# Transformer registry security errors
# ===========================================================================

class TestRegistrySecurity:

    def test_load_transformer_missing_file_returns_none(self, tmp_path):
        result = registry.load_transformer(str(tmp_path / "does_not_exist.py"))
        assert result is None

    def test_load_transformer_outside_allowed_dirs_raises(self, tmp_path, monkeypatch):
        """A .py outside allowed dirs must raise ValueError (security rule)."""
        # Use tmp_path far from cwd so it's not under any allowed prefix
        monkeypatch.chdir(tmp_path)
        evil_dir = tmp_path / "evil"
        evil_dir.mkdir()
        evil_script = evil_dir / "bad.py"
        evil_script.write_text("class BadTransformer: pass\n")

        with pytest.raises(ValueError, match="Security error"):
            registry.load_transformer(str(evil_script))

    def test_load_transformer_non_py_extension_raises(self, tmp_path, monkeypatch):
        """Even allowed-dir scripts must end in .py."""
        monkeypatch.chdir(tmp_path)
        allowed_dir = tmp_path / "src" / "transformations"
        allowed_dir.mkdir(parents=True)
        bad_ext = allowed_dir / "transformer.txt"
        bad_ext.write_text("class FakeTransformer: pass\n")

        with pytest.raises(ValueError, match="must be a .py file"):
            registry.load_transformer(str(bad_ext))

    def test_load_transformer_loads_class_from_allowed_dir(self, tmp_path, monkeypatch):
        """Happy path: a valid Transformer class in src/transformations/ loads."""
        monkeypatch.chdir(tmp_path)
        allowed_dir = tmp_path / "src" / "transformations"
        allowed_dir.mkdir(parents=True)
        script = allowed_dir / "my_transform.py"
        script.write_text(
            "class MyTransformer:\n"
            "    def transform(self, df):\n"
            "        return df\n"
        )

        cls = registry.load_transformer(str(script))
        assert cls is not None
        assert cls.__name__ == "MyTransformer"

    def test_add_and_clear_allowed_directory(self, tmp_path, monkeypatch):
        """Test helpers add_/clear_allowed_directory must round-trip."""
        monkeypatch.chdir(tmp_path)
        extra = tmp_path / "extra_transforms"
        extra.mkdir()
        script = extra / "x.py"
        script.write_text(
            "class XTransformer:\n"
            "    pass\n"
        )

        # Without registration, loading should fail
        with pytest.raises(ValueError, match="Security error"):
            registry.load_transformer(str(script))

        try:
            registry.add_allowed_directory(str(extra))
            cls = registry.load_transformer(str(script))
            assert cls is not None
            assert cls.__name__ == "XTransformer"
        finally:
            registry.clear_allowed_directories()
