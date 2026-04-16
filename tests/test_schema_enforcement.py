"""
Tests for schema type enforcement and null-fill for missing columns.
Both engines must produce the same schema in output parquet.
"""
import pytest
from tests.helpers import build_config, read_curated


def engine_name(engine):
    return "pyspark" if hasattr(engine, "spark") else "duckdb"


class TestSchemaEnforcement:

    def test_string_id_cast_to_integer(self, engine, fixtures_dir, tmp_path):
        """
        bad_schema.csv has id as '001','002','003' (zero-padded strings).
        Schema declares id: INTEGER. Output must have integer id values.
        """
        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "typed",
            "path": str(fixtures_dir / "formats" / "csv" / "bad_schema.csv"),
            "format": "csv",
            "schema": {"id": "INTEGER", "name": "VARCHAR", "value": "STRING"},
            "type_enforcement": {"enabled": True},
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "typed").sort_values("id").reset_index(drop=True)
        assert df["id"].dtype in ("int32", "int64", int), (
            f"Expected INTEGER dtype, got {df['id'].dtype}"
        )
        assert int(df.iloc[0]["id"]) == 1  # '001' → 1

    @pytest.mark.skip(reason="missing column null-fill not implemented")
    def test_missing_schema_column_filled_with_null(self, engine, fixtures_dir, tmp_path):
        """
        simple.csv has columns id, name, value.
        Schema adds missing_col: DOUBLE not present in file.
        Output must contain missing_col as all-null.
        """
        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "padded",
            "path": str(fixtures_dir / "formats" / "csv" / "simple.csv"),
            "format": "csv",
            "schema": {
                "id": "INTEGER",
                "name": "VARCHAR",
                "value": "INTEGER",
                "missing_col": "DOUBLE",
            },
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "padded")
        assert "missing_col" in df.columns, "Schema column not present in output"
        assert df["missing_col"].isna().all(), (
            "Schema column absent from source should be all-null in output"
        )


class TestVarcharNormalization:
    """VARCHAR must be normalized to STRING for PySpark 4.x compatibility."""

    def test_varchar_normalized_to_string_in_cast(self, pyspark_engine, tmp_path):
        """PySpark 4.x rejects bare VARCHAR — engine must normalize to STRING."""
        data = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        df = pyspark_engine.spark.createDataFrame(data)
        input_path = str(tmp_path / "input")
        df.write.mode("overwrite").parquet(input_path)

        output_path = str(tmp_path / "output")
        pyspark_engine.ingest(
            input_path, output_path, format="parquet",
            schema={"id": "INTEGER", "name": "VARCHAR"},
        )

        result = pyspark_engine.spark.read.parquet(output_path)
        assert result.count() == 2
        assert "name" in result.columns
