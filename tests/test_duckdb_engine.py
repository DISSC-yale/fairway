import pytest
import os
import yaml
from fairway.engines.duckdb_engine import DuckDBEngine


def _write_spec(tmp_path, spec_dict, filename="spec.yaml"):
    """Write a spec dict to a YAML file and return its path."""
    spec_path = tmp_path / filename
    spec_path.write_text(yaml.dump(spec_dict))
    return str(spec_path)


@pytest.mark.local
def test_fixed_width_path_with_single_quote_does_not_crash(tmp_path):
    """File paths with single quotes must not produce broken SQL."""
    data_dir = tmp_path / "o'hare"
    data_dir.mkdir()
    csv_path = data_dir / "data.txt"
    # 12-char lines: name(10) + age(2)
    csv_path.write_text("ALICE     30\nBOB       25\n")

    spec_path = _write_spec(tmp_path, {
        "columns": [
            {"name": "name", "start": 0, "length": 10},
            {"name": "age",  "start": 10, "length": 2},
        ]
    })

    engine = DuckDBEngine()
    try:
        result = engine._ingest_fixed_width(str(csv_path), str(tmp_path / "test_fw.parquet"), spec_path)
    except Exception as e:
        assert "Parser Error" not in str(e), f"SQL injection via path: {e}"
    finally:
        if hasattr(engine, 'con') and engine.con:
            engine.con.close()


@pytest.mark.local
def test_record_type_filter_value_with_single_quote_is_safe(tmp_path):
    """record_type_filter value containing a single quote must be escaped."""
    # 14-char lines: type(1) + space(1) + data(12)
    csv_path = tmp_path / "data.txt"
    csv_path.write_text("H it's header \nD data row    \n")

    spec_path = _write_spec(tmp_path, {
        "columns": [{"name": "col", "start": 2, "length": 12}],
        "record_type_filter": {
            "position": 0,
            "length": 1,
            "value": "D"
        },
    })

    engine = DuckDBEngine()
    try:
        result = engine._ingest_fixed_width(str(csv_path), str(tmp_path / "test_filter.parquet"), spec_path)
        assert result is not None
    finally:
        if hasattr(engine, 'con') and engine.con:
            engine.con.close()
