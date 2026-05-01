"""Tests for the TableConfig dataclass."""
import os
import tempfile

import yaml


def test_table_config_provides_typed_access():
    from fairway.config import TableConfig
    t = TableConfig(
        name="sales",
        path="/data/sales/*.csv",
        format="csv",
        schema={"id": "INTEGER", "amount": "FLOAT"},
        partition_by=["year"],
    )
    assert t.name == "sales"
    assert t.format == "csv"
    assert t.partition_by == ["year"]
    assert t.schema["id"] == "INTEGER"
    # Defaults
    assert t.write_mode == "overwrite"
    assert t.preprocess is None


def test_table_config_does_not_leak_methods_through_getitem():
    """Method names like 'get', 'items', '__init__' must not be accessible as keys."""
    from fairway.config import TableConfig
    t = TableConfig(name="x")
    assert t.get('get') is None
    assert t.get('items') is None
    assert t.get('__init__') is None
    assert 'get' not in t
    assert 'items' not in t
    import pytest
    with pytest.raises(KeyError):
        _ = t['get']


def test_table_config_from_dict_ignores_caller_extra():
    """from_dict({'_extra': {...}}) must not overwrite the internal _extra dict."""
    from fairway.config import TableConfig
    t = TableConfig.from_dict({'_extra': {'injected': 'bad'}, 'runtime_x': 1})
    assert 'injected' not in t
    assert t['runtime_x'] == 1


def test_table_config_dict_compatibility():
    """dict(t) and **t unpacking work — requires keys() + __getitem__."""
    from fairway.config import TableConfig
    t = TableConfig(name="x", path="/p")
    t['_runtime'] = 'value'
    d = dict(t)
    assert d['name'] == "x"
    assert d['path'] == "/p"
    assert d['_runtime'] == 'value'


def test_table_config_supports_dict_access():
    """Backward-compat: pipeline code uses table['key'] and table.get('key')."""
    from fairway.config import TableConfig
    t = TableConfig(name="x", path="/data/*.csv")
    assert t['name'] == "x"
    assert t.get('name') == "x"
    assert t.get('missing_key') is None
    assert t.get('missing_key', 'default') == 'default'
    assert 'name' in t
    assert 'missing_key' not in t
    # Setting a runtime field not in the dataclass stores in _extra
    t['_extracted_files'] = ['a', 'b']
    assert t['_extracted_files'] == ['a', 'b']
    assert '_extracted_files' in t
    # Setting a known field updates the attribute
    t['format'] = 'json'
    assert t.format == 'json'


def test_config_tables_returns_table_configs(tmp_path):
    from fairway.config import Config, TableConfig
    # Create a real CSV file so glob validation passes
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "t1.csv").write_text("id\n1\n")

    config_path = tmp_path / "fw.yaml"
    config_path.write_text(yaml.dump({
        "dataset_name": "test",
        "engine": "duckdb",
        "storage": {"root": str(tmp_path / "out")},
        "tables": [{
            "name": "t1",
            "path": str(data_dir / "*.csv"),
            "format": "csv",
        }],
    }))

    config = Config(str(config_path))
    assert len(config.tables) == 1
    assert isinstance(config.tables[0], TableConfig)
    assert config.tables[0].name == "t1"
    assert config.tables[0]['name'] == "t1"
