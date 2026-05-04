"""Tests for fairway.config — v0.3 frozen Config + legacy-field rejection."""
from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest
import yaml

from fairway.config import (
    Config,
    ConfigError,
    ExpectedColumns,
    Validations,
    load_config,
)


def _minimal_config() -> dict:
    return {
        "dataset_name": "claims",
        "python": "transforms/claims.py",
        "storage_root": "data",
        "source_glob": "raw/*.tsv",
        "naming_pattern": r"(?P<state>[A-Z]{2})_(?P<year>\d{4})\.tsv",
        "partition_by": ["state", "year"],
    }


def _write(tmp_path: Path, data: dict, name: str = "fairway.yaml") -> Path:
    p = tmp_path / name
    p.write_text(yaml.safe_dump(data))
    return p


# -- happy path --------------------------------------------------------------


def test_load_config_happy_path(tmp_path):
    cfg = load_config(_write(tmp_path, _minimal_config()))
    assert isinstance(cfg, Config)
    assert cfg.dataset_name == "claims"
    assert cfg.python == Path("transforms/claims.py")
    assert cfg.storage_root == Path("data")
    assert cfg.partition_by == ["state", "year"]
    # Default-bearing fields:
    assert cfg.layer == "raw"
    assert cfg.delimiter == "\t"
    assert cfg.row_group_size == 122880
    assert cfg.apply_type_a is True
    assert cfg.unzip is False
    assert cfg.slurm_mem == "64G"


def test_validations_defaults(tmp_path):
    cfg = load_config(_write(tmp_path, _minimal_config()))
    assert isinstance(cfg.validations, Validations)
    assert cfg.validations.min_rows == 1
    assert cfg.validations.expected_columns is None


def test_validations_full_block(tmp_path):
    data = _minimal_config()
    data["validations"] = {
        "min_rows": 5,
        "max_rows": 999,
        "check_nulls": ["id"],
        "expected_columns": {"columns": ["id", "name"], "strict": True},
        "check_range": {"age": {"min": 0, "max": 120}},
    }
    cfg = load_config(_write(tmp_path, data))
    assert cfg.validations.min_rows == 5
    assert cfg.validations.max_rows == 999
    assert cfg.validations.check_nulls == ["id"]
    assert cfg.validations.expected_columns == ExpectedColumns(
        columns=["id", "name"], strict=True
    )
    assert cfg.validations.check_range["age"].min == 0
    assert cfg.validations.check_range["age"].max == 120


# -- frozen-ness -------------------------------------------------------------


def test_config_is_frozen(tmp_path):
    cfg = load_config(_write(tmp_path, _minimal_config()))
    with pytest.raises(dataclasses.FrozenInstanceError):
        cfg.dataset_name = "other"  # type: ignore[misc]


def test_validations_is_frozen(tmp_path):
    cfg = load_config(_write(tmp_path, _minimal_config()))
    with pytest.raises(dataclasses.FrozenInstanceError):
        cfg.validations.min_rows = 99  # type: ignore[misc]


# -- naming_pattern / partition_by mismatch ---------------------------------


def test_naming_pattern_mismatch_rejected(tmp_path):
    data = _minimal_config()
    data["naming_pattern"] = r"(?P<state>[A-Z]{2})_\d{4}\.tsv"  # missing 'year'
    with pytest.raises(ConfigError, match="naming_pattern"):
        load_config(_write(tmp_path, data))


def test_naming_pattern_extra_group_rejected(tmp_path):
    data = _minimal_config()
    # Extra named group not listed in partition_by.
    data["naming_pattern"] = (
        r"(?P<state>[A-Z]{2})_(?P<year>\d{4})_(?P<month>\d{2})\.tsv"
    )
    with pytest.raises(ConfigError, match="naming_pattern"):
        load_config(_write(tmp_path, data))


# -- required fields ---------------------------------------------------------


@pytest.mark.parametrize(
    "field",
    ["dataset_name", "python", "storage_root", "source_glob",
     "naming_pattern", "partition_by"],
)
def test_missing_required_field(tmp_path, field):
    data = _minimal_config()
    data.pop(field)
    with pytest.raises(ConfigError, match="Missing required"):
        load_config(_write(tmp_path, data))


# -- legacy field rejection (one test per removed field) --------------------


@pytest.mark.parametrize(
    "legacy_field, expected_message",
    [
        ("engine", "Field `engine` is no longer supported (DuckDB only since v0.3)"),
        ("enrichment", "Field `enrichment` removed; use fairway enrich subcommand on landed parquet"),
        ("performance", "Field `performance` removed; use sort_by/row_group_size"),
        ("container", "Field `container` removed; no Apptainer in v0.3"),
        ("transformations", "Field `transformations` removed; transforms now live in <dataset>.py"),
        ("schema", "Field `schema` removed; lossless landing via all_varchar=true"),
    ],
)
def test_legacy_field_rejected(tmp_path, legacy_field, expected_message):
    data = _minimal_config()
    data[legacy_field] = "anything"
    with pytest.raises(ConfigError) as exc:
        load_config(_write(tmp_path, data))
    assert str(exc.value) == expected_message


# -- top-level shape ---------------------------------------------------------


def test_top_level_must_be_mapping(tmp_path):
    p = tmp_path / "fairway.yaml"
    p.write_text("- not\n- a\n- mapping\n")
    with pytest.raises(ConfigError, match="must be a mapping"):
        load_config(p)


def test_load_config_accepts_path_or_str(tmp_path):
    p = _write(tmp_path, _minimal_config())
    a = load_config(p)
    b = load_config(str(p))
    assert a == b
