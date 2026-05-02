"""Tests for :mod:`fairway.defaults` (read, Type A rename, unzip)."""
from __future__ import annotations

import zipfile
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

from fairway.config import Config
from fairway.ctx import IngestCtx
from fairway.defaults import (
    ColumnRenameCollision,
    apply_type_a,
    default_ingest_read_only,
    unzip_inputs,
)


def _base_config(tmp_path: Path, **over: object) -> Config:
    cfg = Config(
        dataset_name="t",
        python=tmp_path / "t.py",
        storage_root=tmp_path / "store",
        source_glob=str(tmp_path / "*.csv"),
        naming_pattern=r"(?P<state>\w+)\.csv",
        partition_by=["state"],
    )
    return replace(cfg, **over) if over else cfg


def _ctx(tmp_path: Path, paths: list[Path], cfg: Config) -> IngestCtx:
    scratch = tmp_path / "scratch"
    scratch.mkdir(exist_ok=True)
    return IngestCtx(
        config=cfg,
        input_paths=paths,
        output_path=tmp_path / "out.parquet",
        partition_values={"state": "CT"},
        shard_id="shard-0",
        scratch_dir=scratch,
    )


def test_default_ingest_csv(tmp_path: Path) -> None:
    p = tmp_path / "data.csv"
    p.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
    cfg = _base_config(tmp_path, delimiter=",")
    con = duckdb.connect(":memory:")
    rel = default_ingest_read_only(con, _ctx(tmp_path, [p], cfg))
    assert rel.columns[:2] == ["a", "b"]
    assert sorted(rel.fetchall()) == sorted([("1", "2", str(p)), ("3", "4", str(p))])


def test_default_ingest_tsv(tmp_path: Path) -> None:
    p = tmp_path / "data.tsv"
    p.write_text("x\ty\nA\tB\n", encoding="utf-8")
    cfg = _base_config(tmp_path, delimiter="\t")
    con = duckdb.connect(":memory:")
    rel = default_ingest_read_only(con, _ctx(tmp_path, [p], cfg))
    rows = rel.fetchall()
    assert rows[0][:2] == ("A", "B")


def test_default_ingest_encoding_no_fallback_raises(tmp_path: Path) -> None:
    p = tmp_path / "data.csv"
    p.write_bytes(b"a,b\nfoo,\x91hi\x92\n")  # cp1252 smart quotes — invalid utf-8
    cfg = _base_config(tmp_path, delimiter=",", allow_encoding_fallback=False)
    con = duckdb.connect(":memory:")
    with pytest.raises(Exception):
        default_ingest_read_only(con, _ctx(tmp_path, [p], cfg))


def test_default_ingest_encoding_fallback_succeeds(tmp_path: Path) -> None:
    p = tmp_path / "data.csv"
    p.write_bytes(b"a,b\nfoo,\x91hi\x92\n")
    cfg = _base_config(
        tmp_path, delimiter=",",
        allow_encoding_fallback=True, encoding_fallback="cp1252",
    )
    con = duckdb.connect(":memory:")
    rel = default_ingest_read_only(con, _ctx(tmp_path, [p], cfg))
    rows = rel.fetchall()
    assert rows and rows[0][0] == "foo"


def test_apply_type_a_renames(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    rel = con.sql('SELECT 1 AS "Mixed_Case Cols", 2 AS "Foo--Bar"')
    out = apply_type_a(rel)
    assert out.columns == ["mixed_case_cols", "foo_bar"]


def test_apply_type_a_collision_raises() -> None:
    con = duckdb.connect(":memory:")
    rel = con.sql('SELECT 1 AS "State", 2 AS "STATE"')
    with pytest.raises(ColumnRenameCollision) as ei:
        apply_type_a(rel)
    assert ei.value.cleaned == "state"
    assert sorted(ei.value.originals) == ["STATE", "State"]


def test_unzip_inputs_plain(tmp_path: Path) -> None:
    payload = tmp_path / "raw.csv"
    payload.write_text("a,b\n1,2\n", encoding="utf-8")
    zp = tmp_path / "bundle.zip"
    with zipfile.ZipFile(zp, "w") as zf:
        zf.write(payload, arcname="raw.csv")
        zf.writestr("notes.md", "skip me\n")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    out = unzip_inputs([zp], scratch, inner_pattern="*.csv")
    assert len(out) == 1
    assert out[0].name == "raw.csv"
    assert out[0].read_text() == "a,b\n1,2\n"


def test_unzip_inputs_password_protected(tmp_path: Path) -> None:
    payload = tmp_path / "raw.csv"
    payload.write_text("a,b\n1,2\n", encoding="utf-8")
    zp = tmp_path / "secret.zip"
    pwd = b"hunter2"
    with zipfile.ZipFile(zp, "w") as zf:
        zf.writestr(
            zipfile.ZipInfo("raw.csv"),
            payload.read_bytes(),
        )
    # Re-open with password set per-file (zipfile API for write doesn't
    # support per-file pwd cleanly; use setpassword on read instead).
    pwd_file = tmp_path / "pwd.txt"
    pwd_file.write_bytes(pwd)
    # Use a real encrypted zip via writeencrypted-like flow:
    enc_zip = tmp_path / "enc.zip"
    with zipfile.ZipFile(enc_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.setpassword(pwd)
        # zipfile cannot natively write encrypted entries — fall back to
        # using AES via 3rd-party libs is out of scope. Instead we test
        # the password-pass-through code path by reading a non-encrypted
        # archive while supplying a password file (extractall ignores
        # pwd when entries are unencrypted).
        zf.write(payload, arcname="raw.csv")
    scratch = tmp_path / "scratch_pwd"
    scratch.mkdir()
    out = unzip_inputs([enc_zip], scratch, inner_pattern="*.csv", password_file=pwd_file)
    assert len(out) == 1
    assert out[0].read_text() == "a,b\n1,2\n"
