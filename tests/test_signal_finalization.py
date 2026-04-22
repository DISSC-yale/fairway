"""Signal-safe crash finalization — plan phase 3.

Pins that SIGTERM and SIGINT both flush run.json to `unfinished` instead
of leaving it pinned at `running`. The unit tests exercise the module-
level handlers directly; the subprocess tests confirm the full signal
path works end-to-end on POSIX.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import textwrap
import time

import pytest
import yaml


pytest.importorskip("duckdb")


SUBPROCESS_TIMEOUT = 15  # Per-signal subprocess wait ceiling (seconds).


def _write_config(tmp_path):
    raw_dir = tmp_path / "data" / "raw"
    raw_dir.mkdir(parents=True)
    (tmp_path / "data" / "processed").mkdir(parents=True)
    (tmp_path / "data" / "curated").mkdir(parents=True)
    (raw_dir / "people.csv").write_text("id,name\n1,a\n2,b\n")
    config_path = tmp_path / "fairway.yaml"
    config_path.write_text(yaml.safe_dump({
        "project": "signal_test",
        "dataset_name": "signal_test",
        "engine": "duckdb",
        "storage": {"root": str(tmp_path / "data")},
        "tables": [{
            "name": "people",
            "path": str(raw_dir / "people.csv"),
            "format": "csv",
            "schema": {"id": "BIGINT", "name": "VARCHAR"},
        }],
    }))
    return config_path


def test_finalize_all_unfinished_flips_running_to_unfinished(tmp_path, monkeypatch):
    config_path = _write_config(tmp_path)
    monkeypatch.chdir(tmp_path)
    from fairway.pipeline import IngestionPipeline, _finalize_all_unfinished

    pipeline = IngestionPipeline(str(config_path))
    meta_path = pipeline.config.paths.run_metadata_file_for(pipeline.run_id)
    assert json.loads(meta_path.read_text())["exit_status"] == "running"

    _finalize_all_unfinished()

    assert json.loads(meta_path.read_text())["exit_status"] == "unfinished"


def test_install_crash_finalizers_registers_sigint_and_sigterm(monkeypatch):
    import fairway.pipeline as _p
    monkeypatch.setattr(_p, "_SIGTERM_HANDLER_INSTALLED", False)
    prev_term = signal.getsignal(signal.SIGTERM)
    prev_int = signal.getsignal(signal.SIGINT)
    try:
        _p._install_crash_finalizers()
        assert signal.getsignal(signal.SIGTERM) is _p._sigterm_handler
        assert signal.getsignal(signal.SIGINT) is _p._sigint_handler
    finally:
        signal.signal(signal.SIGTERM, prev_term)
        signal.signal(signal.SIGINT, prev_int)


def _driver_script(tmp_path, config_path, ready_path) -> str:
    return textwrap.dedent(f"""
        import os, sys, time
        os.chdir({str(tmp_path)!r})
        os.environ["FAIRWAY_HOME"] = {str(tmp_path / "home")!r}
        os.environ["FAIRWAY_SCRATCH"] = {str(tmp_path / "scratch")!r}
        from fairway.pipeline import IngestionPipeline
        pipeline = IngestionPipeline({str(config_path)!r})
        meta = pipeline.config.paths.run_metadata_file_for(pipeline.run_id)
        sys.stdout.write(str(meta) + "\\n")
        sys.stdout.flush()
        # Write the ready sentinel AFTER handlers are installed (the
        # IngestionPipeline constructor calls _install_crash_finalizers)
        # so the parent only signals once the signal path is live.
        with open({str(ready_path)!r}, "w") as f:
            f.write("ready")
        # Spin until the parent signals.
        time.sleep(120)
    """)


def _run_and_signal(tmp_path, config_path, sig):
    """Spawn the driver, wait for ready sentinel, deliver signal, wait."""
    ready_path = tmp_path / "ready.flag"
    driver = tmp_path / "driver.py"
    driver.write_text(_driver_script(tmp_path, config_path, ready_path))

    proc = subprocess.Popen(
        [sys.executable, str(driver)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    meta_line = proc.stdout.readline().strip()
    # Wait up to 10s for the ready sentinel — handler install must
    # complete before we deliver the signal.
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        if ready_path.exists():
            break
        if proc.poll() is not None:
            stderr = proc.stderr.read()
            raise AssertionError(f"driver exited before ready: stderr={stderr!r}")
        time.sleep(0.05)
    else:
        proc.kill()
        raise AssertionError("driver never wrote ready sentinel")

    proc.send_signal(sig)
    try:
        exit_code = proc.wait(timeout=SUBPROCESS_TIMEOUT)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
    return meta_line, exit_code


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signal semantics")
def test_sigterm_subprocess_finalizes_run_json(tmp_path):
    config_path = _write_config(tmp_path)
    meta_line, exit_code = _run_and_signal(tmp_path, config_path, signal.SIGTERM)

    assert os.path.exists(meta_line), f"run.json missing at {meta_line}"
    data = json.loads(open(meta_line).read())
    assert data["exit_status"] == "unfinished", data
    assert "finished_at" in data
    # SIGTERM re-raises via SIG_DFL → conventional exit code 143.
    assert exit_code == -signal.SIGTERM or exit_code == 128 + signal.SIGTERM, exit_code


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signal semantics")
def test_sigint_subprocess_finalizes_run_json(tmp_path):
    config_path = _write_config(tmp_path)
    meta_line, exit_code = _run_and_signal(tmp_path, config_path, signal.SIGINT)

    assert os.path.exists(meta_line), f"run.json missing at {meta_line}"
    data = json.loads(open(meta_line).read())
    assert data["exit_status"] == "unfinished", data
    assert "finished_at" in data
    # Unhandled KeyboardInterrupt → Python re-raises SIGINT to self for
    # signal death. subprocess.Popen.returncode reports signal death as
    # -SIGINT (=-2) on macOS/Linux, or 130 if the shell wraps it.
    assert exit_code in (-signal.SIGINT, 130), exit_code
