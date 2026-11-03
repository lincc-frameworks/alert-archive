import json
import os
import subprocess
from datetime import UTC, datetime

import numpy as np
import pytest
from alert_archive import state as state_module
from alert_archive.state import LockHeldError, PipelineState, RunRecord, new_run_id, slurm_job_running


def make_run(run_id, chunks=(), skipped=None, **kwargs):
    """A RunRecord whose start time is its run ID, for readable failing-since checks."""
    return RunRecord(run_id=run_id, started=run_id, chunks=list(chunks), skipped=skipped or {}, **kwargs)


def make_written_run(run_id, chunks=()):
    """A run that wrote files, named ``<run_id>.parquet``."""
    return make_run(run_id, chunks, num_rows=1)


def test_new_run_id_is_utc_timestamp():
    """Run IDs are the start time in UTC, to the second."""
    now = datetime(2026, 10, 27, 3, 15, 0, tzinfo=UTC)
    assert new_run_id(now) == "20261027T031500Z"


def test_file_name_derived_from_run_id():
    """A run's files are named by its run ID; a run that wrote nothing has no file name."""
    assert make_run("20261027T031500Z", num_rows=5).file_name == "20261027T031500Z.parquet"
    assert make_run("20261027T031500Z").file_name is None
    assert "file_name" not in make_run("r1", num_rows=5).to_json()


def test_run_record_round_trips_unknown_schema_id():
    """None (unknown format) keys survive JSON, which only allows string keys."""
    record = make_run("r1", chunks=["a.parquet"], num_rows=5, schema_id_counts={1100: 4, None: 1})
    assert RunRecord.from_json(record.to_json()) == record


def test_run_record_accepts_numpy_counts():
    """Counts from numpy (e.g. pandas value_counts) become plain ints, so the record can be written."""
    record = make_run(
        "r1", num_rows=np.int64(5), schema_id_counts={np.int64(1100): np.int64(4), None: np.int64(1)}
    )
    assert RunRecord.from_json(record.to_json()) == make_run(
        "r1", num_rows=5, schema_id_counts={1100: 4, None: 1}
    )


def test_run_record_rejects_float_counts():
    """A float count is a bug upstream, so it fails rather than being truncated."""
    with pytest.raises(TypeError):
        make_run("r1", num_rows=4.5)


def test_empty_state(tmp_path):
    """A new state directory has no runs, no ingested chunks and no flag."""
    state = PipelineState(tmp_path / "state")
    assert state.runs() == []
    assert state.ingested_chunks() == set()
    assert state.format_change() is None


def test_append_and_read_runs(tmp_path):
    """Runs come back in the order they were logged, and their chunks count as ingested."""
    state = PipelineState(tmp_path)
    state.append_run(make_run("r1", chunks=["a", "b"]))
    state.append_run(make_run("r2", chunks=["c"]))
    assert [r.run_id for r in state.runs()] == ["r1", "r2"]
    assert state.ingested_chunks() == {"a", "b", "c"}


def test_append_rejects_duplicate_run_id(tmp_path):
    """A repeated run ID would make two runs share catalog file names."""
    state = PipelineState(tmp_path)
    state.append_run(make_run("r1"))
    with pytest.raises(ValueError, match="r1"):
        state.append_run(make_run("r1"))


def test_skipped_chunks_are_not_ingested(tmp_path):
    """A skipped chunk stays unlisted, so the next run finds it again."""
    state = PipelineState(tmp_path)
    state.append_run(make_run("r1", chunks=["a"], skipped={"b": "corrupt footer"}))
    assert state.ingested_chunks() == {"a"}


def write_catalog_files(catalog_dir, file_name, pixels):
    """Create empty files where a run would have written to each pixel, and return their paths."""
    paths = []
    for order, pixel in pixels:
        path = catalog_dir / "dataset" / f"Norder={order}" / "Dir=0" / f"Npix={pixel}" / file_name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
        paths.append(path)
    return paths


def test_append_run_leaves_no_temp_file(tmp_path):
    """The log is replaced atomically, so only the log itself is left in the state directory."""
    state = PipelineState(tmp_path)
    state.append_run(make_run("r1"))
    state.append_run(make_run("r2"))
    assert [p.name for p in tmp_path.iterdir()] == ["ingested.jsonl"]


def test_reingest_runs_deletes_files_and_log_lines(tmp_path):
    """Only the chosen runs' files and log lines go; other runs and runs that wrote nothing are fine."""
    state = PipelineState(tmp_path / "state")
    catalog = tmp_path / "catalog"
    r1_files = write_catalog_files(catalog, "r1.parquet", [(1, 10), (2, 50)])
    r2_files = write_catalog_files(catalog, "r2.parquet", [(1, 10)])
    state.append_run(make_written_run("r1", ["a"]))
    state.append_run(make_written_run("r2", ["b"]))
    state.append_run(make_run("r3", skipped={"c": "corrupt"}))

    removed = state.reingest_runs(catalog, ["r1", "r3"])

    assert [r.run_id for r in removed] == ["r1", "r3"]
    assert not any(p.exists() for p in r1_files)
    assert all(p.exists() for p in r2_files)
    assert state.ingested_chunks() == {"b"}


def test_reingest_refuses_merged_runs(tmp_path):
    """A run whose files are gone (e.g. merged by the weekly re-import) stops everything,
    so other runs listed with it keep their files and log lines too."""
    state = PipelineState(tmp_path / "state")
    catalog = tmp_path / "catalog"
    r1_files = write_catalog_files(catalog, "r1.parquet", [(1, 10)])
    state.append_run(make_written_run("r1", ["a"]))
    state.append_run(make_written_run("r2", ["b"]))

    with pytest.raises(FileNotFoundError, match="r2"):
        state.reingest_runs(catalog, ["r1", "r2"])

    assert all(p.exists() for p in r1_files)
    assert state.ingested_chunks() == {"a", "b"}


def test_reingest_missing_files_ok(tmp_path):
    """With missing_files_ok, a run whose files are already gone is taken off the log,
    e.g. to finish a call that stopped after deleting them."""
    state = PipelineState(tmp_path / "state")
    state.append_run(make_written_run("r1", ["a"]))
    removed = state.reingest_runs(tmp_path / "catalog", ["r1"], missing_files_ok=True)
    assert [r.run_id for r in removed] == ["r1"]
    assert state.runs() == []


def test_reingest_refuses_consolidated_runs(tmp_path):
    """A consolidated run (the baseline) is refused even with missing_files_ok, since its
    alerts would stay in the catalog, so other runs listed with it are untouched too."""
    state = PipelineState(tmp_path / "state")
    catalog = tmp_path / "catalog"
    consolidated = write_catalog_files(catalog, "Npix=10.parquet", [(1, 10)])
    r2_files = write_catalog_files(catalog, "r2.parquet", [(1, 10)])
    state.append_run(make_run("r1", ["a"], num_rows=1, consolidated=True))
    state.append_run(make_written_run("r2", ["b"]))

    with pytest.raises(ValueError, match="r1"):
        state.reingest_runs(catalog, ["r1", "r2"], missing_files_ok=True)

    assert all(p.exists() for p in consolidated + r2_files)
    assert state.ingested_chunks() == {"a", "b"}


def test_run_record_reads_lines_without_consolidated():
    """Log lines written before the consolidated field existed still load, as not consolidated."""
    data = json.loads(make_run("r1").to_json())
    del data["consolidated"]
    assert RunRecord.from_json(json.dumps(data)).consolidated is False


def test_reingest_unknown_run_changes_nothing(tmp_path):
    """A typo in a run ID doesn't delete or remove the other runs listed with it."""
    state = PipelineState(tmp_path / "state")
    catalog = tmp_path / "catalog"
    r1_files = write_catalog_files(catalog, "r1.parquet", [(1, 10)])
    state.append_run(make_written_run("r1", ["a"]))
    with pytest.raises(KeyError, match="nope"):
        state.reingest_runs(catalog, ["r1", "nope"])
    assert all(p.exists() for p in r1_files)
    assert [r.run_id for r in state.runs()] == ["r1"]


def test_reingest_refuses_while_locked(tmp_path):
    """Re-ingesting while a daily or weekly job holds the lock fails and changes nothing."""
    state = PipelineState(tmp_path / "state")
    catalog = tmp_path / "catalog"
    r1_files = write_catalog_files(catalog, "r1.parquet", [(1, 10)])
    state.append_run(make_written_run("r1", ["a"]))
    with state.lock("daily"), pytest.raises(LockHeldError):
        state.reingest_runs(catalog, ["r1"])
    assert all(p.exists() for p in r1_files)
    assert [r.run_id for r in state.runs()] == ["r1"]
    assert not state.lock_path.exists()


def test_failing_chunks_tracks_first_failure(tmp_path):
    """A chunk that keeps failing keeps its first failure time and its latest error.
    A chunk that is later ingested stops counting as failing."""
    state = PipelineState(tmp_path)
    state.append_run(make_run("r1", skipped={"a": "err1", "b": "err1"}))
    state.append_run(make_run("r2", chunks=["b"], skipped={"a": "err2"}))
    state.append_run(make_run("r3", skipped={"a": "err3", "c": "err3"}))
    assert state.failing_chunks() == {"a": ("r1", "err3"), "c": ("r3", "err3")}


def test_format_change_flag(tmp_path):
    """The flag keeps its first time and every run that hit a format change, until cleared."""
    state = PipelineState(tmp_path)
    t1 = datetime(2026, 10, 27, tzinfo=UTC)
    t2 = datetime(2026, 10, 28, tzinfo=UTC)
    state.flag_format_change("r1", {"unknown_schema_ids": [1200]}, now=t1)
    state.flag_format_change("r2", {"dropped": ["diaSource.x"]}, now=t2)

    flag = state.format_change()
    assert flag["flagged_at"] == t1.isoformat()
    assert [r["run_id"] for r in flag["runs"]] == ["r1", "r2"]
    assert flag["runs"][1]["details"] == {"dropped": ["diaSource.x"]}

    state.clear_format_change()
    assert state.format_change() is None
    state.clear_format_change()  # clearing twice is fine


def test_lock_excludes_second_job(tmp_path):
    """A second job can't take the lock while the first holds it, and it's removed afterwards."""
    state = PipelineState(tmp_path)
    with state.lock("daily"):
        with pytest.raises(LockHeldError, match="daily"):
            with state.lock("weekly"):
                pass
    assert not state.lock_path.exists()


def test_lock_released_on_error(tmp_path):
    """A job that raises still releases the lock."""
    state = PipelineState(tmp_path)
    with pytest.raises(RuntimeError, match="boom"):
        with state.lock("daily"):
            raise RuntimeError("boom")
    with state.lock("daily"):
        pass


def test_lock_failed_write_leaves_no_lock(tmp_path, monkeypatch):
    """If writing the holder fails (e.g. disk full), no lock is left behind to block the next job."""
    state = PipelineState(tmp_path)

    def fail_dump(obj, f):
        raise OSError("No space left on device")

    monkeypatch.setattr("alert_archive.state.json.dump", fail_dump)
    with pytest.raises(OSError, match="No space"):
        with state.lock("daily"):
            pass
    assert not state.lock_path.exists()


def write_lock(state, slurm_job_id):
    """Leave a lock behind, as a job killed while holding it would."""
    holder = {"job": "daily", "host": "h", "pid": 1, "since": "t0", "slurm_job_id": slurm_job_id}
    state.lock_path.write_text(json.dumps(holder))
    return holder


@pytest.fixture
def squeue(monkeypatch):
    """Fake whether Slurm jobs are running: set ``squeue.running[job_id]`` to True, False or None."""

    def fake(job_id):
        return fake.running.get(job_id)

    fake.running = {}
    monkeypatch.setattr(state_module, "slurm_job_running", fake)
    return fake


def test_lock_takes_over_lock_of_ended_job(tmp_path, squeue, monkeypatch):
    """A lock whose Slurm job has ended is taken over, and the old holder is recorded."""
    monkeypatch.setenv("SLURM_JOB_ID", "200")
    state = PipelineState(tmp_path)
    holder = write_lock(state, "100")
    squeue.running["100"] = False
    with state.lock("daily"):
        assert json.loads(state.lock_path.read_text())["slurm_job_id"] == "200"
    assert state.stale_lock == holder
    assert not state.lock_path.exists()
    assert list(tmp_path.iterdir()) == []  # nothing left aside


@pytest.mark.parametrize("running", [True, None])
def test_lock_keeps_lock_of_running_or_unknown_job(tmp_path, squeue, running):
    """A lock whose job is running, or can't be checked, still blocks the next job."""
    state = PipelineState(tmp_path)
    write_lock(state, "100")
    squeue.running["100"] = running
    with pytest.raises(LockHeldError, match="100"):
        with state.lock("daily"):
            pass
    assert state.lock_path.exists()
    assert state.stale_lock is None


def test_lock_keeps_lock_without_slurm_job(tmp_path, squeue):
    """A lock from outside Slurm is never taken over, since its job can't be checked."""
    state = PipelineState(tmp_path)
    write_lock(state, None)
    with pytest.raises(LockHeldError):
        with state.lock("daily"):
            pass
    assert state.lock_path.exists()


def test_lock_keeps_newer_lock_after_race(tmp_path, squeue, monkeypatch):
    """If another job took over the stale lock first, its new lock is put back, not removed."""
    state = PipelineState(tmp_path)
    write_lock(state, "100")
    squeue.running["100"] = False
    real_rename = os.rename

    def rename_after_other_job(src, dst):
        write_lock(state, "300")  # the other job took over between our read and our rename
        real_rename(src, dst)

    monkeypatch.setattr(state_module.os, "rename", rename_after_other_job)
    with pytest.raises(LockHeldError):
        with state.lock("daily"):
            pass
    assert json.loads(state.lock_path.read_text())["slurm_job_id"] == "300"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["lock"]


@pytest.mark.parametrize(
    ("returncode", "stdout", "stderr", "expected"),
    [
        (0, "RUNNING\n", "", True),
        (0, "PENDING\n", "", True),
        (0, "PREEMPTED\n", "", False),
        (0, "", "", False),
        (1, "", "slurm_load_jobs error: Invalid job id specified", False),
        (1, "", "slurm_load_jobs error: Unable to contact slurm controller", None),
    ],
)
def test_slurm_job_running(monkeypatch, returncode, stdout, stderr, expected):
    """squeue's answer is read as running, ended (incl. forgotten), or unknown."""
    result = subprocess.CompletedProcess([], returncode, stdout, stderr)
    monkeypatch.setattr(state_module.subprocess, "run", lambda *a, **k: result)
    assert slurm_job_running("100") is expected


def test_slurm_job_running_without_squeue(monkeypatch):
    """No squeue (not on a Slurm host) means the job can't be checked."""

    def missing(*args, **kwargs):
        raise FileNotFoundError("squeue")

    monkeypatch.setattr(state_module.subprocess, "run", missing)
    assert slurm_job_running("100") is None
