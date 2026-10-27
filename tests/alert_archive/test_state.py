from datetime import UTC, datetime

import pytest
from alert_archive.state import LockHeldError, PipelineState, RunRecord, new_run_id


def make_run(run_id, chunks=(), skipped=None, **kwargs):
    """A RunRecord whose start time is its run ID, for readable failing-since checks."""
    return RunRecord(run_id=run_id, started=run_id, chunks=list(chunks), skipped=skipped or {}, **kwargs)


def test_new_run_id_is_utc_timestamp():
    """Run IDs are the start time in UTC, to the second."""
    now = datetime(2026, 10, 27, 3, 15, 0, tzinfo=UTC)
    assert new_run_id(now) == "20261027T031500Z"


def test_run_record_round_trips_unknown_schema_id():
    """None (unknown format) keys survive JSON, which only allows string keys."""
    record = make_run("r1", chunks=["a.parquet"], num_rows=5, schema_id_counts={1100: 4, None: 1})
    assert RunRecord.from_json(record.to_json()) == record


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


def test_remove_runs_frees_their_chunks(tmp_path):
    """Removing a run (to re-ingest it) frees its chunks and keeps the other runs."""
    state = PipelineState(tmp_path)
    for run in [make_run("r1", ["a"]), make_run("r2", ["b"]), make_run("r3", ["c"])]:
        state.append_run(run)
    removed = state.remove_runs(["r2"])
    assert [r.run_id for r in removed] == ["r2"]
    assert [r.run_id for r in state.runs()] == ["r1", "r3"]
    assert state.ingested_chunks() == {"a", "c"}


def test_remove_unknown_run_changes_nothing(tmp_path):
    """A typo in a run ID doesn't remove the other runs listed with it."""
    state = PipelineState(tmp_path)
    state.append_run(make_run("r1", ["a"]))
    with pytest.raises(KeyError, match="nope"):
        state.remove_runs(["r1", "nope"])
    assert [r.run_id for r in state.runs()] == ["r1"]


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
