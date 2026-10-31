import hats
import lsdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from alert_archive.daily import run as daily
from alert_archive.daily.increment import leaf_files
from alert_archive.daily.run import DailyPipeline, check_formats, find_new_chunks, main
from alert_archive.state import PipelineState, RunRecord
from conftest import BASELINE_CHUNK_NAMES, shift_ids
from dask.distributed import Client

NEW_CHUNK = "313629__00000000-00000004.parquet"
"""A landing chunk named like a converter's output, after the baseline."""


@pytest.fixture
def emails(monkeypatch):
    """Capture the emails the daily run sends, as (subject, body) pairs."""
    sent = []
    monkeypatch.setattr(daily, "send_email", lambda subject, body: sent.append((subject, body)))
    return sent


@pytest.fixture
def landing(v2_config):
    """The landing dir, created."""
    path = v2_config.paths.landing_dir
    path.mkdir(parents=True)
    return path


@pytest.fixture
def new_alerts(raw_alerts):
    """Four of the sample alerts with new IDs, so they don't duplicate the baseline's."""
    return shift_ids(raw_alerts.slice(0, 4), 1)


def run_daily(config, tmp_path, email=True):
    """Run the daily pipeline with a small in-process Dask client."""
    with Client(n_workers=1, threads_per_worker=1, processes=False) as client:
        return DailyPipeline(config).run(client, tmp_path, email=email)


def test_daily_ingests_new_chunks(v2_config, landing, new_alerts, tmp_path, emails):
    """New alerts land as <run_id>.parquet files next to the baseline's, and the metadata counts them."""
    pq.write_table(new_alerts, landing / NEW_CHUNK)
    record = run_daily(v2_config, tmp_path)
    working = v2_config.paths.working_dir

    assert record.chunks == [NEW_CHUNK]
    assert record.num_rows == 4
    assert record.schema_id_counts == {1100: 4}
    run_files = [p for files in leaf_files(working).values() for p in files if p.name == record.file_name]
    assert sum(pq.read_metadata(p).num_rows for p in run_files) == 4

    assert hats.read_hats(working).catalog_info.total_rows == 16
    df = lsdb.open_catalog(working).compute()
    assert len(df) == 16
    assert df["diaSourceId"].is_unique

    state = PipelineState(v2_config.paths.state_dir)
    assert state.runs()[-1] == record
    assert NEW_CHUNK in state.ingested_chunks()
    assert not state.lock_path.exists()
    ((subject, body),) = emails
    assert record.run_id in subject and "Ingested 4 alerts" in body


def test_daily_nothing_new_is_logged(v2_config, landing, tmp_path, emails):
    """A run with no new chunks writes nothing but is still logged and emailed."""
    before = hats.read_hats(v2_config.paths.working_dir).catalog_info.total_rows
    record = run_daily(v2_config, tmp_path)
    assert record.chunks == [] and record.num_rows == 0 and record.file_name is None
    assert PipelineState(v2_config.paths.state_dir).runs()[-1] == record
    assert hats.read_hats(v2_config.paths.working_dir).catalog_info.total_rows == before
    assert len(emails) == 1


def test_daily_skips_already_ingested(v2_config, landing, raw_alerts, tmp_path):
    """Chunks the baseline ingested are never ingested again, even if they show up in the landing dir."""
    pq.write_table(raw_alerts, landing / BASELINE_CHUNK_NAMES[0])
    record = run_daily(v2_config, tmp_path, email=False)
    assert record.chunks == []


def test_daily_skips_bad_chunks(v2_config, landing, new_alerts, tmp_path, emails):
    """Badly named or unreadable chunks are skipped and reported; the rest are ingested."""
    pq.write_table(new_alerts, landing / NEW_CHUNK)
    pq.write_table(new_alerts, landing / "17005__00000000-00000004.parquet")
    (landing / "313630__00000000-00000001.parquet").write_text("not parquet")
    (landing / f".{NEW_CHUNK}.tmp").write_text("a copy in progress")

    record = run_daily(v2_config, tmp_path)
    assert record.chunks == [NEW_CHUNK]
    assert set(record.skipped) == {"17005__00000000-00000004.parquet", "313630__00000000-00000001.parquet"}
    assert "unexpected name" in record.skipped["17005__00000000-00000004.parquet"]
    assert "SKIPPED CHUNKS" in emails[0][1]


def test_daily_stops_on_format_change(v2_config, landing, new_alerts, tmp_path, emails):
    """A chunk in an unsupported format stops the run before anything is written, and sets the flag."""
    pq.write_table(new_alerts, landing / NEW_CHUNK)
    changed = new_alerts.append_column("newField", pa.array([1.0] * len(new_alerts)))
    pq.write_table(changed, landing / "313630__00000000-00000004.parquet")
    working = v2_config.paths.working_dir
    files_before = leaf_files(working)

    assert run_daily(v2_config, tmp_path) is None
    state = PipelineState(v2_config.paths.state_dir)
    flag = state.format_change()
    # The extra column also means the columns match no known format.
    expected = {"dropped": ["newField"], "unknown_schema_ids": [None]}
    assert flag["runs"][0]["details"] == {"313630__00000000-00000004.parquet": expected}
    assert len(state.runs()) == 1
    assert leaf_files(working) == files_before
    assert "FORMAT CHANGE" in emails[0][1]

    # While flagged, later runs do nothing but email.
    assert run_daily(v2_config, tmp_path) is None
    assert "flag set since" in emails[1][0]
    assert len(state.runs()) == 1

    # Once a person clears the flag (and removes or fixes the chunk), ingesting resumes.
    state.clear_format_change()
    (landing / "313630__00000000-00000004.parquet").unlink()
    assert run_daily(v2_config, tmp_path).chunks == [NEW_CHUNK]


def test_daily_removes_orphans(v2_config, landing, tmp_path):
    """Files left by a crashed run are deleted before ingesting."""
    working = v2_config.paths.working_dir
    pixel_dir = next(iter(leaf_files(working).values()))[0].parent
    orphan = pixel_dir / "29991231T000000Z.parquet"
    orphan.write_bytes(b"left by a crashed run")
    run_daily(v2_config, tmp_path, email=False)
    assert not orphan.exists()


def test_daily_needs_baseline(v2_config, tmp_path):
    """Without a baseline in the log, the daily run refuses to start."""
    PipelineState(v2_config.paths.state_dir).log_path.unlink()
    with pytest.raises(FileNotFoundError, match="build the baseline first"):
        run_daily(v2_config, tmp_path, email=False)


def test_find_new_chunks(tmp_path):
    """Only new, well-named, visible parquet files are chunks; other parquet files are reported."""
    for name in [
        NEW_CHUNK,
        BASELINE_CHUNK_NAMES[0],
        "17005__00000000-00000001.parquet",
        ".x.parquet",
        "notes.txt",
    ]:
        (tmp_path / name).touch()
    chunks, skipped = find_new_chunks(tmp_path, ingested={BASELINE_CHUNK_NAMES[0]})
    assert [c.name for c in chunks] == [NEW_CHUNK]
    assert list(skipped) == ["17005__00000000-00000001.parquet"]


def test_check_formats_uses_schema_id_column(tmp_path, new_alerts):
    """A real schemaId column is counted rather than inferred."""
    path = tmp_path / NEW_CHUNK
    pq.write_table(new_alerts.append_column("schemaId", pa.array([1101, 1101, 1100, 1100], pa.int32())), path)
    reports, unreadable = check_formats([path])
    assert unreadable == {}
    assert reports[NEW_CHUNK].schema_ids == {1101: 2, 1100: 2}


def test_main_runs_daily(monkeypatch):
    """``main`` runs the daily pipeline with the default config."""
    seen = []
    monkeypatch.setattr(
        DailyPipeline, "execute", lambda self: seen.append(self.config) or RunRecord("x", "y")
    )
    assert main() == RunRecord("x", "y")
    assert seen[0].paths.landing_dir.name == "landing"
