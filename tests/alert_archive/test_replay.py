import shutil

import hats
import lsdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from alert_archive.mock import replay
from alert_archive.mock.replay import ReplayPipeline, main, replay_nights, replay_summary
from alert_archive.state import PipelineState, RunRecord
from conftest import BASELINE_CHUNK_NAMES, shift_ids
from dask.distributed import Client

NIGHTS = [
    ["313629__00000000-00000002.parquet", "313629__00000002-00000004.parquet"],
    ["313637__00000000-00000003.parquet"],
]
"""Two replay nights: one prefix in two chunks, one in a single chunk."""


@pytest.fixture
def emails(monkeypatch):
    """Capture the emails the replay sends, as (subject, body) pairs."""
    sent = []
    monkeypatch.setattr(replay, "send_email", lambda subject, body: sent.append((subject, body)))
    return sent


@pytest.fixture
def dump(v2_config, raw_alerts):
    """The dump: the baseline's chunks, a 5-digit duplicate and two nights to replay, all with new IDs."""
    path = v2_config.paths.dump_dir
    pq.write_table(raw_alerts.slice(0, 6), path / BASELINE_CHUNK_NAMES[0])
    pq.write_table(raw_alerts.slice(6), path / BASELINE_CHUNK_NAMES[1])
    pq.write_table(raw_alerts.slice(0, 2), path / "17005__00000000-00000002.parquet")
    for i, name in enumerate([*NIGHTS[0], *NIGHTS[1]]):
        start, end = (int(n) for n in name.removesuffix(".parquet").split("__")[1].split("-"))
        pq.write_table(shift_ids(raw_alerts.slice(start, end - start), 100 * (i + 1)), path / name)
    return path


def run_replay(config, tmp_path, **kwargs):
    """Run the replay of the fixture dump with a small in-process Dask client."""
    with Client(n_workers=1, threads_per_worker=1, processes=False) as client:
        return ReplayPipeline(config).run(client, tmp_path, after=BASELINE_CHUNK_NAMES[-1], **kwargs)


def test_replay_nights(dump):
    """Chunks after the baseline are grouped by prefix; the baseline's and the duplicates are left out."""
    assert [
        [p.name for p in night] for night in replay_nights(dump, after=BASELINE_CHUNK_NAMES[-1])
    ] == NIGHTS


def test_replay_end_to_end(v2_config, dump, tmp_path, emails):
    """Every night is replayed into the catalog once, and a second replay does nothing.

    One chunk is already in the landing dir (e.g. from an interrupted replay); it isn't copied again.
    """
    landing = v2_config.paths.landing_dir
    landing.mkdir(parents=True)
    shutil.copyfile(dump / NIGHTS[0][0], landing / NIGHTS[0][0])

    records = run_replay(v2_config, tmp_path)
    assert [r.chunks for r in records] == NIGHTS
    assert [r.num_rows for r in records] == [4, 3]
    assert sorted(p.name for p in landing.iterdir()) == [*NIGHTS[0], *NIGHTS[1]]

    working = v2_config.paths.working_dir
    assert hats.read_hats(working).catalog_info.total_rows == 12 + 7
    df = lsdb.open_catalog(working).compute()
    assert len(df) == 19 and df["diaSourceId"].is_unique
    assert len(PipelineState(v2_config.paths.state_dir).runs()) == 3
    ((subject, body),) = emails
    assert "2 run(s), 7 alerts, OK" in subject

    (tmp_path / "again").mkdir()
    assert run_replay(v2_config, tmp_path / "again", email=False) == []


def test_replay_max_nights(v2_config, dump, tmp_path):
    """``max_nights`` stops early; the next replay picks up where it stopped."""
    assert [r.chunks for r in run_replay(v2_config, tmp_path, max_nights=1, email=False)] == [NIGHTS[0]]
    (tmp_path / "again").mkdir()
    assert [r.chunks for r in run_replay(v2_config, tmp_path / "again", email=False)] == [NIGHTS[1]]


def test_replay_stops_on_format_change(v2_config, dump, tmp_path, emails):
    """A night in an unsupported format stops the replay, and no later night is copied."""
    name = NIGHTS[0][0]
    table = pq.read_table(dump / name)
    pq.write_table(table.append_column("newField", pa.array([1.0] * len(table))), dump / name)

    assert run_replay(v2_config, tmp_path) == []
    assert PipelineState(v2_config.paths.state_dir).format_change() is not None
    assert not (v2_config.paths.landing_dir / NIGHTS[1][0]).exists()
    assert "STOPPED" in emails[0][0]


def test_replay_summary_lists_skipped():
    """Skipped chunks from any run are listed in the summary."""
    record = RunRecord("20261101T000000Z", "t", chunks=["a"], num_rows=3, skipped={"b": "OSError: bad"})
    subject, body = replay_summary([record], stopped=None)
    assert "chunks skipped" in subject
    assert "b: OSError: bad" in body


def test_main_runs_replay(monkeypatch):
    """``main`` runs the replay with the default config."""
    seen = []
    monkeypatch.setattr(ReplayPipeline, "execute", lambda self, max_nights: seen.append(max_nights) or [])
    assert main(max_nights=2) == []
    assert seen == [2]
