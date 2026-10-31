import shutil

import hats
import lsdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from alert_archive.baseline import (
    BASELINE_DASK,
    CATALOG_NAME,
    DEFAULT_COLUMNS,
    BaselinePipeline,
    baseline_chunks,
    main,
)
from alert_archive.config import DaskConfig, PathConfig, get_default_config
from alert_archive.daily.increment import consolidated_file_name, leaf_files
from alert_archive.state import PipelineState
from dask.distributed import Client

BASELINE_CHUNKS = ["170019__00000000-00000006.parquet", "170020__00000000-00000006.parquet"]
"""Fixture chunks that sort before the real last baseline chunk."""

LATER_CHUNK = "313629__00000000-00000002.parquet"
"""A fixture chunk named like the dump's Jul 31 chunks, after the baseline."""

DUPLICATE_CHUNK = "17005__00000000-00000002.parquet"
"""A fixture chunk named like the dump's 5-digit duplicates, inside the baseline's name range."""


@pytest.fixture
def dump_dir(tmp_path, raw_alerts):
    """A small dump: the 12 sample alerts split over two baseline chunks, plus a later
    and a duplicate chunk."""
    path = tmp_path / "dump"
    path.mkdir()
    pq.write_table(raw_alerts.slice(0, 6), path / BASELINE_CHUNKS[0])
    pq.write_table(raw_alerts.slice(6), path / BASELINE_CHUNKS[1])
    pq.write_table(raw_alerts.slice(0, 2), path / LATER_CHUNK)
    pq.write_table(raw_alerts.slice(6, 2), path / DUPLICATE_CHUNK)
    return path


@pytest.fixture
def config(tmp_path, dump_dir):
    """Pipeline config pointing at a fresh v2 root and the small dump."""
    return get_default_config(paths=PathConfig(root=tmp_path / "v2", dump_dir=dump_dir, tmp_dir=tmp_path))


def run_baseline(config, tmp_path, chunks=None):
    """Run the baseline with a small in-process Dask client."""
    with Client(n_workers=1, threads_per_worker=1, processes=False) as client:
        return BaselinePipeline(config).run(client, tmp_path, chunks=chunks)


def test_baseline_chunks_stop_at_last_chunk(dump_dir):
    """Chunks up to the last baseline chunk are selected, sorted; later ones are left for the replay.

    The duplicate chunk sorts before the last baseline chunk but is skipped.
    """
    assert [p.name for p in baseline_chunks(dump_dir)] == BASELINE_CHUNKS
    assert [p.name for p in baseline_chunks(dump_dir, last_chunk=BASELINE_CHUNKS[0])] == BASELINE_CHUNKS[:1]


def test_baseline_chunks_none(tmp_path):
    """An empty or wrong dump directory is an error, not an empty baseline."""
    with pytest.raises(FileNotFoundError, match="no parquet chunks"):
        baseline_chunks(tmp_path)


def test_baseline_builds_catalog(config, tmp_path):
    """The baseline writes a leaf-directory catalog of conformed alerts, with consolidated file names."""
    record = run_baseline(config, tmp_path)
    working = config.paths.working_dir

    info = hats.read_hats(working).catalog_info
    assert info.catalog_name == CATALOG_NAME
    assert info.npix_suffix == "/"
    assert info.default_columns == DEFAULT_COLUMNS
    assert info.total_rows == 12
    files = leaf_files(working)
    assert all([p.name for p in f] == [consolidated_file_name(pixel)] for pixel, f in files.items())

    df = lsdb.open_catalog(working, columns="all").compute()
    assert len(df) == 12
    assert set(df["schemaId"]) == {1100}
    # Conformed to format 1101: the new diaSource fields exist, null for these 1100 alerts.
    assert df["diaSource.exposureTime"].isna().all()

    assert record.chunks == BASELINE_CHUNKS
    assert record.num_rows == 12
    assert record.schema_id_counts == {1100: 12}
    state = PipelineState(config.paths.state_dir)
    assert state.runs() == [record]
    assert state.ingested_chunks() == set(BASELINE_CHUNKS)
    assert not state.lock_path.exists()


def test_baseline_refuses_existing_catalog(config, tmp_path):
    """An existing working catalog is never replaced, and nothing is logged."""
    config.paths.working_dir.mkdir(parents=True)
    with pytest.raises(FileExistsError, match="already exists"):
        run_baseline(config, tmp_path)
    assert PipelineState(config.paths.state_dir).runs() == []


def test_baseline_refuses_existing_log(config, tmp_path):
    """A second baseline after a first one fails, since the log already has runs."""
    run_baseline(config, tmp_path)
    working = config.paths.working_dir
    shutil.rmtree(working)
    with pytest.raises(FileExistsError, match="already has runs"):
        run_baseline(config, tmp_path)
    assert not working.exists()


def test_baseline_stops_on_format_change(config, tmp_path, dump_dir, raw_alerts):
    """A chunk in an unsupported format stops the import, and nothing is logged."""
    changed = raw_alerts.append_column("newField", pa.array([1.0] * len(raw_alerts)))
    pq.write_table(changed, dump_dir / BASELINE_CHUNKS[0])
    # The mapping stage reads only coordinates, so the error comes from the splitting
    # stage, which hats-import reports as a generic failure (the FormatChangeError is logged).
    with pytest.raises(RuntimeError, match="splitting stages failed"):
        run_baseline(config, tmp_path)
    assert PipelineState(config.paths.state_dir).runs() == []


def test_main_runs_baseline(config):
    """``main`` runs the whole pipeline, with its own Dask cluster and temp dir."""
    config.dask = DaskConfig(n_workers=1, memory_limit="2GiB")
    record = main(config)
    assert record.num_rows == 12
    assert PipelineState(config.paths.state_dir).runs() == [record]


def test_main_defaults_to_baseline_dask(monkeypatch):
    """Without a config, ``main`` uses the bigger baseline Dask settings and the default paths."""
    seen = []
    monkeypatch.setattr(BaselinePipeline, "execute", lambda self: seen.append(self.config))
    main()
    assert seen[0].dask == BASELINE_DASK
    assert seen[0].paths == PathConfig()
