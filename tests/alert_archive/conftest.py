import shutil
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from alert_archive.baseline import BaselinePipeline
from alert_archive.config import PathConfig, get_default_config
from dask.distributed import Client

DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def alert_sample_pq_path() -> Path:
    """Path to 12 alerts cut from the parquet dump (see data/README.md)."""
    return DATA_DIR / "alert_sample.parquet"


@pytest.fixture
def raw_alerts(alert_sample_pq_path):
    """The sample alerts as stored on disk, before any reshaping."""
    return pq.read_table(alert_sample_pq_path)


BASELINE_CHUNK_NAMES = ["170019__00000000-00000006.parquet", "170020__00000000-00000006.parquet"]
"""Chunks of the small baseline built by :func:`built_baseline`, named like real dump chunks."""


def shift_ids(table, offset):
    """The same alerts with new ``diaSourceId``s, so they don't duplicate the originals."""
    index = table.column_names.index("diaSourceId")
    return table.set_column(index, "diaSourceId", pc.add(table["diaSourceId"], offset))


@pytest.fixture(scope="session")
def built_baseline(tmp_path_factory):
    """A v2 root holding a baseline of the 12 sample alerts, built once per session.

    Use :func:`v2_config` for a fresh copy per test.
    """
    base = tmp_path_factory.mktemp("built_baseline")
    raw = pq.read_table(DATA_DIR / "alert_sample.parquet")
    dump = base / "dump"
    dump.mkdir()
    pq.write_table(raw.slice(0, 6), dump / BASELINE_CHUNK_NAMES[0])
    pq.write_table(raw.slice(6), dump / BASELINE_CHUNK_NAMES[1])
    config = get_default_config(paths=PathConfig(root=base / "v2", dump_dir=dump, tmp_dir=base))
    with Client(n_workers=1, threads_per_worker=1, processes=False) as client:
        BaselinePipeline(config).run(client, base)
    return base


@pytest.fixture
def v2_config(built_baseline, tmp_path):
    """Config for a fresh copy of the built baseline: working catalog, state and an empty dump dir."""
    shutil.copytree(built_baseline / "v2", tmp_path / "v2")
    dump = tmp_path / "dump"
    dump.mkdir()
    return get_default_config(paths=PathConfig(root=tmp_path / "v2", dump_dir=dump, tmp_dir=tmp_path))
