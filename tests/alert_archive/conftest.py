import gzip
import shutil
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from dask.distributed import Client

from alert_archive.baseline import BaselinePipeline
from alert_archive.config import PathConfig, get_default_config

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


@pytest.fixture
def avro_alert_files() -> list[Path]:
    """The Avro sample alerts, of formats 1000, 1100, 1101 and 1102 (see data/README.md)."""
    return [
        DATA_DIR / "avro_alerts" / "170019" / "170019695985952427.avro.gz",
        DATA_DIR / "avro_alerts" / "170450" / "170450700567839112.avro.gz",
        DATA_DIR / "avro_alerts" / "171167" / "171167627158226197.avro.gz",
        DATA_DIR / "avro_alerts" / "171194" / "171194007958847495.avro.gz",
    ]


@pytest.fixture
def avro_alerts(tmp_path) -> Path:
    """A copy of the Avro sample alerts, as a raw archive: four subdirectories,
    one alert each."""
    return Path(
        shutil.copytree(
            DATA_DIR / "avro_alerts",
            tmp_path / "alerts",
            ignore=shutil.ignore_patterns(".*"),
        )
    )


@pytest.fixture
def avro_alerts_manifest(avro_alerts) -> dict:
    """The manifest after converting the avro_alerts archive, with the default chunk size."""
    return {
        "archive": str(avro_alerts),
        "chunk_size": 50_000,
        "n_subdirs": 4,
        "n_files": 4,
        "n_chunks": 4,
        "subdirs": [
            {"subdir": "170019", "n_files": 1},
            {"subdir": "170450", "n_files": 1},
            {"subdir": "171167", "n_files": 1},
            {"subdir": "171194", "n_files": 1},
        ],
        "chunks": [
            {
                "subdir": "170019",
                "start": 0,
                "end": 1,
                "out_name": "170019__00000000-00000001.parquet",
            },
            {
                "subdir": "170450",
                "start": 0,
                "end": 1,
                "out_name": "170450__00000000-00000001.parquet",
            },
            {
                "subdir": "171167",
                "start": 0,
                "end": 1,
                "out_name": "171167__00000000-00000001.parquet",
            },
            {
                "subdir": "171194",
                "start": 0,
                "end": 1,
                "out_name": "171194__00000000-00000001.parquet",
            },
        ],
    }


@pytest.fixture
def unknown_format_alert(tmp_path) -> Path:
    """An alert file whose header has format ID 9999, which the package doesn't know."""
    path = tmp_path / "unknown_format.avro.gz"
    path.write_bytes(gzip.compress(b"\x00" + (9999).to_bytes(4, "big") + b"whatever"))
    return path
