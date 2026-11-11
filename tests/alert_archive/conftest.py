import gzip
import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest

DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def alert_sample_pq_path() -> Path:
    """Path to 12 alerts cut from the parquet dump (see data/README.md)."""
    return DATA_DIR / "alert_sample.parquet"


@pytest.fixture
def raw_alerts(alert_sample_pq_path):
    """The sample alerts as stored on disk, before any reshaping."""
    return pq.read_table(alert_sample_pq_path)


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
