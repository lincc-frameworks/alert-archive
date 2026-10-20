from pathlib import Path

import pyarrow.parquet as pq
import pytest

DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def sample_path() -> Path:
    """Path to 12 alerts cut from the parquet dump (see data/README.md)."""
    return DATA_DIR / "alerts_sample.parquet"


@pytest.fixture
def raw_alerts(sample_path):
    """The sample alerts as stored on disk, before any reshaping."""
    return pq.read_table(sample_path)
