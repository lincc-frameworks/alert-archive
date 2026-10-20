import json

import lsdb
import nested_pandas as npd
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from dask.distributed import Client
from hats_import import ImportArguments, pipeline_with_client

from alert_archive.reader import AlertReader, transform_alerts

NESTED_COLUMNS = ["diaSource", "prvDiaSources", "prvDiaForcedSources", "diaObject"]


def read_all(sample_path, **kwargs):
    """Read the sample with AlertReader and concatenate the batches."""
    return pa.concat_tables(AlertReader(**kwargs).read(sample_path))


def assert_tables_equal(left, right):
    """Compare tables by schema and values. Table.equals treats NaN != NaN."""
    assert left.schema.equals(right.schema)
    assert json.dumps(left.to_pylist()) == json.dumps(right.to_pylist())


def test_adds_coordinates_first(sample_path, raw_alerts):
    """ra and dec are the first two columns and match diaSource."""
    table = read_all(sample_path)
    assert table.column_names[:2] == ["ra", "dec"]
    for name in ["ra", "dec"]:
        expected = pc.struct_field(raw_alerts["diaSource"], name)
        assert table[name].to_pylist() == expected.to_pylist()


def test_struct_columns_become_nested(sample_path):
    """nested-pandas sees every struct column as nested, with one row per alert for scalars."""
    nf = npd.from_pyarrow(read_all(sample_path))
    assert sorted(nf.nested_columns) == sorted(NESTED_COLUMNS)
    assert all(len(nf.iloc[i]["diaSource"]) == 1 for i in range(len(nf)))
    assert all(len(nf.iloc[i]["diaObject"]) == 1 for i in range(len(nf)))


def test_history_is_preserved(sample_path, raw_alerts):
    """List-of-struct values survive conversion; null histories become empty."""
    table = read_all(sample_path)
    raw = raw_alerts["prvDiaSources"].to_pylist()
    expected = [[s["diaSourceId"] for s in row] if row is not None else [] for row in raw]
    got = pc.struct_field(table["prvDiaSources"], "diaSourceId").to_pylist()
    assert got == expected
    # The fixture covers both cases.
    assert any(row is None for row in raw)
    assert any(row for row in raw)


def test_keeps_solar_system_alerts(sample_path, raw_alerts):
    """Alerts with diaObjectId == 0 are kept, not filtered."""
    table = read_all(sample_path)
    assert len(table) == len(raw_alerts)
    object_ids = pc.list_flatten(pc.struct_field(table["diaSource"], "diaObjectId"))
    assert 0 in object_ids.to_pylist()


@pytest.mark.parametrize("chunksize", [1, 5])
def test_batches_match_single_read(sample_path, chunksize):
    """Reading in small batches (sliced arrays) gives the same result as one read."""
    assert_tables_equal(read_all(sample_path, chunksize=chunksize), read_all(sample_path))


def test_read_columns_coordinates_only(sample_path):
    """hats-import's mapping stage asks for ra/dec, which don't exist in the input."""
    batches = list(AlertReader().read(sample_path, read_columns=["ra", "dec"]))
    table = pa.concat_tables(batches)
    assert table.column_names == ["ra", "dec"]
    full = read_all(sample_path)
    assert_tables_equal(table, full.select(["ra", "dec"]))


def test_read_columns_mixed(sample_path):
    """Coordinates can be requested alongside a full struct column."""
    table = pa.concat_tables(AlertReader().read(sample_path, read_columns=["diaSourceId", "ra", "diaSource"]))
    assert table.column_names == ["diaSourceId", "ra", "diaSource"]
    assert_tables_equal(table, read_all(sample_path).select(["diaSourceId", "ra", "diaSource"]))


def test_drops_cutouts(raw_alerts):
    """Columns starting with "cutout" are removed."""
    with_cutout = raw_alerts.append_column("cutoutDifference", pa.array([b"x"] * len(raw_alerts)))
    table = transform_alerts(with_cutout)
    assert not any(name.startswith("cutout") for name in table.column_names)


def test_hats_import(sample_path, tmp_path):
    """The reader works end to end in a hats-import pipeline."""
    args = ImportArguments(
        output_path=tmp_path,
        output_artifact_name="alerts",
        input_file_list=[sample_path],
        file_reader=AlertReader(),
        progress_bar=False,
    )
    with Client(n_workers=1, threads_per_worker=1, processes=False):
        pipeline_with_client(args, Client.current())

    catalog = lsdb.open_catalog(tmp_path / "alerts")
    df = catalog.compute()
    assert len(df) == 12
    assert set(NESTED_COLUMNS) <= set(df.nested_columns)
