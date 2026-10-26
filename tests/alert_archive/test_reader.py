import json

import lsdb
import nested_pandas as npd
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from alert_archive.reader import AlertReader, transform_alerts
from dask.distributed import Client
from hats_import import ImportArguments, pipeline_with_client

NESTED_COLUMNS = ["diaSource", "prvDiaSources", "prvDiaForcedSources", "diaObject"]


def read_all(alert_sample_pq_path, **kwargs):
    """Read the 12-alert test file with AlertReader and return it as one table.

    AlertReader.read yields one table per batch of up to ``chunksize`` rows
    (like hats-import uses it), so the batches are concatenated here.
    ``kwargs`` go to AlertReader, e.g. ``chunksize=5`` gives batches of 5, 5 and 2.
    """
    return pa.concat_tables(AlertReader(**kwargs).read(alert_sample_pq_path))


def assert_tables_equal(left, right):
    """Compare tables by schema and values. Table.equals treats NaN != NaN."""
    assert left.schema.equals(right.schema)
    assert json.dumps(left.to_pylist()) == json.dumps(right.to_pylist())


def test_adds_coordinates_first(alert_sample_pq_path, raw_alerts):
    """ra and dec are the first two columns, and each row's values equal
    that alert's diaSource.ra / diaSource.dec in the raw file."""
    table = read_all(alert_sample_pq_path)
    assert table.column_names[:2] == ["ra", "dec"]
    for col_name in ["ra", "dec"]:
        expected = pc.struct_field(raw_alerts["diaSource"], col_name)
        assert table[col_name].to_pylist() == expected.to_pylist()


def test_struct_columns_become_nested(alert_sample_pq_path):
    """nested-pandas sees every struct column as nested, with one row per alert for scalars.

    Motivation for this test: we see no failure at import-time if struct-to-nested
    conversion goes wrong, but users would hit confusing errors/miss out on nested
    features when they try to use the catalog."""
    nf = npd.from_pyarrow(read_all(alert_sample_pq_path))
    assert sorted(nf.nested_columns) == sorted(NESTED_COLUMNS)
    assert all(len(nf.iloc[i]["diaSource"]) == 1 for i in range(len(nf)))
    assert all(len(nf.iloc[i]["diaObject"]) == 1 for i in range(len(nf)))


def test_history_is_preserved(alert_sample_pq_path, raw_alerts):
    """List-of-struct values survive conversion; null histories become empty."""
    table = read_all(alert_sample_pq_path)
    raw = raw_alerts["prvDiaSources"].to_pylist()

    expected = [[s["diaSourceId"] for s in row] if row is not None else [] for row in raw]
    got = pc.struct_field(table["prvDiaSources"], "diaSourceId").to_pylist()
    assert got == expected

    # Guard against a fixture that would make this test pass vacuously:
    # it must contain at least one null history and one non-empty history.
    assert any(row is None for row in raw)
    assert any(row for row in raw)


def test_keeps_solar_system_alerts(alert_sample_pq_path, raw_alerts):
    """Solar-system alerts (diaObjectId == 0, ssObjectId set) are kept, with their ssObjectId."""
    table = read_all(alert_sample_pq_path)
    object_ids = pc.list_flatten(pc.struct_field(table["diaSource"], "diaObjectId")).to_pylist()
    ss_ids = pc.list_flatten(pc.struct_field(table["diaSource"], "ssObjectId")).to_pylist()

    output_ss_ids = [ss for obj, ss in zip(object_ids, ss_ids, strict=True) if obj == 0]

    all_raw_ss_ids = pc.struct_field(raw_alerts["diaSource"], "ssObjectId").to_pylist()
    raw_ss_ids = [ss for ss in all_raw_ss_ids if ss != 0]
    assert output_ss_ids == raw_ss_ids

    # Guard against a fixture that would make this test pass vacuously.
    assert raw_ss_ids


@pytest.mark.parametrize("chunksize", [1, 5])
def test_batches_match_single_read(alert_sample_pq_path, chunksize):
    """Reading in small batches (sliced arrays) gives the same result as one read."""
    assert_tables_equal(read_all(alert_sample_pq_path, chunksize=chunksize), read_all(alert_sample_pq_path))


def test_read_columns_coordinates_only(alert_sample_pq_path):
    """Asking for only ra and dec works, even though the input files have no such columns.

    hats-import's mapping stage reads just ra and dec. The reader has to read
    diaSource.ra / diaSource.dec from the file instead, and return only ra and
    dec, with the same values as a full read.
    """
    batches = list(AlertReader().read(alert_sample_pq_path, read_columns=["ra", "dec"]))
    table = pa.concat_tables(batches)
    assert table.column_names == ["ra", "dec"]

    # Check against a full read with all columns.
    full = read_all(alert_sample_pq_path)
    assert_tables_equal(table, full.select(["ra", "dec"]))


def test_read_columns_mixed(alert_sample_pq_path):
    """Coordinates can be requested alongside a full struct column."""
    table = pa.concat_tables(
        AlertReader().read(alert_sample_pq_path, read_columns=["diaSourceId", "ra", "diaSource"])
    )
    assert table.column_names == ["diaSourceId", "ra", "diaSource"]
    assert_tables_equal(table, read_all(alert_sample_pq_path).select(["diaSourceId", "ra", "diaSource"]))


@pytest.mark.parametrize("column_names", [["diaSource"], ["ra", "diaSource"], ["dec"]])
def test_column_names_without_coordinates_raises(column_names):
    """column_names, if given, must include ra and dec, or hats-import's splitting stage fails."""
    with pytest.raises(ValueError, match="coordinate columns"):
        AlertReader(column_names=column_names)


def test_column_names_with_coordinates(alert_sample_pq_path):
    """With ra and dec included, column_names selects the output columns."""
    reader = AlertReader(column_names=["ra", "dec", "diaSource"])
    table = pa.concat_tables(reader.read(alert_sample_pq_path))
    assert table.column_names == ["ra", "dec", "diaSource"]


def test_drops_cutouts(raw_alerts):
    """Columns starting with "cutout" are removed."""
    with_cutout = raw_alerts.append_column("cutoutDifference", pa.array([b"x"] * len(raw_alerts)))
    table = transform_alerts(with_cutout)
    assert not any(name.startswith("cutout") for name in table.column_names)


def test_hats_import(alert_sample_pq_path, tmp_path):
    """The reader works end to end in a hats-import pipeline."""
    args = ImportArguments(
        output_path=tmp_path,
        output_artifact_name="alerts",
        input_file_list=[alert_sample_pq_path],
        file_reader=AlertReader(),
        progress_bar=False,
    )
    with Client(n_workers=1, threads_per_worker=1, processes=False):
        pipeline_with_client(args, Client.current())

    catalog = lsdb.open_catalog(tmp_path / "alerts")
    df = catalog.compute()
    assert len(df) == 12
    assert set(NESTED_COLUMNS) <= set(df.nested_columns)
