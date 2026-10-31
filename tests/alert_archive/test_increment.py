import hats
import lsdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from alert_archive.daily.increment import (
    SchemaMismatchError,
    check_schema,
    consolidated_file_name,
    find_orphan_files,
    leaf_files,
    remove_orphan_files,
    update_metadata,
    update_skymaps,
    write_partitions,
)
from hats.io import paths
from hats.io.skymap import read_skymap
from hats.pixel_math import HealpixPixel

ORDER = 2
"""Every test catalog is partitioned at this one order, so new pixels never overlap old ones."""


def make_catalog(start_id, num_rows, ra_range, seed):
    """An LSDB catalog of random points with ids ``start_id..start_id + num_rows - 1``."""
    rng = np.random.default_rng(seed)
    df = pd.DataFrame(
        {
            "id": np.arange(start_id, start_id + num_rows),
            "ra": rng.uniform(*ra_range, num_rows),
            "dec": rng.uniform(-30, 30, num_rows),
        }
    )
    return lsdb.from_dataframe(
        df, ra_column="ra", dec_column="dec", lowest_order=ORDER, highest_order=ORDER, margin_threshold=None
    )


@pytest.fixture
def base_dir(tmp_path):
    """A leaf-directory catalog of 200 points with ra in [0, 90), in consolidated files."""
    path = tmp_path / "working"
    make_catalog(0, 200, (0, 90), seed=0).write_catalog(
        path,
        as_collection=False,
        npix_suffix="/",
        skymap_alt_orders=[1],
        progress_bar=False,
    )
    return path


def add_run(catalog_dir, run_id, catalog):
    """Write ``catalog`` as one run, then update skymaps and metadata like a daily run does."""
    existing = hats.read_hats(catalog_dir)
    rows_written = write_partitions(
        catalog, base_catalog_dir=existing.catalog_base_dir, parquet_name=f"{run_id}.parquet"
    )
    update_skymaps(existing)
    update_metadata(existing)
    return rows_written


def expected_skymap(catalog_dir, order):
    """Count the catalog's rows per pixel at ``order`` from its data, the slow way."""
    df = lsdb.open_catalog(catalog_dir).compute()
    return np.bincount(df.index.to_numpy() >> 2 * (29 - order), minlength=12 * 4**order)


def test_leaf_files_skips_empty_pixel_dirs(base_dir):
    """Only pixel directories with parquet files are listed."""
    (base_dir / "dataset" / "Norder=2" / "Dir=0" / "Npix=7").mkdir(exist_ok=True)
    files = leaf_files(base_dir)
    assert HealpixPixel(ORDER, 7) not in files
    assert all(len(f) == 1 and f[0].name == consolidated_file_name(pixel) for pixel, f in files.items())
    assert set(files) == set(hats.read_hats(base_dir).get_healpix_pixels())


def test_recomputed_skymap_matches_lsdb(base_dir):
    """Rebuilding the base catalog's skymap from disk gives what lsdb wrote."""
    catalog = hats.read_hats(base_dir)
    before = read_skymap(catalog, catalog.catalog_info.skymap_order)
    update_skymaps(catalog)
    np.testing.assert_array_equal(read_skymap(catalog, catalog.catalog_info.skymap_order), before)


def test_increment_adds_rows_and_pixels(base_dir):
    """An increment covering old and new sky is readable, with counts and skymaps to match."""
    add_run(base_dir, "run1", make_catalog(1000, 100, (45, 180), seed=1))

    catalog = hats.read_hats(base_dir)
    info = catalog.catalog_info
    assert info.total_rows == 300
    files = leaf_files(base_dir)
    assert set(catalog.get_healpix_pixels()) == set(files)
    pixel_rows = [sum(len(pd.read_parquet(p)) for p in f) for f in files.values()]
    assert info.hats_max_rows == max(pixel_rows)
    assert all(
        {p.name for p in f} <= {consolidated_file_name(pixel), "run1.parquet"} for pixel, f in files.items()
    )
    assert any(p.name == "run1.parquet" for f in files.values() for p in f)

    df = lsdb.open_catalog(base_dir).compute()
    assert sorted(df["id"]) == [*range(200), *range(1000, 1100)]

    np.testing.assert_array_equal(
        read_skymap(catalog, info.skymap_order), expected_skymap(base_dir, info.skymap_order)
    )
    np.testing.assert_array_equal(read_skymap(catalog, 1), expected_skymap(base_dir, 1))


def test_repeated_update_does_not_double_count(base_dir):
    """Updating skymaps and metadata again, as a repeated run would, changes nothing."""
    add_run(base_dir, "run1", make_catalog(1000, 100, (45, 180), seed=1))
    catalog = hats.read_hats(base_dir)
    update_skymaps(catalog)
    update_metadata(catalog)

    again = hats.read_hats(base_dir)
    assert again.catalog_info.total_rows == 300
    assert again.catalog_info.hats_max_rows == catalog.catalog_info.hats_max_rows
    np.testing.assert_array_equal(
        read_skymap(again, again.catalog_info.skymap_order),
        read_skymap(catalog, catalog.catalog_info.skymap_order),
    )


def test_deleted_run_files_drop_out(base_dir):
    """After a run's files are deleted, the next update forgets them and the pixels only they had."""
    base_pixels = set(hats.read_hats(base_dir).get_healpix_pixels())
    add_run(base_dir, "run1", make_catalog(1000, 100, (90, 180), seed=1))
    assert set(hats.read_hats(base_dir).get_healpix_pixels()) > base_pixels

    for path in base_dir.glob("dataset/**/run1.parquet"):
        path.unlink()
    catalog = hats.read_hats(base_dir)
    update_skymaps(catalog)
    update_metadata(catalog)

    catalog = hats.read_hats(base_dir)
    assert catalog.catalog_info.total_rows == 200
    assert set(catalog.get_healpix_pixels()) == base_pixels
    assert read_skymap(catalog, catalog.catalog_info.skymap_order).sum() == 200


def test_update_metadata_rewrites_parquet_metadata(base_dir):
    """``_metadata`` and the per-partition statistics cover the new run; provenance is kept."""
    add_run(base_dir, "run1", make_catalog(1000, 100, (45, 180), seed=1))

    assert pq.read_metadata(paths.get_parquet_metadata_pointer(base_dir)).num_rows == 300
    catalog = hats.read_hats(base_dir)
    stats = catalog.per_partition_statistics(include_columns=["id"], include_stats=["row_count"])
    assert stats["id: row_count"].astype(int).sum() == 300
    # One row per pixel, not per file: lsdb lines them up with the partitions.
    assert len(stats) == len(catalog.get_healpix_pixels())
    assert catalog.catalog_info.catalog_name == "from_lsdb_dataframe"
    assert catalog.catalog_info.npix_suffix == "/"


def test_sample_after_increment(base_dir):
    """``Catalog.sample`` reads the per-partition statistics, so it works after an increment."""
    add_run(base_dir, "run1", make_catalog(1000, 100, (45, 180), seed=1))
    sample = lsdb.open_catalog(base_dir).sample(50, seed=0)
    assert 0 < len(sample) <= 300


def test_write_partitions_returns_rows_per_pixel(base_dir):
    """The rows written per pixel add up to the new alerts, and match the one-row-group files on disk."""
    rows_written = add_run(base_dir, "run1", make_catalog(1000, 100, (45, 180), seed=1))
    assert sum(rows_written.values()) == 100
    assert all(rows > 0 for rows in rows_written.values())
    for pixel, rows in rows_written.items():
        (path,) = (p for p in leaf_files(base_dir)[pixel] if p.name == "run1.parquet")
        assert pq.read_metadata(path).num_rows == rows
        assert pq.read_metadata(path).num_row_groups == 1


def test_write_partitions_skips_empty_partitions(base_dir):
    """Pixels with no new alerts get no file and aren't in the result."""
    catalog = make_catalog(1000, 100, (0, 90), seed=1)
    nonempty = catalog.query("id < 1010")
    rows_written = write_partitions(nonempty, base_catalog_dir=base_dir, parquet_name="run1.parquet")

    assert sum(rows_written.values()) == 10
    assert len(rows_written) < len(catalog.get_healpix_pixels())
    written = {
        pixel for pixel, files in leaf_files(base_dir).items() if any(f.name == "run1.parquet" for f in files)
    }
    assert written == set(rows_written)


def test_write_partitions_cleans_up_after_crash(base_dir, monkeypatch):
    """If writing fails partway, the files the run already wrote are deleted again."""
    real_write_table = pq.write_table
    calls = []

    def fail_after_first(*args, **kwargs):
        calls.append(1)
        if len(calls) > 1:
            raise OSError("disk full")
        real_write_table(*args, **kwargs)

    monkeypatch.setattr(pq, "write_table", fail_after_first)
    # lsdb wraps errors from a partition in a RuntimeError.
    with pytest.raises(RuntimeError, match="disk full"):
        write_partitions(
            make_catalog(1000, 100, (0, 90), seed=1), base_catalog_dir=base_dir, parquet_name="run1.parquet"
        )
    assert len(calls) > 1
    assert not list(base_dir.glob("dataset/**/run1.parquet"))


def test_write_partitions_refuses_schema_change(base_dir):
    """New alerts with an extra column fail before any file is written."""
    catalog = make_catalog(1000, 100, (45, 180), seed=1)
    catalog = catalog.map_partitions(lambda df: df.assign(extra=1.0))
    with pytest.raises(SchemaMismatchError, match=r"Added: \['extra'\]"):
        write_partitions(catalog, base_catalog_dir=base_dir, parquet_name="run1.parquet")
    assert not list(base_dir.glob("dataset/**/run1.parquet"))


def test_write_partitions_refuses_schema_change_in_one_partition(base_dir):
    """A type that changes in only the last partition still raises SchemaMismatchError, and the
    files already written for the other partitions are deleted again."""
    catalog = make_catalog(1000, 300, (0, 360), seed=1)
    last = catalog.get_healpix_pixels()[-1]

    def change_last(df, pixel):
        return df.assign(id=df["id"].astype("float64")) if pixel == last else df

    catalog = catalog.map_partitions(change_last, include_pixel=True)
    with pytest.raises(SchemaMismatchError, match=r"Changed: \['id: int64 -> double'\]"):
        write_partitions(catalog, base_catalog_dir=base_dir, parquet_name="run1.parquet")
    assert not list(base_dir.glob("dataset/**/run1.parquet"))


def test_write_partitions_casts_nullability(base_dir):
    """New files get the catalog's nullability, so their footers can be combined with the old ones."""
    common = paths.get_common_metadata_pointer(base_dir)
    schema = pq.read_schema(common)
    strict = schema.set(schema.get_field_index("id"), schema.field("id").with_nullable(False))
    pq.write_metadata(strict, common)

    write_partitions(
        make_catalog(1000, 100, (45, 180), seed=1), base_catalog_dir=base_dir, parquet_name="run1.parquet"
    )
    for path in base_dir.glob("dataset/**/run1.parquet"):
        assert not pq.read_schema(path).field("id").nullable


def test_update_metadata_mixed_schemas_keeps_counts(base_dir):
    """If the footers can't be combined, the pixels and row counts are already up to date."""
    pixel_dir = base_dir / "dataset" / "Norder=2" / "Dir=0" / "Npix=100"
    pixel_dir.mkdir(parents=True)
    pq.write_table(pa.table({"id": [1, 2], "other": [1.0, 2.0]}), pixel_dir / "stray.parquet")

    with pytest.raises(RuntimeError, match="equal schemas"):
        update_metadata(hats.read_hats(base_dir))
    catalog = hats.read_hats(base_dir)
    assert catalog.catalog_info.total_rows == 202
    assert HealpixPixel(ORDER, 100) in catalog.get_healpix_pixels()


NESTED = pa.struct([("a", pa.list_(pa.float64())), ("b", pa.list_(pa.float64()))])


@pytest.mark.parametrize(
    ("actual", "match"),
    [
        pytest.param(
            pa.struct([*NESTED, ("c", pa.list_(pa.float64()))]),
            r"Added: \['s.c\[list\]'\]",
            id="added subfield",
        ),
        pytest.param(pa.struct([NESTED.field(0)]), r"Missing: \['s.b\[list\]'\]", id="missing subfield"),
        pytest.param(
            pa.struct([NESTED.field(0), ("b", pa.list_(pa.float32()))]),
            r"Changed: \['s.b\[list\]: double -> float'\]",
            id="changed type",
        ),
    ],
)
def test_check_schema_nested_differences(actual, match):
    """Nested changes raise; a plain cast would silently drop or null them."""
    with pytest.raises(SchemaMismatchError, match=match):
        check_schema(pa.schema([("s", actual)]), pa.schema([("s", NESTED)]))


def test_check_schema_ignores_nullability_and_order():
    """The same fields in another order, or with other nullability, pass; cast fixes both."""
    expected = pa.schema([pa.field("id", pa.int64(), nullable=False), ("s", NESTED)])
    check_schema(
        pa.schema([("s", pa.struct([NESTED.field(1), NESTED.field(0)])), ("id", pa.int64())]), expected
    )


def test_update_metadata_empty_catalog(base_dir):
    """A catalog with no files left is an error, not an empty partition list."""
    for path in base_dir.glob("dataset/**/*.parquet"):
        path.unlink()
    with pytest.raises(ValueError, match="no parquet files"):
        update_metadata(hats.read_hats(base_dir))


BASELINE, RUN1, RUN2, CRASHED = "20261026T120000Z", "20261027T031500Z", "20261028T031500Z", "20261029T031500Z"
"""Run IDs in start order: the baseline (whose files are consolidated), two logged daily runs,
then one that crashed before reaching the log."""


@pytest.fixture
def crashed_dir(base_dir):
    """The base catalog plus two logged runs and a newer crashed run, all touching the same sky."""
    for seed, run_id in enumerate([RUN1, RUN2, CRASHED], start=1):
        add_run(base_dir, run_id, make_catalog(1000 * seed, 50, (0, 90), seed=seed))
    return base_dir


def all_files(catalog_dir):
    """Every leaf parquet file in the catalog."""
    return sorted(p for files in leaf_files(catalog_dir).values() for p in files)


def test_find_and_remove_orphan_files(crashed_dir):
    """Files from the crashed run are found and deleted; known and consolidated files stay."""
    known = [f"{BASELINE}.parquet", f"{RUN1}.parquet", f"{RUN2}.parquet"]
    crashed = sorted(crashed_dir.glob(f"dataset/**/{CRASHED}.parquet"))
    assert crashed

    assert find_orphan_files(crashed_dir, [*known, f"{CRASHED}.parquet"]) == []
    assert find_orphan_files(crashed_dir, known) == crashed

    assert remove_orphan_files(crashed_dir, known) == crashed
    assert not any(p.exists() for p in crashed)
    for pixel, files in leaf_files(crashed_dir).items():
        assert {p.name for p in files} <= {consolidated_file_name(pixel), *known}


def test_remove_orphan_files_no_orphans(crashed_dir):
    """With every file known, nothing is deleted."""
    known = [f"{BASELINE}.parquet", f"{RUN1}.parquet", f"{RUN2}.parquet", f"{CRASHED}.parquet"]
    assert remove_orphan_files(crashed_dir, known) == []


@pytest.mark.parametrize(
    ("known", "match"),
    [
        pytest.param([], "empty", id="empty"),
        pytest.param([f"{BASELINE}.parquet", f"{RUN2}.parquet"], "from 2 runs", id="log missing a run"),
        pytest.param([f"{BASELINE}.parquet"], "from 3 runs", id="log has only the baseline"),
    ],
)
def test_remove_orphan_files_refuses_incomplete_known(crashed_dir, known, match):
    """If the known names are incomplete, nothing is deleted."""
    before = all_files(crashed_dir)
    with pytest.raises(ValueError, match=match):
        remove_orphan_files(crashed_dir, known)
    assert all_files(crashed_dir) == before


def test_remove_orphan_files_refuses_older_run(crashed_dir):
    """A lone unknown run older than the newest known run means the log lost it."""
    before = all_files(crashed_dir)
    known = [f"{BASELINE}.parquet", f"{RUN2}.parquet", f"{CRASHED}.parquet"]
    with pytest.raises(ValueError, match="older than the newest known run"):
        remove_orphan_files(crashed_dir, known)
    assert all_files(crashed_dir) == before


def test_orphan_check_refuses_stray_files(crashed_dir):
    """A file that is neither consolidated nor named like a run is never deleted, even if unknown.

    A consolidated name in the wrong pixel's directory counts as stray too."""
    pixel, files = next(iter(leaf_files(crashed_dir).items()))
    other = HealpixPixel(pixel.order, pixel.pixel + 1)
    (files[0].parent / consolidated_file_name(other)).touch()
    known = [f"{BASELINE}.parquet", f"{RUN1}.parquet", f"{RUN2}.parquet", f"{CRASHED}.parquet"]
    before = all_files(crashed_dir)
    with pytest.raises(ValueError, match="no daily run writes"):
        remove_orphan_files(crashed_dir, known)
    assert all_files(crashed_dir) == before
