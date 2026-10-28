import hats
import lsdb
import numpy as np
import pandas as pd
import pytest
from alert_archive.daily.increment import (
    PER_PARTITION_STATISTICS,
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
    """A leaf-directory catalog of 200 points with ra in [0, 90), in files named ``base.parquet``."""
    path = tmp_path / "working"
    make_catalog(0, 200, (0, 90), seed=0).write_catalog(
        path,
        as_collection=False,
        npix_suffix="/",
        npix_parquet_name="base.parquet",
        skymap_alt_orders=[1],
        progress_bar=False,
    )
    return path


def add_run(catalog_dir, run_id, catalog):
    """Write ``catalog`` as one run, then update skymaps and metadata like a daily run does."""
    existing = hats.read_hats(catalog_dir)
    write_partitions(
        catalog,
        base_catalog_dir=existing.catalog_base_dir,
        histogram_order=existing.catalog_info.skymap_order,
        parquet_name=f"{run_id}.parquet",
    )
    update_skymaps(existing)
    update_metadata(existing)


def expected_skymap(catalog_dir, order):
    """Count the catalog's rows per pixel at ``order`` from its data, the slow way."""
    df = lsdb.open_catalog(catalog_dir).compute()
    return np.bincount(df.index.to_numpy() >> 2 * (29 - order), minlength=12 * 4**order)


def test_leaf_files_skips_empty_pixel_dirs(base_dir):
    """Only pixel directories with parquet files are listed."""
    (base_dir / "dataset" / "Norder=2" / "Dir=0" / "Npix=7").mkdir(exist_ok=True)
    files = leaf_files(base_dir)
    assert HealpixPixel(ORDER, 7) not in files
    assert all(len(f) == 1 and f[0].name == "base.parquet" for f in files.values())
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
    assert {p.name for f in files.values() for p in f} == {"base.parquet", "run1.parquet"}

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


def test_update_metadata_removes_stale_files(base_dir):
    """``_metadata`` and the per-partition statistics are deleted; provenance is kept."""
    assert paths.get_parquet_metadata_pointer(base_dir).exists()
    assert (base_dir / PER_PARTITION_STATISTICS).exists()
    catalog = hats.read_hats(base_dir)
    update_metadata(catalog)

    assert not paths.get_parquet_metadata_pointer(base_dir).exists()
    assert not (base_dir / PER_PARTITION_STATISTICS).exists()
    updated = hats.read_hats(base_dir).catalog_info
    assert updated.catalog_name == catalog.catalog_info.catalog_name
    assert updated.npix_suffix == "/"


def test_update_metadata_empty_catalog(base_dir):
    """A catalog with no files left is an error, not an empty partition list."""
    for path in base_dir.glob("dataset/**/*.parquet"):
        path.unlink()
    with pytest.raises(ValueError, match="no parquet files"):
        update_metadata(hats.read_hats(base_dir))


def test_find_and_remove_orphan_files(base_dir):
    """Files from a run that never reached the log are found and deleted; known files stay."""
    add_run(base_dir, "crashed", make_catalog(1000, 50, (0, 90), seed=1))
    crashed = sorted(base_dir.glob("dataset/**/crashed.parquet"))
    assert crashed

    assert find_orphan_files(base_dir, ["base.parquet", "crashed.parquet"]) == []
    assert find_orphan_files(base_dir, ["base.parquet"]) == crashed

    assert remove_orphan_files(base_dir, ["base.parquet"]) == crashed
    assert not any(p.exists() for p in crashed)
    assert all(f[0].name == "base.parquet" for f in leaf_files(base_dir).values())


def test_remove_orphan_files_needs_known_names(base_dir):
    """An empty list of known names deletes nothing, rather than the whole catalog."""
    with pytest.raises(ValueError, match="empty"):
        remove_orphan_files(base_dir, [])
    assert find_orphan_files(base_dir, ["base.parquet"]) == []
