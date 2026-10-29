"""Write a daily increment into the working catalog and bring its metadata up to date.

Adapted from ``ppdb_hats/daily/increment.py`` in ppdb_hats
(https://github.com/astronomy-commons/ppdb_hats, commit 241b4f2), with one
change: the skymaps and metadata are recomputed from the files on disk instead
of adding the new run's numbers to the old totals. A run that crashes after
writing and is repeated, or whose files
:meth:`~alert_archive.state.PipelineState.reingest_runs` deleted, then can't
leave the counts wrong.

The catalog must use leaf pixel directories (``npix_suffix="/"``), with one
parquet file per run in each ``Npix=<pixel>/`` directory it touched.
"""

import logging
import re
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
from hats.catalog import PartitionInfo
from hats.io import paths
from hats.io.file_io import write_fits_image
from hats.io.parquet_metadata import write_parquet_metadata
from hats.io.skymap import write_skymap
from hats.pixel_math import HealpixPixel
from hats.pixel_math.healpix_shim import order2npix
from lsdb.io.common import new_provenance_properties
from lsdb.io.to_hats import (
    create_modified_catalog_structure,
    remove_done_files,
    remove_histogram_files,
)
from lsdb.io.to_hats import write_partitions as lsdb_write_partitions

logger = logging.getLogger(__name__)

RUN_FILE_NAME = re.compile(r"\d{8}T\d{6}Z\.parquet")
"""Name of the files a daily run writes: its run ID (see ``state.new_run_id``) plus ``.parquet``.

The IDs are UTC timestamps of fixed width, so sorting the names sorts the runs by start time.
"""


def write_partitions(catalog, *, base_catalog_dir, histogram_order, parquet_name, **kwargs):
    """Write all catalog partitions to disk as parquet files.

    Each partition is written to its leaf pixel directory as
    ``Npix=<pixel>/<parquet_name>`` with LSDB's partition writer.

    Parameters
    ----------
    catalog
        LSDB catalog of the new alerts to add, partitioned to match the
        existing catalog's pixels (hats-import's ``existing_pixels``).
    base_catalog_dir
        Base path of the existing HATS catalog to write into.
    histogram_order : int
        HEALPix order for histogram calculations.
    parquet_name : str
        Name of the parquet file in each pixel directory (e.g., ``"20261027T031500Z.parquet"``).
    **kwargs
        Additional keyword arguments passed to ``to_parquet``.

    Returns
    -------
    tuple
        Tuple of ``(new_pixels, new_counts, new_histograms)`` where:
        - ``new_pixels`` is a list of new HEALPix pixel objects written.
        - ``new_counts`` is a list of row counts for each partition.
        - ``new_histograms`` is a list of ``SparseHistogram`` objects.
        :func:`update_skymaps` and :func:`update_metadata` don't use them,
        since they recompute everything from disk.
    """
    logger.info("Writing partitions...")
    try:
        return lsdb_write_partitions(
            catalog,
            base_catalog_dir,
            histogram_order,
            npix_suffix="/",
            npix_parquet_name=parquet_name,
            progress_bar=False,
            **kwargs,
        )
    finally:
        # Cleanup resume state files that LSDB writes in the catalog directory.
        remove_histogram_files(base_catalog_dir)
        remove_done_files(base_catalog_dir)


def leaf_files(catalog_dir) -> dict[HealpixPixel, list[Path]]:
    """Find the parquet files in each leaf pixel directory.

    Parameters
    ----------
    catalog_dir : str or path-like
        Base HATS catalog path, on a local filesystem.

    Returns
    -------
    dict[HealpixPixel, list[pathlib.Path]]
        The sorted parquet files of every pixel directory that has any.
        Empty pixel directories are left out.
    """
    dataset_dir = Path(catalog_dir) / "dataset"
    files = {}
    for path in sorted(dataset_dir.glob("Norder=*/Dir=*/Npix=*/*.parquet")):
        files.setdefault(paths.get_healpix_from_path(path.parent), []).append(path)
    return files


def update_skymaps(existing_catalog):
    """Rebuild the skymap FITS files from the catalog's files on disk.

    Each file's spatial index column is read, so the cost grows with the whole
    catalog, not with the new run (about 8 bytes per row).

    Parameters
    ----------
    existing_catalog
        HATS catalog object for the catalog to update. Its ``skymap_order``,
        ``skymap_alt_orders`` and spatial index column are used.
    """
    logger.info("Updating skymaps...")
    catalog_base_dir = existing_catalog.catalog_path
    info = existing_catalog.catalog_info
    skymap_order = info.skymap_order
    # Nested HEALPix: each order down splits a pixel in 4 (2 bits), so the
    # skymap pixel is the spatial index with 2 bits per order dropped.
    shift = 2 * (info.healpix_order - skymap_order)

    histogram = np.zeros(order2npix(skymap_order), dtype=np.int64)
    for pixel_files in leaf_files(catalog_base_dir).values():
        for path in pixel_files:
            spatial_index = pq.read_table(path, columns=[info.healpix_column])[info.healpix_column]
            histogram += np.bincount(spatial_index.to_numpy() >> shift, minlength=len(histogram))

    # Write skymaps to point_map.fits and skymap_*.fits
    map_file_path = paths.get_point_map_file_pointer(catalog_base_dir)
    write_fits_image(histogram, map_file_pointer=map_file_path)
    write_skymap(histogram, catalog_dir=catalog_base_dir, orders=info.skymap_alt_orders)


def update_metadata(existing_catalog):
    """Rebuild the catalog metadata from the catalog's files on disk.

    ``partition_info.csv`` lists every pixel directory that has files, and
    ``hats.properties`` gets row counts summed from the parquet footers.
    ``dataset/_metadata``, ``dataset/_common_metadata`` and
    ``per_partition_statistics.parquet`` are rewritten from those footers too.
    lsdb reads the statistics, e.g. in ``Catalog.sample``, so they must cover
    every file.

    Parameters
    ----------
    existing_catalog
        HATS catalog object for the catalog to update.

    Raises
    ------
    ValueError
        If the catalog has no parquet files.
    """
    logger.info("Updating metadata...")
    catalog_base_dir = existing_catalog.catalog_path
    rows_per_pixel = {
        pixel: sum(pq.read_metadata(path).num_rows for path in files)
        for pixel, files in leaf_files(catalog_base_dir).items()
    }
    if not rows_per_pixel:
        raise ValueError(f"no parquet files in {catalog_base_dir}")

    # Update `partition_info.csv` with new set of pixels
    partition_info = PartitionInfo.from_healpix(list(rows_per_pixel.keys()))
    partition_info_file = paths.get_partition_info_pointer(catalog_base_dir)
    partition_info.write_to_file(partition_info_file)

    # Rewrite `_metadata`, `_common_metadata` and the column statistics.
    # This runs before `hats.properties` is updated below, so that its
    # `hats_estsize` (the catalog's size on disk) includes these files.
    write_parquet_metadata(catalog_base_dir, create_per_partition_stats=True)

    # Update the `hats.properties`
    new_hc_structure = create_modified_catalog_structure(
        existing_catalog,
        catalog_base_dir,
        existing_catalog.catalog_name,
        total_rows=sum(rows_per_pixel.values()),
        hats_max_rows=max(rows_per_pixel.values()),
        hats_order=partition_info.get_highest_order(),
        moc_sky_fraction=round(partition_info.calculate_fractional_coverage(), 5),
        **new_provenance_properties(catalog_base_dir, inherit_provenance=True),
    )
    new_hc_structure.catalog_info.to_properties_file(catalog_base_dir)


def find_orphan_files(catalog_dir, known_file_names) -> list[Path]:
    """Find leaf parquet files that no known run wrote.

    A daily run that crashes after writing its partitions, but before adding
    itself to the ingested log, leaves files behind. Its chunks stay off the
    log, so the next run ingests them again under a new name, and the
    leftover files would duplicate those alerts.

    Parameters
    ----------
    catalog_dir : str or path-like
        Base HATS catalog path.
    known_file_names : iterable of str
        File names that belong in the catalog, e.g. every ``file_name`` in
        the ingested log plus the baseline's file name.

    Returns
    -------
    list[pathlib.Path]
        The sorted orphaned files.
    """
    known = set(known_file_names)
    return [path for files in leaf_files(catalog_dir).values() for path in files if path.name not in known]


def remove_orphan_files(catalog_dir, known_file_names) -> list[Path]:
    """Delete leaf parquet files that no known run wrote.

    See :func:`find_orphan_files`, which lists the same files without
    deleting them. Call this while holding the pipeline lock, before
    ingesting, so no running job is still writing the files. The skymaps and
    metadata still count the deleted files until the next
    :func:`update_skymaps` and :func:`update_metadata`.

    Runs never overlap, and each one cleans up before it writes, so real
    orphans always come from one crashed run, newer than every run in the
    log. Anything else more likely means ``known_file_names`` is incomplete,
    e.g. a truncated log or a missing baseline name, so nothing is deleted.

    Parameters
    ----------
    catalog_dir : str or path-like
        Base HATS catalog path.
    known_file_names : iterable of str
        File names that belong in the catalog.

    Returns
    -------
    list[pathlib.Path]
        The deleted files.

    Raises
    ------
    ValueError
        If ``known_file_names`` is empty, or the orphans don't look like the
        files of one crashed run: their names aren't run IDs, they come from
        more than one run, or their run is older than the newest known run.
        Nothing is deleted then.
    """
    known = set(known_file_names)
    if not known:
        raise ValueError("known_file_names is empty, so every file would be deleted")
    orphans = find_orphan_files(catalog_dir, known)
    _check_orphans_safe_to_delete(orphans, known)
    for path in orphans:
        logger.warning("Removing orphaned file %s", path)
        path.unlink()
    return orphans


def _check_orphans_safe_to_delete(orphans: list[Path], known: set[str]):
    """Raise unless ``orphans`` look like the leftovers of one crashed run.

    That means all of these hold (see :func:`remove_orphan_files` for why):

    1. Every name is a run ID (:data:`RUN_FILE_NAME`), not e.g. a baseline file.
    2. They all share one name, so they come from one run.
    3. That run is newer than every run ID in ``known``.

    Raises
    ------
    ValueError
        If any of the checks fails, naming the files.
    """
    names = sorted({path.name for path in orphans})
    if not names:
        return
    hint = "Check that known_file_names lists every run in the log and the baseline."
    not_runs = [name for name in names if not RUN_FILE_NAME.fullmatch(name)]
    if not_runs:
        raise ValueError(f"orphaned files with names no daily run writes: {not_runs}. {hint}")
    if len(names) > 1:
        raise ValueError(f"orphaned files from {len(names)} runs, but a crash leaves one: {names}. {hint}")
    latest = max((name for name in known if RUN_FILE_NAME.fullmatch(name)), default=None)
    if latest is not None and names[0] < latest:
        raise ValueError(f"orphaned run {names[0]} is older than the newest known run {latest}. {hint}")
