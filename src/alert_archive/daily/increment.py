"""Write a daily increment into the working catalog and bring its metadata up to date.

Adapted from ``ppdb_hats/daily/increment.py`` in ppdb_hats
(https://github.com/astronomy-commons/ppdb_hats, commit 241b4f2), with two
changes:

- The skymaps and metadata are recomputed from the files on disk instead of
  adding the new run's numbers to the old totals. A run that crashes after
  writing and is repeated, or whose files
  :meth:`~alert_archive.state.PipelineState.reingest_runs` deleted, then
  can't leave the counts wrong.
- New partitions are checked against the catalog's schema and cast to it
  before they are written, so every file shares one schema. A schema change
  fails and leaves none of the run's files, rather than failing when the footers
  are combined.

The catalog must use leaf pixel directories (``npix_suffix="/"``), with one
parquet file per run in each ``Npix=<pixel>/`` directory it touched.
"""

import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from hats.catalog import PartitionInfo
from hats.io import paths
from hats.io.file_io import write_fits_image
from hats.io.file_io.file_io import get_parquet_write_table_kwargs
from hats.io.parquet_metadata import write_parquet_metadata
from hats.io.skymap import write_skymap
from hats.pixel_math import HealpixPixel
from hats.pixel_math.healpix_shim import order2npix
from lsdb.io.common import new_provenance_properties
from lsdb.io.to_hats import create_modified_catalog_structure

logger = logging.getLogger(__name__)

RUN_FILE_NAME = re.compile(r"\d{8}T\d{6}Z\.parquet")
"""Name of the files a daily run writes: its run ID (see ``state.new_run_id``) plus ``.parquet``.

The IDs are UTC timestamps of fixed width, so sorting the names sorts the runs by start time.
"""


class SchemaMismatchError(ValueError):
    """Raised when new alerts don't have the catalog's columns and types."""


def write_partitions(catalog, *, base_catalog_dir, parquet_name) -> dict:
    """Write all catalog partitions into the existing catalog as parquet files.

    Each partition is written to its leaf pixel directory as
    ``Npix=<pixel>/<parquet_name>``, with the existing catalog's schema from
    ``dataset/_common_metadata``. Every file in the catalog then has the same
    schema, which :func:`update_metadata` needs to combine their footers.

    The new alerts' columns and types are checked against that schema before
    anything is written. Only nullability and field order may differ; they
    are cast to match.

    Each file is one row group. Nightly files are small, and more row groups
    would only grow ``_metadata`` and the column statistics. The row-group
    layout of the long-lived files is left to the weekly merge.

    Parameters
    ----------
    catalog
        LSDB catalog of the new alerts to add, partitioned to match the
        existing catalog's pixels (hats-import's ``existing_pixels``).
    base_catalog_dir
        Base path of the existing HATS catalog to write into.
    parquet_name : str
        Name of the parquet file in each pixel directory (e.g., ``"20261027T031500Z.parquet"``).

    Returns
    -------
    dict[HealpixPixel, int]
        Number of rows written to each pixel. Empty partitions write no files
        and are left out.

    Raises
    ------
    SchemaMismatchError
        If the new alerts have columns or types the catalog doesn't, or lack
        some it has. If only some partitions differ, the files already written
        for the others are deleted again, so nothing of this run is left.
    """
    logger.info("Writing partitions...")
    schema = pq.read_schema(paths.get_common_metadata_pointer(base_catalog_dir))
    check_schema(catalog.meta.to_pyarrow().schema, schema)

    try:
        results = catalog.map_partitions(
            _write_partition,
            paths.get_upath(base_catalog_dir),
            parquet_name,
            schema,
            include_pixel=True,
            meta=pd.DataFrame(
                {
                    "order": pd.Series(dtype=int),
                    "pixel": pd.Series(dtype=int),
                    "rows": pd.Series(dtype=int),
                    "mismatch": pd.Series(dtype=object),
                }
            ),
        ).compute(progress_bar=False)
    except BaseException:
        # With a distributed client, other partitions may still be writing; the next
        # run's remove_orphan_files catches anything they leave.
        _remove_run_files(base_catalog_dir, parquet_name)
        raise
    mismatches = results["mismatch"].dropna()
    if len(mismatches):
        _remove_run_files(base_catalog_dir, parquet_name)
        raise SchemaMismatchError(mismatches.iloc[0])
    return {
        HealpixPixel(order, pixel): rows
        for order, pixel, rows in zip(results["order"], results["pixel"], results["rows"], strict=True)
        if rows
    }


def _write_partition(df, pixel, base_catalog_dir, parquet_name, schema) -> pd.DataFrame:
    """Write one partition as ``Npix=<pixel>/<parquet_name>``, cast to ``schema``.

    Follows LSDB's own partition writer (``lsdb.io.common.write_partition_parquet``),
    with the schema check and cast added. Returns the pixel, its row count and,
    if the partition doesn't match ``schema``, the mismatch message (nothing is
    written then).
    """
    result = pd.DataFrame(
        {
            "order": [pixel.order],
            "pixel": [pixel.pixel],
            "rows": [len(df)],
            "mismatch": pd.Series([None], dtype=object),
        }
    )
    if len(df) == 0:
        return result
    table = df.to_pyarrow()

    # Rechecked per partition, since data can differ from the catalog's meta. check_schema
    # refuses real changes (cast would silently drop or null-fill them); cast then fixes
    # only nullability and order, which _metadata needs to match exactly.
    # A mismatch is returned rather than raised: lsdb wraps errors in RuntimeError, and
    # a distributed client doesn't keep the cause, so write_partitions couldn't tell.
    try:
        check_schema(table.schema, schema)
    except SchemaMismatchError as e:
        return result.assign(rows=0, mismatch=pd.Series([str(e)], dtype=object))
    table = table.select(schema.names).cast(schema)

    pixel_path = paths.new_pixel_catalog_file(
        base_catalog_dir, pixel, create_dirs=True, npix_suffix="/", npix_parquet_name=parquet_name
    )
    # One row group per file; hats' default would split every 100k rows.
    write_kwargs = get_parquet_write_table_kwargs(
        table.schema, write_table_kwargs={"row_group_size": len(table)}
    )
    pq.write_table(table, pixel_path.path, filesystem=pixel_path.fs, **write_kwargs)
    return result


def _remove_run_files(catalog_dir, parquet_name):
    """Delete every leaf file named ``parquet_name``, i.e. one run's files."""
    for path in Path(catalog_dir).glob(f"dataset/Norder=*/Dir=*/Npix=*/{parquet_name}"):
        logger.warning("Removing %s of the failed run", path)
        path.unlink()


def check_schema(actual: pa.Schema, expected: pa.Schema):
    """Raise unless two schemas have the same columns and types, nested ones included.

    Nullability and field order are ignored, since selecting the columns and
    :meth:`pyarrow.Table.cast` fix them. Every field's path and type must
    match. ``cast`` alone isn't enough: it matches struct fields by name,
    silently dropping fields the target lacks and filling missing ones with
    nulls.

    Raises
    ------
    SchemaMismatchError
        If any column or nested field is added, missing or of another type.
    """
    actual_fields, expected_fields = _field_types(actual), _field_types(expected)
    if actual_fields == expected_fields:
        return
    added = sorted(actual_fields.keys() - expected_fields.keys())
    missing = sorted(expected_fields.keys() - actual_fields.keys())
    changed = sorted(
        f"{path}: {expected_fields[path]} -> {actual_fields[path]}"
        for path in actual_fields.keys() & expected_fields.keys()
        if actual_fields[path] != expected_fields[path]
    )
    raise SchemaMismatchError(
        f"new alerts don't match the catalog's schema. Added: {added}. Missing: {missing}. "
        f"Changed: {changed}. A schema change needs a re-import of the catalog."
    )


def _field_types(schema: pa.Schema) -> dict[str, str]:
    """Map each nested field's dotted path to its type, ignoring nullability.

    Structs add ``.<name>`` to the path and lists add ``[list]`` or ``[large_list]``,
    so turning a list into a plain value counts as a change.
    """
    fields = {}

    def visit(path: str, data_type: pa.DataType):
        if pa.types.is_struct(data_type):
            for i in range(data_type.num_fields):
                visit(f"{path}.{data_type.field(i).name}", data_type.field(i).type)
        elif pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
            kind = "large_list" if pa.types.is_large_list(data_type) else "list"
            visit(f"{path}[{kind}]", data_type.value_type)
        else:
            fields[path] = str(data_type)

    for field in schema:
        visit(field.name, field.type)
    return fields


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
    every file. They are written last, since combining the footers needs every
    file to share one schema (see :func:`write_partitions`).

    Parameters
    ----------
    existing_catalog
        HATS catalog object for the catalog to update.

    Raises
    ------
    ValueError
        If the catalog has no parquet files.
    RuntimeError
        From pyarrow, if the files' schemas differ. ``partition_info.csv`` and
        ``hats.properties`` are already updated then.
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

    # Rewrite `_metadata`, `_common_metadata` and the column statistics last.
    # Combining the footers fails if any files' schemas differ, and by then the
    # pixels and row counts above are already right. `hats_estsize` above still
    # counts the previous run's copies of these files, which is close enough.
    write_parquet_metadata(catalog_base_dir, create_per_partition_stats=True)


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
