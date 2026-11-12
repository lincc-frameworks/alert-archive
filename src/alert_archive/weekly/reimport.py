"""Re-import the current catalog as a new version of the collection, with a margin and an index.

Needs the hats and hats-import fixes that let ``ImportArguments.reimport_from_hats``
read, and strictly validate, leaf pixel directories holding several files.
"""

import logging
from pathlib import Path

import pyarrow.parquet as pq
from dask.distributed import Client
from hats.io import paths
from hats_import import CollectionArguments, ImportArguments, pipeline_with_client

from ..baseline import CATALOG_NAME
from ..config import ImportConfig
from ..daily.increment import check_schema, leaf_files

logger = logging.getLogger(__name__)

MARGIN_THRESHOLD = 10.0
"""Width of the margin catalog, in arcseconds. The same as ppdb_hats' collections."""

INDEX_COLUMN = "diaSourceId"
"""Column the index catalog is built on. Each alert has its own ``diaSourceId``."""


class ReimportCheckError(RuntimeError):
    """Raised when a re-imported catalog doesn't have the rows or schema of its input."""


def build_version(
    client: Client,
    source_dir: Path,
    collection_dir: Path,
    version: str,
    import_config: ImportConfig,
) -> Path:
    """Re-import a catalog as the collection ``<collection_dir>/<version>/``, and return its main catalog.

    The main catalog, ``rubin_alert_archive_<version>``, is repartitioned to at
    most ``import_config.pixel_threshold`` rows per pixel, in leaf directories
    the daily runs can add files to. hats-import then adds the margin and index
    and writes the version's ``collection.properties``. ``source_dir``'s
    metadata must match its files (see
    :func:`~alert_archive.daily.increment.update_metadata`), or hats-import's
    validation and row-count check fail.
    """
    name = f"{CATALOG_NAME}_{version}"
    logger.info("Re-importing %s as %s/%s...", source_dir, version, name)
    import_args = ImportArguments.reimport_from_hats(
        source_dir,
        collection_dir / version,
        output_artifact_name=name,
        npix_suffix="/",
        pixel_threshold=import_config.pixel_threshold,
        resume=False,  # a failed run is redone from scratch
        simple_progress_bar=import_config.simple_progress_bar,
    )
    pipeline_with_client(import_args, client)

    logger.info("Adding a %s arcsec margin and a %s index...", MARGIN_THRESHOLD, INDEX_COLUMN)
    catalog_dir = collection_dir / version / name
    collection_args = (
        # Not resume=False here: it would delete <version>/, catalog included.
        CollectionArguments(
            output_path=collection_dir,
            output_artifact_name=version,
            simple_progress_bar=import_config.simple_progress_bar,
        )
        .catalog(catalog_path=catalog_dir)
        .add_margin(margin_threshold=MARGIN_THRESHOLD, is_default=True, resume=False)
        .add_index(indexing_column=INDEX_COLUMN, resume=False)
    )
    pipeline_with_client(collection_args, client)
    return catalog_dir


def check_reimport(source_dir: Path, new_dir: Path):
    """Raise unless a re-imported catalog has its input's row count and schema.

    The schemas are compared as the daily run compares new alerts
    (:func:`~alert_archive.daily.increment.check_schema`), so a re-import the
    next daily run would fail on never goes live.

    Raises
    ------
    ReimportCheckError
        If the row counts differ.
    alert_archive.daily.increment.SchemaMismatchError
        If the schemas differ.
    """
    source_rows, new_rows = count_rows(source_dir), count_rows(new_dir)
    if source_rows != new_rows:
        raise ReimportCheckError(f"{new_dir} has {new_rows} rows, but {source_dir} has {source_rows}")
    check_schema(
        pq.read_schema(paths.get_common_metadata_pointer(new_dir)),
        pq.read_schema(paths.get_common_metadata_pointer(source_dir)),
    )
    logger.info("Re-import check passed: %d rows, same schema.", new_rows)


def count_rows(catalog_dir: Path) -> int:
    """Count a catalog's rows from the footers of its leaf parquet files."""
    files = leaf_files(catalog_dir).values()
    return sum(pq.read_metadata(path).num_rows for pixel_files in files for path in pixel_files)
