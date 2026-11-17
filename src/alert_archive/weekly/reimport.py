"""Re-import the current catalog as a new version of the collection, with a margin and an index.

Needs the hats and hats-import fixes that let ``ImportArguments.reimport_from_hats``
read, and strictly validate, leaf pixel directories holding several files.
"""

import logging
from pathlib import Path

from dask.distributed import Client
from hats_import import CollectionArguments, ImportArguments, pipeline_with_client

from ..baseline import CATALOG_NAME
from ..config import ImportConfig

logger = logging.getLogger(__name__)

MARGIN_THRESHOLD = 10.0
"""Width of the margin catalog, in arcseconds. The same as ppdb_hats' collections."""

INDEX_COLUMN = "diaSourceId"
"""Column the index catalog is built on. Each alert has its own ``diaSourceId``."""


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
    and writes the version's ``collection.properties``.

    hats-import writes the new catalog with ``source_dir``'s schema and raises
    a ``ValueError`` unless it read as many rows as ``source_dir``'s metadata
    counts. So that metadata must match the files (see
    :func:`~alert_archive.daily.increment.update_metadata`).
    """
    name = f"{CATALOG_NAME}_{version}"
    logger.info("Re-importing %s as %s/%s...", source_dir, version, name)
    import_args = ImportArguments.reimport_from_hats(
        source_dir,
        collection_dir / version,
        output_artifact_name=name,
        npix_suffix="/",
        pixel_threshold=import_config.pixel_threshold,
        resume=False,
        simple_progress_bar=import_config.simple_progress_bar,
    )
    pipeline_with_client(import_args, client)

    logger.info("Adding a %s arcsec margin and a %s index...", MARGIN_THRESHOLD, INDEX_COLUMN)
    catalog_dir = collection_dir / version / name
    collection_args = (
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
