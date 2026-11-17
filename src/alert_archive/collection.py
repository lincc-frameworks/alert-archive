"""The collections and their versions.

Each version is a collection of its own, in a directory named by its ID, the
UTC date it was made. The top-level ``collection.properties`` is a copy of the
current version's, with its paths prefixed by the version's directory::

    collections/
    ├── collection.properties                 -> <B>/rubin_alert_archive_<B>, ...
    ├── <A>/                                  previous version, kept for users
    └── <B>/                                  current version
        ├── collection.properties
        ├── rubin_alert_archive_<B>/          the daily runs write here
        ├── rubin_alert_archive_<B>_10arcs/
        └── rubin_alert_archive_<B>_diaSourceId/

The weekly re-import writes each version with hats-import. The first version is
the baseline's ``working/`` catalog, moved in once by :func:`adopt_working`,
with no margin or index.
"""

import logging
import re
import shutil
from pathlib import Path

from hats.catalog.dataset.collection_properties import CollectionProperties

from .baseline import CATALOG_NAME
from .config import PathConfig
from .state import PipelineState, new_run_id, write_atomic

logger = logging.getLogger(__name__)

VERSION_DIR = re.compile(r"\d{8}")
"""A version's directory name: its ID, e.g. ``20261107``."""


def collections_dir(paths: PathConfig) -> Path:
    """The collection users read and the pipeline maintains, ``collections/``."""
    return paths.root / "collections"


def current_catalog_dir(paths: PathConfig) -> Path:
    """The collection's current main catalog, which the daily runs write to and the weekly re-import reads.

    Raises
    ------
    FileNotFoundError
        If the collection has no current version yet (see :func:`adopt_working`).
    """
    collections = collections_dir(paths)
    if not (collections / "collection.properties").exists():
        raise FileNotFoundError(f"{collections} has no current version; move working/ in with adopt_working")
    return collections / CollectionProperties.read_from_dir(collections).hats_primary_table_url


def adopt_working(paths: PathConfig, version: str | None = None) -> Path:
    """Move the baseline's ``working/`` catalog into ``collections/`` as its first version, once.

    The catalog is moved, not copied, to ``<version>/rubin_alert_archive_<version>/``,
    and becomes current. It has no margin or index until the first weekly re-import.
    It holds the pipeline lock, so no daily run writes to ``working/`` meanwhile.

    Parameters
    ----------
    paths : PathConfig
        The archive's paths.
    version : str or None
        The version's ID. Defaults to today's UTC date.

    Returns
    -------
    pathlib.Path
        The moved catalog.

    Raises
    ------
    FileExistsError
        If the collection already has a current version.
    FileNotFoundError
        If there's no ``working/`` to move.
    alert_archive.state.LockHeldError
        If another job holds the pipeline lock.
    """
    collection_dir = collections_dir(paths)
    with PipelineState(paths.state_dir).lock("adopt"):
        if (collection_dir / "collection.properties").exists():
            raise FileExistsError(f"{collection_dir} already has a current version")
        if not paths.working_dir.is_dir():
            raise FileNotFoundError(f"{paths.working_dir} doesn't exist")

        version = version or new_run_id(date_only=True)
        version_dir = collection_dir / version
        catalog_dir = version_dir / f"{CATALOG_NAME}_{version}"
        version_dir.mkdir(parents=True)
        properties = CollectionProperties(name=version, hats_primary_table_url=catalog_dir.name)
        properties.to_properties_file(version_dir)
        logger.info("Moving %s to %s...", paths.working_dir, catalog_dir)
        paths.working_dir.rename(catalog_dir)
        set_current_version(collection_dir, version)
    return catalog_dir


def set_current_version(collection_dir: Path, version: str):
    """Point the collection at a version, in one atomic write of its ``collection.properties``."""
    properties = CollectionProperties.read_from_dir(collection_dir / version)

    def in_version(path: str) -> str:
        return f"{version}/{path}"

    update = {
        "name": CATALOG_NAME,  # the version's own name is its ID
        "hats_primary_table_url": in_version(properties.hats_primary_table_url),
    }
    if properties.all_margins:  # the first version, from working/, has no margin or index
        update["all_margins"] = [in_version(m) for m in properties.all_margins]
        update["default_margin"] = in_version(properties.default_margin)
    if properties.all_indexes:
        update["all_indexes"] = {field: in_version(d) for field, d in properties.all_indexes.items()}
    current = properties.model_copy(update=update)
    # Not to_properties_file: users read this file, so it must be replaced atomically.
    lines = current.model_dump(by_alias=True, exclude_none=True)
    text = "".join(f"{key}={value}\n" for key, value in lines.items())
    write_atomic(collection_dir / "collection.properties", text)


def collection_versions(collection_dir: Path) -> set[str]:
    """The IDs of the versions in the collection."""
    return {p.name for p in collection_dir.iterdir() if p.is_dir() and VERSION_DIR.fullmatch(p.name)}


def remove_versions(collection_dir: Path, versions: set[str]):
    """Delete the directories of the given versions, if they exist."""
    for version in sorted(versions):
        path = collection_dir / version
        if path.is_dir():
            logger.info("Deleting %s...", path)
            shutil.rmtree(path)
