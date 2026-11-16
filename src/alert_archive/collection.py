"""The ``collections/`` collection and its versions.

Each version is a collection of its own, written by hats-import into a
directory named by its ID, the UTC date it was made. The top-level
``collection.properties`` is a copy of the current version's, with its
paths prefixed by the version's directory::

    collections/
    ├── collection.properties                 -> <B>/rubin_alert_archive_<B>, ...
    ├── <A>/                                  previous version, kept for users
    └── <B>/                                  current version
        ├── collection.properties
        ├── rubin_alert_archive_<B>/          the daily runs write here
        ├── rubin_alert_archive_<B>_10arcs/
        └── rubin_alert_archive_<B>_diaSourceId/
"""

import logging
import re
import shutil
from pathlib import Path

from hats.catalog.dataset.collection_properties import CollectionProperties

from .baseline import CATALOG_NAME
from .config import PathConfig
from .state import write_atomic

logger = logging.getLogger(__name__)

VERSION_DIR = re.compile(r"\d{8}")
"""A version's directory name: its ID, e.g. ``20261107``."""


def collections_dir(paths: PathConfig) -> Path:
    """The collection the weekly re-import maintains, ``collections/``, next to ``working/``."""
    return paths.root / "collections"


def current_catalog_dir(paths: PathConfig) -> Path:
    """The catalog the daily runs write to and the weekly re-import reads: ``working/`` until the
    first re-import makes ``collections/``, then the collection's current main catalog.
    """
    collections = collections_dir(paths)
    if not (collections / "collection.properties").exists():
        return paths.working_dir
    return collections / CollectionProperties.read_from_dir(collections).hats_primary_table_url


def set_current_version(collection_dir: Path, version: str):
    """Point the collection at a version, in one atomic write of its ``collection.properties``."""
    properties = CollectionProperties.read_from_dir(collection_dir / version)

    def in_version(path: str) -> str:
        return f"{version}/{path}"

    current = properties.model_copy(
        update={
            "name": CATALOG_NAME,  # the version's own name is its ID
            "hats_primary_table_url": in_version(properties.hats_primary_table_url),
            "all_margins": [in_version(m) for m in properties.all_margins],
            "default_margin": in_version(properties.default_margin),
            "all_indexes": {field: in_version(d) for field, d in properties.all_indexes.items()},
        }
    )
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
