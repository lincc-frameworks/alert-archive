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

The first weekly re-import reads ``working/``, the baseline's plain catalog;
later ones read the collection's current main catalog (see
:func:`current_catalog_dir`).
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
    first re-import makes ``collections/``, then the collection's current main catalog."""
    collections = collections_dir(paths)
    if not (collections / "collection.properties").exists():
        return paths.working_dir
    return collections / CollectionProperties.read_from_dir(collections).hats_primary_table_url


def make_current(collection_dir: Path, version: str):
    """Point the collection at a version, in one atomic write of its ``collection.properties``.

    It's the version's own, with paths prefixed by ``<version>/``. Only that
    version's margin and index are listed, so lsdb can't pair its main
    catalog with another version's margin.
    """
    properties = CollectionProperties.read_from_dir(collection_dir / version)
    properties = properties.model_copy(
        update={
            "name": CATALOG_NAME,
            "hats_primary_table_url": f"{version}/{properties.hats_primary_table_url}",
            "all_margins": [f"{version}/{m}" for m in properties.all_margins],
            "default_margin": f"{version}/{properties.default_margin}",
            "all_indexes": {k: f"{version}/{v}" for k, v in properties.all_indexes.items()},
        }
    )
    # The key=value lines hats would write, written atomically.
    text = "".join(f"{k}={v}\n" for k, v in properties.model_dump(by_alias=True, exclude_none=True).items())
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
