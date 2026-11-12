"""Weekly re-import: rebalance the catalog and build a new collection.

The daily runs only add files to existing pixels, and the margin and index don't
cover their alerts. Once a week, this re-imports the current catalog as a new
version of the alert archive collection, with a margin and a ``diaSourceId`` index,
and points ``collection.properties`` at it. One previous version is kept.
Versions are UTC dates (see :mod:`alert_archive.collection` for the layout).
"""

import logging
from pathlib import Path

import hats
from dask.distributed import Client

from ..collection import (
    collection_versions,
    collections_dir,
    current_catalog_dir,
    make_current,
    remove_versions,
)
from ..config import DaskConfig, PathConfig, get_default_config
from ..daily.increment import remove_orphan_files, update_metadata
from ..pipeline import Pipeline
from ..state import PipelineState, new_run_id
from .reimport import build_version, check_reimport

logger = logging.getLogger(__name__)

WEEKLY_DASK = DaskConfig(n_workers=16, memory_limit="24GiB")
"""Dask settings for the re-import, the same as the baseline's: a whole milano node."""


class WeeklyPipeline(Pipeline):
    """Re-import the catalog as a new version of the collection and make it current."""

    def run(self, client: Client, tmp_dir: Path) -> Path | None:
        """Re-import the catalog as a new version and make it current.

        It reads ``working/`` the first time, then the collection's current main
        catalog (:func:`~alert_archive.collection.current_catalog_dir`). Holding
        the pipeline lock, it:

        1. skips if the format-change flag is set, and refuses if the current
           version is already today's;
        2. deletes orphaned daily files and rebuilds the metadata from disk, so
           orphans aren't merged and then ingested again, and files a re-ingest
           deleted aren't counted;
        3. re-imports into ``<version>/``, a collection with the margin and index;
        4. checks the new catalog has the old one's row count and schema;
        5. marks the merged runs consolidated;
        6. points ``collection.properties`` at the new version, atomically;
        7. deletes every version but the new and previous ones.

        If a step before 6 fails, the collection is unchanged, and the next
        run (even a retry the same day) deletes the failed version's directory.

        Returns
        -------
        pathlib.Path or None
            The new main catalog, or None if the format-change flag is set.

        Raises
        ------
        ValueError
            If the current version is already today's.
        alert_archive.state.LockHeldError
            If another job holds the pipeline lock.
        alert_archive.weekly.reimport.ReimportCheckError
            If the row counts differ.
        alert_archive.daily.increment.SchemaMismatchError
            If the schemas differ.
        """
        paths = self.config.paths
        state = PipelineState(paths.state_dir)
        with state.lock("weekly"):
            if state.format_change() is not None:
                logger.warning("The format-change flag is set; skipping the re-import until it's cleared.")
                return None

            collection_dir = collections_dir(paths)
            source_dir = current_catalog_dir(paths)
            # None on the first re-import, which reads working/.
            previous = None if source_dir == paths.working_dir else source_dir.parent.name
            version = new_run_id(date_only=True)
            if version == previous:
                raise ValueError(
                    f"{source_dir} is already today's version. Versions are named by their date, "
                    "so the re-import runs at most once a day."
                )
            # clean leftovers of a failed run earlier today
            remove_versions(collection_dir, {version})

            runs = state.runs()
            remove_orphan_files(source_dir, [r.file_name for r in runs if r.file_name])
            update_metadata(hats.read_hats(source_dir))

            catalog_dir = build_version(
                client, source_dir, collection_dir, version, self.config.import_config
            )
            check_reimport(source_dir, catalog_dir)

            state.mark_consolidated(r.run_id for r in runs if not r.consolidated)
            make_current(collection_dir, version)
            logger.info("%s now points to version %s.", collection_dir, version)
            remove_versions(collection_dir, collection_versions(collection_dir) - {version, previous})
        return catalog_dir


def main(root=None):
    """Run the re-import with :data:`WEEKLY_DASK`, on the archive under ``root`` (by default the real one)."""
    paths = PathConfig() if root is None else PathConfig(root=root)
    return WeeklyPipeline(get_default_config(paths=paths, dask=WEEKLY_DASK)).execute()
