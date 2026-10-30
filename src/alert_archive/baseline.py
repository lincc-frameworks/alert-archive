"""Build the working catalog from the first chunks of the parquet dump.

This runs once, before any daily run. It imports the dump's chunks from
2026-06-26 (very likely the frozen catalog's input) with hats-import into an
empty ``working/`` directory. The later chunks are replayed through the daily
pipeline afterwards.

The catalog uses leaf pixel directories, and follows ppdb_hats' file naming:
consolidated files, from this import and later weekly re-imports, keep
hats-import's default name ``Npix=<pixel>/Npix=<pixel>.parquet``, while daily
runs add ``Npix=<pixel>/<run_id>.parquet``.

The import is logged in ``ingested.jsonl`` as one run, so its chunks count as
ingested and the replay skips them.
"""

import logging
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import hats
import pyarrow.compute as pc
import pyarrow.parquet as pq
from dask.distributed import Client
from hats_import import pipeline_with_client
from hats_import.catalog.arguments import ImportArguments

from .daily.increment import leaf_files
from .pipeline import Pipeline
from .reader import AlertReader
from .schema import SCHEMA_ID_COLUMN
from .state import PipelineState, RunRecord, new_run_id

logger = logging.getLogger(__name__)

LAST_BASELINE_CHUNK = "170587__00100000-00138048.parquet"
"""The last dump chunk in the baseline, by name.

The dump's chunks are named ``<alertId prefix>__<start>-<end>.parquet``, and
all 347 chunks written on 2026-06-26 sort before every later one. Selecting by
name rather than by file date keeps the baseline the same if files are copied.
"""

CATALOG_NAME = "rubin_alert_archive"
"""Catalog name in ``hats.properties``, the same as the frozen catalog's."""

DEFAULT_COLUMNS = [
    "ra",
    "dec",
    "diaSourceId",
    "diaSource.diaObjectId",
    "diaSource.ssObjectId",
    "diaSource.visit",
    "diaSource.detector",
    "diaSource.midpointMjdTai",
    "diaSource.band",
    "diaSource.psfFlux",
    "diaSource.psfFluxErr",
    "diaSource.scienceFlux",
    "diaSource.scienceFluxErr",
    "diaSource.snr",
    "diaSource.reliability",
    "diaObject.diaObjectId",
    "diaObject.nDiaSources",
    "diaObject.firstDiaSourceMjdTai",
    "diaObject.lastDiaSourceMjdTai",
    "prvDiaForcedSources.diaForcedSourceId",
    "prvDiaForcedSources.visit",
    "prvDiaForcedSources.midpointMjdTai",
    "prvDiaForcedSources.band",
    "prvDiaForcedSources.psfFlux",
    "prvDiaForcedSources.psfFluxErr",
]
"""Columns ``lsdb.open_catalog`` loads by default, copied from the frozen catalog."""


def baseline_chunks(dump_dir, last_chunk: str = LAST_BASELINE_CHUNK) -> list[Path]:
    """List the dump chunks that make up the baseline.

    Parameters
    ----------
    dump_dir : str or path-like
        Directory of the parquet dump.
    last_chunk : str
        Name of the last chunk to include. Every chunk whose name sorts at or
        before it is included.

    Returns
    -------
    list[pathlib.Path]
        The chunks, sorted by name.

    Raises
    ------
    FileNotFoundError
        If no chunk in ``dump_dir`` sorts at or before ``last_chunk``.
    """
    chunks = sorted(p for p in Path(dump_dir).glob("*.parquet") if p.name <= last_chunk)
    if not chunks:
        raise FileNotFoundError(f"no parquet chunks up to {last_chunk} in {dump_dir}")
    return chunks


class BaselinePipeline(Pipeline):
    """Import the baseline chunks into a new, empty working catalog."""

    def run(self, client: Client, tmp_dir: Path, chunks=None) -> RunRecord:
        """Import the chunks and log the import as one run.

        Parameters
        ----------
        client : dask.distributed.Client
            Dask client for hats-import.
        tmp_dir : pathlib.Path
            Temporary directory for hats-import's intermediate files.
        chunks : list of path-like or None
            Chunks to import. Defaults to :func:`baseline_chunks` of the
            configured dump.

        Returns
        -------
        RunRecord
            The run added to the ingested log.

        Raises
        ------
        FileExistsError
            If the working catalog already exists, or the ingested log already
            has runs. The baseline never replaces either; delete them first to
            rebuild from scratch.
        alert_archive.state.LockHeldError
            If another job holds the pipeline lock.
        """
        paths = self.config.paths
        import_config = self.config.import_config
        chunks = [Path(c) for c in chunks] if chunks is not None else baseline_chunks(paths.dump_dir)
        state = PipelineState(paths.state_dir)

        with state.lock("baseline"):
            if paths.working_dir.exists():
                raise FileExistsError(f"{paths.working_dir} already exists")
            if state.runs():
                raise FileExistsError(f"{state.log_path} already has runs")

            run_id = new_run_id()
            started = datetime.now(UTC).isoformat()
            logger.info("Importing %d chunks into %s as run %s...", len(chunks), paths.working_dir, run_id)
            args = ImportArguments(
                output_path=paths.working_dir.parent,
                output_artifact_name=paths.working_dir.name,
                input_file_list=[str(c) for c in chunks],
                file_reader=AlertReader(),
                ra_column=import_config.ra_column,
                dec_column=import_config.dec_column,
                byte_pixel_threshold=import_config.byte_pixel_threshold,
                npix_suffix="/",
                addl_hats_properties={"hats_cols_default": DEFAULT_COLUMNS},
                tmp_dir=tmp_dir,
                resume=import_config.resume,
                simple_progress_bar=import_config.simple_progress_bar,
            )
            pipeline_with_client(args, client)
            _set_catalog_name(paths.working_dir, CATALOG_NAME)

            schema_id_counts = _schema_id_counts(paths.working_dir)
            record = RunRecord(
                run_id=run_id,
                started=started,
                chunks=[c.name for c in chunks],
                num_rows=sum(schema_id_counts.values()),
                schema_id_counts=schema_id_counts,
            )
            state.append_run(record)
        logger.info("Baseline complete: %d alerts.", record.num_rows)
        return record


def _set_catalog_name(catalog_dir, name: str):
    """Rename the catalog in ``hats.properties``.

    hats-import always names it after its output directory (``working``).
    """
    catalog = hats.read_hats(catalog_dir)
    catalog.catalog_info.copy_and_update(catalog_name=name).to_properties_file(catalog_dir)


def _schema_id_counts(catalog_dir) -> dict[int | None, int]:
    """Count the catalog's alerts per format ID, reading only that column."""
    counts = Counter()
    for files in leaf_files(catalog_dir).values():
        for path in files:
            column = pq.read_table(path, columns=[SCHEMA_ID_COLUMN])[SCHEMA_ID_COLUMN]
            for entry in pc.value_counts(column.combine_chunks()):
                counts[entry["values"].as_py()] += entry["counts"].as_py()
    return dict(counts)


def main(config=None):
    """Build the baseline with the default (or given) configuration."""
    return BaselinePipeline(config).execute()
