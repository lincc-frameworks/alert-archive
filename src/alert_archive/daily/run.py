"""The daily run: ingest new chunks from the landing dir into the working catalog.

Each run, while holding the pipeline lock:

1. Stops (and emails) if the format-change flag is set.
2. Deletes files left by a crashed run (:func:`~.increment.remove_orphan_files`).
3. Finds landing chunks that no run has ingested yet. Chunks with unexpected
   names (e.g. the raw archive's duplicate 5-digit prefix directories) or
   unreadable footers are skipped and reported; the next run retries them.
4. Checks every chunk's format from its footer, before anything is imported.
   If any chunk is in a format the catalog doesn't support, the run writes
   nothing, sets the format-change flag and emails.
5. Imports the chunks with hats-import, aligned to the working catalog's
   pixels, then writes them into the catalog as ``Npix=<pixel>/<run_id>.parquet``
   and rebuilds the skymaps and metadata from disk.
6. Logs the run in ``ingested.jsonl`` and sends the summary email.

A run with no new chunks is still logged and emailed, so a missing email
means the run didn't happen.
"""

import logging
import re
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import hats
import lsdb
import pyarrow.compute as pc
import pyarrow.parquet as pq
from dask.distributed import Client
from hats_import import pipeline_with_client
from hats_import.catalog.arguments import ImportArguments

from ..config import get_default_config
from ..notify import daily_summary, flag_set_summary, send_email
from ..pipeline import Pipeline
from ..reader import AlertReader
from ..schema import SCHEMA_ID_COLUMN, ConformReport, conform_alerts, infer_schema_id
from ..state import PipelineState, RunRecord, new_run_id
from .increment import remove_orphan_files, update_metadata, update_skymaps, write_partitions

logger = logging.getLogger(__name__)

CHUNK_NAME = re.compile(r"\d{6}__\d{8}-\d{8}\.parquet")
"""Name of a landing chunk: ``<6-digit alertId prefix>__<start>-<end>.parquet``.

Other names are skipped. In particular, the raw archive once filed alerts
under 5-digit prefix directories as well as their usual 6-digit ones (see
:data:`alert_archive.baseline.DUPLICATE_PREFIXES`), and converting those
would ingest every alert twice.
"""

INCREMENT_NAME = "increment"
"""Name of the temporary catalog the night's alerts are imported into."""


class DailyPipeline(Pipeline):
    """Ingest the landing dir's new chunks into the working catalog."""

    def run(self, client: Client, tmp_dir: Path, email: bool = True) -> RunRecord | None:
        """Run once, as described in the module docstring.

        Parameters
        ----------
        client : dask.distributed.Client
            Dask client for hats-import and the partition writes.
        tmp_dir : pathlib.Path
            Temporary directory for the night's import.
        email : bool
            Whether to send the summary email. The replay turns this off and
            sends one email at the end.

        Returns
        -------
        RunRecord or None
            The run added to the ingested log, or None if the run stopped for
            a format change (new, or already flagged) and logged nothing.

        Raises
        ------
        FileNotFoundError
            If the ingested log has no runs, i.e. the baseline hasn't been built.
        alert_archive.state.LockHeldError
            If another job holds the pipeline lock.
        """
        paths = self.config.paths
        state = PipelineState(paths.state_dir)

        with state.lock("daily"):
            if flag := state.format_change():
                logger.error("Format-change flag set since %s; not ingesting.", flag["flagged_at"])
                if email:
                    send_email(*flag_set_summary(flag))
                return None

            runs = state.runs()
            if not runs:
                raise FileNotFoundError(f"{state.log_path} has no runs; build the baseline first")
            remove_orphan_files(paths.working_dir, [r.file_name for r in runs if r.file_name])

            run_id = new_run_id()
            started = datetime.now(UTC).isoformat()
            chunks, skipped = find_new_chunks(paths.landing_dir, state.ingested_chunks())
            reports, unreadable = check_formats(chunks)
            skipped.update(unreadable)
            chunks = [c for c in chunks if c.name in reports]

            changes = {name: format_change_details(r) for name, r in reports.items() if r.is_format_change}
            if changes:
                logger.error("Format change in %d chunk(s); not ingesting: %s", len(changes), changes)
                state.flag_format_change(run_id, changes)
                if email:
                    record = RunRecord(run_id=run_id, started=started, skipped=skipped)
                    send_email(*daily_summary(record, state.failing_chunks(), format_change=changes))
                return None

            num_rows = 0
            if chunks:
                logger.info("Ingesting %d chunks as run %s...", len(chunks), run_id)
                increment = import_increment(
                    client, chunks, paths.working_dir, tmp_dir, self.config.import_config
                )
                rows = write_partitions(
                    increment, base_catalog_dir=paths.working_dir, parquet_name=f"{run_id}.parquet"
                )
                num_rows = sum(rows.values())
                working = hats.read_hats(paths.working_dir)
                update_skymaps(working)
                update_metadata(working)

            schema_id_counts = Counter()
            for report in reports.values():
                schema_id_counts.update(report.schema_ids)
            record = RunRecord(
                run_id=run_id,
                started=started,
                chunks=[c.name for c in chunks],
                num_rows=num_rows,
                schema_id_counts=dict(schema_id_counts),
                skipped=skipped,
            )
            state.append_run(record)

        logger.info("Run %s complete: %d alerts from %d chunks.", run_id, num_rows, len(chunks))
        if email:
            send_email(*daily_summary(record, state.failing_chunks()))
        return record


def find_new_chunks(landing_dir, ingested) -> tuple[list[Path], dict[str, str]]:
    """List the landing chunks that no run has ingested yet.

    Parameters
    ----------
    landing_dir : str or path-like
        The landing directory.
    ingested : set of str
        Names of chunks already ingested.

    Returns
    -------
    list[pathlib.Path]
        New chunks named like :data:`CHUNK_NAME`, sorted by name.
    dict[str, str]
        New ``.parquet`` files with any other name, with the reason they're
        skipped. Hidden files (e.g. a converter's temporary files) are ignored.
    """
    chunks, skipped = [], {}
    for path in sorted(Path(landing_dir).glob("*.parquet")):
        if path.name.startswith(".") or path.name in ingested:
            continue
        if CHUNK_NAME.fullmatch(path.name):
            chunks.append(path)
        else:
            skipped[path.name] = "unexpected name, not <6-digit alertId prefix>__<start>-<end>.parquet"
    return chunks, skipped


def check_formats(chunks) -> tuple[dict[str, ConformReport], dict[str, str]]:
    """Check each chunk's alert format from its parquet footer, without reading the alerts.

    The footer's schema goes through :func:`~alert_archive.schema.conform_alerts`
    as an empty table, so fields that would be dropped, filled or converted
    are reported just as for the real alerts. Format IDs come from the
    ``schemaId`` column if the chunk has one, else they're inferred from the
    columns. Values that fail to convert can only be found by reading the
    alerts, which the import does.

    Parameters
    ----------
    chunks : list of path-like
        Chunks to check.

    Returns
    -------
    dict[str, ConformReport]
        Report per readable chunk, by name.
    dict[str, str]
        Chunks whose footer couldn't be read, with the error.
    """
    reports, unreadable = {}, {}
    for chunk in chunks:
        chunk = Path(chunk)
        try:
            parquet_file = pq.ParquetFile(chunk)
            schema = parquet_file.schema_arrow
            _, report = conform_alerts(schema.empty_table())
            if SCHEMA_ID_COLUMN in schema.names:
                column = parquet_file.read(columns=[SCHEMA_ID_COLUMN]).column(0).combine_chunks()
                report.schema_ids = {
                    c["values"].as_py(): c["counts"].as_py() for c in pc.value_counts(column)
                }
            else:
                report.schema_ids = {infer_schema_id(schema): parquet_file.metadata.num_rows}
        except Exception as exc:  # noqa: BLE001 - any unreadable chunk is skipped and reported
            logger.warning("Skipping %s: %s", chunk.name, exc)
            unreadable[chunk.name] = f"{type(exc).__name__}: {exc}"
            continue
        reports[chunk.name] = report
    return reports, unreadable


def format_change_details(report: ConformReport) -> dict:
    """The parts of a report that make it a format change, as JSON-serializable values."""
    details = {
        "unknown_schema_ids": [None if i is None else int(i) for i in report.unknown_schema_ids],
        "dropped": report.dropped,
        "cast": report.cast,
        "nulled": report.nulled,
    }
    return {k: v for k, v in details.items() if v}


def import_increment(client: Client, chunks, working_dir, tmp_dir: Path, import_config):
    """Import the chunks into a temporary catalog aligned to the working catalog's pixels.

    hats-import's ``existing_pixels`` keeps every existing pixel as it is
    (never split), and places alerts outside them in new pixels.

    Returns
    -------
    lsdb.Catalog
        The new alerts, with all columns.
    """
    existing_pixels = [(p.order, p.pixel) for p in hats.read_hats(working_dir).get_healpix_pixels()]
    args = ImportArguments(
        output_path=tmp_dir,
        output_artifact_name=INCREMENT_NAME,
        input_file_list=[str(c) for c in chunks],
        file_reader=AlertReader(),
        ra_column=import_config.ra_column,
        dec_column=import_config.dec_column,
        pixel_threshold=import_config.pixel_threshold,
        existing_pixels=existing_pixels,
        tmp_dir=tmp_dir,
        resume=False,
        simple_progress_bar=import_config.simple_progress_bar,
    )
    pipeline_with_client(args, client)
    return lsdb.open_catalog(tmp_dir / INCREMENT_NAME, columns="all")


def main(config=None):
    """Run the daily pipeline with the given configuration, or the defaults."""
    return DailyPipeline(config or get_default_config()).execute()
