"""Replay the parquet dump's later chunks through the daily run, one night at a time.

The baseline imported the dump's first chunks. The replay feeds the rest to
the daily pipeline as if they had arrived night by night: for each alertId
prefix (roughly one night) it copies that prefix's chunks into the landing
dir, then runs :class:`~alert_archive.daily.run.DailyPipeline`. That's the
backfill, and the mock source until live alerts flow.

Every run sees the whole landing dir, so chunks already there (e.g. from the
converter) are ingested along with the first night. The replay can be
stopped and started again: prefixes whose chunks are all ingested are
skipped. It stops at the first run that hits a format change.
"""

import logging
import shutil
from itertools import groupby
from pathlib import Path

from dask.distributed import Client

from ..baseline import LAST_BASELINE_CHUNK
from ..config import get_default_config
from ..daily.run import CHUNK_NAME, DailyPipeline
from ..notify import OK_MARK, PROBLEM_MARK, send_email
from ..pipeline import Pipeline
from ..state import PipelineState, RunRecord

logger = logging.getLogger(__name__)


def replay_nights(dump_dir, after: str = LAST_BASELINE_CHUNK) -> list[list[Path]]:
    """Group the dump's chunks after the baseline by alertId prefix.

    Parameters
    ----------
    dump_dir : str or path-like
        Directory of the parquet dump.
    after : str
        Name of the last baseline chunk. Only chunks whose names sort after
        it are replayed.

    Returns
    -------
    list[list[pathlib.Path]]
        One list of chunks per prefix, in name order. Chunks not named like
        :data:`~alert_archive.daily.run.CHUNK_NAME` (e.g. the 5-digit
        duplicates) are left out.
    """
    chunks = sorted(
        p for p in Path(dump_dir).glob("*.parquet") if p.name > after and CHUNK_NAME.fullmatch(p.name)
    )
    return [list(group) for _, group in groupby(chunks, key=lambda p: p.name.split("__")[0])]


class ReplayPipeline(Pipeline):
    """Replay the dump's later chunks through daily runs, one prefix per run."""

    def run(
        self,
        client: Client,
        tmp_dir: Path,
        max_nights: int | None = None,
        email: bool = True,
        after: str = LAST_BASELINE_CHUNK,
    ):
        """Copy each night's chunks into the landing dir and ingest them.

        Parameters
        ----------
        client : dask.distributed.Client
            Dask client, shared by every daily run.
        tmp_dir : pathlib.Path
            Temporary directory; each daily run gets its own subdirectory.
        max_nights : int or None
            Stop after this many daily runs. None replays everything.
        email : bool
            Whether to send one summary email at the end. The daily runs
            themselves never email.
        after : str
            Name of the last baseline chunk; see :func:`replay_nights`.

        Returns
        -------
        list[RunRecord]
            The daily runs, in order. Ends early if a run found a format change.
        """
        paths = self.config.paths
        state = PipelineState(paths.state_dir)
        paths.landing_dir.mkdir(parents=True, exist_ok=True)

        records, stopped = [], None
        for night in replay_nights(paths.dump_dir, after):
            if max_nights is not None and len(records) >= max_nights:
                break
            ingested = state.ingested_chunks()
            if all(c.name in ingested for c in night):
                continue
            for chunk in night:
                _copy_into(chunk, paths.landing_dir)
            run_tmp = tmp_dir / f"night{len(records)}"
            run_tmp.mkdir()
            record = DailyPipeline(self.config).run(client, run_tmp, email=False)
            if record is None:
                stopped = f"format change at {night[0].name}; see {state.flag_path}"
                logger.error("Replay stopped: %s", stopped)
                break
            records.append(record)

        if email:
            send_email(*replay_summary(records, stopped))
        return records


def _copy_into(chunk: Path, landing_dir: Path):
    """Copy a chunk into the landing dir under a hidden name, then rename it.

    The daily run ignores hidden files, so it never sees a partial copy.
    """
    target = landing_dir / chunk.name
    if target.exists():
        return
    tmp = landing_dir / f".{chunk.name}.tmp"
    shutil.copyfile(chunk, tmp)
    tmp.rename(target)


def replay_summary(records: list[RunRecord], stopped: str | None) -> tuple[str, str]:
    """Build the email for a replay: one line per daily run."""
    skipped = {chunk: error for r in records for chunk, error in r.skipped.items()}
    total = sum(r.num_rows for r in records)
    status = "STOPPED" if stopped else ("chunks skipped" if skipped else "OK")
    mark = PROBLEM_MARK if stopped or skipped else OK_MARK
    subject = f"{mark} replay: {len(records)} run(s), {total} alerts, {status}"
    lines = [f"  {r.run_id}: {r.num_rows} alerts from {len(r.chunks)} chunk(s)" for r in records]
    if stopped:
        lines += ["", f"Stopped: {stopped}"]
    if skipped:
        lines += ["", "Skipped chunks:", *(f"  {chunk}: {error}" for chunk, error in sorted(skipped.items()))]
    return subject, "\n".join(["Daily runs:", *lines]) + "\n"


def main(config=None, max_nights=None):
    """Replay with the given configuration, or the defaults."""
    return ReplayPipeline(config or get_default_config()).execute(max_nights=max_nights)
