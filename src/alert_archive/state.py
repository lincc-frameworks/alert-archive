"""Pipeline state kept between runs: the ingested log, the format-change flag and the lock.

The state lives in its own directory (``rubin_alert_archive_v2/state/``),
outside the catalog, so it survives the weekly re-import:

- ``ingested.jsonl``: one line per daily run (see :class:`RunRecord`).
  A chunk counts as ingested once a run lists it in ``chunks``. The log is
  always rewritten atomically, so a crash never leaves a partial line.
- ``format_change.json``: present only while flagged. Its existence is the flag.
- ``lock``: held while a daily or weekly job runs, so they never overlap.

Nothing here catches errors or sends email; the jobs decide what to do.
"""

import json
import os
import socket
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

INGESTED_LOG = "ingested.jsonl"
"""File name of the ingested log, one JSON line per daily run."""

FORMAT_CHANGE_FLAG = "format_change.json"
"""File name of the format-change flag."""

LOCK_FILE = "lock"
"""File name of the lock."""

UNKNOWN_SCHEMA_ID = "unknown"
"""JSON key for alerts whose format ID is unknown (None in Python)."""


class LockHeldError(RuntimeError):
    """Raised when another job holds the pipeline lock."""


@dataclass
class RunRecord:
    """One daily run, as written to ``ingested.jsonl``."""

    run_id: str
    """Unique ID of the run. Also the name of the parquet files it wrote."""
    started: str
    """When the run started, as an ISO 8601 UTC timestamp."""
    file_name: str | None = None
    """Name of the parquet file written in each pixel, or None if nothing was written."""
    chunks: list[str] = field(default_factory=list)
    """Input chunks ingested by this run."""
    num_rows: int = 0
    """Number of alerts written."""
    schema_id_counts: dict[int | None, int] = field(default_factory=dict)
    """Number of alerts per format ID. None means the format is unknown."""
    skipped: dict[str, str] = field(default_factory=dict)
    """Input chunks that failed, with their error. They aren't ingested, so the next run retries them."""

    def to_json(self) -> str:
        """Serialize as one JSON line. Unknown format IDs are written as ``"unknown"``."""
        data = asdict(self)
        data["schema_id_counts"] = {_schema_id_key(k): v for k, v in self.schema_id_counts.items()}
        return json.dumps(data)

    @classmethod
    def from_json(cls, line: str) -> "RunRecord":
        """Parse one line written by :meth:`to_json`."""
        data = json.loads(line)
        data["schema_id_counts"] = {_schema_id_from_key(k): v for k, v in data["schema_id_counts"].items()}
        return cls(**data)


def _schema_id_key(schema_id: int | None) -> str:
    """Turn a format ID into a JSON key: ``1100`` -> ``"1100"``, None -> ``"unknown"``."""
    return UNKNOWN_SCHEMA_ID if schema_id is None else str(schema_id)


def _schema_id_from_key(key: str) -> int | None:
    """Inverse of :func:`_schema_id_key`: ``"1100"`` -> ``1100``, ``"unknown"`` -> None."""
    return None if key == UNKNOWN_SCHEMA_ID else int(key)


def new_run_id(now: datetime | None = None) -> str:
    """Make a run ID from the current UTC time, e.g. ``"20261027T031500Z"``.

    Runs never overlap (see :meth:`PipelineState.lock`), so a timestamp to
    the second is unique.

    Parameters
    ----------
    now : datetime or None
        Time to use instead of the current time. Must be timezone-aware.

    Returns
    -------
    str
        The run ID.
    """
    now = now or datetime.now(UTC)
    return now.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


class PipelineState:
    """The pipeline's state directory.

    Parameters
    ----------
    root : str or path-like
        The state directory. It is created if it doesn't exist.
    """

    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    @property
    def log_path(self) -> Path:
        """Path to ``ingested.jsonl``."""
        return self.root / INGESTED_LOG

    @property
    def flag_path(self) -> Path:
        """Path to ``format_change.json``."""
        return self.root / FORMAT_CHANGE_FLAG

    @property
    def lock_path(self) -> Path:
        """Path to the lock file."""
        return self.root / LOCK_FILE

    # ---------- ingested log ----------

    def runs(self) -> list[RunRecord]:
        """All runs in the ingested log, oldest first."""
        if not self.log_path.exists():
            return []
        with open(self.log_path) as f:
            return [RunRecord.from_json(line) for line in f if line.strip()]

    def append_run(self, record: RunRecord):
        """Add a run to the end of the ingested log.

        Call this only after the run's files are written, so a crash before
        then leaves its chunks unlisted and the next run retries them.

        The whole log is rewritten atomically, so a crash leaves either the old
        log or the new one, never a partial line. The log grows by one line
        per run, so rewriting it is cheap.

        Raises
        ------
        ValueError
            If a run with the same ID is already in the log.
        """
        runs = self.runs()
        if any(r.run_id == record.run_id for r in runs):
            raise ValueError(f"run {record.run_id!r} is already in {self.log_path}")
        _write_atomic(self.log_path, "".join(r.to_json() + "\n" for r in [*runs, record]))

    def reingest_runs(self, catalog_dir, run_ids, missing_files_ok: bool = False) -> list[RunRecord]:
        """Delete the catalog files that runs wrote and take the runs off the log.

        The next daily run then ingests their chunks again, into new files
        named by its own run ID. Use this to redo runs that wrote bad data,
        e.g. the runs flagged for a format change, once the code handles the
        new format.

        Only use this before the weekly re-import has merged the runs' files
        into the catalog. After that, there are no separate files to delete,
        and re-ingesting the chunks would add their alerts a second time.
        While the format-change flag is set, the weekly re-import doesn't run,
        so flagged runs are safe to redo until the flag is cleared.

        Every run is checked before anything is deleted. Files are deleted
        before the log is changed, so if this stops partway, the runs are
        still in the log and nothing is ingested twice.

        This takes the pipeline lock (as job ``"reingest"``), so it can't run
        while a daily or weekly job is writing the catalog or the log.

        Only the runs' parquet files are deleted. The catalog's skymaps and
        metadata (partition info, row counts) still count them until the next
        weekly re-import rebuilds them. That's accepted: re-ingesting happens
        only while the format-change flag is set, so nothing is published
        from the working catalog in the meantime.

        Parameters
        ----------
        catalog_dir : str or path-like
            The working catalog the runs wrote to.
        run_ids : iterable of str
            IDs of the runs to redo.
        missing_files_ok : bool
            If True, runs with no files left are taken off the log anyway.
            Use this only when you know the files were deleted rather than
            merged, e.g. when an earlier call stopped after deleting them
            but before changing the log.

        Returns
        -------
        list[RunRecord]
            The removed runs.

        Raises
        ------
        KeyError
            If any of the IDs isn't in the log. Nothing is deleted or removed then.
        FileNotFoundError
            If a run that wrote files has none left in the catalog, most
            likely because the weekly re-import merged them, and
            ``missing_files_ok`` is False. Nothing is deleted or removed then.
        LockHeldError
            If another job holds the pipeline lock. Nothing is deleted or removed then.
        """
        catalog_dir = Path(catalog_dir)
        with self.lock("reingest"):
            runs = self._find_runs(run_ids)
            files = {r.run_id: list(catalog_dir.glob(f"**/Npix=*/{r.file_name}")) for r in runs if r.file_name}
            merged = [run_id for run_id, paths in files.items() if not paths]
            if merged and not missing_files_ok:
                raise FileNotFoundError(
                    f"no Npix=*/<file> files in {catalog_dir} for runs {merged}. If the weekly "
                    "re-import merged them, re-ingesting would add their alerts a second time. "
                    "If you know they were deleted instead, pass missing_files_ok=True."
                )
            for path in (p for paths in files.values() for p in paths):
                path.unlink()
            return self._remove_runs(r.run_id for r in runs)

    def _remove_runs(self, run_ids) -> list[RunRecord]:
        """Take runs off the ingested log, so the next daily run ingests their chunks again.

        Only :meth:`reingest_runs` should call this, holding the lock, after
        deleting the runs' catalog files. Re-ingested chunks go into new files named by the new
        run ID, so any old files left in place would duplicate their alerts.

        The log is rewritten atomically: a reader sees either the old log or
        the new one, never a partial file.

        Parameters
        ----------
        run_ids : iterable of str
            IDs of the runs to remove.

        Returns
        -------
        list[RunRecord]
            The removed runs' records, in log order.

        Raises
        ------
        KeyError
            If any of the IDs isn't in the log. Nothing is removed then.
        """
        removed = self._find_runs(run_ids)
        removed_ids = {r.run_id for r in removed}
        kept = [r for r in self.runs() if r.run_id not in removed_ids]
        _write_atomic(self.log_path, "".join(r.to_json() + "\n" for r in kept))
        return removed

    def _find_runs(self, run_ids) -> list[RunRecord]:
        """Look up runs in the ingested log by ID.

        Parameters
        ----------
        run_ids : iterable of str
            IDs of the runs to look up.

        Returns
        -------
        list[RunRecord]
            The log's records for those runs, in the order they appear in the log.

        Raises
        ------
        KeyError
            If any of the IDs isn't in the log.
        """
        run_ids = set(run_ids)
        runs = [r for r in self.runs() if r.run_id in run_ids]
        missing = run_ids - {r.run_id for r in runs}
        if missing:
            raise KeyError(f"runs not in {self.log_path}: {sorted(missing)}")
        return runs

    def ingested_chunks(self) -> set[str]:
        """Names of all input chunks that some run has ingested."""
        return {chunk for run in self.runs() for chunk in run.chunks}

    def failing_chunks(self) -> dict[str, tuple[str, str]]:
        """Chunks whose latest attempt failed, with when they started failing.

        Returns
        -------
        dict[str, tuple[str, str]]
            For each chunk that was skipped and not ingested since: the
            ``started`` time of the first run in its current run of failures,
            and the latest error.
        """
        failing = {}
        for run in self.runs():
            for chunk in run.chunks:
                failing.pop(chunk, None)
            for chunk, error in run.skipped.items():
                since = failing[chunk][0] if chunk in failing else run.started
                failing[chunk] = (since, error)
        return failing

    # ---------- format-change flag ----------

    def format_change(self) -> dict | None:
        """The format-change flag's contents, or None if the flag isn't set."""
        if not self.flag_path.exists():
            return None
        with open(self.flag_path) as f:
            return json.load(f)

    def flag_format_change(self, run_id: str, details: dict, now: datetime | None = None):
        """Set the format-change flag, or add a run to it if it's already set.

        The flag keeps when it was first set and every run that hit a format
        change since, so a person knows which runs to re-ingest.

        Parameters
        ----------
        run_id : str
            The run that found the format change.
        details : dict
            What changed, e.g. unknown format IDs and the fields dropped or
            set to null. Must be JSON-serializable.
        now : datetime or None
            Time to record instead of the current time.
        """
        now = (now or datetime.now(UTC)).isoformat()
        flag = self.format_change() or {"flagged_at": now, "runs": []}
        flag["runs"].append({"run_id": run_id, "time": now, "details": details})
        _write_atomic(self.flag_path, json.dumps(flag, indent=2) + "\n")

    def clear_format_change(self):
        """Clear the format-change flag. Does nothing if it isn't set."""
        self.flag_path.unlink(missing_ok=True)

    # ---------- lock ----------

    @contextmanager
    def lock(self, job: str) -> Generator[None]:
        """Hold the pipeline lock while the ``with`` block runs.

        The lock file records who holds it. It is removed on exit, also when
        the block raises. If a job dies without removing it, the next job
        fails with :class:`LockHeldError` until a person checks and deletes it.

        Parameters
        ----------
        job : str
            Name of the job taking the lock, e.g. ``"daily"``.

        Raises
        ------
        LockHeldError
            If the lock is already held.
        """
        holder = {
            "job": job,
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "since": datetime.now(UTC).isoformat(),
        }
        try:
            fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            raise LockHeldError(
                f"{self.lock_path} is held by {self.lock_path.read_text().strip()}. "
                "If that job is no longer running, delete the file."
            ) from None
        try:
            # Inside the try, so a failed write (e.g. disk full) doesn't leave an empty lock behind.
            with os.fdopen(fd, "w") as f:
                json.dump(holder, f)
            yield
        finally:
            self.lock_path.unlink(missing_ok=True)


def _write_atomic(path: Path, text: str):
    """Replace a file's contents so readers see either the old or the new version."""
    tmp = path.with_name(f".{path.name}.tmp")
    with open(tmp, "w") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
