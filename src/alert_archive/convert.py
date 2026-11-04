"""Convert the raw Avro alerts to parquet chunks for the daily run.

Each alert file in the raw archive (:data:`ARCHIVE_DIR`) is decoded, and the
alerts are written to parquet files of up to :data:`CHUNK_SIZE` alerts
("chunks"), with the columns of :func:`~alert_archive.schema.target_schema`.

Conversion has three steps, each also a command (``alert-archive convert``):

1. :func:`plan` lists the new archive subdirectories and plans their chunks.
2. :func:`run` converts the planned chunks; several jobs can share the work.
3. :func:`finish` records the fully converted subdirectories in the manifest.

See ``scripts/README.md`` for how to run them.
"""

import contextlib
import glob
import gzip
import io
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq

from alert_archive.schema import (
    EXCLUDED_COLUMNS,
    SCHEMA_ID_COLUMN,
    known_avro_schema,
    known_schema_ids,
    target_schema,
)

logger = logging.getLogger(__name__)

ARCHIVE_DIR = Path("/sdf/data/rubin/shared/alert-archive/v2/alerts")
"""The raw alert archive on S3DF."""

MANIFEST_NAME = "manifest.json"
"""File in the output directory listing the converted subdirectories and their chunks."""

PLAN_DIR_NAME = "plan"
"""Directory in the output directory holding the plan, while there is one."""

PLAN_NAME = "plan.json"
"""File in the plan directory listing the planned subdirectories and their chunks."""

CHUNK_SIZE = 50_000
"""Alerts per chunk. A subdirectory's last chunk holds the rest."""

THREADS = 64
"""Files decoded at once. Opening a file on S3DF's shared filesystem takes about 220 ms,
so many are opened at once; 64 per node was the fastest (about 270 files/s)."""

MAX_LOGGED_ERRORS = 10
"""Decoding errors logged per chunk, followed by how many more there are."""


# ---------------------------------------------------------------- decoding


class UnknownFormatError(Exception):
    """An alert's format ID has no schema in ``alert_archive/schemas``."""

    def __init__(self, schema_id: int):
        super().__init__(f"unknown alert format {schema_id}")
        self.schema_id = schema_id


def decode_alert(path) -> dict:
    """Decode one alert file from the archive.

    The file is a gzipped Confluent wire-format message: a zero byte, a
    4-byte format ID, then the alert in Avro.

    Parameters
    ----------
    path : str or path-like
        An ``.avro.gz`` file.

    Returns
    -------
    dict
        The alert's fields plus ``schemaId``, minus
        :data:`~alert_archive.schema.EXCLUDED_COLUMNS`.

    Raises
    ------
    UnknownFormatError
        If the package has no schema for the alert's format.
    ValueError
        If the file isn't a Confluent wire-format message.
    """
    with open(path, "rb") as fh:
        message = gzip.decompress(fh.read())
    if len(message) < 5 or message[0] != 0:
        raise ValueError(f"not a Confluent wire-format message (starts with {message[:5]!r})")
    schema_id = int.from_bytes(message[1:5], "big")
    if schema_id not in known_schema_ids():
        raise UnknownFormatError(schema_id)
    alert = fastavro.schemaless_reader(io.BytesIO(message[5:]), _parsed_schema(schema_id))
    for name in EXCLUDED_COLUMNS:
        alert.pop(name, None)  # the cutouts are ~99% of the bytes, so drop them right away
    alert[SCHEMA_ID_COLUMN] = schema_id
    return alert


@cache
def _parsed_schema(schema_id: int):
    """fastavro's parsed form of a known format's schema."""
    return fastavro.parse_schema(known_avro_schema(schema_id))


# ---------------------------------------------------------------- chunks


@dataclass
class ChunkResult:
    """What happened to one chunk."""

    name: str
    """File name of the chunk, e.g. ``"170019__00000000-00050000.parquet"``."""
    n_files: int
    """Alert files in the chunk."""
    status: str
    """``"written"``, ``"exists"`` (written earlier) or ``"failed"`` (not written)."""
    reason: str = ""
    """Why the chunk failed."""
    errors: list[str] = field(default_factory=list)
    """Files that failed to decode, with their errors."""
    seconds: float = 0.0
    """Time taken to convert the chunk."""


def chunk_name(subdir: str, start: int, end: int) -> str:
    """Name of the chunk holding files ``start`` to ``end - 1`` of a subdirectory, sorted by name."""
    return f"{subdir}__{start:08d}-{end:08d}.parquet"


def convert_chunk(paths, out_path, threads: int = THREADS) -> ChunkResult:
    """Convert alert files to one parquet file, unless it exists already.

    The chunk is written whole or not at all: if any file fails to decode,
    or has an unknown format, nothing is written.

    Parameters
    ----------
    paths : list[str]
        The chunk's alert files.
    out_path : str or path-like
        Parquet file to write.
    threads : int
        Files decoded at once.

    Returns
    -------
    ChunkResult
        What happened.
    """
    out_path = Path(out_path)
    if _is_converted(out_path):
        return ChunkResult(out_path.name, len(paths), "exists")

    start = time.monotonic()
    alerts, errors, unknown_formats = _decode_all(paths, threads)
    if unknown_formats:
        reason = (
            f"unknown alert format(s) {sorted(unknown_formats)}; "
            "add the schema to alert_archive/schemas, then run again"
        )
        return ChunkResult(out_path.name, len(paths), "failed", reason, errors)
    if errors:
        reason = f"{len(errors)} of {len(paths)} files failed to decode"
        return ChunkResult(out_path.name, len(paths), "failed", reason, errors)

    _write_parquet(pa.Table.from_pylist(alerts, schema=target_schema()), out_path)
    return ChunkResult(out_path.name, len(paths), "written", seconds=time.monotonic() - start)


def _decode_all(paths, threads: int) -> tuple[list[dict], list[str], set[int]]:
    """Decode files in parallel. Returns the alerts, the errors and the unknown format IDs."""

    # Failures come back as small values rather than exceptions, because a
    # traceback would keep the whole decompressed file (cutouts included) alive.
    def decode(path):
        try:
            return decode_alert(path)
        except UnknownFormatError as exc:
            return exc.schema_id
        except Exception as exc:  # keep going, to report every bad file at once
            return f"{path}: {type(exc).__name__}: {exc}"

    alerts, errors, unknown_formats = [], [], set()
    with ThreadPoolExecutor(max_workers=threads) as pool:
        for result in pool.map(decode, paths):
            if isinstance(result, dict):
                alerts.append(result)
            elif isinstance(result, int):
                unknown_formats.add(result)
            else:
                errors.append(result)
    return alerts, errors, unknown_formats


def _is_converted(path: Path) -> bool:
    """Whether a chunk exists. Chunks are written atomically, so an existing one is complete."""
    return path.is_file() and path.stat().st_size > 0


def _write_parquet(table: pa.Table, path: Path):
    """Write a table under a temporary name, then rename it to ``path``."""
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        pq.write_table(table, tmp, compression="zstd", row_group_size=65_536)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _log_result(result: ChunkResult, position: str):
    """Log one line per chunk, plus the first few decoding errors."""
    if result.status == "exists":
        logger.info("%s %s: already converted", position, result.name)
    elif result.status == "written":
        rate = result.n_files / result.seconds if result.seconds else 0
        logger.info(
            "%s %s: %d alerts, %.0f s (%.0f files/s)",
            position,
            result.name,
            result.n_files,
            result.seconds,
            rate,
        )
    else:
        logger.error("%s %s: FAILED, %s", position, result.name, result.reason)
    for error in result.errors[:MAX_LOGGED_ERRORS]:
        logger.warning("    %s", error)
    if len(result.errors) > MAX_LOGGED_ERRORS:
        logger.warning("    ... and %d more", len(result.errors) - MAX_LOGGED_ERRORS)


# ---------------------------------------------------------------- archive, manifest and plan


def list_subdirs(archive_dir) -> list[Path]:
    """The archive's subdirectories, sorted by name."""
    return sorted(Path(p) for p in glob.glob(os.path.join(archive_dir, "*")) if os.path.isdir(p))


def list_alerts(subdir) -> list[str]:
    """Paths of a subdirectory's alert files, sorted. Each chunk is a slice of this list."""
    return sorted(glob.glob(os.path.join(subdir, "*.avro.gz")))


def read_manifest(out_dir) -> dict | None:
    """The converted subdirectories and chunks in ``out_dir``, or None if nothing is converted yet."""
    return _read_json(Path(out_dir) / MANIFEST_NAME)


def read_plan(out_dir) -> dict | None:
    """The planned subdirectories and chunks in ``out_dir``, or None if nothing is planned."""
    return _read_json(Path(out_dir) / PLAN_DIR_NAME / PLAN_NAME)


def _read_json(path: Path) -> dict | None:
    return json.loads(path.read_text()) if path.exists() else None


def _empty_listing(archive_dir, chunk_size: int) -> dict:
    """A manifest or plan with no subdirectories. Both have the same layout."""
    return {"archive": str(archive_dir), "chunk_size": chunk_size, "subdirs": [], "chunks": []}


def _chunks(subdir: str, n_files: int, chunk_size: int) -> list[dict]:
    """Split a subdirectory's files, sorted by name, into chunks."""
    chunks = []
    for start in range(0, n_files, chunk_size):
        end = min(start + chunk_size, n_files)
        name = chunk_name(subdir, start, end)
        chunks.append({"subdir": subdir, "start": start, "end": end, "out_name": name})
    return chunks


def _file_list(out_dir: Path, chunk: dict) -> Path:
    """File holding the names of a planned chunk's alert files, one per line."""
    return out_dir / PLAN_DIR_NAME / f"{chunk['out_name'].removesuffix('.parquet')}.txt"


def _save_listing(path: Path, listing: dict):
    """Write a manifest or plan atomically, with totals, in the original ``import_alerts.py`` layout."""
    content = {
        "archive": listing["archive"],
        "chunk_size": listing["chunk_size"],
        "n_subdirs": len(listing["subdirs"]),
        "n_files": sum(s["n_files"] for s in listing["subdirs"]),
        "n_chunks": len(listing["chunks"]),
        "subdirs": listing["subdirs"],
        "chunks": listing["chunks"],
    }
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(content))
    os.replace(tmp, path)


# ---------------------------------------------------------------- steps


def plan(
    out_dir, archive_dir=ARCHIVE_DIR, threads: int = THREADS, chunk_size: int = CHUNK_SIZE
) -> list[dict]:
    """Plan the chunks of the archive subdirectories that are new.

    A subdirectory is new if it's in neither the manifest nor the plan. The
    names of each chunk's files are saved, so a retried chunk holds the same
    files. Empty subdirectories are skipped until they have alerts.

    Parameters
    ----------
    out_dir : str or path-like
        Output directory.
    archive_dir : str or path-like
        The raw archive. Must match the existing manifest and plan.
    threads : int
        Subdirectories listed at once.
    chunk_size : int
        Alerts per chunk in a new output directory. An existing one keeps its own.

    Returns
    -------
    list[dict]
        The chunks added to the plan.

    Raises
    ------
    ValueError
        If the manifest or plan is for a different archive.
    """
    out_dir = Path(out_dir)
    manifest, planned = read_manifest(out_dir), read_plan(out_dir)
    for listing in (manifest, planned):
        if listing and Path(listing["archive"]) != Path(archive_dir):
            raise ValueError(f"{out_dir} is for archive {listing['archive']}, not {archive_dir}")
    if planned is None:
        planned = _empty_listing(archive_dir, manifest["chunk_size"] if manifest else chunk_size)

    known = {s["subdir"] for s in planned["subdirs"] + (manifest["subdirs"] if manifest else [])}
    new = [d for d in list_subdirs(archive_dir) if d.name not in known]
    logger.info("%d new subdirectories", len(new))

    def plan_subdir(subdir: Path) -> tuple[int, list[dict]]:
        names = [os.path.basename(p) for p in list_alerts(subdir)]
        chunks = _chunks(subdir.name, len(names), planned["chunk_size"])
        if chunks:
            (out_dir / PLAN_DIR_NAME).mkdir(parents=True, exist_ok=True)
        for chunk in chunks:
            lines = "".join(f"{name}\n" for name in names[chunk["start"] : chunk["end"]])
            _file_list(out_dir, chunk).write_text(lines)
        return len(names), chunks

    added = []
    with ThreadPoolExecutor(max_workers=threads) as pool:
        for subdir, (n_files, chunks) in zip(new, pool.map(plan_subdir, new), strict=True):
            if n_files:
                planned["subdirs"].append({"subdir": subdir.name, "n_files": n_files})
                added.extend(chunks)
    if added:
        planned["chunks"].extend(added)
        _save_listing(out_dir / PLAN_DIR_NAME / PLAN_NAME, planned)
    logger.info("planned %d chunks; %d planned in total", len(added), len(planned["chunks"]))
    return added


def run(out_dir, shard: tuple[int, int] = (0, 1), threads: int = THREADS) -> list[ChunkResult]:
    """Convert the planned chunks that aren't written yet.

    Parameters
    ----------
    out_dir : str or path-like
        Output directory.
    shard : tuple[int, int]
        ``(i, n)``: convert every ``n``-th chunk, starting at chunk ``i``,
        so that ``n`` jobs can share the work.
    threads : int
        Files decoded at once.

    Returns
    -------
    list[ChunkResult]
        One per chunk of this shard. Empty if nothing is planned.

    Raises
    ------
    ValueError
        If ``shard`` isn't ``(i, n)`` with ``0 <= i < n``.
    """
    out_dir = Path(out_dir)
    index, count = shard
    if not 0 <= index < count:
        raise ValueError(f"shard must be (i, n) with 0 <= i < n, got {shard}")
    planned = read_plan(out_dir)
    if planned is None:
        logger.info("nothing planned")
        return []
    chunks = planned["chunks"][index::count]
    logger.info("shard %d/%d: %d of %d planned chunks", index, count, len(chunks), len(planned["chunks"]))

    results = []
    for i, chunk in enumerate(chunks, 1):
        out_path = out_dir / chunk["out_name"]
        if _is_converted(out_path):  # checked here too, to skip reading the file list
            result = ChunkResult(out_path.name, chunk["end"] - chunk["start"], "exists")
        else:
            subdir = os.path.join(planned["archive"], chunk["subdir"])
            names = _file_list(out_dir, chunk).read_text().splitlines()
            result = convert_chunk([os.path.join(subdir, name) for name in names], out_path, threads)
        _log_result(result, f"[{i}/{len(chunks)}]")
        results.append(result)
    return results


def finish(out_dir) -> list[str]:
    """Move the planned subdirectories whose chunks are all written to the manifest.

    The others stay planned, for the next :func:`run` to retry.

    Parameters
    ----------
    out_dir : str or path-like
        Output directory.

    Returns
    -------
    list[str]
        Names of the planned chunks that aren't written yet.
    """
    out_dir = Path(out_dir)
    planned = read_plan(out_dir)
    if planned is None:
        logger.info("nothing planned")
        return []

    missing = [c for c in planned["chunks"] if not _is_converted(out_dir / c["out_name"])]
    unfinished = {c["subdir"] for c in missing}
    manifest = read_manifest(out_dir) or _empty_listing(planned["archive"], planned["chunk_size"])
    # Skip subdirectories that are recorded already, in case an earlier finish was interrupted.
    recorded = {s["subdir"] for s in manifest["subdirs"]}
    to_record = {s["subdir"] for s in planned["subdirs"]} - unfinished - recorded
    if to_record:
        manifest["subdirs"] += [s for s in planned["subdirs"] if s["subdir"] in to_record]
        manifest["chunks"] += [c for c in planned["chunks"] if c["subdir"] in to_record]
        _save_listing(out_dir / MANIFEST_NAME, manifest)

    finished_chunks = [c for c in planned["chunks"] if c["subdir"] not in unfinished]
    planned["subdirs"] = [s for s in planned["subdirs"] if s["subdir"] in unfinished]
    planned["chunks"] = [c for c in planned["chunks"] if c["subdir"] in unfinished]
    plan_dir = out_dir / PLAN_DIR_NAME
    if planned["subdirs"]:
        _save_listing(plan_dir / PLAN_NAME, planned)
    for chunk in finished_chunks:
        _file_list(out_dir, chunk).unlink(missing_ok=True)
    if not planned["subdirs"]:  # nothing left: remove the plan
        (plan_dir / PLAN_NAME).unlink()
        with contextlib.suppress(OSError):  # not empty if an interrupted plan left files behind
            plan_dir.rmdir()

    logger.info("recorded %d subdirectories in %s", len(to_record), MANIFEST_NAME)
    if missing:
        logger.error(
            "%d planned chunks aren't written yet (first: %s); %d subdirectories stay planned",
            len(missing),
            missing[0]["out_name"],
            len(planned["subdirs"]),
        )
    return [c["out_name"] for c in missing]
