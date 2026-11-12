"""Convert the raw Avro alerts to parquet chunks for the daily run.

:func:`update` converts the raw archive's subdirectories that aren't in the
output directory's manifest yet. Their alerts are written to parquet files of
up to :data:`CHUNK_SIZE` alerts ("chunks"), with the columns of
:func:`~alert_archive.schema.target_schema`.

Run it as ``alert-archive convert-from-avro``. See ``scripts/README.md``.
"""

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
from alert_archive.state import write_atomic

logger = logging.getLogger(__name__)

ARCHIVE_DIR = Path("/sdf/data/rubin/shared/alert-archive/v2/alerts")
"""The raw alert archive on S3DF."""

MANIFEST_NAME = "manifest.json"
"""File in the output directory listing the converted subdirectories and their chunks."""

CHUNK_LISTS_DIR_NAME = "chunk_lists"
"""Directory in the output directory with the file list of each subdirectory not in the
manifest yet. Its chunks are slices of that list."""

CHUNK_SIZE = 50_000
"""Alerts per chunk. A subdirectory's last chunk holds the rest."""

THREADS = 64
"""Files decoded at once. Most of the time goes to opening each file on the shared
filesystem, so many are opened at once."""

MAX_LOGGED_ERRORS = 10
"""Decoding errors logged per chunk, followed by how many more there are."""


def update(
    out_dir,
    archive_dir=ARCHIVE_DIR,
    threads: int = THREADS,
    chunk_size: int = CHUNK_SIZE,
) -> list["ChunkResult"]:
    """Convert the archive subdirectories that aren't in the manifest yet.

    A subdirectory is added to the manifest once all its chunks are written,
    and isn't looked at again. One with a failed chunk isn't added, so the next
    update tries it again, with the same files (see :func:`_alert_files`),
    skipping the chunks already written. Empty subdirectories are skipped
    until they have alerts.

    Parameters
    ----------
    out_dir : str or path-like
        Directory for the chunks and the manifest.
    archive_dir : str or path-like
        The raw archive. Must match the manifest's.
    threads : int
        Files decoded at once.
    chunk_size : int
        Alerts per chunk in a new output directory. An existing one keeps its own.

    Returns
    -------
    list[ChunkResult]
        What happened to each chunk.

    Raises
    ------
    ValueError
        If the manifest is for a different archive.
    FileNotFoundError
        If the archive isn't there.
    """
    out_dir = Path(out_dir)
    manifest = read_manifest(out_dir, archive_dir, chunk_size)
    if Path(manifest["archive"]) != Path(archive_dir):
        raise ValueError(f"Manifest in {out_dir} is for archive {manifest['archive']}, not {archive_dir}")
    if not Path(archive_dir).is_dir():
        raise FileNotFoundError(f"Archive in {archive_dir} not found")

    converted = {s["subdir"] for s in manifest["subdirs"]}
    new = [d for d in list_subdirs(archive_dir) if d.name not in converted]
    logger.info("%d new subdirectories", len(new))
    out_dir.mkdir(parents=True, exist_ok=True)
    if not (out_dir / MANIFEST_NAME).exists():
        # Saved before anything is converted, so retries keep the chunk size even if nothing finishes now.
        _save_manifest(out_dir, manifest)

    results = []
    for i, subdir in enumerate(new, 1):
        paths = _alert_files(out_dir, subdir)
        chunks = _chunks(subdir.name, len(paths), manifest["chunk_size"])
        logger.info("[%d/%d] %s: %d files", i, len(new), subdir.name, len(paths))
        failed = False
        for chunk in chunks:
            result = convert_chunk(
                paths[chunk["start"] : chunk["end"]],
                out_dir / chunk["out_name"],
                threads,
            )
            _log_result(result)
            results.append(result)
            failed |= result.status == "failed"
        if chunks and not failed:
            manifest["subdirs"].append({"subdir": subdir.name, "n_files": len(paths)})
            manifest["chunks"] += chunks
            _save_manifest(out_dir, manifest)
            _chunk_list(out_dir, subdir).unlink()
    return results


def list_subdirs(archive_dir) -> list[Path]:
    """The archive's subdirectories, sorted by name."""
    return sorted(Path(p) for p in glob.glob(os.path.join(archive_dir, "*")) if os.path.isdir(p))


def list_alerts(subdir) -> list[str]:
    """Paths of a subdirectory's alert files, sorted."""
    return sorted(glob.glob(os.path.join(subdir, "*.avro.gz")))


def _alert_files(out_dir: Path, subdir: Path) -> list[str]:
    """A subdirectory's alert files, sorted, as they were listed on its first try.

    Each chunk is a slice of this list. The list is saved until the
    subdirectory is in the manifest, so a retry splits the same files into
    the same chunks even if the subdirectory changed, and never writes an
    alert twice. Empty subdirectories aren't saved, so they're listed again.
    """
    saved = _chunk_list(out_dir, subdir)
    if saved.exists():
        names = saved.read_text().splitlines()
    else:
        names = [os.path.basename(p) for p in list_alerts(subdir)]
        if names:
            saved.parent.mkdir(exist_ok=True)
            write_atomic(saved, "".join(f"{name}\n" for name in names))
    return [os.path.join(subdir, name) for name in names]


def _chunk_list(out_dir: Path, subdir: Path) -> Path:
    """File with the names of a subdirectory's alert files, while it's not in the manifest."""
    return out_dir / CHUNK_LISTS_DIR_NAME / f"{subdir.name}.txt"


def _chunks(subdir: str, n_files: int, chunk_size: int) -> list[dict]:
    """Split a subdirectory's files into chunks, as the manifest lists them."""
    chunks = []
    for start in range(0, n_files, chunk_size):
        end = min(start + chunk_size, n_files)
        name = f"{subdir}__{start:08d}-{end:08d}.parquet"
        chunks.append({"subdir": subdir, "start": start, "end": end, "out_name": name})
    return chunks


def read_manifest(out_dir, archive_dir=ARCHIVE_DIR, chunk_size: int = CHUNK_SIZE) -> dict:
    """The converted subdirectories and chunks in ``out_dir``.

    If nothing is converted there yet, an empty manifest for ``archive_dir``,
    with ``chunk_size`` alerts per chunk.
    """
    path = Path(out_dir) / MANIFEST_NAME
    if path.exists():
        return json.loads(path.read_text())
    return {
        "archive": str(archive_dir),
        "chunk_size": chunk_size,
        "subdirs": [],
        "chunks": [],
    }


def _save_manifest(out_dir: Path, manifest: dict):
    """Write the manifest atomically, with totals, in the original ``import_alerts.py`` layout."""
    content = {
        "archive": manifest["archive"],
        "chunk_size": manifest["chunk_size"],
        "n_subdirs": len(manifest["subdirs"]),
        "n_files": sum(s["n_files"] for s in manifest["subdirs"]),
        "n_chunks": len(manifest["chunks"]),
        "subdirs": manifest["subdirs"],
        "chunks": manifest["chunks"],
    }
    write_atomic(out_dir / MANIFEST_NAME, json.dumps(content))


@dataclass
class ChunkResult:
    """What happened to one chunk."""

    name: str
    """File name, e.g. ``"170019__00000000-00050000.parquet"``."""
    n_files: int
    """Alert files in the chunk."""
    status: str
    """``"written"``, ``"exists"`` (written earlier) or ``"failed"`` (not written)."""
    errors: list[str] = field(default_factory=list)
    """Files that failed to decode, with their errors."""
    seconds: float = 0.0
    """Time taken to convert the chunk."""


def convert_chunk(paths, out_path, threads: int = THREADS) -> ChunkResult:
    """Convert alert files to one parquet file, unless it exists already.

    The chunk is written whole or not at all: if any file fails to decode,
    nothing is written.

    Parameters
    ----------
    paths : list[str] or list[path-like]
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
    if out_path.exists():  # written atomically, so it's complete
        return ChunkResult(out_path.name, len(paths), "exists")

    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=threads) as pool:
        decoded = list(pool.map(_try_decode, paths))
    errors = [d for d in decoded if isinstance(d, str)]
    if errors:
        return ChunkResult(out_path.name, len(paths), "failed", errors)

    _write_parquet_atomic(pa.Table.from_pylist(decoded, schema=target_schema()), out_path)
    return ChunkResult(out_path.name, len(paths), "written", seconds=time.monotonic() - start)


def _write_parquet_atomic(table: pa.Table, path: Path):
    """Write a parquet file under a temporary name, sync it to disk, then rename it.

    So a file under its final name is always complete, even after a crash.
    """
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with open(tmp, "wb") as f:
            pq.write_table(table, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _try_decode(path) -> dict | str:
    """Decode one alert file. Returns the alert, or an error message if it fails."""
    try:
        return decode_alert(path)
    except Exception as exc:
        return f"{path}: {type(exc).__name__}: {exc}"


def _log_result(result: ChunkResult):
    """Log one line for a chunk, plus its first few errors."""
    if result.status == "exists":
        logger.info("    %s: already converted", result.name)
    elif result.status == "written":
        rate = result.n_files / result.seconds if result.seconds else 0
        logger.info(
            "    %s: %d alerts, %.0f s (%.0f files/s)",
            result.name,
            result.n_files,
            result.seconds,
            rate,
        )
    else:
        logger.error(
            "    %s: FAILED, %d of %d files failed to decode",
            result.name,
            len(result.errors),
            result.n_files,
        )
        for error in result.errors[:MAX_LOGGED_ERRORS]:
            logger.warning("        %s", error)
        if len(result.errors) > MAX_LOGGED_ERRORS:
            logger.warning("        ... and %d more", len(result.errors) - MAX_LOGGED_ERRORS)


class UnknownFormatError(Exception):
    """An alert's format ID has no schema in ``alert_archive/schemas``."""

    def __init__(self, schema_id: int):
        super().__init__(f"Add schema of ID {schema_id} to alert_archive/schemas.")
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
