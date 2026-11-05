"""The ``alert-archive`` command which the pipeline's jobs run.

Subcommands:

- ``convert-from-avro``: :mod:`alert_archive.convert_from_avro`.
"""

import argparse
import logging
from collections import Counter
from pathlib import Path

from alert_archive import convert_from_avro

logger = logging.getLogger(__name__)


def main(argv=None) -> int:
    """Run the command.

    Parameters
    ----------
    argv : list[str] or None
        Arguments, without the program name.

    Returns
    -------
    int
        Exit status: 0 on success, 1 if the step failed. Errors raise, so the log has their traceback.
    """
    args = _parser().parse_args(argv)
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("alert_archive").setLevel(logging.INFO)
    return args.handler(args)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="alert-archive", description="Run a step of the alert archive's pipeline."
    )
    commands = parser.add_subparsers(dest="command", required=True)

    convert_cmd = commands.add_parser(
        "convert-from-avro",
        help="convert the new raw Avro alerts to parquet chunks",
        description="Convert the archive subdirectories that are new. See scripts/README.md.",
    )
    convert_cmd.set_defaults(handler=_convert)
    convert_cmd.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="directory for the chunks and manifest",
    )
    convert_cmd.add_argument(
        "--archive-dir",
        type=Path,
        default=convert_from_avro.ARCHIVE_DIR,
        help=f"default: {convert_from_avro.ARCHIVE_DIR}",
    )
    convert_cmd.add_argument(
        "--threads",
        type=int,
        default=convert_from_avro.THREADS,
        help=f"default: {convert_from_avro.THREADS}",
    )
    convert_cmd.add_argument(
        "--chunk-size",
        type=int,
        default=convert_from_avro.CHUNK_SIZE,
        help=f"alerts per chunk, for a new output directory; default: {convert_from_avro.CHUNK_SIZE}",
    )
    return parser


def _convert(args) -> int:
    """Convert the new archive subdirectories. Returns 1 if a chunk failed."""
    results = convert_from_avro.update(args.out_dir, args.archive_dir, args.threads, args.chunk_size)
    counts = Counter(r.status for r in results)
    logger.info(
        "done: %d written, %d already converted, %d failed",
        counts["written"],
        counts["exists"],
        counts["failed"],
    )
    return 1 if counts["failed"] else 0
