"""The ``alert-archive`` command which the pipeline's jobs run.

Subcommands:

- ``convert plan|run|finish``: :mod:`alert_archive.convert`.
"""

import argparse
import logging
from collections import Counter
from pathlib import Path

from alert_archive import convert

logger = logging.getLogger(__name__)


def main(argv=None) -> int:
    """Run the command.

    Parameters
    ----------
    argv : list[str] or None
        Arguments, without the program name. Defaults to ``sys.argv[1:]``.

    Returns
    -------
    int
        Exit status: 0 on success, 1 if the step failed. Errors raise, so the log has their traceback.
    """
    args = _parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    return args.handler(args)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="alert-archive", description="Run a step of the alert archive's pipeline."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    _add_convert(commands)
    return parser


def _convert(args) -> int:
    """Run one conversion step. Returns 1 if a chunk failed (run) or is missing (finish)."""
    if args.step == "plan":
        convert.plan(args.out_dir, args.archive_dir, args.threads, args.chunk_size)
        return 0
    if args.step == "finish":
        return 1 if convert.finish(args.out_dir) else 0

    results = convert.run(args.out_dir, args.shard, args.threads)
    counts = Counter(r.status for r in results)
    logger.info(
        "done: %d written, %d already converted, %d failed",
        counts["written"],
        counts["exists"],
        counts["failed"],
    )
    return 1 if counts["failed"] else 0


def _add_convert(commands):
    """Add ``convert`` and its steps: plan, run, finish."""
    out_dir = argparse.ArgumentParser(add_help=False)
    out_dir.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="directory with the chunks and manifest",
    )
    threads = argparse.ArgumentParser(add_help=False)
    threads.add_argument(
        "--threads",
        type=int,
        default=convert.THREADS,
        help=f"default: {convert.THREADS}",
    )

    parser = commands.add_parser(
        "convert",
        help="convert raw Avro alerts to parquet chunks",
        description="Convert raw Avro alerts to parquet chunks: plan, run, finish. See scripts/README.md.",
    )
    parser.set_defaults(handler=_convert)
    steps = parser.add_subparsers(dest="step", required=True)
    plan_step = steps.add_parser(
        "plan",
        parents=[out_dir, threads],
        help="list the new archive subdirectories and plan their chunks",
    )
    plan_step.add_argument(
        "--archive-dir",
        type=Path,
        default=convert.ARCHIVE_DIR,
        help=f"default: {convert.ARCHIVE_DIR}",
    )
    plan_step.add_argument(
        "--chunk-size",
        type=int,
        default=convert.CHUNK_SIZE,
        help=f"default: {convert.CHUNK_SIZE}",
    )
    run_step = steps.add_parser(
        "run",
        parents=[out_dir, threads],
        help="convert the planned chunks that aren't written yet",
    )
    run_step.add_argument(
        "--shard",
        type=_shard,
        default=(0, 1),
        metavar="I/N",
        help="convert only every N-th chunk, starting at chunk I (0 <= I < N)",
    )
    steps.add_parser(
        "finish",
        parents=[out_dir],
        help="record the fully converted subdirectories in manifest.json",
    )


def _shard(text: str) -> tuple[int, int]:
    """Parse ``--shard I/N``."""
    try:
        index, count = (int(part) for part in text.split("/"))
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"expected I/N, e.g. 0/20, got {text!r}"
        ) from None
    if not 0 <= index < count:
        raise argparse.ArgumentTypeError(f"need 0 <= I < N, got {text!r}")
    return index, count
