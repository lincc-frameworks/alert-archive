# Runbook

What to do when the pipeline needs a hand. The paths below are relative to
`/sdf/data/rubin/shared/lsdb_commissioning/rubin_alert_archive_v2/`, and the
commands assume the LSST environment with this package installed (see
[The daily run](daily_run.md)).

## Are there new alerts?

Follow the alerts down the pipeline.

```bash
ROOT=/sdf/data/rubin/shared/lsdb_commissioning/rubin_alert_archive_v2

# 1. The newest prefix directories in the raw archive
ls -t /sdf/data/rubin/shared/alert-archive/v2/alerts | head -3

# 2. The newest chunks the converter wrote
ls -t $ROOT/landing | head -3

# 3. The last run, and the chunks the next run would ingest
tail -1 $ROOT/state/ingested.jsonl | cut -c1-200
python -c "
from alert_archive.daily.run import find_new_chunks
from alert_archive.state import PipelineState
chunks, skipped = find_new_chunks('$ROOT/landing', PipelineState('$ROOT/state').ingested_chunks())
print(len(chunks), 'new chunks:', [c.name for c in chunks][:5], 'skipped:', skipped)
"
```

If the raw archive has prefixes that `landing/` doesn't, the converter is
behind. If `landing/` has new chunks, start a daily run.

## A job was preempted, cancelled or ran out of time

The job was killed before it could remove its lock. Usually you just start a
new run:

```bash
sbatch --mail-user=<you@example.org> scripts/daily.sbatch
```

The lock records the Slurm job that took it. The new run asks Slurm about that
job, sees it has ended, and takes the lock over. Its summary email says so
(⚠️ `stale lock taken over`), and it cleans up any files the killed run had
written and ingests its chunks again. Check the killed job's log to see why it
stopped.

A lock is only taken over when Slurm confirms its job has ended. A lock taken
outside Slurm (e.g. a run or `reingest_runs` started on a login node) has no
job to check, so the next run fails with `LockHeldError`. Make sure that
process really is gone, then delete the lock by hand:

```bash
cat $ROOT/state/lock          # which job, host and PID holds it
rm $ROOT/state/lock
```

## The format-change flag is set

Both the email that set it and `state/format_change.json` say what changed. Daily
runs stop until you clear the flag, and nothing from the new chunks has been
written, so there's nothing to undo.

1. Teach the catalog the new format: add its schema to
   `src/alert_archive/schemas/` and update {py:mod}`alert_archive.schema`.
2. If the catalog's columns change, every file in the catalog has to be
   rewritten to the new columns. That needs a full re-import, which isn't
   automated yet.
3. Merge the change, then clear the flag:

   ```bash
   rm $ROOT/state/format_change.json
   ```

The next daily run ingests the chunks that were waiting.

## Chunks keep getting skipped

The summary email lists each skipped chunk, its error, and how long it has been
failing. A chunk with an unexpected name is skipped, and listed, on every run
until someone renames or removes it. For a chunk whose footer can't be read,
the file is probably damaged or still being written: ask whoever runs the
converter to rewrite it. The next run picks it up.

## A run wrote bad data

If you've fixed a bug and want to redo some runs, take them off the log. This
deletes their files from the catalog, and the next daily run ingests their
chunks again:

```bash
python -c "
from alert_archive.state import PipelineState
PipelineState('$ROOT/state').reingest_runs('$ROOT/working', ['<run_id>', ...])
"
```

It refuses runs whose alerts are in consolidated `Npix=<pixel>.parquet` files,
such as the baseline import, since their alerts can't be deleted separately.
The catalog's metadata counts the deleted files until the next run rebuilds it.
