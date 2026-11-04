# The daily run

The daily run adds the alerts in `landing/` that aren't in the catalog yet, then
emails a summary of what it did. It's {py:func}`alert_archive.daily.run.main`,
started by `scripts/daily.sbatch`.

## What a run does

Each run holds the pipeline lock (`state/lock`) while it works, so two runs can
never write to the catalog at once. The run:

1. **Stops if the format-change flag is set.** It writes nothing, and its email
   says it didn't run. Runs stay stopped until a person clears the flag (see
   [Runbook](runbook.md)).
2. **Cleans up after a crashed run.** A Parquet file in the catalog that no
   logged run wrote is left over from a run that died partway, so it's deleted.
   That run's chunks were never logged, so this run ingests them again. To be
   safe, it only deletes leftovers that look like one crashed run's files: all
   named after the same run ID, newer than every logged run. Anything else
   (e.g. a hand-copied file) stops the run with an error, and nothing is
   deleted.
3. **Finds new chunks.** These are the files in `landing/` that aren't in any
   logged run. A chunk is skipped if its name isn't
   `<6-digit prefix>__<first>-<last>.parquet` or its footer can't be read.
   Skipped chunks are listed in the summary email and retried by the next run.
4. **Checks every chunk's alert format** from its Parquet footer, before reading
   any alerts. If any chunk has a format the catalog doesn't support, the run
   writes nothing, sets the format-change flag, and its summary email says what
   changed.
5. **Imports the new alerts** with hats-import, into the catalog's existing
   pixels. Existing pixels are never split. Alerts outside the catalog's
   current coverage go into new pixels.
6. **Writes them into the catalog** as one file per pixel, named after the
   run: `Npix=<pixel>/<run_id>.parquet`. Then it rebuilds the catalog's
   skymaps and metadata from what's on disk.
7. **Logs the run** in `state/ingested.jsonl`: its run ID, chunks, alert count
   per format and skipped chunks.
8. **Emails the summary**, after releasing the lock: how many alerts it
   ingested, from which chunks, and any problems. A run with nothing new is
   still logged and emailed, so **no email means no run**.

The run ID is the run's start time in UTC, like `20261102T225212Z`.

## Running it

Run it from the root of your clone:

```bash
cd ~/alert-archive
sbatch --mail-user=<you@example.org> scripts/daily.sbatch
```

The job refuses to start unless:

- the clone has no uncommitted changes, so the log records exactly which commit
  built the catalog, and
- `alert_archive` imports from this clone (`pip install -e .`).

It runs on a whole `milano` node on the `rubin:default` account, with a
two-hour limit. Jobs on that account are preemptable: Slurm can cancel them at
any time to make room for higher-priority work. A cancelled run leaves its lock
behind, and the next run takes it over (see [Runbook](runbook.md)).

Its log is `alert-archive-daily-<jobid>.log`, in the repo root. Follow it with:

```bash
tail -F alert-archive-daily-<jobid>.log
```

:::{note}
**Coming soon:** a cron job will start the daily run automatically every day.
Until then, it's run by hand.
:::

## What to expect

A typical run takes a few minutes. Most of that time is the import. Updating
the skymaps has occasionally taken far longer, close to an hour
([#22](https://github.com/lincc-frameworks/alert-archive/issues/22)).

When it finishes you get two emails:

- **The run's summary**, from the pipeline. Its subject starts with
  `[alert-archive]`, then ✅ if everything was ingested, ⚠️ if the run took over
  a stale lock, or ❌ if it skipped chunks or found a format change.
- **Slurm's job email**, saying the job ended or failed.

If a run crashes, it sends no summary, so Slurm's email and the job log are
how you find out. The next run cleans up whatever the crashed one wrote.

At the end of the log, you may see a Dask `CommClosedError` about a heartbeat
after the run has finished. That's a harmless race while Dask shuts down.
