# Scripts

Slurm jobs for running the pipeline on S3DF. The code they run lives in the
`alert_archive` package.

- `convert_from_avro.sbatch`: converts the new raw alerts to parquet, nightly.
- `daily.sbatch`: ingests the new parquet chunks into the working catalog, nightly.
- `submit_daily.sh`: cron entry point for `daily.sbatch`; runs it from a clone of `main`.
- `weekly.sbatch`: re-imports the catalog as a new version of `collections/`, weekly.

## Schedule

Times are Pacific. Each job runs from its owner's crontab on `sdfcron001`. The
weekly time is the one planned in `weekly.sbatch`; it's started by hand for now.

| Time | Job | Owner | Reads | Writes |
|---|---|---|---|---|
| 04:00 | `convert_from_avro.sbatch` | Sandro | raw Avro alerts | `landing/` |
| 05:07 | `submit_daily.sh` → `daily.sbatch` | Olivia | `landing/` | `working/` catalog, `state/` |
| 06:17 | nightly benchmarks (`benchmarks/nightly/`) | Olivia | `working/` catalog | benchmark results |
| Sat 08:00 (not scheduled yet) | `weekly.sbatch` | Sandro | current version | new version of `collections/` |

The daily run sits between the converter and the benchmarks:

- **After the converter.** The converter usually finishes in seconds. A chunk
  it writes during a daily run isn't lost, but will simply be ingested by the next night's run.
- **Before the benchmarks.** The daily run holds `state/lock` while it writes,
  and the benchmarks skip the night if the lock exists, rather than time a
  catalog that's changing. `daily.sbatch` has a one-hour limit, so a run
  that starts on time ends before 06:17. The limit counts from when the job
  starts, though, so if it waits in the queue it can still overlap the
  benchmarks; they skip that night (or, if the job starts after they do, time
  a catalog that's changing). Fresh data matters more than one night of
  benchmarks, so there's no `--deadline`.
- **The weekly re-import** takes the same lock, so it can't overlap a daily run.

`submit_daily.sh` resets its own clone (under
`/sdf/data/rubin/user/<you>/rubin_alert_archive_daily/`) to `origin/main` and
submits `daily.sbatch` from it, so the catalog is only built from merged code,
never from anyone's checkout. It skips the night if a daily job is still
queued or running. Logs go to `logs/` in the archive root: `cron.log` has one
line per step of every run, and each job writes `alert-archive-daily-<jobid>.log`.

To set it up, or to try a branch (this writes the real catalog):

```bash
git clone https://github.com/lincc-frameworks/alert-archive.git /sdf/data/rubin/user/$USER/rubin_alert_archive_daily/alert-archive
/sdf/data/rubin/user/$USER/rubin_alert_archive_daily/alert-archive/scripts/submit_daily.sh [branch]
```

then add the crontab line from the top of `submit_daily.sh` with `crontab -e`
on `sdfcron001`.

## Converting the raw alerts

`alert-archive convert-from-avro` turns the raw Rubin alerts, one gzipped Avro
file per alert, into the parquet files the daily run ingests. Each run
converts the archive subdirectories that aren't in the landing directory's
manifest yet:

```
landing/
├── <subdir>__<start>-<end>.parquet   files start to end - 1 of a subdirectory, at most 50,000
├── manifest.json                     the subdirectories converted so far, and their chunks
└── chunk_lists/<subdir>.txt          the files of a subdirectory that isn't finished yet
```

- **Columns:** every field of the newest alert format (1101), plus
  `schemaId`, each alert's format ID. Older formats get nulls for the fields
  they lack. The cutouts, `ssSource` and `mpc_orbits` are left out.
- **A chunk is written whole or not at all.** If any of its files fails to
  decode, nothing is written, and the log lists the files that failed.
- **A subdirectory goes into the manifest once all its chunks are written**,
  and is never looked at again. Until then, each run tries it again, with the
  same files (from `chunk_lists/`), skipping the chunks already written.

## Setup

Install the package once in the LSST environment:

```bash
source /sdf/group/rubin/sw/loadLSST.sh
setup lsst_distrib
pip install -e <checkout>
```

## Running it

```bash
sbatch scripts/convert_from_avro.sbatch <dir>       # SLURM job, in a milano node
alert-archive convert-from-avro --out-dir <dir>     # or in an interactive node
```

If a chunk fails, the job exits with 1, so Slurm emails. If the job fails or
runs out of time, the next run continues where it stopped. Don't run two
conversions into the same directory at once.

**Full reconversion** (e.g. to get `schemaId` and the 1101 fields for the
alerts the original script converted): run the job into a new directory,
`sbatch scripts/convert_from_avro.sbatch <new-dir>`. Older alerts might
be slow to read (see below).

## When something fails

- **A file doesn't decode** (corrupt, truncated, or unreadable at that
  moment): if it was temporary, the next run converts the chunk. A file
  that's broken for good fails its chunk on every run until it's fixed.
- **Unknown alert format**: the error says to add the format's schema to
  `src/alert_archive/schemas/`. The next run then converts the chunk.

## Known limitations

- **Each subdirectory is listed once.** Alerts added to a subdirectory later,
  after it has finished processing, e.g. because it was still being written
  during the nightly run, are never converted (`docs/pipeline/known_issues.md`).

## Performance

- **June 2026:** most of the time goes to opening each file (about 220 ms on
  the shared filesystem). 64 threads was the fastest, at about 270 files/s.
- **November 2026:** recent alerts converted at about 2,000 files/s on one
  node, but old ones took 0.2 to 4 s each to read (roughly 50 files/s),
  probably because they're on colder storage. Keep this in mind for a full
  reconversion. Multi-node support might be needed.
