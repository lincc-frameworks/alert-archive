# Scripts

Slurm jobs for running the pipeline on S3DF. The code they run lives in the
`alert_archive` package.

- `convert_from_avro.sbatch`: converts the new raw alerts to parquet, nightly.

## Converting the raw alerts

`alert-archive convert-from-avro` turns the raw Rubin alerts, one gzipped Avro
file per alert, into the parquet files the daily run ingests. Each run
converts the archive subdirectories that aren't in the landing directory's
manifest yet:

```
landing/
├── <subdir>__<start>-<end>.parquet   files start to end - 1 of a subdirectory, at most 50,000
├── manifest.json                     the subdirectories converted so far, and their chunks
└── pending/<subdir>.txt              the files of a subdirectory that isn't finished yet
```

- **Columns:** every field of the newest alert format (1101), plus
  `schemaId`, each alert's format ID. Older formats get nulls for the fields
  they lack. The cutouts, `ssSource` and `mpc_orbits` are left out.
- **A chunk is written whole or not at all.** If any of its files fails to
  decode, nothing is written, and the log lists the files that failed.
- **A subdirectory goes into the manifest once all its chunks are written**,
  and is never looked at again. Until then, each run tries it again, with the
  same files (from `pending/`), skipping the chunks already written.

## Setup

Install the package once in the LSST environment:

```bash
source /sdf/group/rubin/sw/loadLSST.sh
setup lsst_distrib
pip install -e <checkout>
```

This adds the `alert-archive` command. `python -m alert_archive` is the same
command; the job uses it, so it doesn't depend on `PATH`. In
`convert_from_avro.sbatch`, check the `--output` (log file) and `--mail-user`
lines.

## Running it

```bash
sbatch scripts/convert_from_avro.sbatch             # nightly, into landing/
alert-archive convert-from-avro --out-dir <dir>     # or by hand
```

If a chunk fails, the job exits with 1, so Slurm emails. If the job fails or
runs out of time, the next run continues where it stopped. Don't run two
conversions into the same directory at once.

**Full reconversion** (e.g. to get `schemaId` and the 1101 fields for the
alerts the original script converted): run the job into a new directory,
`sbatch scripts/convert_from_avro.sbatch <new-dir>`, again and again until
its log says `0 new subdirectories`, then make that directory the job's
default. Older alerts might be slow to read (see below).

## When something fails

- **A file doesn't decode** (corrupt, truncated, or unreadable at that
  moment): if it was temporary, the next run converts the chunk. A file
  that's broken for good fails its chunk on every run until it's fixed.
- **Unknown alert format**: the error says to add the format's schema to
  `src/alert_archive/schemas/`. The next run then converts the chunk.

## Known limitations

- **Each subdirectory is listed once.** Alerts added to it later, e.g. because
  it was still being written during the nightly run, are never converted
  (`docs/pipeline/known_issues.md`).

## Performance

- **June 2026:** most of the time goes to opening each file (about 220 ms on
  the shared filesystem). 64 threads was the fastest, at about 270 files/s.
- **November 2026:** recent alerts converted at about 2,000 files/s on one
  node, but old ones took 0.2 to 4 s each to read (roughly 50 files/s),
  probably because they're on slower storage. Keep this in mind for a full
  reconversion.
