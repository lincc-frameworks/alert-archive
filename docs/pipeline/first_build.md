# How the catalog was first built

You don't need this page to run the pipeline. It explains where the catalog's
alerts from before the daily run came from, which matters if you ever need to
rebuild the catalog from scratch.

The catalog was started from Neven's Parquet dump of the archive
(`/sdf/data/rubin/user/ncaplar/alert-parquet`), which runs until alerts stopped
flowing in mid-July 2026: its last alert is from July 14. It was built in two
steps on October 31, 2026: a full import of the dump's first chunks (the
baseline), then the rest of the dump fed through the daily run one night at a
time (the replay).

| Step | Runs | Chunks | Alerts |
|---|---|---|---|
| Baseline | 1 | 263 | 10,772,712 |
| Replay | 72 | 163 | 5,694,257 |
| **Total** | 73 | 426 | 16,466,969 |

The first daily run on live alerts followed on November 2.

## The baseline import

The baseline ({py:mod}`alert_archive.baseline`) imports the dump's oldest
chunks into an empty `working/` with hats-import. It takes every chunk the dump
wrote on June 26, 2026, its first write date. They're picked by name, not by
file date, so copying the dump doesn't change the baseline: all of them sort
before every later chunk, up to `170587__00100000-00138048.parquet`
({py:data}`~alert_archive.baseline.LAST_BASELINE_CHUNK`).

It skips the dump's 84 chunks with 5-digit prefixes (`17005__*` and
`17006__*`). The raw archive filed those alerts under a 5-digit directory as
well as their usual 6-digit one, so every alert in them is also in a 6-digit
chunk. That's why the dump has 20.6 million rows but only 16.5 million unique
alerts.

hats-import cut the catalog into pixels of at most 200,000 alerts. Daily runs
never split a pixel, so the catalog's pixels are still the baseline's, plus new
ones for sky the baseline didn't cover. Many have grown far past 200,000 (see
[Known issues](known_issues.md)).

The import is logged in `ingested.jsonl` as a single run, with
`"consolidated": true` because its alerts are in `Npix=<pixel>.parquet` files.
Its chunks count as ingested, so the replay skips them.

### Running it

```bash
cd ~/alert-archive
sbatch --mail-user=<you@example.org> scripts/baseline.sbatch
```

It has the same checks as [the daily run](daily_run.md#running-it): a clean
clone with an editable install. It runs on a whole `milano` node with 16 Dask
workers of 24 GiB each, since reading one dump chunk can take about 8 GB, and
has a four-hour limit. It refuses to run if `working/` exists or
`ingested.jsonl` already has runs, so it never replaces a catalog.

## The replay

The replay ({py:mod}`alert_archive.mock.replay`) fed the rest of the dump
through the daily run as if it had arrived one night at a time. For each alertId
prefix after the baseline (roughly one night), it copies that prefix's chunks
into `landing/` and starts a daily run. Its 72 runs took about three hours,
a couple of minutes each.

We could have imported the whole dump in the baseline. Replaying it instead
tested the daily run on real alerts, 72 times over, before any live alerts
reached it.

During the replay, the daily runs send no email of their own. The replay sends
one at the end, listing every run, including when it stops early. It stops at
the first format change.

The replay can be stopped and resubmitted. Prefixes whose chunks are all logged
are skipped, and the next run cleans up after a killed one. Two of its jobs were
preempted, and resubmitting picked up where they left off.

### Running it

```bash
cd ~/alert-archive
sbatch --mail-user=<you@example.org> scripts/replay.sbatch

# Or, as a trial, only the first night:
sbatch --mail-user=<you@example.org> --export=ALL,MAX_NIGHTS=1 scripts/replay.sbatch
```

It needs the baseline first. It uses the daily run's Dask settings (8 workers of
16 GiB) and has an eight-hour limit.

### The result

The frozen catalog was built from the same dump, so after the replay the two
should hold the same alerts. Comparing them:

- **Same alerts.** 16,466,969 in each, all with unique `diaSourceId`s, none in
  only one catalog, and the same count on each of the 132 observation nights.
- **Every chunk ingested once.** Each of the 426 chunks is logged exactly once,
  and the chunks' footers, the catalog's files, `ingested.jsonl` and
  `hats.properties` agree on the number of alerts. No chunk was skipped.
- **Nothing left behind by the preempted jobs.** Every file in the catalog is
  the baseline's or a logged run's.
- **Same values,** in a sample of 4,000 alerts, except that solar-system alerts
  have a null `diaObjectId` here and 0 in the frozen catalog (see
  [Data quirks](../catalog/data_quirks.md)).

The expected differences: this catalog adds a `schemaId` column and the four
fields new in alert format 1101, and has 153 pixels to the frozen catalog's 227,
the largest with 2.2 million alerts.

## Rebuilding from scratch

You shouldn't need to, and it hasn't been done since the first build.
Repartitioning will be the weekly re-import's job, not a rebuild's. If the
catalog ever has to be rebuilt from the dump:

1. Move `working/` and `state/` aside. The baseline won't run while either has
   anything in it.
2. Run the baseline, then the replay. The replay's first run also ingests
   every chunk the converter has written to `landing/`, since the new log
   doesn't list them, so when the replay finishes the catalog is up to date.

The `baseline_*` and `replay_*` directories next to `working/` are backups from
the first build: the catalog and its state as each step left them.
`replay_landing/` holds the dump chunks the replay copied.
