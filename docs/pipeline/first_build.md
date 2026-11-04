# How the catalog was first built

You don't need this page to run the pipeline. It explains where the catalog's
alerts from before the daily run came from, which matters if you ever need to
rebuild the catalog from scratch.

The catalog was started from Neven's Parquet dump of the archive
(`/sdf/data/rubin/user/ncaplar/alert-parquet`), which runs until alerts stopped
flowing in mid-July 2026: its last alert is from July 14. It was built in two
steps: a full import of the dump's first chunks (the baseline), then the rest of
the dump fed through the daily run one night at a time (the replay).

## The baseline import

:::{admonition} Stub
:class: note

To do:

- Which chunks the baseline takes, and why it stops where it does
- Running it: `scripts/baseline.sbatch`
- How it's logged in `ingested.jsonl` as one run
- When you'd run it again (rebuilding from scratch, or re-partitioning)
:::

## The replay

A one-time backfill that ran the rest of the dump through the daily run, one
night at a time. It doubled as a test of the daily run.

:::{admonition} Stub
:class: note

To do:

- Why we replayed instead of importing the whole dump in the baseline
- How it works: one alertId prefix per run, copied into `landing/`
- Running it: `scripts/replay.sbatch`, `MAX_NIGHTS` for a trial, resuming
  after it stops
- The result: the replayed catalog matched the frozen catalog exactly
  (link the notebook)
- Using it as a mock source for testing
:::
