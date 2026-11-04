# The replay

A one-time backfill that ran the rest of the parquet dump through the daily
run, one night at a time. It doubled as a test of the daily run.

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
