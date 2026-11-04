# The daily run

How each day's new alerts get from `landing/` into the catalog.

:::{admonition} Stub
:class: note

To do:

- The steps, in order: take the lock, check the format-change flag, clean
  up orphans, find new chunks, check their formats, import into the
  existing pixels, update the metadata and sky maps, log the run, email
- How it decides a chunk is new (by chunk name)
- Running it: `scripts/daily.sbatch`, and how the schedule launches it
- What a successful run's log and email look like
:::
