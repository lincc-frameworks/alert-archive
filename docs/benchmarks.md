# Benchmarks

Timing benchmarks for common queries on the alert archive run every night on
S3DF. The results are at
**[alert-dashboard.lsdb.io](https://alert-dashboard.lsdb.io)**.

:::{admonition} Stub
:class: note

To do:

- What each benchmark measures: one night's sources, a cone search for a
  night, new objects in a night, light-curve aggregation, per-night and
  per-band counts, a Gaia crossmatch
- Reading the dashboard: what's plotted, and what counts as a regression
- How the nightly runs work: cron → `benchmarks/nightly/submit.sh` → Slurm,
  then results copied to the dashboard
- Running them by hand: `pytest --lbench benchmarks/`, and against a branch
- Adding a benchmark
- Link to `benchmarks/README.md` for the details
:::
