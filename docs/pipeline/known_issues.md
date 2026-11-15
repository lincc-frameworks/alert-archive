# Known issues

Problems we know about and haven't fixed yet.

## The catalog

**Pixels grow without splitting.** A daily run adds alerts to the catalog's
existing pixels and never splits one, so busy pixels keep growing (the largest
has over two million rows). Queries touching them get slower and use more
memory. The weekly re-import will repartition the catalog; until it exists,
fixing this means re-importing everything.

**No margin catalog or `diaSourceId` index yet.** Crossmatches can miss pairs
that straddle a pixel boundary, and looking up an alert by ID scans the whole
catalog. The weekly re-import will build both.

**Readers can see a run in progress.** Users read `working/` while daily runs
write to it. During a run, a query can see some of the run's files but not
others, or metadata that doesn't match the files yet. We accepted this to keep
the pipeline simple. Snapshots would fix it if it ever causes real problems.

## The converter

**Alerts archived late are missed.** The converter converts each raw prefix
directory once and never looks at it again. Alerts added to a directory after
it was converted never reach `landing/`. On the first night of live
conversion, 148 alerts arrived after their directory had been converted and
were missed until the converter was rerun by hand.

**Reconverted chunks could be ingested twice.** The daily run tracks chunks by
file name. If a directory is reconverted and its chunks come out with different
names (e.g. because it now holds more alerts), the new chunks look new, and
alerts already in the catalog would be added again. So far, every reconverted
chunk has replaced the original before any run ingested it.

## Running the pipeline

**Updating the skymaps is sometimes very slow.** It usually takes a minute or
two, but has taken close to an hour
([#22](https://github.com/lincc-frameworks/alert-archive/issues/22)).

**Preempted jobs can't finish.** Jobs on `rubin:default` can be cancelled at
any moment, with no chance to clean up. The next run recovers (it takes over
the lock and redoes the killed run's chunks), but the alerts arrive a run late.

**A crashed daily run sends no summary email.** Only Slurm's job email reports
it.
