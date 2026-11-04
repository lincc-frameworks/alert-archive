# Rubin alert archive

The Rubin alert archive is a [HATS](https://hats.readthedocs.io/) catalog of
every alert Rubin Observatory has sent, one row per alert, kept up to date by
a daily pipeline. You can query it with [LSDB](https://docs.lsdb.io/):

```python
import lsdb

alerts = lsdb.open_catalog("http://slac.data.lsdb.io:10942/lsdb/rubin_alert_archive/")
```

This site has two halves:

- **Using the catalog** is for scientists: how to open it, what's in it, and
  what to watch out for.
- **Maintaining the pipeline** is for whoever runs the archive: how the catalog
  is built and updated, and what to do when something goes wrong.

```{toctree}
:hidden:

Home <self>
```

```{toctree}
:caption: Using the catalog
:maxdepth: 1

catalog/access
catalog/schema
catalog/data_quirks
catalog/examples
```

```{toctree}
:caption: Maintaining the pipeline
:maxdepth: 1

pipeline/overview
pipeline/baseline
pipeline/daily_run
pipeline/replay
pipeline/state_and_notifications
pipeline/runbook
pipeline/known_issues
```

```{toctree}
:caption: Benchmarks
:maxdepth: 1

benchmarks
```

```{toctree}
:caption: Reference
:maxdepth: 1

design/incremental_data_flow
developer_guide
API reference <autoapi/index>
```
