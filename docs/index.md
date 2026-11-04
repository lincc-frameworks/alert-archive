# Rubin alert archive

The Rubin alert archive is a [HATS](https://hats.readthedocs.io/) catalog of
every alert Rubin Observatory has sent, one row per alert, kept up to date by
a daily pipeline. You can query it with [LSDB](https://docs.lsdb.io/):

```python
import lsdb

alerts = lsdb.open_catalog("http://slac.data.lsdb.io:10942/lsdb/rubin_alert_archive/")
```

If you want to use the alerts, start with Using the catalog. If you're running or changing the pipeline, see Maintaining the pipeline.

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
```

```{toctree}
:caption: Maintaining the pipeline
:maxdepth: 1

pipeline/overview
pipeline/daily_run
pipeline/state_and_notifications
pipeline/runbook
pipeline/known_issues
pipeline/first_build
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
