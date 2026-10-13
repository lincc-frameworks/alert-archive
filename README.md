
# alert-archive

Tools and benchmarks for the Rubin alert archive, built on LSDB.

[![PyPI](https://img.shields.io/pypi/v/alert-archive?color=blue&logo=pypi&logoColor=white)](https://pypi.org/project/alert-archive/)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/lincc-frameworks/alert-archive/smoke-test.yml)](https://github.com/lincc-frameworks/alert-archive/actions/workflows/smoke-test.yml)
[![Codecov](https://codecov.io/gh/lincc-frameworks/alert-archive/branch/main/graph/badge.svg)](https://codecov.io/gh/lincc-frameworks/alert-archive)
[![Read The Docs](https://img.shields.io/readthedocs/alert-archive)](https://alert-archive.readthedocs.io/)

## To test a single benchmark
```
cd ~/alert-archive
ALERT_ARCHIVE_CPROFILE=0 pytest --lbench benchmarks/test_alert_archive.py -k per_band
```

- `ALERT_ARCHIVE_CPROFILE=0` skips the lsdb-benchmarking default cProfile run (this is already specified for full runs via conftest override)
- `--lbench` turns on lf-bench: it creates the run folder, enables pytest-benchmark and sets up the JSON output.
- `-k per_band` runs only `test_per_band_counts`.

Early in the output you'll see `[lbench] running benchmarks in .../lbench_results/<timestamp>`. That folder is where your results go. The test makes 7 full computes, so it takes a few minutes.
