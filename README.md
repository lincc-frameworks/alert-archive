[![PyPI](https://img.shields.io/pypi/v/alert-archive?color=blue&logo=pypi&logoColor=white)](https://pypi.org/project/alert-archive/)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/lincc-frameworks/alert-archive/smoke-test.yml)](https://github.com/lincc-frameworks/alert-archive/actions/workflows/smoke-test.yml)
[![Codecov](https://codecov.io/gh/lincc-frameworks/alert-archive/branch/main/graph/badge.svg)](https://codecov.io/gh/lincc-frameworks/alert-archive)
[![Read The Docs](https://img.shields.io/readthedocs/alert-archive)](https://alert-archive.readthedocs.io/)

# alert-archive

Tools and benchmarks for the Rubin alert archive, built on LSDB.

## Benchmarks

`benchmarks/` holds timing benchmarks for common queries on the alert archive
(all sources from one night, cone searches, light-curve aggregation, a Gaia
crossmatch, and more), run with
[lf-bench](https://github.com/lincc-frameworks/lsdb-benchmarking). They run
nightly on S3DF via a cron job that benchmarks `main`, and you can also run
them by hand or against a branch.

See [benchmarks/README.md](benchmarks/README.md) for how to run them, where
results are stored, and how to read them.
