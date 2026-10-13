"""Shared fixtures for the alert archive benchmarks."""

import contextlib
import os
from pathlib import Path

import lsdb
import pytest
from dask.distributed import Client, LocalCluster
from lbench.runner import run_cprofile, run_memray

# Default to the catalog store on the HPC; override with ALERT_ARCHIVE_CATALOG_DIR.
# DEFAULT_CATALOG_DIR = "/astro/store/shire/hats/catalogs"


@pytest.hookimpl(trylast=True)
def pytest_configure(config):
    """Work around lf-bench handing pytest-benchmark a file handle instead of a path.

    lf-bench's ``--lbench`` setup assigns an open file object to
    ``config.option.benchmark_json``; pytest-benchmark >= 5.3.0 caches that into its
    session and then calls ``open()`` on it at finish, crashing before any
    results are written. Patch the live session's ``json`` attribute to the
    handle's path so the results JSON is saved. Runs ``trylast`` so
    pytest-benchmark's session already exists. Remove once lf-bench passes a path.
    """
    session = getattr(config, "_benchmarksession", None)
    handle = getattr(session, "json", None)
    if handle is not None and hasattr(handle, "write"):
        path = getattr(handle, "name", None)
        with contextlib.suppress(OSError):
            handle.close()
        if path:
            session.json = path


# @pytest.fixture(scope="session")
# def catalog_dir():
#     """Root directory holding the HATS catalogs."""
#     return Path(os.environ.get("ALERT_ARCHIVE_CATALOG_DIR", DEFAULT_CATALOG_DIR))


ALERT_ARCHIVE_PATH = "/sdf/data/rubin/shared/lsdb_commissioning/rubin_alert_archive"
GAIA_PATH = "/sdf/data/rubin/cosmicdata/lsdb/gaia_dr3"


@pytest.fixture(scope="session")
def alert_archive():
    """Return an opener for the Rubin alert archive catalog.

    The catalog has no ``default_columns``, so opening it bare loads every
    column, including the ``prvDiaSources`` and ``prvDiaForcedSources`` histories
    (~134 MB per partition in memory, vs ~5-10 MB for a few ``diaSource``
    fields). Each benchmark passes the columns it needs; nested sub-columns
    use dot notation, e.g. ``"diaSource.band"``.
    """
    # return lsdb.open_catalog(catalog_dir / "rubin_alert_archive")
    return lambda columns: lsdb.open_catalog(ALERT_ARCHIVE_PATH, columns=columns)


@pytest.fixture(scope="session")
def gaia():
    """Return an opener for the Gaia DR3 catalog used for crossmatch benchmarks.

    Gaia has 152 columns and no ``default_columns``, so always select a subset.
    """
    # return lsdb.open_catalog(catalog_dir / "gaia_dr3")
    return lambda columns: lsdb.open_catalog(GAIA_PATH, columns=columns)


@pytest.fixture(scope="session")
def single_thread_dask_client():
    """Override lf-bench's Dask client with the cluster the benchmarks were tuned for.

    lf-bench's ``lbench_dask`` fixture depends on a fixture named
    ``single_thread_dask_client``; defining it here (same name) shadows the
    default so the benchmarks run on the 2-worker ``LocalCluster`` the original
    suite used. Worker count is configurable with ALERT_ARCHIVE_DASK_WORKERS.
    """
    n_workers = int(os.environ.get("ALERT_ARCHIVE_DASK_WORKERS", "2"))
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=1, dashboard_address=":0")
    client = Client(cluster)
    yield client
    client.close()
    cluster.close()


@pytest.fixture
def lbench(benchmark_results_dir, benchmark, request):
    """Override lf-bench's ``lbench`` fixture to fix the run count and allow skipping cProfile.

    Differs from lf-bench's original ``lbench`` fixture in two ways:

    - Timing uses ``benchmark.pedantic`` with one untimed warm-up run and 5
      timed rounds, so results are deliberately warm-cache. Plain
      ``benchmark(func)`` instead adds an untimed calibration run and an extra
      untimed call to produce a return value that is never used.
    - The extra cProfile run is skipped when ALERT_ARCHIVE_CPROFILE=0. Under
      Dask that profile only covers the driver process, which mostly waits on
      workers.

    ``lbench_dask`` depends on this fixture by name, so it picks up the override.
    """
    with_cprofile = os.environ.get("ALERT_ARCHIVE_CPROFILE", "1") != "0"

    def lbench_benchmark_func(func, *args, **kwargs):
        benchmark.pedantic(func, args=args, kwargs=kwargs, rounds=5, iterations=1, warmup_rounds=1)

        if request.node.get_closest_marker("lbench_memory") is not None:
            peak_memory = run_memray(func, benchmark_results_dir, *args, **kwargs)
            benchmark.extra_info["peak_memory_bytes"] = peak_memory

        if with_cprofile:
            cprof_path = run_cprofile(func, benchmark_results_dir, *args, **kwargs)
            benchmark.extra_info["cprofile_path"] = cprof_path

    return lbench_benchmark_func
