"""Shared fixtures for the alert archive benchmarks."""

import contextlib
import os
from pathlib import Path

import lsdb
import pytest
from dask.distributed import Client, LocalCluster

# Default to the catalog store on the HPC; override with ALERT_ARCHIVE_CATALOG_DIR.
DEFAULT_CATALOG_DIR = "/astro/store/shire/hats/catalogs"


@pytest.hookimpl(trylast=True)
def pytest_configure(config):
    """Work around lf-bench handing pytest-benchmark a file handle instead of a path.

    lf-bench's ``--lbench`` setup assigns an open file object to
    ``config.option.benchmark_json``; pytest-benchmark >= 5 caches that into its
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


@pytest.fixture(scope="session")
def catalog_dir():
    """Root directory holding the HATS catalogs."""
    return Path(os.environ.get("ALERT_ARCHIVE_CATALOG_DIR", DEFAULT_CATALOG_DIR))


@pytest.fixture(scope="session")
def alert_archive(catalog_dir):
    """Open the Rubin alert archive catalog."""
    return lsdb.open_catalog(catalog_dir / "rubin_alert_archive")


@pytest.fixture(scope="session")
def gaia(catalog_dir):
    """Open the Gaia DR3 catalog used for crossmatch benchmarks."""
    return lsdb.open_catalog(catalog_dir / "gaia_dr3")


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
