"""
Benchmarks for common operations with the Rubin Alert Archive.

Display the runtimes on the console with::

    pytest benchmarks/test_alert_archive.py --durations=0
"""
from pathlib import Path

import lsdb
import numpy as np
import pytest
from astropy.time import Time, TimeDelta
from dask.distributed import Client
from distributed import LocalCluster
from hats.pixel_math.spatial_index import SPATIAL_INDEX_COLUMN
from nested_pandas import NestedFrame

SAMPLES_PATH = Path(__file__).parent / "diaObjectId_samples.npy"


@pytest.fixture(scope="session")
def dask_client():
    """Create a single client for use by all benchmarks."""
    cluster = LocalCluster(n_workers=2, threads_per_worker=1, dashboard_address=":0")
    client = Client(cluster)
    yield client
    client.close()
    cluster.close()


@pytest.fixture(scope="session")
def alert_archive():
    return lsdb.open_catalog("/astro/store/shire/hats/catalogs/rubin_alert_archive")


def mjd_tai_interval(day="2026-02-24"):
    """Return the half-open TAI MJD interval [start, end) for a UTC calendar day."""
    t0 = Time(f"{day}T00:00:00", scale="utc")
    t1 = t0 + TimeDelta(1, format="jd")
    return t0.tai.mjd, t1.tai.mjd


def test_get_sources_for_night(alert_archive, dask_client):
    """Get all sources for a particular night"""
    mjd_start, mjd_end = mjd_tai_interval()
    night_data = alert_archive.query(
        f"diaSource.midpointMjdTai >= {mjd_start} and diaSource.midpointMjdTai < {mjd_end}"
    )
    night_data = night_data.map_partitions(lambda df: df.dropna(subset="diaSource"))
    night_data.compute()


def test_cone_search_for_night(alert_archive, dask_client):
    """One degree cone search over a particular night"""
    mjd_start, mjd_end = mjd_tai_interval()
    cone = alert_archive.cone_search(ra=63.2, dec=-47.8, radius_arcsec=3600)
    night_cone = cone.query(
        f"diaSource.midpointMjdTai >= {mjd_start} and diaSource.midpointMjdTai < {mjd_end}"
    )
    night_cone = night_cone.map_partitions(lambda df: df.dropna(subset="diaSource"))
    night_cone.compute()


def test_new_objects_for_night(alert_archive, dask_client):
    """New objects for particular night"""
    mjd_start, mjd_end = mjd_tai_interval()
    night_data = alert_archive.query(
        f"diaSource.midpointMjdTai >= {mjd_start} and diaSource.midpointMjdTai < {mjd_end}"
    )
    night_data = night_data.query("diaObject.nDiaSources == 1")
    night_data = night_data.map_partitions(lambda df: df.dropna(subset=["diaSource", "diaObject"]))
    night_data.compute()


@pytest.mark.parametrize("num_samples", [1, 100, 10_000])
def test_naive_lc_aggregation(num_samples, alert_archive, dask_client):
    """Light curves for N objects by `diaObjectId`.

    It assumes (naively) that the sources for an object are all contained
    within the same HEALPix partition.
    """
    sample_ids = np.load(SAMPLES_PATH)[:num_samples]

    def aggregate_lightcurves(df):
        # Push source columns of interest to the base
        df["diaObjectId"] = df["diaSource.diaObjectId"]
        df["midpointMjdTai"] = df["diaSource.midpointMjdTai"]
        df["psfFlux"] = df["diaSource.psfFlux"]
        df["psfFluxErr"] = df["diaSource.psfFluxErr"]
        df["band"] = df["diaSource.band"]
        # Query for desired object IDs
        df = df.query(f"diaObjectId in {sample_ids.tolist()}")
        # Save sources healpix index
        df = df.reset_index(drop=False)
        # Create object light curves
        return (
            NestedFrame.from_flat(
                df,
                base_columns=["ra", "dec", SPATIAL_INDEX_COLUMN],
                nested_columns=["diaSourceId", "midpointMjdTai", "band", "psfFlux", "psfFluxErr"],
                on="diaObjectId",
                name="diaSource",
            )
            .reset_index(drop=False, names=["diaObjectId"])
            .set_index(SPATIAL_INDEX_COLUMN)
        )

    alert_archive.map_partitions(aggregate_lightcurves).compute()


def test_per_night_counts(alert_archive, dask_client):
    """Per-night aggregate counts"""

    def get_counts(df):
        return np.floor(df["diaSource.midpointMjdTai"]).astype(int).value_counts()

    per_partition = alert_archive.map_partitions(get_counts).compute()
    per_partition.groupby(level=0).sum()


def test_per_band_counts(alert_archive, dask_client):
    """Per-band aggregate counts"""

    def get_counts(df):
        return df["diaSource.band"].value_counts()

    per_partition = alert_archive.map_partitions(get_counts).compute()
    per_partition.groupby(level=0).sum()


def test_crossmatch(alert_archive, dask_client):
    """Crossmatch with another catalog"""
    gaia = lsdb.open_catalog("/astro/store/shire/hats/catalogs/gaia_dr3")
    # This cone has >2M rows
    cone = alert_archive.cone_search(ra=63.2, dec=-47.8, radius_arcsec=7200)
    cone.crossmatch(gaia).compute()
