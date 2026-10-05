"""
Benchmarks for common operations with the Rubin Alert Archive.

Run under lf-bench to record timings, cProfile output, and Dask performance
reports::

    pytest --lbench benchmarks/test_alert_archive.py

The catalog location defaults to the HPC path; override it with the
ALERT_ARCHIVE_CATALOG_DIR environment variable (see ``conftest.py``).
"""

from pathlib import Path

import numpy as np
import pytest
from astropy.time import Time, TimeDelta
from hats.pixel_math.spatial_index import SPATIAL_INDEX_COLUMN
from nested_pandas import NestedFrame

SAMPLES_PATH = Path(__file__).parent / "diaObjectId_samples.npy"


def mjd_tai_interval(day="2026-02-24"):
    """Return the half-open TAI MJD interval [start, end) for a UTC calendar day."""
    t0 = Time(f"{day}T00:00:00", scale="utc")
    t1 = t0 + TimeDelta(1, format="jd")
    return t0.tai.mjd, t1.tai.mjd


def test_get_sources_for_night(alert_archive, lbench_dask):
    """Get all sources for a particular night."""
    mjd_start, mjd_end = mjd_tai_interval()
    night_data = alert_archive.query(
        f"diaSource.midpointMjdTai >= {mjd_start} and diaSource.midpointMjdTai < {mjd_end}"
    ).map_partitions(lambda df: df.dropna(subset="diaSource"))
    lbench_dask(night_data.compute)


def test_cone_search_for_night(alert_archive, lbench_dask):
    """One degree cone search over a particular night."""
    mjd_start, mjd_end = mjd_tai_interval()
    night_cone = (
        alert_archive.cone_search(ra=63.2, dec=-47.8, radius_arcsec=3600)
        .query(f"diaSource.midpointMjdTai >= {mjd_start} and diaSource.midpointMjdTai < {mjd_end}")
        .map_partitions(lambda df: df.dropna(subset="diaSource"))
    )
    lbench_dask(night_cone.compute)


def test_new_objects_for_night(alert_archive, lbench_dask):
    """New objects for a particular night."""
    mjd_start, mjd_end = mjd_tai_interval()
    night_data = (
        alert_archive.query(
            f"diaSource.midpointMjdTai >= {mjd_start} and diaSource.midpointMjdTai < {mjd_end}"
        )
        .query("diaObject.nDiaSources == 1")
        .map_partitions(lambda df: df.dropna(subset=["diaSource", "diaObject"]))
    )
    lbench_dask(night_data.compute)


@pytest.mark.parametrize("num_samples", [1, 100, 10_000])
def test_naive_lc_aggregation(num_samples, alert_archive, lbench_dask):
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

    light_curves = alert_archive.map_partitions(aggregate_lightcurves)
    lbench_dask(light_curves.compute)


def test_per_night_counts(alert_archive, lbench_dask):
    """Per-night aggregate counts."""

    def get_counts(df):
        return np.floor(df["diaSource.midpointMjdTai"]).astype(int).value_counts()

    per_partition = alert_archive.map_partitions(get_counts)

    def counts_per_night():
        per_partition.compute().groupby(level=0).sum()

    lbench_dask(counts_per_night)


def test_per_band_counts(alert_archive, lbench_dask):
    """Per-band aggregate counts."""

    def get_counts(df):
        return df["diaSource.band"].value_counts()

    per_partition = alert_archive.map_partitions(get_counts)

    def counts_per_band():
        per_partition.compute().groupby(level=0).sum()

    lbench_dask(counts_per_band)


def test_crossmatch(alert_archive, gaia, lbench_dask):
    """Crossmatch with another catalog."""
    # This cone has >2M rows
    cone = alert_archive.cone_search(ra=63.2, dec=-47.8, radius_arcsec=7200)
    crossmatched = cone.crossmatch(gaia)
    lbench_dask(crossmatched.compute)
