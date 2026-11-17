import shutil
from pathlib import Path

import hats
import lsdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from alert_archive import baseline
from alert_archive.baseline import CATALOG_NAME, BaselinePipeline
from alert_archive.collection import (
    adopt_working,
    collection_versions,
    collections_dir,
    current_catalog_dir,
)
from alert_archive.config import ImportConfig, PathConfig, get_default_config
from alert_archive.daily.increment import (
    consolidated_file_name,
    leaf_files,
    update_metadata,
)
from alert_archive.state import PipelineState, RunRecord
from alert_archive.weekly import run as weekly_run
from alert_archive.weekly.reimport import INDEX_COLUMN
from alert_archive.weekly.run import WEEKLY_DASK, WeeklyPipeline, main
from conftest import DATA_DIR
from dask.distributed import Client

BASELINE_ROWS = 12
"""Alerts in the baseline: the whole sample (see data/README.md)."""

DAILY_ROWS = 3
"""Alerts :func:`add_daily_run` adds."""

PIXEL_THRESHOLD = 4
"""Rows per partition for the re-import, small enough to split the sample alerts."""

BASELINE_RUN = "20261027T031500Z"
"""The run ID of the baseline. Orphan cleanup needs every later run and orphan to sort after it."""

DAILY_RUN = "20261102T225212Z"
"""The run ID of a daily run after the baseline."""

ADOPTED = "20261101"
"""The collection's first version: the baseline's working/, moved in by ``adopt_working``."""

FIRST_WEEKLY = "20261107"
"""The version the test re-imports make."""


def run_weekly(config, version):
    """Run the weekly re-import as if on the day ``version``, with a small in-process Dask client."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(weekly_run, "new_run_id", lambda date_only=False: version)
        with Client(n_workers=1, threads_per_worker=1, processes=False) as client:
            return WeeklyPipeline(config).run(client, config.paths.tmp_dir)


def add_daily_run(config) -> Path:
    """Add the run :data:`DAILY_RUN` as the daily pipeline leaves it: a file, a log line, fresh metadata.

    The file goes into the first pixel and holds copies of its first alerts,
    with new, higher ``diaSourceId``s so every alert stays unique. Returns the file's path.
    """
    catalog_dir = current_catalog_dir(config.paths)
    first_file = next(iter(leaf_files(catalog_dir).values()))[0]
    table = pq.ParquetFile(first_file).read().slice(0, DAILY_ROWS)
    index = table.schema.get_field_index(INDEX_COLUMN)
    table = table.set_column(index, INDEX_COLUMN, pc.add(table[INDEX_COLUMN], 10**15))
    path = first_file.parent / f"{DAILY_RUN}.parquet"
    pq.write_table(table, path)
    run = RunRecord(DAILY_RUN, started=DAILY_RUN, chunks=["chunk"], num_rows=DAILY_ROWS)
    PipelineState(config.paths.state_dir).append_run(run)
    update_metadata(hats.read_hats(catalog_dir))
    return path


def read_alerts(catalog_dir) -> pa.Table:
    """Every alert in a catalog's leaf files, sorted by ``diaSourceId``."""
    files = [path for pixel_files in leaf_files(catalog_dir).values() for path in pixel_files]
    return pa.concat_tables(pq.ParquetFile(path).read() for path in files).sort_by(INDEX_COLUMN)


def assert_same_alerts(actual: pa.Table, expected: pa.Table):
    """Check two tables have the same schema and the same values in every row.

    The rows are compared by their ``repr``, in which NaN equals NaN: the nested
    float columns have NaNs, which ``Table.equals`` treats as different.
    """
    assert actual.schema.equals(expected.schema)
    assert [repr(row) for row in actual.to_pylist()] == [repr(row) for row in expected.to_pylist()]


def logged_runs(config) -> dict[str, RunRecord]:
    """The runs in the ingested log, by run ID."""
    return {run.run_id: run for run in PipelineState(config.paths.state_dir).runs()}


@pytest.fixture(scope="module")
def adopted_root(tmp_path_factory):
    """A v2 root whose collection's only version, :data:`ADOPTED`, is the baseline of the sample alerts.

    Built once per module, since the baseline is slow; :func:`config` gives each test its own copy.
    """
    tmp_path = tmp_path_factory.mktemp("adopted")
    dump = tmp_path / "dump"
    dump.mkdir()
    shutil.copy(DATA_DIR / "alert_sample.parquet", dump / "170019__00000000-00000012.parquet")
    paths = PathConfig(root=tmp_path / "v2", dump_dir=dump, tmp_dir=tmp_path)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(baseline, "new_run_id", lambda: BASELINE_RUN)
        with Client(n_workers=1, threads_per_worker=1, processes=False) as client:
            BaselinePipeline(get_default_config(paths=paths)).run(client, tmp_path)
    adopt_working(paths, ADOPTED)
    return paths.root


@pytest.fixture
def config(tmp_path, adopted_root):
    """Config for a test's own copy of the adopted root, with a pixel threshold the sample alerts exceed."""
    shutil.copytree(adopted_root, tmp_path / "v2")
    return get_default_config(
        paths=PathConfig(root=tmp_path / "v2", tmp_dir=tmp_path),
        import_config=ImportConfig(pixel_threshold=PIXEL_THRESHOLD),
    )


def test_weekly_makes_new_version_current(config):
    """A re-import merges the daily runs into a new version, rebalanced and with a margin and an
    index, and makes it current."""
    paths = config.paths
    collection_dir = collections_dir(paths)
    previous_dir = current_catalog_dir(paths)
    add_daily_run(config)
    alerts = read_alerts(previous_dir)

    catalog_dir = run_weekly(config, FIRST_WEEKLY)

    # The new version is current: the daily runs write to it from now on.
    name = f"{CATALOG_NAME}_{FIRST_WEEKLY}"
    assert catalog_dir == collection_dir / FIRST_WEEKLY / name
    assert current_catalog_dir(paths) == catalog_dir

    # It has every alert, in more, smaller pixels, each with one file.
    assert_same_alerts(read_alerts(catalog_dir), alerts)
    files = leaf_files(catalog_dir)
    assert len(files) > 1
    for pixel, pixel_files in files.items():
        assert [p.name for p in pixel_files] == [consolidated_file_name(pixel)]
        assert pq.read_metadata(pixel_files[0]).num_rows <= PIXEL_THRESHOLD

    # It keeps the leaf directories the daily runs add files to.
    assert hats.read_hats(catalog_dir).catalog_info.npix_suffix == "/"

    # Users open it through the collection, with its margin and index.
    catalog = lsdb.open_catalog(collection_dir)
    assert catalog.margin is not None
    daily_alert = alerts[INDEX_COLUMN][-1].as_py()  # the highest ID is a daily run's
    assert len(catalog.id_search(values={INDEX_COLUMN: daily_alert}).compute()) == 1

    # Every run is now consolidated.
    assert all(run.consolidated for run in logged_runs(config).values())


def test_weekly_keeps_previous_version(config):
    """The version the re-import read is kept and still opens on its own; older ones are deleted."""
    collection_dir = collections_dir(config.paths)
    add_daily_run(config)
    (collection_dir / "20261025").mkdir()  # a version older than the current one

    run_weekly(config, FIRST_WEEKLY)

    assert collection_versions(collection_dir) == {ADOPTED, FIRST_WEEKLY}
    previous = lsdb.open_catalog(collection_dir / ADOPTED)
    assert len(previous.compute()) == BASELINE_ROWS + DAILY_ROWS


def test_failed_weekly_changes_nothing(config, monkeypatch):
    """A re-import that fails after building the version leaves the collection and the log as they were,
    and a retry the same day replaces the directory it left behind."""
    paths = config.paths
    collection_dir = collections_dir(paths)
    properties_file = collection_dir / "collection.properties"
    properties = properties_file.read_bytes()
    add_daily_run(config)

    build_version = weekly_run.build_version

    def build_then_fail(*args):
        build_version(*args)
        raise RuntimeError("failed on purpose")

    with monkeypatch.context() as mp:
        mp.setattr(weekly_run, "build_version", build_then_fail)
        with pytest.raises(RuntimeError, match="failed on purpose"):
            run_weekly(config, FIRST_WEEKLY)

    # Nothing changed, apart from the failed version's directory.
    assert properties_file.read_bytes() == properties
    assert not logged_runs(config)[DAILY_RUN].consolidated
    assert collection_versions(collection_dir) == {ADOPTED, FIRST_WEEKLY}

    # The retry replaces that directory and goes live.
    catalog_dir = run_weekly(config, FIRST_WEEKLY)

    assert current_catalog_dir(paths) == catalog_dir
    assert len(read_alerts(catalog_dir)) == BASELINE_ROWS + DAILY_ROWS
    assert collection_versions(collection_dir) == {ADOPTED, FIRST_WEEKLY}


def test_weekly_runs_once_a_day(config):
    """A second re-import on the current version's day would have to delete it, so it's refused."""
    catalog_dir = current_catalog_dir(config.paths)

    with pytest.raises(ValueError, match="at most once a day"):
        run_weekly(config, ADOPTED)

    assert current_catalog_dir(config.paths) == catalog_dir


def test_weekly_skips_while_format_change_flagged(config, monkeypatch):
    """With the format-change flag set, nothing is written and an email says why."""
    emails = []
    monkeypatch.setattr(weekly_run, "send_email", lambda subject, body: emails.append((subject, body)))
    state = PipelineState(config.paths.state_dir)
    state.flag_format_change(DAILY_RUN, {"unknown": [9999]})

    assert run_weekly(config, FIRST_WEEKLY) is None

    assert collection_versions(collections_dir(config.paths)) == {ADOPTED}
    [(subject, body)] = emails
    assert "weekly: skipped, format-change flag set since" in subject
    assert DAILY_RUN in body


def test_weekly_deletes_orphans(config):
    """Files left by a crashed daily run are deleted, not merged: their chunks get ingested again."""
    daily_file = add_daily_run(config)
    orphan = daily_file.with_name("20261103T225212Z.parquet")  # a later run, not in the log
    shutil.copy(daily_file, orphan)

    catalog_dir = run_weekly(config, FIRST_WEEKLY)

    assert not orphan.exists()
    assert len(read_alerts(catalog_dir)) == BASELINE_ROWS + DAILY_ROWS


def test_weekly_rebuilds_stale_metadata(config):
    """A re-ingest deletes a run's files but leaves them counted in the metadata, which would
    fail hats-import's row check, so the re-import rebuilds the metadata first."""
    source_dir = current_catalog_dir(config.paths)
    add_daily_run(config)
    PipelineState(config.paths.state_dir).reingest_runs(source_dir, [DAILY_RUN])
    assert hats.read_hats(source_dir).catalog_info.total_rows == BASELINE_ROWS + DAILY_ROWS  # stale

    catalog_dir = run_weekly(config, FIRST_WEEKLY)

    assert hats.read_hats(catalog_dir).catalog_info.total_rows == BASELINE_ROWS


def test_main_defaults_to_weekly_dask(monkeypatch, tmp_path):
    """``main`` uses the bigger Dask settings, on the real root or the one given."""
    seen = []
    monkeypatch.setattr(WeeklyPipeline, "execute", lambda self: seen.append(self.config))
    main()
    main(str(tmp_path))
    assert [c.dask for c in seen] == [WEEKLY_DASK, WEEKLY_DASK]
    assert [c.paths.root for c in seen] == [PathConfig().root, tmp_path]
